use std::time::Instant;

use futures_util::future::join_all;
use simd_erasure_core::ReedSolomonDecoder;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::checksum::{ChecksumAlgorithm, Digest, StreamHasher};
use crate::metrics;
use crate::{EcLayout, EcsError, EcsErrorKind, Result};

use super::ShardReaders;

#[derive(Clone, Debug, Default)]
pub struct DecodeOptions {
    pub source_checksum: Option<ChecksumAlgorithm>,
    pub expected_source_checksum: Option<Digest>,
}

#[derive(Clone, Debug)]
pub struct DecodeReport {
    pub layout: EcLayout,
    pub source_bytes: u64,
    pub shard_bytes: u64,
    pub stripe_count: u64,
    pub reconstructed_data_pieces: u64,
    pub failed_readers: Vec<usize>,
    pub source_checksum: Option<Digest>,
}

pub struct DecodeOutput<R, W> {
    pub report: DecodeReport,
    pub readers: ShardReaders<R>,
    pub destination: W,
}

/// Streaming reconstruction into one logical destination.
pub struct ErasureStreamDecoder<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    layout: EcLayout,
    source_bytes: u64,
    readers: Vec<Option<R>>,
    destination: W,
    options: DecodeOptions,
    decoder: Option<ReedSolomonDecoder>,
    failed_readers: Vec<usize>,
    reconstructed_data_pieces: u64,
    source_hasher: Option<StreamHasher>,
}

impl<R, W> ErasureStreamDecoder<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    pub fn new(
        layout: EcLayout,
        readers: ShardReaders<R>,
        destination: W,
        source_bytes: u64,
        options: DecodeOptions,
    ) -> Result<Self> {
        readers.validate(layout)?;
        layout.encoded_source_capacity(source_bytes)?;
        if let Some(expected) = &options.expected_source_checksum
            && options.source_checksum != Some(expected.algorithm())
        {
            return Err(EcsError::new(
                EcsErrorKind::InvalidLayout,
                "expected source checksum does not match decode checksum algorithm",
            ));
        }
        let decoder = if layout.parity_shards() == 0 {
            None
        } else {
            Some(
                ReedSolomonDecoder::new(
                    layout.data_shards(),
                    layout.parity_shards(),
                    layout.block_size(),
                )
                .map_err(|error| {
                    EcsError::new(
                        EcsErrorKind::CodecFailure,
                        format!("initialize Reed-Solomon decoder: {error:?}"),
                    )
                })?,
            )
        };
        let source_hasher = options.source_checksum.map(StreamHasher::new);
        Ok(Self {
            layout,
            source_bytes,
            readers: readers.into_flat(),
            destination,
            options,
            decoder,
            failed_readers: Vec::new(),
            reconstructed_data_pieces: 0,
            source_hasher,
        })
    }

    pub async fn decode(mut self) -> Result<DecodeOutput<R, W>> {
        let stripe_count = self.layout.stripe_count(self.source_bytes);
        let mut remaining = self.source_bytes;
        for _ in 0..stripe_count {
            let blocks = self.read_stripe().await?;
            self.write_decoded_stripe(&blocks, &mut remaining).await?;
        }
        if remaining != 0 {
            metrics::record_error();
            return Err(EcsError::new(
                EcsErrorKind::IncompleteInput,
                format!("decoder left {remaining} source bytes unresolved"),
            ));
        }
        self.destination.flush().await.map_err(|error| {
            metrics::record_error();
            EcsError::from_io(
                EcsErrorKind::WriterFailure,
                "flush decoded destination",
                None,
                error,
            )
        })?;
        let source_checksum = self.source_hasher.take().map(StreamHasher::finalize);
        if let (Some(expected), Some(actual)) = (
            self.options.expected_source_checksum.as_ref(),
            source_checksum.as_ref(),
        ) && expected != actual
        {
            metrics::record_error();
            crate::report_error!("decoded source checksum mismatch", "stream", "decode");
            return Err(EcsError::new(
                EcsErrorKind::ChecksumMismatch,
                format!(
                    "decoded source checksum mismatch: expected {}, got {}",
                    expected.to_hex(),
                    actual.to_hex()
                ),
            ));
        }
        let report = DecodeReport {
            layout: self.layout,
            source_bytes: self.source_bytes,
            shard_bytes: self.layout.shard_bytes(self.source_bytes)?,
            stripe_count,
            reconstructed_data_pieces: self.reconstructed_data_pieces,
            failed_readers: self.failed_readers,
            source_checksum,
        };
        let readers = ShardReaders::from_flat(self.readers, self.layout.data_shards());
        Ok(DecodeOutput {
            report,
            readers,
            destination: self.destination,
        })
    }

    async fn read_stripe(&mut self) -> Result<Vec<Option<Vec<u8>>>> {
        let block_size = self.layout.block_size();
        let started = Instant::now();
        let reads = self
            .readers
            .iter_mut()
            .enumerate()
            .filter_map(|(index, reader)| {
                let reader = reader.as_mut()?;
                Some(async move {
                    let mut block = vec![0u8; block_size];
                    let result = reader.read_exact(&mut block).await;
                    (index, result, block)
                })
            });
        let results = join_all(reads).await;
        metrics::record_read(started.elapsed().as_nanos() as u64);
        let mut blocks = (0..self.layout.total_shards())
            .map(|_| None)
            .collect::<Vec<_>>();
        for (index, result, block) in results {
            match result {
                Ok(_) => blocks[index] = Some(block),
                Err(error) => {
                    metrics::record_error();
                    metrics::record_degraded_piece();
                    crate::report_warning!(&error, "shard_read", index);
                    self.readers[index] = None;
                    if !self.failed_readers.contains(&index) {
                        self.failed_readers.push(index);
                    }
                }
            }
        }
        let available = blocks.iter().filter(|block| block.is_some()).count();
        if available < self.layout.data_shards() {
            return Err(EcsError::new(
                EcsErrorKind::InsufficientShards,
                format!(
                    "stripe has {available} readable pieces; K={} is required",
                    self.layout.data_shards()
                ),
            ));
        }
        Ok(blocks)
    }

    async fn write_decoded_stripe(
        &mut self,
        blocks: &[Option<Vec<u8>>],
        remaining: &mut u64,
    ) -> Result<()> {
        let data_count = self.layout.data_shards();
        let missing_data = (0..data_count)
            .filter(|index| blocks[*index].is_none())
            .collect::<Vec<_>>();

        if missing_data.is_empty() {
            for block in blocks.iter().take(data_count).flatten() {
                write_source_block(
                    &mut self.destination,
                    &mut self.source_hasher,
                    block,
                    remaining,
                )
                .await?;
            }
            return Ok(());
        }

        let decoder = self.decoder.as_mut().ok_or_else(|| {
            EcsError::new(
                EcsErrorKind::InsufficientShards,
                "data piece is missing from a layout without parity",
            )
        })?;
        for (index, block) in blocks.iter().enumerate() {
            let Some(block) = block else {
                continue;
            };
            let result = if index < data_count {
                decoder.add_original_shard(index, block)
            } else {
                decoder.add_recovery_shard(index - data_count, block)
            };
            result.map_err(|error| {
                EcsError::new(
                    EcsErrorKind::CodecFailure,
                    format!("add piece {index} to Reed-Solomon decoder: {error:?}"),
                )
            })?;
        }
        let started = Instant::now();
        let restored = decoder.decode().map_err(|error| {
            metrics::record_error();
            EcsError::new(
                EcsErrorKind::CodecFailure,
                format!("decode Reed-Solomon stripe: {error:?}"),
            )
        })?;
        metrics::record_codec(started.elapsed().as_nanos() as u64);

        for (index, block) in blocks.iter().enumerate().take(data_count) {
            if let Some(block) = block {
                write_source_block(
                    &mut self.destination,
                    &mut self.source_hasher,
                    block,
                    remaining,
                )
                .await?;
            } else {
                let block = restored.restored_original(index).ok_or_else(|| {
                    EcsError::new(
                        EcsErrorKind::InternalInvariant,
                        format!("decoder omitted missing data piece {index}"),
                    )
                })?;
                self.reconstructed_data_pieces = self.reconstructed_data_pieces.saturating_add(1);
                write_source_block(
                    &mut self.destination,
                    &mut self.source_hasher,
                    block,
                    remaining,
                )
                .await?;
            }
        }
        Ok(())
    }
}

async fn write_source_block<W>(
    destination: &mut W,
    source_hasher: &mut Option<StreamHasher>,
    block: &[u8],
    remaining: &mut u64,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let count = (*remaining).min(block.len() as u64) as usize;
    if count == 0 {
        return Ok(());
    }
    let bytes = &block[..count];
    destination.write_all(bytes).await.map_err(|error| {
        metrics::record_error();
        EcsError::from_io(
            EcsErrorKind::WriterFailure,
            "write decoded destination",
            None,
            error,
        )
    })?;
    let started = Instant::now();
    if let Some(hasher) = source_hasher.as_mut() {
        hasher.update(bytes);
    }
    metrics::record_checksum(started.elapsed().as_nanos() as u64);
    *remaining -= count as u64;
    Ok(())
}
