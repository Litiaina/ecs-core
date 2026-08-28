use std::time::Instant;

use futures_util::future::join_all;
use simd_erasure_core::{ReedSolomonDecoder, ReedSolomonEncoder};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::checksum::{ChecksumAlgorithm, Digest, StreamHasher};
use crate::metrics;
use crate::{EcLayout, EcsError, EcsErrorKind, Result};

use super::slots::role_for;
use super::{PieceRole, ShardReaders};

#[derive(Clone, Debug)]
pub struct RepairReport {
    pub layout: EcLayout,
    pub repaired_index: usize,
    pub repaired_role: PieceRole,
    pub shard_bytes: u64,
    pub stripe_count: u64,
    pub failed_readers: Vec<usize>,
    pub checksum: Option<Digest>,
}

pub struct RepairOutput<R, W> {
    pub report: RepairReport,
    pub readers: ShardReaders<R>,
    pub destination: W,
}

/// Rebuilds exactly one stable piece index while retaining bounded memory.
pub struct ErasureStreamRepairer<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    layout: EcLayout,
    source_bytes: u64,
    repair_index: usize,
    readers: Vec<Option<R>>,
    destination: W,
    decoder: Option<ReedSolomonDecoder>,
    encoder: Option<ReedSolomonEncoder>,
    failed_readers: Vec<usize>,
    hasher: Option<StreamHasher>,
}

impl<R, W> ErasureStreamRepairer<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    pub fn new(
        layout: EcLayout,
        mut readers: ShardReaders<R>,
        destination: W,
        source_bytes: u64,
        repair_index: usize,
        checksum: Option<ChecksumAlgorithm>,
    ) -> Result<Self> {
        if repair_index >= layout.total_shards() {
            return Err(EcsError::new(
                EcsErrorKind::InvalidLayout,
                format!(
                    "repair index {repair_index} is outside K+M={}",
                    layout.total_shards()
                ),
            ));
        }
        layout.encoded_source_capacity(source_bytes)?;
        let slot = if repair_index < layout.data_shards() {
            &mut readers.data[repair_index]
        } else {
            &mut readers.parity[repair_index - layout.data_shards()]
        };
        *slot = None;
        readers.validate(layout)?;

        let decoder = if readers.data.iter().any(Option::is_none) {
            if layout.parity_shards() == 0 {
                return Err(EcsError::new(
                    EcsErrorKind::InsufficientShards,
                    "cannot repair a missing data piece without parity",
                ));
            }
            Some(
                ReedSolomonDecoder::new(
                    layout.data_shards(),
                    layout.parity_shards(),
                    layout.block_size(),
                )
                .map_err(|error| {
                    EcsError::new(
                        EcsErrorKind::CodecFailure,
                        format!("initialize repair decoder: {error:?}"),
                    )
                })?,
            )
        } else {
            None
        };
        let encoder = if repair_index >= layout.data_shards() {
            Some(
                ReedSolomonEncoder::new(
                    layout.data_shards(),
                    layout.parity_shards(),
                    layout.block_size(),
                )
                .map_err(|error| {
                    EcsError::new(
                        EcsErrorKind::CodecFailure,
                        format!("initialize repair encoder: {error:?}"),
                    )
                })?,
            )
        } else {
            None
        };

        Ok(Self {
            layout,
            source_bytes,
            repair_index,
            readers: readers.into_flat(),
            destination,
            decoder,
            encoder,
            failed_readers: Vec::new(),
            hasher: checksum.map(StreamHasher::new),
        })
    }

    pub async fn repair(mut self) -> Result<RepairOutput<R, W>> {
        let stripe_count = self.layout.stripe_count(self.source_bytes);
        for _ in 0..stripe_count {
            let blocks = self.read_stripe().await?;
            let repaired = self.repair_stripe(&blocks)?;
            self.destination
                .write_all(&repaired)
                .await
                .map_err(|error| {
                    metrics::record_error();
                    EcsError::from_io(
                        EcsErrorKind::WriterFailure,
                        "write repaired piece",
                        Some(self.repair_index),
                        error,
                    )
                })?;
            if let Some(hasher) = self.hasher.as_mut() {
                hasher.update(&repaired);
            }
        }
        self.destination.flush().await.map_err(|error| {
            metrics::record_error();
            EcsError::from_io(
                EcsErrorKind::WriterFailure,
                "flush repaired piece",
                Some(self.repair_index),
                error,
            )
        })?;
        let report = RepairReport {
            layout: self.layout,
            repaired_index: self.repair_index,
            repaired_role: role_for(self.repair_index, self.layout.data_shards()),
            shard_bytes: self.layout.shard_bytes(self.source_bytes)?,
            stripe_count,
            failed_readers: self.failed_readers,
            checksum: self.hasher.map(StreamHasher::finalize),
        };
        Ok(RepairOutput {
            report,
            readers: ShardReaders::from_flat(self.readers, self.layout.data_shards()),
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
        let mut blocks = vec![None; self.layout.total_shards()];
        for (index, result, block) in results {
            match result {
                Ok(_) => blocks[index] = Some(block),
                Err(error) => {
                    metrics::record_error();
                    metrics::record_degraded_piece();
                    crate::report_warning!(&error, "repair_shard_read", index);
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
                    "repair stripe has {available} readable pieces; K={} is required",
                    self.layout.data_shards()
                ),
            ));
        }
        Ok(blocks)
    }

    fn repair_stripe(&mut self, blocks: &[Option<Vec<u8>>]) -> Result<Vec<u8>> {
        let data_count = self.layout.data_shards();
        let missing_data = (0..data_count)
            .filter(|index| blocks[*index].is_none())
            .collect::<Vec<_>>();

        if self.repair_index < data_count {
            let decoder = self.decoder.as_mut().ok_or_else(|| {
                EcsError::new(EcsErrorKind::InternalInvariant, "missing repair decoder")
            })?;
            add_decoder_blocks(decoder, blocks, data_count)?;
            let started = Instant::now();
            let restored = decoder.decode().map_err(|error| {
                EcsError::new(
                    EcsErrorKind::CodecFailure,
                    format!("decode repair stripe: {error:?}"),
                )
            })?;
            metrics::record_codec(started.elapsed().as_nanos() as u64);
            return restored
                .restored_original(self.repair_index)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| {
                    EcsError::new(
                        EcsErrorKind::InternalInvariant,
                        "decoder omitted the requested repaired data piece",
                    )
                });
        }

        let mut data = blocks[..data_count].to_vec();
        if !missing_data.is_empty() {
            let decoder = self.decoder.as_mut().ok_or_else(|| {
                EcsError::new(EcsErrorKind::InternalInvariant, "missing repair decoder")
            })?;
            add_decoder_blocks(decoder, blocks, data_count)?;
            let started = Instant::now();
            let restored = decoder.decode().map_err(|error| {
                EcsError::new(
                    EcsErrorKind::CodecFailure,
                    format!("decode data for parity repair: {error:?}"),
                )
            })?;
            metrics::record_codec(started.elapsed().as_nanos() as u64);
            for index in missing_data {
                data[index] = Some(
                    restored
                        .restored_original(index)
                        .ok_or_else(|| {
                            EcsError::new(
                                EcsErrorKind::InternalInvariant,
                                format!("decoder omitted data piece {index}"),
                            )
                        })?
                        .to_vec(),
                );
            }
        }

        let encoder = self.encoder.as_mut().ok_or_else(|| {
            EcsError::new(EcsErrorKind::InternalInvariant, "missing repair encoder")
        })?;
        for (index, block) in data.iter().enumerate() {
            encoder
                .add_original_shard(block.as_ref().ok_or_else(|| {
                    EcsError::new(
                        EcsErrorKind::InternalInvariant,
                        format!("data piece {index} remains unavailable during parity repair"),
                    )
                })?)
                .map_err(|error| {
                    EcsError::new(
                        EcsErrorKind::CodecFailure,
                        format!("add data piece {index} for parity repair: {error:?}"),
                    )
                })?;
        }
        let started = Instant::now();
        let encoded = encoder.encode().map_err(|error| {
            EcsError::new(
                EcsErrorKind::CodecFailure,
                format!("encode repaired parity: {error:?}"),
            )
        })?;
        metrics::record_codec(started.elapsed().as_nanos() as u64);
        encoded
            .recovery(self.repair_index - data_count)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| {
                EcsError::new(
                    EcsErrorKind::InternalInvariant,
                    "encoder omitted the requested repaired parity piece",
                )
            })
    }
}

fn add_decoder_blocks(
    decoder: &mut ReedSolomonDecoder,
    blocks: &[Option<Vec<u8>>],
    data_count: usize,
) -> Result<()> {
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
                format!("add piece {index} to repair decoder: {error:?}"),
            )
        })?;
    }
    Ok(())
}
