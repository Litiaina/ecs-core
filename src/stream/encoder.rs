use std::time::Instant;

use futures_util::future::join_all;
use rayon::prelude::*;
use simd_erasure_core::ReedSolomonEncoder;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::buffer::AlignedBuffer;
use crate::checksum::{ChecksumPolicy, Digest, StreamHasher, digest_bytes};
use crate::metrics;
use crate::{EcLayout, EcsError, EcsErrorKind, Result};

use super::slots::{
    CompletionPolicy, PieceFailure, PieceReport, PieceState, ShardWriters, role_for,
};

const PARALLEL_CHECKSUM_MIN_BYTES: usize = 256 * 1024;

/// Encoding behavior selected before the first source byte is accepted.
#[derive(Clone, Debug)]
pub struct EncodeOptions {
    pub completion: CompletionPolicy,
    pub checksum: Option<ChecksumPolicy>,
    pub expected_source_checksum: Option<Digest>,
}

impl Default for EncodeOptions {
    fn default() -> Self {
        Self {
            completion: CompletionPolicy::RequireAll,
            checksum: Some(ChecksumPolicy::sha256_with_blocks(true)),
            expected_source_checksum: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EncodeReport {
    pub layout: EcLayout,
    pub source_bytes: u64,
    pub shard_bytes: u64,
    pub padded_source_bytes: u64,
    pub stripe_count: u64,
    pub source_checksum: Option<Digest>,
    pub pieces: Vec<PieceReport>,
    pub reconstructable: bool,
    pub fully_protected: bool,
}

pub struct EncodeOutput<W> {
    pub report: EncodeReport,
    pub writers: ShardWriters<W>,
}

/// Bounded-memory streaming encoder extracted from the N1/N2 data plane.
pub struct ErasureStreamEncoder<W>
where
    W: AsyncWrite + Unpin + Send,
{
    layout: EcLayout,
    options: EncodeOptions,
    expected_source_bytes: u64,
    source_bytes: u64,
    writers: Vec<Option<W>>,
    buffer: AlignedBuffer,
    source_hasher: Option<StreamHasher>,
    piece_hashers: Vec<Option<StreamHasher>>,
    block_checksums: Vec<Vec<Digest>>,
    piece_failures: Vec<Option<PieceFailure>>,
    piece_bytes: Vec<u64>,
    initially_present: Vec<bool>,
    encoder: Option<ReedSolomonEncoder>,
    terminal_failure: bool,
}

impl<W> ErasureStreamEncoder<W>
where
    W: AsyncWrite + Unpin + Send,
{
    pub fn new(
        layout: EcLayout,
        writers: ShardWriters<W>,
        expected_source_bytes: u64,
        options: EncodeOptions,
    ) -> Result<Self> {
        writers.validate(layout)?;
        layout.encoded_source_capacity(expected_source_bytes)?;
        let flat = writers.into_flat();
        let available = flat.iter().filter(|writer| writer.is_some()).count();
        if available < layout.data_shards() {
            return Err(EcsError::new(
                EcsErrorKind::InsufficientShards,
                format!(
                    "encoding requires K={} writers, but only {available} are available",
                    layout.data_shards()
                ),
            ));
        }
        if options.completion == CompletionPolicy::RequireAll && available != layout.total_shards()
        {
            return Err(EcsError::new(
                EcsErrorKind::WriterFailure,
                format!(
                    "full completion requires all {} writers, but only {available} are available",
                    layout.total_shards()
                ),
            ));
        }
        if let Some(expected) = &options.expected_source_checksum {
            let Some(policy) = options.checksum else {
                return Err(EcsError::new(
                    EcsErrorKind::InvalidLayout,
                    "an expected source checksum requires an enabled checksum policy",
                ));
            };
            if !policy.source_checksum || policy.algorithm != expected.algorithm() {
                return Err(EcsError::new(
                    EcsErrorKind::InvalidLayout,
                    "expected source checksum does not match the configured source algorithm",
                ));
            }
        }

        let checksum = options.checksum;
        let source_hasher = checksum
            .filter(|policy| policy.source_checksum)
            .map(|policy| StreamHasher::new(policy.algorithm));
        let piece_hashers = flat
            .iter()
            .map(|writer| {
                writer
                    .as_ref()
                    .and_then(|_| checksum.map(|policy| StreamHasher::new(policy.algorithm)))
            })
            .collect();
        let initially_present = flat.iter().map(Option::is_some).collect();
        let mut piece_failures = vec![None; layout.total_shards()];
        for (index, writer) in flat.iter().enumerate() {
            if writer.is_none() {
                let failure = PieceFailure {
                    raw_os_error: None,
                    reason: "writer was unavailable before encoding".to_string(),
                };
                crate::report_warning!(failure.reason, "shard", index);
                piece_failures[index] = Some(failure);
                metrics::record_degraded_piece();
            }
        }
        let encoder = if layout.parity_shards() == 0 {
            None
        } else {
            Some(
                ReedSolomonEncoder::new(
                    layout.data_shards(),
                    layout.parity_shards(),
                    layout.block_size(),
                )
                .map_err(|error| {
                    EcsError::new(
                        EcsErrorKind::CodecFailure,
                        format!("initialize Reed-Solomon encoder: {error:?}"),
                    )
                })?,
            )
        };

        Ok(Self {
            layout,
            options,
            expected_source_bytes,
            source_bytes: 0,
            writers: flat,
            buffer: AlignedBuffer::new(layout.stripe_size()),
            source_hasher,
            piece_hashers,
            block_checksums: vec![Vec::new(); layout.total_shards()],
            piece_failures,
            piece_bytes: vec![0; layout.total_shards()],
            initially_present,
            encoder,
            terminal_failure: false,
        })
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.ensure_active()?;
        let next = self
            .source_bytes
            .checked_add(data.len() as u64)
            .ok_or_else(|| EcsError::new(EcsErrorKind::InputTooLarge, "source length overflow"))?;
        if next > self.expected_source_bytes {
            return Err(EcsError::new(
                EcsErrorKind::InputTooLarge,
                format!(
                    "source exceeds declared length {}",
                    self.expected_source_bytes
                ),
            ));
        }
        self.update_source_checksum(data);
        metrics::record_source_bytes(data.len());
        self.source_bytes = next;

        let mut offset = 0usize;
        while offset < data.len() {
            let copied = self.buffer.extend_from_slice(&data[offset..]);
            offset += copied;
            if self.buffer.len() == self.buffer.capacity() {
                self.process_buffered_stripe().await?;
            }
        }
        Ok(())
    }

    pub async fn copy_exact<R>(&mut self, source: &mut R) -> Result<u64>
    where
        R: AsyncRead + Unpin + Send,
    {
        self.ensure_active()?;
        while self.source_bytes < self.expected_source_bytes {
            if self.buffer.len() == self.buffer.capacity() {
                self.process_buffered_stripe().await?;
            }
            let before = self.buffer.len();
            let remaining =
                (self.expected_source_bytes - self.source_bytes).min(usize::MAX as u64) as usize;
            let read = self
                .buffer
                .read_from(source, remaining)
                .await
                .map_err(|error| {
                    metrics::record_error();
                    EcsError::from_io(
                        EcsErrorKind::ReaderFailure,
                        "read source stream",
                        None,
                        error,
                    )
                })?;
            if read == 0 {
                metrics::record_error();
                return Err(EcsError::new(
                    EcsErrorKind::IncompleteInput,
                    format!(
                        "source ended at {} bytes; expected {}",
                        self.source_bytes, self.expected_source_bytes
                    ),
                ));
            }
            let end = before + read;
            let bytes = &self.buffer.as_slice()[before..end];
            let checksum_started = Instant::now();
            if let Some(hasher) = self.source_hasher.as_mut() {
                hasher.update(bytes);
            }
            metrics::record_checksum(checksum_started.elapsed().as_nanos() as u64);
            metrics::record_source_bytes(read);
            self.source_bytes += read as u64;
        }
        Ok(self.source_bytes)
    }

    pub async fn finish(mut self) -> Result<EncodeOutput<W>> {
        self.ensure_active()?;
        if self.source_bytes != self.expected_source_bytes {
            metrics::record_error();
            return Err(EcsError::new(
                EcsErrorKind::IncompleteInput,
                format!(
                    "source supplied {} bytes; expected {}",
                    self.source_bytes, self.expected_source_bytes
                ),
            ));
        }
        if self.buffer.len() > 0 {
            self.buffer.fill_zeros();
            self.process_buffered_stripe().await?;
        }
        self.flush_writers().await?;

        let source_checksum = self.source_hasher.take().map(StreamHasher::finalize);
        if let (Some(expected), Some(actual)) = (
            self.options.expected_source_checksum.as_ref(),
            source_checksum.as_ref(),
        ) && expected != actual
        {
            metrics::record_error();
            crate::report_error!("source checksum mismatch", "stream", "encode");
            return Err(EcsError::new(
                EcsErrorKind::ChecksumMismatch,
                format!(
                    "source checksum mismatch: expected {}, got {}",
                    expected.to_hex(),
                    actual.to_hex()
                ),
            ));
        }

        let checksums = self
            .piece_hashers
            .drain(..)
            .map(|hasher| hasher.map(StreamHasher::finalize))
            .collect::<Vec<_>>();
        let completed = self
            .writers
            .iter()
            .filter(|writer| writer.is_some())
            .count();
        let pieces = (0..self.layout.total_shards())
            .map(|index| {
                let state = if self.writers[index].is_some() {
                    PieceState::Complete
                } else if !self.initially_present[index] {
                    PieceState::Missing
                } else {
                    PieceState::Failed
                };
                PieceReport {
                    index,
                    role: role_for(index, self.layout.data_shards()),
                    bytes_written: self.piece_bytes[index],
                    checksum: checksums[index].clone(),
                    block_checksums: std::mem::take(&mut self.block_checksums[index]),
                    state,
                    failure: self.piece_failures[index].take(),
                }
            })
            .collect();
        let shard_bytes = self.layout.shard_bytes(self.source_bytes)?;
        let encoded_source_capacity = self.layout.encoded_source_capacity(self.source_bytes)?;
        let report = EncodeReport {
            layout: self.layout,
            source_bytes: self.source_bytes,
            shard_bytes,
            padded_source_bytes: encoded_source_capacity.saturating_sub(self.source_bytes),
            stripe_count: self.layout.stripe_count(self.source_bytes),
            source_checksum,
            pieces,
            reconstructable: completed >= self.layout.data_shards(),
            fully_protected: completed == self.layout.total_shards(),
        };
        let writers = ShardWriters::from_flat(self.writers, self.layout.data_shards());
        Ok(EncodeOutput { report, writers })
    }

    fn update_source_checksum(&mut self, data: &[u8]) {
        let started = Instant::now();
        if let Some(hasher) = self.source_hasher.as_mut() {
            hasher.update(data);
        }
        metrics::record_checksum(started.elapsed().as_nanos() as u64);
    }

    fn ensure_active(&self) -> Result<()> {
        if self.terminal_failure {
            return Err(EcsError::new(
                EcsErrorKind::StreamFailed,
                "encoder cannot continue after an earlier stripe failure",
            ));
        }
        Ok(())
    }

    async fn process_buffered_stripe(&mut self) -> Result<()> {
        match self.process_stripe().await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.terminal_failure = true;
                Err(error)
            }
        }
    }

    async fn process_stripe(&mut self) -> Result<()> {
        let block_size = self.layout.block_size();
        let data_count = self.layout.data_shards();
        let parity_count = self.layout.parity_shards();
        let buffer = self.buffer.as_full_slice();
        let mut slices = Vec::with_capacity(self.layout.total_shards());
        for index in 0..data_count {
            let start = index * block_size;
            slices.push(&buffer[start..start + block_size]);
        }

        if parity_count > 0 {
            let encoder = self.encoder.as_mut().ok_or_else(|| {
                EcsError::new(
                    EcsErrorKind::InternalInvariant,
                    "missing initialized encoder",
                )
            })?;
            for slice in &slices {
                encoder.add_original_shard(slice).map_err(|error| {
                    metrics::record_error();
                    EcsError::new(
                        EcsErrorKind::CodecFailure,
                        format!("add data piece to Reed-Solomon encoder: {error:?}"),
                    )
                })?;
            }
            let started = Instant::now();
            let result = encoder.encode().map_err(|error| {
                metrics::record_error();
                EcsError::new(
                    EcsErrorKind::CodecFailure,
                    format!("encode Reed-Solomon stripe: {error:?}"),
                )
            })?;
            metrics::record_codec(started.elapsed().as_nanos() as u64);
            for parity_index in 0..parity_count {
                slices.push(result.recovery(parity_index).ok_or_else(|| {
                    EcsError::new(
                        EcsErrorKind::InternalInvariant,
                        format!("encoder omitted parity piece {parity_index}"),
                    )
                })?);
            }
            Self::update_piece_checksums(
                self.options.checksum,
                &mut self.piece_hashers,
                &mut self.block_checksums,
                &slices,
            );
            write_piece_slices(
                self.layout,
                self.options.completion,
                &mut self.writers,
                &mut self.piece_hashers,
                &mut self.block_checksums,
                &mut self.piece_failures,
                &mut self.piece_bytes,
                &slices,
            )
            .await?;
        } else {
            Self::update_piece_checksums(
                self.options.checksum,
                &mut self.piece_hashers,
                &mut self.block_checksums,
                &slices,
            );
            write_piece_slices(
                self.layout,
                self.options.completion,
                &mut self.writers,
                &mut self.piece_hashers,
                &mut self.block_checksums,
                &mut self.piece_failures,
                &mut self.piece_bytes,
                &slices,
            )
            .await?;
        }

        self.buffer.clear();
        metrics::record_stripe();
        Ok(())
    }

    fn update_piece_checksums(
        policy: Option<ChecksumPolicy>,
        hashers: &mut [Option<StreamHasher>],
        block_checksums: &mut [Vec<Digest>],
        slices: &[&[u8]],
    ) {
        let Some(policy) = policy else {
            return;
        };
        let started = Instant::now();
        let total_bytes = slices.iter().map(|slice| slice.len()).sum::<usize>();
        if total_bytes >= PARALLEL_CHECKSUM_MIN_BYTES && slices.len() > 1 {
            hashers
                .par_iter_mut()
                .zip(block_checksums.par_iter_mut())
                .zip(slices.par_iter())
                .for_each(|((hasher, blocks), slice)| {
                    let Some(hasher) = hasher.as_mut() else {
                        return;
                    };
                    if policy.block_checksums {
                        let (_, block_checksum) = rayon::join(
                            || hasher.update(slice),
                            || digest_bytes(policy.algorithm, slice),
                        );
                        blocks.push(block_checksum);
                    } else {
                        hasher.update(slice);
                    }
                });
        } else {
            hashers
                .iter_mut()
                .zip(block_checksums.iter_mut())
                .zip(slices.iter())
                .for_each(|((hasher, blocks), slice)| {
                    if let Some(hasher) = hasher.as_mut() {
                        hasher.update(slice);
                        if policy.block_checksums {
                            blocks.push(digest_bytes(policy.algorithm, slice));
                        }
                    }
                });
        }
        metrics::record_checksum(started.elapsed().as_nanos() as u64);
    }

    async fn flush_writers(&mut self) -> Result<()> {
        let started = Instant::now();
        let flushes = self
            .writers
            .iter_mut()
            .enumerate()
            .filter_map(|(index, writer)| {
                let writer = writer.as_mut()?;
                Some(async move { (index, writer.flush().await) })
            });
        let results = join_all(flushes).await;
        metrics::record_flush(started.elapsed().as_nanos() as u64);
        let mut first_strict_error = None;
        for (index, result) in results {
            if let Err(error) = result {
                metrics::record_error();
                metrics::record_degraded_piece();
                let raw_os_error = error.raw_os_error();
                let reason = error.to_string();
                crate::report_warning!(&reason, "shard_flush", index);
                self.piece_failures[index] = Some(PieceFailure {
                    raw_os_error,
                    reason: reason.clone(),
                });
                self.piece_hashers[index] = None;
                self.block_checksums[index].clear();
                self.writers[index] = None;
                if first_strict_error.is_none() {
                    first_strict_error = Some(EcsError::from_io(
                        EcsErrorKind::WriterFailure,
                        "flush encoded piece",
                        Some(index),
                        error,
                    ));
                }
            }
        }
        validate_remaining(
            self.layout,
            self.options.completion,
            &self.writers,
            first_strict_error,
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_piece_slices<W>(
    layout: EcLayout,
    completion: CompletionPolicy,
    writers: &mut [Option<W>],
    piece_hashers: &mut [Option<StreamHasher>],
    block_checksums: &mut [Vec<Digest>],
    piece_failures: &mut [Option<PieceFailure>],
    piece_bytes: &mut [u64],
    slices: &[&[u8]],
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let started = Instant::now();
    let writes = writers
        .iter_mut()
        .enumerate()
        .filter_map(|(index, writer)| {
            let writer = writer.as_mut()?;
            let slice = slices[index];
            Some(async move { (index, writer.write_all(slice).await) })
        });
    let results = join_all(writes).await;
    metrics::record_write(started.elapsed().as_nanos() as u64);
    let mut first_strict_error = None;
    for (index, result) in results {
        match result {
            Ok(()) => {
                piece_bytes[index] = piece_bytes[index].saturating_add(layout.block_size() as u64);
            }
            Err(error) => {
                metrics::record_error();
                metrics::record_degraded_piece();
                let raw_os_error = error.raw_os_error();
                let reason = error.to_string();
                crate::report_warning!(&reason, "shard_write", index);
                piece_failures[index] = Some(PieceFailure {
                    raw_os_error,
                    reason: reason.clone(),
                });
                piece_hashers[index] = None;
                block_checksums[index].clear();
                writers[index] = None;
                if first_strict_error.is_none() {
                    first_strict_error = Some(EcsError::from_io(
                        EcsErrorKind::WriterFailure,
                        "write encoded piece",
                        Some(index),
                        error,
                    ));
                }
            }
        }
    }
    validate_remaining(layout, completion, writers, first_strict_error)
}

fn validate_remaining<W>(
    layout: EcLayout,
    completion: CompletionPolicy,
    writers: &[Option<W>],
    strict_error: Option<EcsError>,
) -> Result<()> {
    let available = writers.iter().filter(|writer| writer.is_some()).count();
    if available < layout.data_shards() {
        return Err(EcsError::new(
            EcsErrorKind::InsufficientShards,
            format!(
                "only {available} encoded pieces remain; K={} is required",
                layout.data_shards()
            ),
        ));
    }
    if completion == CompletionPolicy::RequireAll && available != layout.total_shards() {
        return Err(strict_error.unwrap_or_else(|| {
            EcsError::new(
                EcsErrorKind::WriterFailure,
                format!(
                    "full completion requires {} pieces, but only {available} remain",
                    layout.total_shards()
                ),
            )
        }));
    }
    Ok(())
}
