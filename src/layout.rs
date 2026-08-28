use crate::{EcsError, EcsErrorKind, Result};
use simd_erasure_core::ReedSolomonEncoder;
use std::alloc::Layout;

/// Alignment retained from the proven N1/N2 streaming data plane.
pub const ALIGNMENT: usize = 4096;

/// Immutable erasure layout for one encoded stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EcLayout {
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
}

impl EcLayout {
    pub fn new(data_shards: usize, parity_shards: usize, block_size: usize) -> Result<Self> {
        if data_shards == 0 {
            return Err(EcsError::new(
                EcsErrorKind::InvalidLayout,
                "data_shards must be greater than zero",
            ));
        }
        if block_size == 0 || !block_size.is_multiple_of(ALIGNMENT) {
            return Err(EcsError::new(
                EcsErrorKind::InvalidStripeSize,
                format!("block_size must be a non-zero multiple of {ALIGNMENT}"),
            ));
        }
        data_shards.checked_add(parity_shards).ok_or_else(|| {
            EcsError::new(EcsErrorKind::InvalidLayout, "K+M shard count overflow")
        })?;
        let stripe_size = data_shards
            .checked_mul(block_size)
            .ok_or_else(|| EcsError::new(EcsErrorKind::InvalidLayout, "stripe size overflow"))?;
        Layout::from_size_align(stripe_size, ALIGNMENT).map_err(|_| {
            EcsError::new(
                EcsErrorKind::InvalidLayout,
                "stripe size cannot be represented by the platform allocator",
            )
        })?;
        if parity_shards > 0 && !ReedSolomonEncoder::supports(data_shards, parity_shards) {
            return Err(EcsError::new(
                EcsErrorKind::InvalidLayout,
                format!("unsupported Reed-Solomon layout {data_shards}+{parity_shards}"),
            ));
        }
        Ok(Self {
            data_shards,
            parity_shards,
            block_size,
        })
    }

    #[must_use]
    pub fn data_shards(self) -> usize {
        self.data_shards
    }

    #[must_use]
    pub fn parity_shards(self) -> usize {
        self.parity_shards
    }

    #[must_use]
    pub fn total_shards(self) -> usize {
        self.data_shards + self.parity_shards
    }

    #[must_use]
    pub fn block_size(self) -> usize {
        self.block_size
    }

    #[must_use]
    pub fn stripe_size(self) -> usize {
        self.data_shards * self.block_size
    }

    #[must_use]
    pub fn stripe_count(self, source_bytes: u64) -> u64 {
        source_bytes.div_ceil(self.stripe_size() as u64)
    }

    pub fn shard_bytes(self, source_bytes: u64) -> Result<u64> {
        self.stripe_count(source_bytes)
            .checked_mul(self.block_size as u64)
            .ok_or_else(|| {
                EcsError::new(
                    EcsErrorKind::InputTooLarge,
                    "encoded piece length exceeds u64",
                )
            })
    }

    pub fn encoded_source_capacity(self, source_bytes: u64) -> Result<u64> {
        self.shard_bytes(source_bytes)?
            .checked_mul(self.data_shards as u64)
            .ok_or_else(|| {
                EcsError::new(
                    EcsErrorKind::InputTooLarge,
                    "padded encoded source length exceeds u64",
                )
            })
    }
}
