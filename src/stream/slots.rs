use crate::{Digest, EcLayout, EcsError, EcsErrorKind, Result};

/// Whether a stream requires every configured piece or permits a
/// reconstructable degraded result containing at least K pieces.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CompletionPolicy {
    #[default]
    RequireAll,
    AllowReconstructable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceRole {
    Data,
    Parity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceState {
    Complete,
    Missing,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PieceFailure {
    pub raw_os_error: Option<i32>,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PieceReport {
    pub index: usize,
    pub role: PieceRole,
    pub bytes_written: u64,
    pub checksum: Option<Digest>,
    pub block_checksums: Vec<Digest>,
    pub state: PieceState,
    pub failure: Option<PieceFailure>,
}

/// Writers are indexed explicitly as K data slots followed by M parity slots.
pub struct ShardWriters<W> {
    pub data: Vec<Option<W>>,
    pub parity: Vec<Option<W>>,
}

impl<W> ShardWriters<W> {
    #[must_use]
    pub fn new(data: Vec<Option<W>>, parity: Vec<Option<W>>) -> Self {
        Self { data, parity }
    }

    pub(crate) fn validate(&self, layout: EcLayout) -> Result<()> {
        if self.data.len() != layout.data_shards() || self.parity.len() != layout.parity_shards() {
            return Err(EcsError::new(
                EcsErrorKind::InvalidLayout,
                format!(
                    "writer layout mismatch: expected {} data and {} parity slots, got {} and {}",
                    layout.data_shards(),
                    layout.parity_shards(),
                    self.data.len(),
                    self.parity.len()
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn into_flat(self) -> Vec<Option<W>> {
        self.data.into_iter().chain(self.parity).collect()
    }

    pub(crate) fn from_flat(mut slots: Vec<Option<W>>, data_shards: usize) -> Self {
        let parity = slots.split_off(data_shards);
        Self {
            data: slots,
            parity,
        }
    }
}

/// Readers use the same stable K-data then M-parity index contract.
pub struct ShardReaders<R> {
    pub data: Vec<Option<R>>,
    pub parity: Vec<Option<R>>,
}

impl<R> ShardReaders<R> {
    #[must_use]
    pub fn new(data: Vec<Option<R>>, parity: Vec<Option<R>>) -> Self {
        Self { data, parity }
    }

    pub(crate) fn validate(&self, layout: EcLayout) -> Result<()> {
        if self.data.len() != layout.data_shards() || self.parity.len() != layout.parity_shards() {
            return Err(EcsError::new(
                EcsErrorKind::InvalidLayout,
                format!(
                    "reader layout mismatch: expected {} data and {} parity slots, got {} and {}",
                    layout.data_shards(),
                    layout.parity_shards(),
                    self.data.len(),
                    self.parity.len()
                ),
            ));
        }
        let available = self
            .data
            .iter()
            .chain(&self.parity)
            .filter(|reader| reader.is_some())
            .count();
        if available < layout.data_shards() {
            return Err(EcsError::new(
                EcsErrorKind::InsufficientShards,
                format!(
                    "decode requires K={} pieces, but only {available} readers are available",
                    layout.data_shards()
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn into_flat(self) -> Vec<Option<R>> {
        self.data.into_iter().chain(self.parity).collect()
    }

    pub(crate) fn from_flat(mut slots: Vec<Option<R>>, data_shards: usize) -> Self {
        let parity = slots.split_off(data_shards);
        Self {
            data: slots,
            parity,
        }
    }
}

pub(crate) fn role_for(index: usize, data_shards: usize) -> PieceRole {
    if index < data_shards {
        PieceRole::Data
    } else {
        PieceRole::Parity
    }
}
