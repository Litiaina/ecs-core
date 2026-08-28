use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;

/// ECS Core failure category suitable for stable caller-side handling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EcsErrorKind {
    InvalidLayout,
    InvalidStripeSize,
    IncompleteInput,
    InputTooLarge,
    ReaderFailure,
    WriterFailure,
    CapacityExhausted,
    InsufficientShards,
    ChecksumMismatch,
    CodecFailure,
    StreamFailed,
    Cancelled,
    InternalInvariant,
}

/// Structured ECS Core error. I/O errors retain their operating-system code.
#[derive(Debug)]
pub struct EcsError {
    kind: EcsErrorKind,
    reason: String,
    shard_index: Option<usize>,
    raw_os_error: Option<i32>,
    source: Option<io::Error>,
}

impl EcsError {
    #[must_use]
    pub fn new(kind: EcsErrorKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
            shard_index: None,
            raw_os_error: None,
            source: None,
        }
    }

    #[must_use]
    pub fn for_shard(mut self, shard_index: usize) -> Self {
        self.shard_index = Some(shard_index);
        self
    }

    #[must_use]
    pub fn from_io(
        kind: EcsErrorKind,
        reason: impl Into<String>,
        shard_index: Option<usize>,
        source: io::Error,
    ) -> Self {
        let raw_os_error = source.raw_os_error();
        let kind = if matches!(raw_os_error, Some(code) if code == libc::ENOSPC || code == libc::EDQUOT)
            || source.kind() == io::ErrorKind::StorageFull
        {
            EcsErrorKind::CapacityExhausted
        } else {
            kind
        };
        Self {
            kind,
            reason: reason.into(),
            shard_index,
            raw_os_error,
            source: Some(source),
        }
    }

    #[must_use]
    pub fn kind(&self) -> EcsErrorKind {
        self.kind
    }

    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }

    #[must_use]
    pub fn shard_index(&self) -> Option<usize> {
        self.shard_index
    }

    #[must_use]
    pub fn raw_os_error(&self) -> Option<i32> {
        self.raw_os_error
    }

    #[must_use]
    pub fn is_capacity_error(&self) -> bool {
        self.kind == EcsErrorKind::CapacityExhausted
    }
}

impl Display for EcsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.reason)?;
        if let Some(index) = self.shard_index {
            write!(formatter, " (shard {index})")?;
        }
        if let Some(code) = self.raw_os_error {
            write!(formatter, " (os error {code})")?;
        }
        Ok(())
    }
}

impl Error for EcsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

pub type Result<T> = std::result::Result<T, EcsError>;
