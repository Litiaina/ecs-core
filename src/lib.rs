//! ECS Core — Erasure Coding Streaming Core.
//!
//! ECS Core converts a bounded input stream into Reed-Solomon data and
//! parity pieces supplied by the caller. It deliberately does not choose
//! disks or nodes and does not publish storage-engine metadata.

mod buffer;
mod checksum;
mod error;
mod layout;
mod macros;
mod metrics;
pub mod stream;

pub use buffer::AlignedBuffer;
pub use checksum::{ChecksumAlgorithm, ChecksumPolicy, Digest, Sha256Digest};
pub use error::{EcsError, EcsErrorKind, Result};
pub use layout::{ALIGNMENT, EcLayout};
pub use metrics::{EcsMetricsSnapshot, metrics_snapshot, reset_metrics};
pub use simd_erasure_core::{ReedSolomonDecoder, ReedSolomonEncoder};
pub use stream::{
    CompletionPolicy, DecodeOptions, DecodeOutput, DecodeReport, EncodeOptions, EncodeOutput,
    EncodeReport, ErasureStreamDecoder, ErasureStreamEncoder, ErasureStreamRepairer, PieceFailure,
    PieceReport, PieceRole, PieceState, RepairOutput, RepairReport, ShardReaders, ShardWriters,
};

#[doc(hidden)]
pub mod __private {
    pub use tracing;
}
