use std::sync::atomic::{AtomicU64, Ordering};

static STRIPES: AtomicU64 = AtomicU64::new(0);
static SOURCE_BYTES: AtomicU64 = AtomicU64::new(0);
static CODEC_NANOSECONDS: AtomicU64 = AtomicU64::new(0);
static CHECKSUM_NANOSECONDS: AtomicU64 = AtomicU64::new(0);
static WRITE_NANOSECONDS: AtomicU64 = AtomicU64::new(0);
static FLUSH_NANOSECONDS: AtomicU64 = AtomicU64::new(0);
static READ_NANOSECONDS: AtomicU64 = AtomicU64::new(0);
static ERRORS: AtomicU64 = AtomicU64::new(0);
static DEGRADED_PIECES: AtomicU64 = AtomicU64::new(0);

/// Process-wide ECS Core counters. They are observational and never gate I/O.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EcsMetricsSnapshot {
    pub stripes: u64,
    pub source_bytes: u64,
    pub codec_nanoseconds: u64,
    pub checksum_nanoseconds: u64,
    pub write_nanoseconds: u64,
    pub flush_nanoseconds: u64,
    pub read_nanoseconds: u64,
    pub errors: u64,
    pub degraded_pieces: u64,
}

#[must_use]
pub fn metrics_snapshot() -> EcsMetricsSnapshot {
    EcsMetricsSnapshot {
        stripes: STRIPES.load(Ordering::Relaxed),
        source_bytes: SOURCE_BYTES.load(Ordering::Relaxed),
        codec_nanoseconds: CODEC_NANOSECONDS.load(Ordering::Relaxed),
        checksum_nanoseconds: CHECKSUM_NANOSECONDS.load(Ordering::Relaxed),
        write_nanoseconds: WRITE_NANOSECONDS.load(Ordering::Relaxed),
        flush_nanoseconds: FLUSH_NANOSECONDS.load(Ordering::Relaxed),
        read_nanoseconds: READ_NANOSECONDS.load(Ordering::Relaxed),
        errors: ERRORS.load(Ordering::Relaxed),
        degraded_pieces: DEGRADED_PIECES.load(Ordering::Relaxed),
    }
}

pub fn reset_metrics() {
    for counter in [
        &STRIPES,
        &SOURCE_BYTES,
        &CODEC_NANOSECONDS,
        &CHECKSUM_NANOSECONDS,
        &WRITE_NANOSECONDS,
        &FLUSH_NANOSECONDS,
        &READ_NANOSECONDS,
        &ERRORS,
        &DEGRADED_PIECES,
    ] {
        counter.store(0, Ordering::Relaxed);
    }
}

pub(crate) fn record_stripe() {
    STRIPES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_source_bytes(bytes: usize) {
    SOURCE_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub(crate) fn record_codec(nanoseconds: u64) {
    CODEC_NANOSECONDS.fetch_add(nanoseconds, Ordering::Relaxed);
}

pub(crate) fn record_checksum(nanoseconds: u64) {
    CHECKSUM_NANOSECONDS.fetch_add(nanoseconds, Ordering::Relaxed);
}

pub(crate) fn record_write(nanoseconds: u64) {
    WRITE_NANOSECONDS.fetch_add(nanoseconds, Ordering::Relaxed);
}

pub(crate) fn record_flush(nanoseconds: u64) {
    FLUSH_NANOSECONDS.fetch_add(nanoseconds, Ordering::Relaxed);
}

pub(crate) fn record_read(nanoseconds: u64) {
    READ_NANOSECONDS.fetch_add(nanoseconds, Ordering::Relaxed);
}

pub(crate) fn record_error() {
    ERRORS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_degraded_piece() {
    DEGRADED_PIECES.fetch_add(1, Ordering::Relaxed);
}
