# ECS Core

ECS Core is the **Erasure Coding Streaming Core** extracted from the proven N1 and N2 data planes. It turns one bounded byte stream into `K` data pieces and `M` parity pieces, reconstructs the original stream from any valid `K` pieces, and repairs one missing data or parity piece without loading an object into memory.

The SIMD Reed-Solomon codec is preserved byte-for-byte under `vendor/simd-erasure-core`. ECS Core adds the storage-engine-facing streaming contract around that codec: aligned bounded buffers, concurrent shard I/O, CRC32 or SHA-256 checksums, strict and reconstructable completion policies, structured errors, metrics, decode, and repair.

## Boundary

ECS Core owns:

- stable piece ordering: data indexes `0..K`, followed by parity indexes `K..K+M`;
- aligned stripe encoding with bounded memory;
- concurrent writes to caller-supplied sinks;
- streaming reconstruction and single-piece repair;
- source, piece, and per-block checksum generation;
- exact reporting of missing, failed, complete, and capacity-exhausted pieces.

The caller owns:

- selecting disks, nodes, and network destinations;
- opening temporary files or streams;
- `fsync`, atomic rename, and final publication;
- NDX, NCDX, NCNX, namespace, version, and topology metadata;
- retry, placement, lifecycle, authentication, and authorization.

This boundary lets N1, N2, and a future N2 Lite share one EC implementation without forcing any one engine's placement model into another.

```text
network / file / memory
          |
          v
   caller's object logic
          |
          v
       ECS Core
  aligned stripe stream
          |
     +----+----+----+
     |    |    |    |
   data data parity parity  -> caller-selected disk or network sinks
          |
          v
 caller fsyncs, writes metadata, and publishes
```

## Encoding

```rust,no_run
use ecs_core::{
    EcLayout, EncodeOptions, ErasureStreamEncoder, ShardWriters,
};
use tokio::fs::File;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let layout = EcLayout::new(3, 1, 1024 * 1024)?;
let source_size = 10 * 1024 * 1024;

// ECS Core does not choose these paths. The engine supplies temporary sinks.
let writers = ShardWriters::new(
    vec![
        Some(File::create("data-0.tmp").await?),
        Some(File::create("data-1.tmp").await?),
        Some(File::create("data-2.tmp").await?),
    ],
    vec![Some(File::create("parity-0.tmp").await?)],
);

let mut encoder = ErasureStreamEncoder::new(
    layout,
    writers,
    source_size,
    EncodeOptions::default(),
)?;
let mut source = File::open("object.bin").await?;
encoder.copy_exact(&mut source).await?;
let output = encoder.finish().await?;

assert!(output.report.fully_protected);
// The caller now syncs returned files, commits metadata, and atomically publishes.
# Ok(())
# }
```

`ErasureStreamEncoder::write` accepts arbitrary client chunks. `copy_exact` pipes an `AsyncRead` directly into the same bounded encoder. Only one aligned stripe is buffered, independent of total object size.

## Strict and survival completion

- `CompletionPolicy::RequireAll` rejects the stream if any configured piece cannot finish. This is the normal fully protected commit boundary.
- `CompletionPolicy::AllowReconstructable` continues only while at least `K` piece sinks remain. The report explicitly states `reconstructable=true` and `fully_protected=false`; the caller decides whether its topology permits publishing that degraded object.

ENOSPC and EDQUOT remain distinguishable as `EcsErrorKind::CapacityExhausted`, including the raw operating-system error and piece index.

## Decode and repair

`ErasureStreamDecoder` streams the exact unpadded source length into one destination. `ErasureStreamRepairer` reconstructs one stable piece index and works for both data and parity. Both accept any reader type implementing Tokio `AsyncRead`, so a piece may come from a local disk, a remote node, memory, or another storage layer.

The decoder can validate an expected source checksum. Per-piece and per-block checksums are emitted in `EncodeReport`; storage engines should persist them in their own metadata and reject corrupt inputs before decode or repair. A decoded or repaired temporary file must never be published until the caller completes its durability and checksum checks.

## Operational rules

1. Declare the source length before encoding.
2. Never change `K`, `M`, block size, or piece index ordering within an object version.
3. Write pieces to temporary destinations.
4. Treat `finish()` as EC completion, not durable publication.
5. Sync pieces and parent directories according to the engine's durability contract.
6. Commit object metadata only after the required completion policy is satisfied.
7. Do not publish partial output after an error or cancelled future.

## Verification

```bash
cargo fmt --all -- --check
cargo check -p ecs-core --all-targets
cargo test -p ecs-core
cargo clippy -p ecs-core --all-targets --no-deps -- -D warnings
cargo bench -p ecs-core --bench streaming
```

See [ARCHITECTURE.md](ARCHITECTURE.md) and [TESTING.md](TESTING.md) for invariants and coverage.

## Licensing

The ECS Core wrapper is Apache-2.0. The preserved SIMD codec includes MIT and BSD-3-Clause code. Its complete license is retained at [LICENSES/SIMD_ERASURE_CORE_LICENSE](LICENSES/SIMD_ERASURE_CORE_LICENSE).
