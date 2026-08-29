# ECS Core Testing

The test suite proves the library boundary independently from N1 and N2.

## Automated coverage

- CRC32 and SHA-256 canonical vectors;
- arbitrary client chunk boundaries across multiple stripes;
- exact final-stripe zero padding and source-length trimming;
- zero-length streams;
- byte-for-byte parity compatibility with the preserved SIMD codec;
- reconstruction under every zero-, one-, and two-piece loss pattern for `4+2`;
- exact repair of every data and parity index for `3+2`;
- strict source-length enforcement;
- strict ENOSPC propagation with piece index and raw OS code;
- reconstructable completion after data and parity writer failures;
- mandatory rejection after live sinks fall below `K`.

## Explicit hard tests

The ignored hard suite is deterministic and must be invoked intentionally:

```bash
cargo test --release -p ecs-core --test hard_stress -- --ignored --nocapture
```

It covers exhaustive legal loss and repair combinations, 1,000 randomized layout/chunk/loss cases, later-stripe reader and writer failures, flush failures, destination failures, 128 concurrent independent object streams, and a synthetic 4 GiB input whose sinks retain only byte counters.

Nightly diagnostics can be run without changing the project's stable toolchain:

```bash
cargo +nightly miri test -p ecs-core --lib buffer::tests

RUSTFLAGS='-Zsanitizer=address' \
ASAN_OPTIONS='detect_leaks=1:halt_on_error=1' \
cargo +nightly test -p ecs-core --target x86_64-unknown-linux-gnu --lib --tests

RUSTFLAGS='-Zsanitizer=thread' \
TSAN_OPTIONS='halt_on_error=1' \
cargo +nightly test -Zbuild-std -p ecs-core \
  --target x86_64-unknown-linux-gnu \
  --test hard_stress concurrent_128_independent_streams -- --ignored
```

## Required release checks

```bash
cargo fmt --all -- --check
cargo check -p ecs-core --all-targets
cargo test -p ecs-core
cargo clippy -p ecs-core --all-targets --no-deps -- -D warnings
cargo test --manifest-path vendor/simd-erasure-core/Cargo.toml
cargo bench -p ecs-core --bench streaming
```

The vendored codec is kept byte-identical to its N1/N2 source. Its own upstream test suite is run separately because its historical lint policy is independent from the ECS wrapper.

## Explicit real-disk gate

The ignored real-disk test refuses to run unless exactly four caller-owned
paths on four distinct devices are supplied. It creates unique temporary
subdirectories, encodes a strict `3+1` stream, syncs and atomically publishes
all pieces, decodes with every single missing-piece position, repairs every
data and parity position, and removes its run directories afterward.

```bash
ECS_CORE_REAL_ROOTS=/test/disk-a,/test/disk-b,/test/disk-c,/test/disk-d \
ECS_CORE_REAL_BYTES=4294967296 \
cargo test --release --test real_disk \
  four_disk_encode_decode_repair_and_publish -- --ignored --nocapture
```

The paths must be dedicated test directories. ECS Core never selects or
formats the underlying devices.

## Engine integration gates

Before N1 or N2 switches to ECS Core, each engine must additionally prove:

1. old and new encoders produce identical piece bytes for golden objects;
2. existing object versions remain readable and repairable;
3. temporary pieces are removed after cancellation and all injected failures;
4. no namespace or metadata publication occurs before sync and EC completion;
5. the engine's exact degraded-write policy matches `EncodeReport`;
6. local-disk and remote-node throughput do not regress from the current implementation.

Passing ECS Core tests does not by itself authorize an engine migration.
