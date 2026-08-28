# ECS Core Architecture

## Purpose

ECS Core is a narrow data-plane library. It provides one implementation of streaming erasure coding for local and distributed Litiaina engines while leaving storage topology and publication semantics to those engines.

## Stable mapping

For `K+M`, every stripe has exactly `K+M` fixed piece indexes:

```text
0 .. K-1       data pieces
K .. K+M-1     parity pieces
```

An object version records this mapping once. Disks and nodes may differ between engines, but a piece index cannot change identity during encode, read, recovery, or repair.

## Streaming encode

The source is divided into stripes of `K * block_size`. ECS Core retains one 4096-byte-aligned stripe buffer:

```text
source chunks
    |
    v
[ data 0 | data 1 | ... | data K-1 ]
    |          SIMD Reed-Solomon
    +----------> parity 0 ... parity M-1
    |
    +---------- concurrent AsyncWrite to every available piece sink
```

The final partial stripe is zero padded. `source_bytes` in the caller's immutable metadata removes padding during decode. Memory is `O(K * block_size)` plus codec workspace and checksum state, never `O(object_size)`.

## Failure semantics

Writer failure is local to its piece slot. No mutex, global writer, placement database, or retry loop exists inside ECS Core.

- Fewer than `K` live sinks: always fail.
- At least `K`, but fewer than `K+M`: allowed only under `AllowReconstructable`.
- `RequireAll`: any failed or initially missing sink fails the operation.
- A failed piece never receives a complete checksum in the final report.
- A stripe failure poisons that encoder instance so a caller cannot accidentally duplicate partial output by continuing it.
- Normal I/O failures return `EcsError`; only impossible internal invariants may panic.

This supports N2 survivor writes without embedding N2 server-pool policy in the library.

## Checksums

ECS Core supports the two proven engine policies:

- CRC32 compatibility for existing N1-style records;
- SHA-256 source, full-piece, and per-block checksums for N2-style integrity.

Checksumming is observational and never changes piece placement. Large sets of block checksums are computed through Rayon using the same threshold as the N2 data plane.

## Decode

Decode reads available piece blocks concurrently. If all data blocks exist, they stream directly to the destination. Otherwise the decoder reconstructs missing data blocks from any valid `K` inputs and writes only the declared source length.

The destination is still temporary. Source-checksum validation happens before the caller may publish it.

## Repair

Repair targets exactly one stable piece index:

- missing data: decode that data block from surviving pieces;
- missing parity: recover any unavailable data needed for the stripe, then regenerate the requested parity block.

Repair remains bounded by one stripe and never moves or names the destination. N1 chooses a disk; N2 chooses a node and disk; ECS Core only supplies the rebuilt bytes.

## Concurrency

The library has no process-wide data lock and no single writer. Each encoder/decoder/repairer owns independent codec state. Within an encoder stripe, piece writes proceed concurrently. Multiple object streams are naturally concurrent under the caller's scheduler.

The global metrics use relaxed atomics and never gate I/O.

## Deliberately excluded

- object IDs and version IDs;
- fragment or bucket semantics;
- NCNX/NCDX/NDX encoding;
- server pools and reserve promotion;
- disk selection and capacity policy;
- direct-I/O flags and filesystem selection;
- temporary-file naming, `fsync`, and atomic publication;
- HTTP, NOA, `n1://`, or `n2://` protocols.

Keeping these out is what makes ECS Core reusable rather than a hidden copy of either engine.
