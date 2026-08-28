# ECS Core Benchmark

Measured on 2026-08-28 with a 32 MiB source, 1 MiB blocks, release mode, and in-memory counting sinks.

These results isolate the ECS CPU, checksum, and streaming-control path. They do **not** include filesystem, disk, network, TLS, metadata, `fsync`, or publication cost and must not be presented as end-to-end N1 or N2 throughput.

| Layout | Integrity policy | Median throughput | Median time |
| --- | --- | ---: | ---: |
| 3+1 | codec only | 3.94 GiB/s | 7.93 ms |
| 6+2 | codec only | 2.22 GiB/s | 14.09 ms |
| 3+1 | SHA-256 source + piece + blocks | 130.21 MiB/s | 245.76 ms |
| 6+2 | SHA-256 source + piece + blocks | 122.02 MiB/s | 262.25 ms |

Restoring N2's nested parallel checksum technique improved the measured `3+1` full-integrity path by approximately 19%. Criterion found no statistically significant change for `6+2` on this machine.

Run the benchmark with:

```bash
cargo bench -p ecs-core --bench streaming
```

Real engine integration must separately measure local disks, remote nodes, concurrency, tail latency, `fsync`, cancellation cleanup, and metadata commit.

## Large bounded stream

The explicit hard suite generated a 4 GiB source without allocating the source object and encoded it as `4+2` using 1 MiB blocks and counting sinks:

- source throughput: 1,233.19 MiB/s;
- stripes: 1,024;
- measured codec CPU time: 715.24 ms;
- process peak RSS: 23,196 KiB;
- emitted piece data: 6 GiB total across six 1 GiB pieces.

This proves memory remains independent of object size for this path. Counting sinks still exclude disk, network, and durability costs.
