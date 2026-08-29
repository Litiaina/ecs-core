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

## Four-disk XFS gate

Measured on 2026-08-28 with a generated 4 GiB source, `3+1`, 1 MiB blocks,
and four distinct XFS HDDs. The test used ordinary Tokio files; the durable
measurement includes piece flush, `sync_all`, atomic rename, and parent
directory sync. It did not include HTTP, TLS, namespace, or NDX publication.

| Operation | Position | Throughput |
| --- | ---: | ---: |
| Encode pipeline before durability sync | all | 537.34 MiB/s |
| Encode durable publication | all | 205.74 MiB/s |
| Degraded decode | missing data 0 | 314.66 MiB/s |
| Degraded decode | missing data 1 | 261.91 MiB/s |
| Degraded decode | missing data 2 | 299.14 MiB/s |
| Degraded decode | missing parity 0 | 295.28 MiB/s |
| Repair pipeline | data 0 | 117.50 MiB/s |
| Repair pipeline | data 1 | 111.06 MiB/s |
| Repair pipeline | data 2 | 97.51 MiB/s |
| Repair pipeline | parity 0 | 110.62 MiB/s |
| Repair durable publication | data 0 | 59.24 MiB/s |
| Repair durable publication | data 1 | 62.76 MiB/s |
| Repair durable publication | data 2 | 62.56 MiB/s |
| Repair durable publication | parity 0 | 63.23 MiB/s |

The complete gate passed in 169.13 seconds, including all decode and repair
positions plus cleanup. These values describe this hardware run, not an ECS
Core throughput ceiling.

## Large bounded stream

The explicit hard suite generated a 4 GiB source without allocating the source object and encoded it as `4+2` using 1 MiB blocks and counting sinks:

- source throughput: 1,233.19 MiB/s;
- stripes: 1,024;
- measured codec CPU time: 715.24 ms;
- process peak RSS: 23,196 KiB;
- emitted piece data: 6 GiB total across six 1 GiB pieces.

This proves memory remains independent of object size for this path. Counting sinks still exclude disk, network, and durability costs.
