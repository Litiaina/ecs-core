use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ecs_core::{ChecksumPolicy, EcLayout, EncodeOptions, ErasureStreamEncoder, ShardWriters};
use tokio::io::AsyncWrite;

#[derive(Default)]
struct CountingSink(u64);

impl AsyncWrite for CountingSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.0 += bytes.len() as u64;
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn encode_benchmarks(criterion: &mut Criterion) {
    const SOURCE_BYTES: usize = 32 * 1024 * 1024;
    const BLOCK_BYTES: usize = 1024 * 1024;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let source = vec![0xA5; SOURCE_BYTES];
    let mut group = criterion.benchmark_group("stream_encode");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Bytes(SOURCE_BYTES as u64));

    for (data, parity) in [(3, 1), (6, 2)] {
        let layout = EcLayout::new(data, parity, BLOCK_BYTES).unwrap();
        for (name, checksum) in [
            ("codec", None),
            (
                "sha256_blocks",
                Some(ChecksumPolicy::sha256_with_blocks(true)),
            ),
        ] {
            group.bench_with_input(
                BenchmarkId::new(format!("{data}+{parity}"), name),
                &checksum,
                |bencher, checksum| {
                    bencher.iter(|| {
                        runtime.block_on(async {
                            let writers = ShardWriters::new(
                                (0..data).map(|_| Some(CountingSink::default())).collect(),
                                (0..parity).map(|_| Some(CountingSink::default())).collect(),
                            );
                            let options = EncodeOptions {
                                checksum: *checksum,
                                ..EncodeOptions::default()
                            };
                            let mut encoder = ErasureStreamEncoder::new(
                                layout,
                                writers,
                                SOURCE_BYTES as u64,
                                options,
                            )
                            .unwrap();
                            encoder.write(black_box(&source)).await.unwrap();
                            black_box(encoder.finish().await.unwrap());
                        });
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, encode_benchmarks);
criterion_main!(benches);
