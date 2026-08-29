mod support;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use ecs_core::{
    ChecksumAlgorithm, CompletionPolicy, DecodeOptions, EcLayout, EcsErrorKind, EncodeOptions,
    ErasureStreamDecoder, ErasureStreamEncoder, ErasureStreamRepairer, ShardReaders, ShardWriters,
    metrics_snapshot,
};
use rand::seq::SliceRandom;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use support::MemoryWriter;

#[derive(Debug)]
struct FaultReader {
    bytes: Vec<u8>,
    position: usize,
    fail_after: Option<usize>,
    raw_os_error: i32,
}

impl FaultReader {
    fn healthy(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            position: 0,
            fail_after: None,
            raw_os_error: libc::EIO,
        }
    }

    fn failing(bytes: Vec<u8>, fail_after: usize, raw_os_error: i32) -> Self {
        Self {
            bytes,
            position: 0,
            fail_after: Some(fail_after),
            raw_os_error,
        }
    }
}

impl AsyncRead for FaultReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self
            .fail_after
            .is_some_and(|fail_after| self.position >= fail_after)
        {
            return Poll::Ready(Err(io::Error::from_raw_os_error(self.raw_os_error)));
        }
        if self.position == self.bytes.len() || output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let before_failure = self
            .fail_after
            .map_or(usize::MAX, |fail_after| fail_after - self.position);
        let count = output
            .remaining()
            .min(self.bytes.len() - self.position)
            .min(before_failure);
        output.put_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Default)]
struct FaultWriter {
    bytes: Vec<u8>,
    fail_after: Option<usize>,
    write_error: i32,
    flush_error: Option<i32>,
}

impl FaultWriter {
    fn failing_write(after: usize, raw_os_error: i32) -> Self {
        Self {
            fail_after: Some(after),
            write_error: raw_os_error,
            ..Self::default()
        }
    }

    fn failing_flush(raw_os_error: i32) -> Self {
        Self {
            write_error: libc::EIO,
            flush_error: Some(raw_os_error),
            ..Self::default()
        }
    }
}

impl AsyncWrite for FaultWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(fail_after) = self.fail_after {
            if self.bytes.len() >= fail_after {
                return Poll::Ready(Err(io::Error::from_raw_os_error(self.write_error)));
            }
            let count = bytes.len().min(fail_after - self.bytes.len());
            self.bytes.extend_from_slice(&bytes[..count]);
            return Poll::Ready(Ok(count));
        }
        self.bytes.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(raw_os_error) = self.flush_error {
            Poll::Ready(Err(io::Error::from_raw_os_error(raw_os_error)))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct PatternReader {
    remaining: u64,
    position: u64,
}

impl AsyncRead for PatternReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let count = output
            .remaining()
            .min(self.remaining.min(usize::MAX as u64) as usize);
        if count == 0 {
            return Poll::Ready(Ok(()));
        }
        let position = self.position;
        let bytes = output.initialize_unfilled_to(count);
        for (offset, byte) in bytes.iter_mut().enumerate() {
            *byte = (position.wrapping_add(offset as u64) as u8).wrapping_mul(31);
        }
        output.advance(count);
        self.position += count as u64;
        self.remaining -= count as u64;
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Default)]
struct CountingWriter(u64);

impl AsyncWrite for CountingWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0 += bytes.len() as u64;
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn peak_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or(0)
}

async fn encode_memory(
    layout: EcLayout,
    source: &[u8],
    random: &mut ChaCha20Rng,
) -> (ecs_core::EncodeReport, Vec<Vec<u8>>) {
    let writers = ShardWriters::new(
        (0..layout.data_shards())
            .map(|_| Some(MemoryWriter::default()))
            .collect(),
        (0..layout.parity_shards())
            .map(|_| Some(MemoryWriter::default()))
            .collect(),
    );
    let mut encoder = ErasureStreamEncoder::new(
        layout,
        writers,
        source.len() as u64,
        EncodeOptions::default(),
    )
    .unwrap();
    let mut position = 0;
    while position < source.len() {
        let maximum = (source.len() - position).min(layout.stripe_size() + 101);
        let count = random.random_range(1..=maximum);
        encoder
            .write(&source[position..position + count])
            .await
            .unwrap();
        position += count;
    }
    let output = encoder.finish().await.unwrap();
    let pieces = output
        .writers
        .data
        .into_iter()
        .chain(output.writers.parity)
        .map(|writer| writer.unwrap().into_inner())
        .collect();
    (output.report, pieces)
}

async fn decode_memory(
    layout: EcLayout,
    source_bytes: u64,
    pieces: &[Vec<u8>],
    missing: &[usize],
    expected_source_checksum: Option<ecs_core::Digest>,
) -> Vec<u8> {
    let readers = pieces
        .iter()
        .enumerate()
        .map(|(index, piece)| {
            (!missing.contains(&index)).then(|| FaultReader::healthy(piece.clone()))
        })
        .collect::<Vec<_>>();
    let readers = ShardReaders::new(
        readers[..layout.data_shards()]
            .iter()
            .map(|reader| {
                reader.as_ref().map(|reader| FaultReader {
                    bytes: reader.bytes.clone(),
                    position: 0,
                    fail_after: reader.fail_after,
                    raw_os_error: reader.raw_os_error,
                })
            })
            .collect(),
        readers[layout.data_shards()..]
            .iter()
            .map(|reader| {
                reader.as_ref().map(|reader| FaultReader {
                    bytes: reader.bytes.clone(),
                    position: 0,
                    fail_after: reader.fail_after,
                    raw_os_error: reader.raw_os_error,
                })
            })
            .collect(),
    );
    let decoder = ErasureStreamDecoder::new(
        layout,
        readers,
        MemoryWriter::default(),
        source_bytes,
        DecodeOptions {
            source_checksum: expected_source_checksum
                .as_ref()
                .map(ecs_core::Digest::algorithm),
            expected_source_checksum,
        },
    )
    .unwrap();
    decoder.decode().await.unwrap().destination.into_inner()
}

fn combinations(values: &[usize], count: usize) -> Vec<Vec<usize>> {
    fn visit(
        values: &[usize],
        count: usize,
        start: usize,
        current: &mut Vec<usize>,
        output: &mut Vec<Vec<usize>>,
    ) {
        if current.len() == count {
            output.push(current.clone());
            return;
        }
        for index in start..values.len() {
            current.push(values[index]);
            visit(values, count, index + 1, current, output);
            current.pop();
        }
    }

    let mut output = Vec::new();
    visit(values, count, 0, &mut Vec::new(), &mut output);
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "hard ECS exhaustive matrix"]
async fn exhaustive_loss_and_repair_matrix() {
    let mut random = ChaCha20Rng::seed_from_u64(0xECA5_0001);
    let mut decode_cases = 0usize;
    let mut repair_cases = 0usize;

    for (data, parity) in [(1, 1), (2, 1), (3, 2), (4, 3), (6, 2)] {
        let layout = EcLayout::new(data, parity, 4096).unwrap();
        let mut source = vec![0u8; layout.stripe_size() * 2 + 137];
        random.fill_bytes(&mut source);
        let (report, pieces) = encode_memory(layout, &source, &mut random).await;
        let indexes = (0..layout.total_shards()).collect::<Vec<_>>();
        for loss_count in 0..=parity {
            for missing in combinations(&indexes, loss_count) {
                let decoded = decode_memory(
                    layout,
                    source.len() as u64,
                    &pieces,
                    &missing,
                    report.source_checksum.clone(),
                )
                .await;
                assert_eq!(decoded, source, "decode {data}+{parity}, loss {missing:?}");
                decode_cases += 1;
            }
        }

        if parity > 0 && data <= 4 {
            for target in 0..layout.total_shards() {
                let others = indexes
                    .iter()
                    .copied()
                    .filter(|index| *index != target)
                    .collect::<Vec<_>>();
                for additional_count in 0..parity {
                    for additional_missing in combinations(&others, additional_count) {
                        let readers = pieces
                            .iter()
                            .enumerate()
                            .map(|(index, piece)| {
                                (index != target && !additional_missing.contains(&index))
                                    .then(|| FaultReader::healthy(piece.clone()))
                            })
                            .collect::<Vec<_>>();
                        let readers = ShardReaders::new(
                            readers[..data]
                                .iter()
                                .map(|reader| {
                                    reader
                                        .as_ref()
                                        .map(|reader| FaultReader::healthy(reader.bytes.clone()))
                                })
                                .collect(),
                            readers[data..]
                                .iter()
                                .map(|reader| {
                                    reader
                                        .as_ref()
                                        .map(|reader| FaultReader::healthy(reader.bytes.clone()))
                                })
                                .collect(),
                        );
                        let repairer = ErasureStreamRepairer::new(
                            layout,
                            readers,
                            MemoryWriter::default(),
                            source.len() as u64,
                            target,
                            Some(ChecksumAlgorithm::Sha256),
                        )
                        .unwrap();
                        let repaired = repairer.repair().await.unwrap();
                        assert_eq!(
                            repaired.destination.into_inner(),
                            pieces[target],
                            "repair {data}+{parity}, target {target}, extra {additional_missing:?}"
                        );
                        repair_cases += 1;
                    }
                }
            }
        }
    }
    eprintln!("exhaustive matrix: {decode_cases} decode, {repair_cases} repair cases");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "hard ECS randomized matrix"]
async fn randomized_1000_case_roundtrip_matrix() {
    let mut random = ChaCha20Rng::seed_from_u64(0xECA5_0002);
    let layouts = [(1, 1), (2, 1), (3, 2), (4, 2), (4, 3), (6, 2), (8, 4)];
    for case in 0..1000usize {
        let (data, parity) = layouts[random.random_range(0..layouts.len())];
        let block_size = [4096, 8192, 65_536][random.random_range(0..3)];
        let layout = EcLayout::new(data, parity, block_size).unwrap();
        let length = random.random_range(0..=layout.stripe_size() * 3 + 211);
        let mut source = vec![0u8; length];
        random.fill_bytes(&mut source);
        let (report, pieces) = encode_memory(layout, &source, &mut random).await;
        let loss_count = random.random_range(0..=parity);
        let mut indexes = (0..layout.total_shards()).collect::<Vec<_>>();
        indexes.shuffle(&mut random);
        indexes.truncate(loss_count);
        let decoded = decode_memory(
            layout,
            length as u64,
            &pieces,
            &indexes,
            report.source_checksum,
        )
        .await;
        assert_eq!(decoded, source, "random case {case}, {data}+{parity}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "hard ECS injected I/O faults"]
async fn midstream_and_flush_faults_preserve_correctness() {
    let layout = EcLayout::new(3, 2, 4096).unwrap();
    let mut source = vec![0u8; layout.stripe_size() * 4 + 77];
    ChaCha20Rng::seed_from_u64(0xECA5_0003).fill_bytes(&mut source);

    let writers = ShardWriters::new(
        vec![
            Some(FaultWriter::failing_write(4096 + 113, libc::EIO)),
            Some(FaultWriter::default()),
            Some(FaultWriter::default()),
        ],
        vec![
            Some(FaultWriter::default()),
            Some(FaultWriter::failing_flush(libc::EIO)),
        ],
    );
    let options = EncodeOptions {
        completion: CompletionPolicy::AllowReconstructable,
        ..EncodeOptions::default()
    };
    let mut encoder =
        ErasureStreamEncoder::new(layout, writers, source.len() as u64, options).unwrap();
    encoder.write(&source).await.unwrap();
    let output = encoder.finish().await.unwrap();
    assert!(output.report.reconstructable);
    assert!(!output.report.fully_protected);
    let pieces = output
        .writers
        .data
        .into_iter()
        .chain(output.writers.parity)
        .map(|writer| writer.map(|writer| writer.bytes))
        .collect::<Vec<_>>();
    assert!(pieces[0].is_none());
    assert!(pieces[4].is_none());
    let readers = pieces
        .into_iter()
        .map(|piece| piece.map(FaultReader::healthy))
        .collect::<Vec<_>>();
    let readers = ShardReaders::new(
        readers[..layout.data_shards()]
            .iter()
            .map(|reader| {
                reader
                    .as_ref()
                    .map(|reader| FaultReader::healthy(reader.bytes.clone()))
            })
            .collect(),
        readers[layout.data_shards()..]
            .iter()
            .map(|reader| {
                reader
                    .as_ref()
                    .map(|reader| FaultReader::healthy(reader.bytes.clone()))
            })
            .collect(),
    );
    let decoder = ErasureStreamDecoder::new(
        layout,
        readers,
        MemoryWriter::default(),
        source.len() as u64,
        DecodeOptions {
            source_checksum: Some(ChecksumAlgorithm::Sha256),
            expected_source_checksum: output.report.source_checksum,
        },
    )
    .unwrap();
    assert_eq!(
        decoder.decode().await.unwrap().destination.into_inner(),
        source
    );

    let mut random = ChaCha20Rng::seed_from_u64(0xECA5_0004);
    let (report, pieces) = encode_memory(layout, &source, &mut random).await;
    let mut readers = pieces
        .iter()
        .cloned()
        .map(FaultReader::healthy)
        .map(Some)
        .collect::<Vec<_>>();
    readers[1] = Some(FaultReader::failing(
        pieces[1].clone(),
        layout.block_size() + 91,
        libc::EIO,
    ));
    let readers = ShardReaders::new(readers.drain(..layout.data_shards()).collect(), readers);
    let decoder = ErasureStreamDecoder::new(
        layout,
        readers,
        MemoryWriter::default(),
        source.len() as u64,
        DecodeOptions {
            source_checksum: Some(ChecksumAlgorithm::Sha256),
            expected_source_checksum: report.source_checksum,
        },
    )
    .unwrap();
    let decoded = decoder.decode().await.unwrap();
    assert_eq!(decoded.destination.into_inner(), source);
    assert_eq!(decoded.report.failed_readers, vec![1]);

    let readers = pieces
        .iter()
        .enumerate()
        .map(|(index, piece)| match index {
            0 => None,
            1 => Some(FaultReader::failing(
                piece.clone(),
                layout.block_size() + 17,
                libc::EIO,
            )),
            _ => Some(FaultReader::healthy(piece.clone())),
        })
        .collect::<Vec<_>>();
    let readers = ShardReaders::new(
        readers[..layout.data_shards()]
            .iter()
            .map(|reader| {
                reader.as_ref().map(|reader| FaultReader {
                    bytes: reader.bytes.clone(),
                    position: 0,
                    fail_after: reader.fail_after,
                    raw_os_error: reader.raw_os_error,
                })
            })
            .collect(),
        readers[layout.data_shards()..]
            .iter()
            .map(|reader| {
                reader.as_ref().map(|reader| FaultReader {
                    bytes: reader.bytes.clone(),
                    position: 0,
                    fail_after: reader.fail_after,
                    raw_os_error: reader.raw_os_error,
                })
            })
            .collect(),
    );
    let repairer = ErasureStreamRepairer::new(
        layout,
        readers,
        MemoryWriter::default(),
        source.len() as u64,
        0,
        Some(ChecksumAlgorithm::Sha256),
    )
    .unwrap();
    assert_eq!(
        repairer.repair().await.unwrap().destination.into_inner(),
        pieces[0]
    );

    let readers = pieces
        .iter()
        .cloned()
        .map(FaultReader::healthy)
        .map(Some)
        .collect::<Vec<_>>();
    let readers = ShardReaders::new(
        readers[..layout.data_shards()]
            .iter()
            .map(|reader| {
                reader
                    .as_ref()
                    .map(|reader| FaultReader::healthy(reader.bytes.clone()))
            })
            .collect(),
        readers[layout.data_shards()..]
            .iter()
            .map(|reader| {
                reader
                    .as_ref()
                    .map(|reader| FaultReader::healthy(reader.bytes.clone()))
            })
            .collect(),
    );
    let decoder = ErasureStreamDecoder::new(
        layout,
        readers,
        FaultWriter::failing_write(17, libc::EIO),
        source.len() as u64,
        DecodeOptions::default(),
    )
    .unwrap();
    let error = match decoder.decode().await {
        Ok(_) => panic!("failing destination unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), EcsErrorKind::WriterFailure);
    assert_eq!(error.raw_os_error(), Some(libc::EIO));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "hard ECS concurrent streams"]
async fn concurrent_128_independent_streams() {
    let mut tasks = Vec::new();
    for task_id in 0..128u64 {
        tasks.push(tokio::spawn(async move {
            let layout = EcLayout::new(4, 2, 4096).unwrap();
            let mut random = ChaCha20Rng::seed_from_u64(0xECA5_1000 + task_id);
            let length = 65_537 + (task_id as usize * 97 % 8192);
            let mut source = vec![0u8; length];
            random.fill_bytes(&mut source);
            let (report, pieces) = encode_memory(layout, &source, &mut random).await;
            let missing = [task_id as usize % layout.total_shards()];
            let decoded = decode_memory(
                layout,
                source.len() as u64,
                &pieces,
                &missing,
                report.source_checksum,
            )
            .await;
            assert_eq!(decoded, source);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "hard ECS multi-gibibyte bounded-memory stream"]
async fn four_gibibyte_stream_is_bounded_by_one_stripe() {
    const SOURCE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
    let layout = EcLayout::new(4, 2, 1024 * 1024).unwrap();
    let writers = ShardWriters::new(
        (0..layout.data_shards())
            .map(|_| Some(CountingWriter::default()))
            .collect(),
        (0..layout.parity_shards())
            .map(|_| Some(CountingWriter::default()))
            .collect(),
    );
    let options = EncodeOptions {
        completion: CompletionPolicy::RequireAll,
        checksum: None,
        expected_source_checksum: None,
    };
    let mut encoder = ErasureStreamEncoder::new(layout, writers, SOURCE_BYTES, options).unwrap();
    let mut source = PatternReader {
        remaining: SOURCE_BYTES,
        position: 0,
    };
    let started = Instant::now();
    encoder.copy_exact(&mut source).await.unwrap();
    let output = encoder.finish().await.unwrap();
    let elapsed = started.elapsed();
    let expected_piece_bytes = layout.shard_bytes(SOURCE_BYTES).unwrap();
    for writer in output.writers.data.into_iter().chain(output.writers.parity) {
        assert_eq!(writer.unwrap().0, expected_piece_bytes);
    }
    assert_eq!(output.report.source_bytes, SOURCE_BYTES);
    assert_eq!(
        output.report.stripe_count,
        layout.stripe_count(SOURCE_BYTES)
    );
    let metrics = metrics_snapshot();
    assert!(metrics.source_bytes >= SOURCE_BYTES);
    assert!(metrics.stripes >= layout.stripe_count(SOURCE_BYTES));
    eprintln!(
        "4 GiB stream: {:.2} MiB/s, {} stripes, codec {:.2} ms, peak RSS {} KiB",
        SOURCE_BYTES as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64(),
        output.report.stripe_count,
        metrics.codec_nanoseconds as f64 / 1_000_000.0,
        peak_rss_kib(),
    );
}
