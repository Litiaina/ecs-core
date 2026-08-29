use std::env;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ecs_core::{
    ChecksumAlgorithm, ChecksumPolicy, CompletionPolicy, DecodeOptions, EcLayout, EncodeOptions,
    ErasureStreamDecoder, ErasureStreamEncoder, ErasureStreamRepairer, ShardReaders, ShardWriters,
};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const DATA_SHARDS: usize = 3;
const PARITY_SHARDS: usize = 1;
const BLOCK_SIZE: usize = 1024 * 1024;
const DEFAULT_SOURCE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug)]
struct PatternReader {
    remaining: u64,
    position: u64,
}

impl PatternReader {
    fn new(bytes: u64) -> Self {
        Self {
            remaining: bytes,
            position: 0,
        }
    }
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
            *byte = pattern_byte(position + offset as u64);
        }
        output.advance(count);
        self.position += count as u64;
        self.remaining -= count as u64;
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Default)]
struct PatternVerifier {
    position: u64,
}

impl AsyncWrite for PatternVerifier {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        for (offset, actual) in bytes.iter().enumerate() {
            let position = self.position + offset as u64;
            let expected = pattern_byte(position);
            if *actual != expected {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "decoded byte mismatch at {position}: expected {expected}, got {actual}"
                    ),
                )));
            }
        }
        self.position += bytes.len() as u64;
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn pattern_byte(position: u64) -> u8 {
    (position as u8).wrapping_mul(31).wrapping_add(17)
}

fn configured_roots() -> Vec<PathBuf> {
    env::var_os("ECS_CORE_REAL_ROOTS")
        .expect("ECS_CORE_REAL_ROOTS must contain four comma-separated, dedicated test paths")
        .to_string_lossy()
        .split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .collect()
}

fn configured_source_bytes() -> u64 {
    env::var("ECS_CORE_REAL_BYTES")
        .ok()
        .map(|value| value.parse::<u64>().expect("invalid ECS_CORE_REAL_BYTES"))
        .unwrap_or(DEFAULT_SOURCE_BYTES)
}

fn throughput_mib_s(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::EPSILON)
}

async fn sync_directory(path: &Path) -> io::Result<()> {
    OpenOptions::new()
        .read(true)
        .open(path)
        .await?
        .sync_all()
        .await
}

fn discard_cached_pages(file: &File) -> io::Result<()> {
    let result = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

async fn open_piece_readers(piece_paths: &[PathBuf], missing: usize) -> ShardReaders<File> {
    let mut readers = Vec::with_capacity(piece_paths.len());
    for (index, path) in piece_paths.iter().enumerate() {
        if index == missing {
            readers.push(None);
        } else {
            let file = File::open(path).await.unwrap();
            discard_cached_pages(&file).unwrap();
            readers.push(Some(file));
        }
    }
    let parity = readers.split_off(DATA_SHARDS);
    ShardReaders::new(readers, parity)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires four explicitly supplied disposable paths on distinct real disks"]
async fn four_disk_encode_decode_repair_and_publish() {
    let roots = configured_roots();
    assert_eq!(
        roots.len(),
        DATA_SHARDS + PARITY_SHARDS,
        "the 3+1 hardware gate requires exactly four roots"
    );

    let mut devices = Vec::with_capacity(roots.len());
    for root in &roots {
        let metadata = fs::metadata(root)
            .await
            .unwrap_or_else(|error| panic!("test root {} is unavailable: {error}", root.display()));
        assert!(metadata.is_dir(), "{} is not a directory", root.display());
        devices.push(metadata.dev());
    }
    devices.sort_unstable();
    devices.dedup();
    assert_eq!(
        devices.len(),
        roots.len(),
        "every ECS_CORE_REAL_ROOTS entry must reside on a distinct device"
    );

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let run_dirs = roots
        .iter()
        .map(|root| root.join(format!("ecs-core-real-{run_id}")))
        .collect::<Vec<_>>();
    for run_dir in &run_dirs {
        fs::create_dir(run_dir).await.unwrap();
        sync_directory(run_dir.parent().unwrap()).await.unwrap();
    }

    let result = async {
        let source_bytes = configured_source_bytes();
        let layout = EcLayout::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE).unwrap();
        let temporary_paths = run_dirs
            .iter()
            .enumerate()
            .map(|(index, root)| root.join(format!("piece-{index}.tmp")))
            .collect::<Vec<_>>();
        let stable_paths = run_dirs
            .iter()
            .enumerate()
            .map(|(index, root)| root.join(format!("piece-{index}.ec")))
            .collect::<Vec<_>>();

        let mut writer_slots = Vec::with_capacity(layout.total_shards());
        for path in &temporary_paths {
            writer_slots.push(Some(File::create(path).await.unwrap()));
        }
        let parity_writers = writer_slots.split_off(DATA_SHARDS);
        let writers = ShardWriters::new(writer_slots, parity_writers);
        let mut source = PatternReader::new(source_bytes);
        let encode_started = Instant::now();
        let mut encoder = ErasureStreamEncoder::new(
            layout,
            writers,
            source_bytes,
            EncodeOptions {
                completion: CompletionPolicy::RequireAll,
                checksum: Some(ChecksumPolicy::crc32(true)),
                expected_source_checksum: None,
            },
        )
        .unwrap();
        encoder.copy_exact(&mut source).await.unwrap();
        let output = encoder.finish().await.unwrap();
        let pipeline_elapsed = encode_started.elapsed();
        assert!(output.report.fully_protected);

        let mut completed_writers = output
            .writers
            .data
            .into_iter()
            .chain(output.writers.parity)
            .collect::<Vec<_>>();
        for writer in completed_writers.iter_mut().flatten() {
            writer.sync_all().await.unwrap();
        }
        drop(completed_writers);
        for (temporary, stable) in temporary_paths.iter().zip(&stable_paths) {
            fs::rename(temporary, stable).await.unwrap();
        }
        for run_dir in &run_dirs {
            sync_directory(run_dir).await.unwrap();
        }
        let durable_elapsed = encode_started.elapsed();

        let expected_piece_bytes = layout.shard_bytes(source_bytes).unwrap();
        for path in &stable_paths {
            assert_eq!(
                fs::metadata(path).await.unwrap().len(),
                expected_piece_bytes
            );
        }

        println!(
            "REAL_DISK encode_pipeline source_bytes={} elapsed_ms={:.3} source_mib_s={:.2}",
            source_bytes,
            pipeline_elapsed.as_secs_f64() * 1000.0,
            throughput_mib_s(source_bytes, pipeline_elapsed)
        );
        println!(
            "REAL_DISK encode_durable source_bytes={} elapsed_ms={:.3} source_mib_s={:.2}",
            source_bytes,
            durable_elapsed.as_secs_f64() * 1000.0,
            throughput_mib_s(source_bytes, durable_elapsed)
        );

        for missing in 0..layout.total_shards() {
            let readers = open_piece_readers(&stable_paths, missing).await;
            let decode_started = Instant::now();
            let decoded = ErasureStreamDecoder::new(
                layout,
                readers,
                PatternVerifier::default(),
                source_bytes,
                DecodeOptions {
                    source_checksum: Some(ChecksumAlgorithm::Crc32),
                    expected_source_checksum: output.report.source_checksum.clone(),
                },
            )
            .unwrap()
            .decode()
            .await
            .unwrap();
            let decode_elapsed = decode_started.elapsed();
            assert_eq!(decoded.destination.position, source_bytes);
            println!(
                "REAL_DISK decode missing={} elapsed_ms={:.3} source_mib_s={:.2}",
                missing,
                decode_elapsed.as_secs_f64() * 1000.0,
                throughput_mib_s(source_bytes, decode_elapsed)
            );
        }

        for (repair_index, repair_root) in run_dirs.iter().enumerate() {
            let readers = open_piece_readers(&stable_paths, repair_index).await;
            let repair_temporary = repair_root.join("repaired.tmp");
            let repair_stable = repair_root.join("repaired.ec");
            let destination = File::create(&repair_temporary).await.unwrap();
            let repair_started = Instant::now();
            let repaired = ErasureStreamRepairer::new(
                layout,
                readers,
                destination,
                source_bytes,
                repair_index,
                Some(ChecksumAlgorithm::Crc32),
            )
            .unwrap()
            .repair()
            .await
            .unwrap();
            let repair_pipeline_elapsed = repair_started.elapsed();
            assert_eq!(
                repaired.report.checksum, output.report.pieces[repair_index].checksum,
                "repair checksum differs for piece {repair_index}"
            );
            repaired.destination.sync_all().await.unwrap();
            drop(repaired);
            fs::rename(&repair_temporary, &repair_stable).await.unwrap();
            sync_directory(repair_root).await.unwrap();
            let repair_durable_elapsed = repair_started.elapsed();
            assert_eq!(
                fs::metadata(&repair_stable).await.unwrap().len(),
                expected_piece_bytes
            );
            println!(
                "REAL_DISK repair_pipeline piece={} elapsed_ms={:.3} piece_mib_s={:.2}",
                repair_index,
                repair_pipeline_elapsed.as_secs_f64() * 1000.0,
                throughput_mib_s(expected_piece_bytes, repair_pipeline_elapsed)
            );
            println!(
                "REAL_DISK repair_durable piece={} elapsed_ms={:.3} piece_mib_s={:.2}",
                repair_index,
                repair_durable_elapsed.as_secs_f64() * 1000.0,
                throughput_mib_s(expected_piece_bytes, repair_durable_elapsed)
            );
            fs::remove_file(repair_stable).await.unwrap();
        }

        Ok::<(), io::Error>(())
    }
    .await;

    for run_dir in &run_dirs {
        if let Err(error) = fs::remove_dir_all(run_dir).await {
            eprintln!(
                "failed to clean hardware test directory {}: {error}",
                run_dir.display()
            );
        } else if let Some(parent) = run_dir.parent() {
            let _ = sync_directory(parent).await;
        }
    }
    result.unwrap();
}
