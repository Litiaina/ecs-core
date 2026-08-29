mod support;

use ecs_core::{
    CompletionPolicy, EcLayout, EcsErrorKind, EncodeOptions, ErasureStreamEncoder, PieceState,
    ShardWriters,
};
use support::MemoryWriter;

#[tokio::test]
async fn strict_mode_preserves_capacity_exhaustion() {
    let layout = EcLayout::new(2, 1, 4096).unwrap();
    let writers = ShardWriters::new(
        vec![
            Some(MemoryWriter::failing(0, libc::ENOSPC)),
            Some(MemoryWriter::default()),
        ],
        vec![Some(MemoryWriter::default())],
    );
    let mut encoder = ErasureStreamEncoder::new(
        layout,
        writers,
        layout.stripe_size() as u64,
        EncodeOptions::default(),
    )
    .unwrap();
    let error = encoder
        .write(&vec![7u8; layout.stripe_size()])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EcsErrorKind::CapacityExhausted);
    assert_eq!(error.shard_index(), Some(0));
    assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
}

#[tokio::test]
async fn strict_minimum_layout_preserves_capacity_exhaustion() {
    let layout = EcLayout::new(1, 0, 4096).unwrap();
    let writers = ShardWriters::new(vec![Some(MemoryWriter::failing(0, libc::ENOSPC))], vec![]);
    let mut encoder = ErasureStreamEncoder::new(
        layout,
        writers,
        layout.stripe_size() as u64,
        EncodeOptions::default(),
    )
    .unwrap();
    let error = encoder
        .write(&vec![7u8; layout.stripe_size()])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EcsErrorKind::CapacityExhausted);
    assert_eq!(error.shard_index(), Some(0));
    assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
}

#[tokio::test]
async fn degraded_mode_completes_only_while_k_survives() {
    let layout = EcLayout::new(3, 2, 4096).unwrap();
    let writers = ShardWriters::new(
        vec![
            Some(MemoryWriter::failing(0, libc::EDQUOT)),
            Some(MemoryWriter::default()),
            Some(MemoryWriter::default()),
        ],
        vec![
            Some(MemoryWriter::failing(0, libc::ENOSPC)),
            Some(MemoryWriter::default()),
        ],
    );
    let options = EncodeOptions {
        completion: CompletionPolicy::AllowReconstructable,
        ..EncodeOptions::default()
    };
    let mut encoder =
        ErasureStreamEncoder::new(layout, writers, layout.stripe_size() as u64, options).unwrap();
    encoder
        .write(&vec![3u8; layout.stripe_size()])
        .await
        .unwrap();
    let output = encoder.finish().await.unwrap();
    assert!(output.report.reconstructable);
    assert!(!output.report.fully_protected);
    assert_eq!(output.report.pieces[0].state, PieceState::Failed);
    assert_eq!(output.report.pieces[3].state, PieceState::Failed);
    assert!(output.report.pieces[0].checksum.is_some());
    assert_eq!(output.report.pieces[0].block_checksums.len(), 1);
    assert!(output.report.pieces[3].checksum.is_some());
    assert_eq!(output.report.pieces[3].block_checksums.len(), 1);
}

#[tokio::test]
async fn missing_piece_keeps_its_deterministic_repair_identity() {
    let layout = EcLayout::new(2, 1, 4096).unwrap();
    let writers = ShardWriters::new(
        vec![None, Some(MemoryWriter::default())],
        vec![Some(MemoryWriter::default())],
    );
    let options = EncodeOptions {
        completion: CompletionPolicy::AllowReconstructable,
        ..EncodeOptions::default()
    };
    let source = vec![9u8; layout.stripe_size() * 2 + 17];
    let mut encoder =
        ErasureStreamEncoder::new(layout, writers, source.len() as u64, options).unwrap();
    encoder.write(&source).await.unwrap();
    let output = encoder.finish().await.unwrap();

    let missing = &output.report.pieces[0];
    assert_eq!(missing.state, PieceState::Missing);
    assert_eq!(missing.bytes_written, 0);
    assert!(missing.checksum.is_some());
    assert_eq!(
        missing.block_checksums.len(),
        output.report.stripe_count as usize
    );
}

#[tokio::test]
async fn degraded_mode_rejects_the_stripe_when_fewer_than_k_remain() {
    let layout = EcLayout::new(3, 1, 4096).unwrap();
    let writers = ShardWriters::new(
        vec![
            Some(MemoryWriter::failing(0, libc::ENOSPC)),
            Some(MemoryWriter::failing(0, libc::ENOSPC)),
            Some(MemoryWriter::default()),
        ],
        vec![Some(MemoryWriter::default())],
    );
    let options = EncodeOptions {
        completion: CompletionPolicy::AllowReconstructable,
        ..EncodeOptions::default()
    };
    let mut encoder =
        ErasureStreamEncoder::new(layout, writers, layout.stripe_size() as u64, options).unwrap();
    let error = encoder
        .write(&vec![1u8; layout.stripe_size()])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EcsErrorKind::InsufficientShards);
    let second_error = encoder.write(&[]).await.unwrap_err();
    assert_eq!(second_error.kind(), EcsErrorKind::StreamFailed);
}
