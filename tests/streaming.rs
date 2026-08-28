mod support;

use std::io::Cursor;

use ecs_core::{
    ChecksumAlgorithm, ChecksumPolicy, CompletionPolicy, DecodeOptions, EcLayout, EncodeOptions,
    ErasureStreamDecoder, ErasureStreamEncoder, ErasureStreamRepairer, PieceState,
    ReedSolomonEncoder, ShardReaders, ShardWriters,
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use support::MemoryWriter;

async fn encode(
    layout: EcLayout,
    source: &[u8],
    options: EncodeOptions,
) -> (ecs_core::EncodeReport, Vec<Vec<u8>>) {
    let writers = ShardWriters::new(
        (0..layout.data_shards())
            .map(|_| Some(MemoryWriter::default()))
            .collect(),
        (0..layout.parity_shards())
            .map(|_| Some(MemoryWriter::default()))
            .collect(),
    );
    let mut encoder =
        ErasureStreamEncoder::new(layout, writers, source.len() as u64, options).unwrap();
    for chunk in source.chunks(7919) {
        encoder.write(chunk).await.unwrap();
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

async fn decode(
    layout: EcLayout,
    source_bytes: u64,
    pieces: &[Vec<u8>],
    missing: &[usize],
    options: DecodeOptions,
) -> Vec<u8> {
    let readers = pieces
        .iter()
        .enumerate()
        .map(|(index, piece)| (!missing.contains(&index)).then(|| Cursor::new(piece.clone())))
        .collect::<Vec<_>>();
    let readers = ShardReaders::new(
        readers[..layout.data_shards()].to_vec(),
        readers[layout.data_shards()..].to_vec(),
    );
    let decoder = ErasureStreamDecoder::new(
        layout,
        readers,
        MemoryWriter::default(),
        source_bytes,
        options,
    )
    .unwrap();
    decoder.decode().await.unwrap().destination.into_inner()
}

#[tokio::test]
async fn arbitrary_chunks_round_trip_with_every_legal_loss_pattern() {
    let layout = EcLayout::new(4, 2, 4096).unwrap();
    let mut source = vec![0u8; layout.stripe_size() * 3 + 1237];
    ChaCha20Rng::seed_from_u64(0x0EC5_C0DE).fill_bytes(&mut source);
    let (report, pieces) = encode(layout, &source, EncodeOptions::default()).await;
    assert!(report.fully_protected);
    assert!(report.reconstructable);
    assert_eq!(report.source_bytes, source.len() as u64);
    assert_eq!(report.pieces.len(), 6);
    assert!(
        report
            .pieces
            .iter()
            .all(|piece| piece.state == PieceState::Complete)
    );

    let mut loss_patterns = vec![vec![]];
    for first in 0..layout.total_shards() {
        loss_patterns.push(vec![first]);
        for second in first + 1..layout.total_shards() {
            loss_patterns.push(vec![first, second]);
        }
    }
    for missing in loss_patterns {
        let restored = decode(
            layout,
            source.len() as u64,
            &pieces,
            &missing,
            DecodeOptions {
                source_checksum: Some(ChecksumAlgorithm::Sha256),
                expected_source_checksum: report.source_checksum.clone(),
            },
        )
        .await;
        assert_eq!(restored, source, "failed loss pattern {missing:?}");
    }
}

#[tokio::test]
async fn stream_mapping_is_byte_identical_to_the_extracted_codec() {
    let layout = EcLayout::new(3, 2, 4096).unwrap();
    let mut source = vec![0u8; layout.stripe_size() * 2 - 917];
    ChaCha20Rng::seed_from_u64(81).fill_bytes(&mut source);
    let (_, pieces) = encode(
        layout,
        &source,
        EncodeOptions {
            completion: CompletionPolicy::RequireAll,
            checksum: Some(ChecksumPolicy::crc32(true)),
            expected_source_checksum: None,
        },
    )
    .await;

    let mut expected = vec![Vec::new(); layout.total_shards()];
    for stripe in source.chunks(layout.stripe_size()) {
        let mut padded = vec![0u8; layout.stripe_size()];
        padded[..stripe.len()].copy_from_slice(stripe);
        let data = padded.chunks_exact(layout.block_size()).collect::<Vec<_>>();
        for (index, block) in data.iter().enumerate() {
            expected[index].extend_from_slice(block);
        }
        let mut codec = ReedSolomonEncoder::new(
            layout.data_shards(),
            layout.parity_shards(),
            layout.block_size(),
        )
        .unwrap();
        for block in data {
            codec.add_original_shard(block).unwrap();
        }
        let encoded = codec.encode().unwrap();
        for parity in 0..layout.parity_shards() {
            expected[layout.data_shards() + parity]
                .extend_from_slice(encoded.recovery(parity).unwrap());
        }
    }
    assert_eq!(pieces, expected);
}

#[tokio::test]
async fn zero_length_stream_is_valid_and_deterministic() {
    let layout = EcLayout::new(1, 1, 4096).unwrap();
    let (report, pieces) = encode(layout, &[], EncodeOptions::default()).await;
    assert_eq!(report.stripe_count, 0);
    assert_eq!(pieces, vec![Vec::<u8>::new(), Vec::<u8>::new()]);
    let restored = decode(layout, 0, &pieces, &[0], DecodeOptions::default()).await;
    assert!(restored.is_empty());
}

#[tokio::test]
async fn every_data_and_parity_piece_can_be_repaired_exactly() {
    let layout = EcLayout::new(3, 2, 4096).unwrap();
    let mut source = vec![0u8; layout.stripe_size() * 2 + 33];
    ChaCha20Rng::seed_from_u64(99).fill_bytes(&mut source);
    let (_, pieces) = encode(layout, &source, EncodeOptions::default()).await;

    for repair_index in 0..layout.total_shards() {
        let readers = pieces
            .iter()
            .enumerate()
            .map(|(index, piece)| (index != repair_index).then(|| Cursor::new(piece.clone())))
            .collect::<Vec<_>>();
        let readers = ShardReaders::new(
            readers[..layout.data_shards()].to_vec(),
            readers[layout.data_shards()..].to_vec(),
        );
        let repairer = ErasureStreamRepairer::new(
            layout,
            readers,
            MemoryWriter::default(),
            source.len() as u64,
            repair_index,
            Some(ChecksumAlgorithm::Sha256),
        )
        .unwrap();
        let output = repairer.repair().await.unwrap();
        assert_eq!(output.destination.into_inner(), pieces[repair_index]);
        assert_eq!(output.report.repaired_index, repair_index);
    }
}

#[tokio::test]
async fn declared_length_is_a_hard_boundary() {
    let layout = EcLayout::new(2, 1, 4096).unwrap();
    let writers = ShardWriters::new(
        vec![Some(MemoryWriter::default()), Some(MemoryWriter::default())],
        vec![Some(MemoryWriter::default())],
    );
    let mut encoder =
        ErasureStreamEncoder::new(layout, writers, 3, EncodeOptions::default()).unwrap();
    let error = encoder.write(b"four").await.unwrap_err();
    assert_eq!(error.kind(), ecs_core::EcsErrorKind::InputTooLarge);
}

#[tokio::test]
async fn randomized_layouts_and_tail_lengths_remain_exact() {
    let mut random = ChaCha20Rng::seed_from_u64(0x5EED_F00D);
    for (data, parity) in [(1, 0), (1, 1), (2, 1), (3, 2), (6, 2)] {
        let layout = EcLayout::new(data, parity, 4096).unwrap();
        let lengths = [
            0,
            1,
            4095,
            4096,
            layout.stripe_size() - 1,
            layout.stripe_size(),
            layout.stripe_size() + 1,
            layout.stripe_size() * 2 + 177,
        ];
        for length in lengths {
            let mut source = vec![0u8; length];
            random.fill_bytes(&mut source);
            let (report, pieces) = encode(layout, &source, EncodeOptions::default()).await;
            let missing = if parity == 0 {
                Vec::new()
            } else {
                vec![(random.next_u32() as usize) % layout.total_shards()]
            };
            let restored = decode(
                layout,
                length as u64,
                &pieces,
                &missing,
                DecodeOptions {
                    source_checksum: Some(ChecksumAlgorithm::Sha256),
                    expected_source_checksum: report.source_checksum,
                },
            )
            .await;
            assert_eq!(restored, source, "layout {data}+{parity}, length {length}");
        }
    }
}

#[tokio::test]
async fn source_checksum_prevents_corrupt_output_publication() {
    let layout = EcLayout::new(2, 1, 4096).unwrap();
    let source = vec![0x5A; layout.stripe_size() + 9];
    let (report, mut pieces) = encode(layout, &source, EncodeOptions::default()).await;
    pieces[0][17] ^= 0xFF;
    let readers = ShardReaders::new(
        pieces[..layout.data_shards()]
            .iter()
            .map(|piece| Some(Cursor::new(piece.clone())))
            .collect(),
        pieces[layout.data_shards()..]
            .iter()
            .map(|piece| Some(Cursor::new(piece.clone())))
            .collect(),
    );
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
    let error = match decoder.decode().await {
        Ok(_) => panic!("corrupt input unexpectedly decoded successfully"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ecs_core::EcsErrorKind::ChecksumMismatch);
}

#[tokio::test]
async fn copy_exact_rejects_a_short_source() {
    let layout = EcLayout::new(2, 1, 4096).unwrap();
    let writers = ShardWriters::new(
        vec![Some(MemoryWriter::default()), Some(MemoryWriter::default())],
        vec![Some(MemoryWriter::default())],
    );
    let mut encoder =
        ErasureStreamEncoder::new(layout, writers, 10, EncodeOptions::default()).unwrap();
    let mut source = Cursor::new(vec![1u8; 9]);
    let error = encoder.copy_exact(&mut source).await.unwrap_err();
    assert_eq!(error.kind(), ecs_core::EcsErrorKind::IncompleteInput);
}
