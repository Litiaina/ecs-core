use std::io::Cursor;

use ecs_core::{
    DecodeOptions, EcLayout, EncodeOptions, ErasureStreamDecoder, ErasureStreamEncoder,
    ShardReaders, ShardWriters,
};
use tokio::io::AsyncWrite;

#[derive(Default)]
struct Buffer(Vec<u8>);

impl AsyncWrite for Buffer {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.0.extend_from_slice(bytes);
        std::task::Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = b"ECS Core keeps placement in the caller and streams only bytes.";
    let layout = EcLayout::new(2, 1, 4096)?;
    let writers = ShardWriters::new(
        vec![Some(Buffer::default()), Some(Buffer::default())],
        vec![Some(Buffer::default())],
    );
    let mut encoder = ErasureStreamEncoder::new(
        layout,
        writers,
        source.len() as u64,
        EncodeOptions::default(),
    )?;
    encoder.write(source).await?;
    let encoded = encoder.finish().await?;
    let mut pieces = encoded
        .writers
        .data
        .into_iter()
        .chain(encoded.writers.parity)
        .map(|writer| writer.map(|buffer| buffer.0))
        .collect::<Vec<_>>();

    pieces[0] = None;
    let readers = ShardReaders::new(
        pieces[..2]
            .iter()
            .map(|piece| piece.clone().map(Cursor::new))
            .collect(),
        pieces[2..]
            .iter()
            .map(|piece| piece.clone().map(Cursor::new))
            .collect(),
    );
    let decoder = ErasureStreamDecoder::new(
        layout,
        readers,
        Buffer::default(),
        source.len() as u64,
        DecodeOptions::default(),
    )?;
    let decoded = decoder.decode().await?;
    assert_eq!(decoded.destination.0, source);
    println!("round trip restored {} source bytes", source.len());
    Ok(())
}
