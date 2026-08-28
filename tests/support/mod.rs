use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

#[derive(Debug, Default)]
pub struct MemoryWriter {
    bytes: Vec<u8>,
    failure: Option<Failure>,
}

#[derive(Clone, Copy, Debug)]
struct Failure {
    after: usize,
    raw_os_error: i32,
}

impl MemoryWriter {
    #[allow(dead_code)]
    pub fn failing(after: usize, raw_os_error: i32) -> Self {
        Self {
            bytes: Vec::new(),
            failure: Some(Failure {
                after,
                raw_os_error,
            }),
        }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl AsyncWrite for MemoryWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(failure) = self.failure {
            if self.bytes.len() >= failure.after {
                return Poll::Ready(Err(io::Error::from_raw_os_error(failure.raw_os_error)));
            }
            let count = buffer.len().min(failure.after - self.bytes.len());
            self.bytes.extend_from_slice(&buffer[..count]);
            return Poll::Ready(Ok(count));
        }
        self.bytes.extend_from_slice(buffer);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
