use std::alloc::{Layout, alloc, dealloc};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::Poll;
use tokio::io::{AsyncRead, ReadBuf};

use crate::layout::ALIGNMENT;

/// Fixed-capacity 4096-byte-aligned buffer for storage-engine I/O adapters.
///
/// ECS Core uses the same type internally for stripe assembly. Engines may use
/// it when their `AsyncWrite` adapter requires aligned direct-I/O buffers.
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
    len: usize,
}

impl AlignedBuffer {
    #[must_use]
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, ALIGNMENT).expect("validated ECS alignment");
        if size == 0 {
            return Self {
                ptr: NonNull::dangling(),
                layout,
                len: 0,
            };
        }
        let ptr = NonNull::new(unsafe { alloc(layout) })
            .unwrap_or_else(|| panic!("ECS aligned allocation failed"));
        Self {
            ptr,
            layout,
            len: 0,
        }
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[must_use]
    pub fn as_full_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) -> usize {
        let copied = (self.layout.size() - self.len).min(data.len());
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr().add(self.len), copied);
        }
        self.len += copied;
        copied
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.layout.size()
    }

    pub fn fill_zeros(&mut self) {
        let remaining = self.layout.size() - self.len;
        if remaining > 0 {
            unsafe {
                std::ptr::write_bytes(self.ptr.as_ptr().add(self.len), 0, remaining);
            }
            self.len = self.layout.size();
        }
    }

    pub async fn read_from<R>(&mut self, reader: &mut R, max_bytes: usize) -> std::io::Result<usize>
    where
        R: AsyncRead + Unpin,
    {
        let spare = (self.layout.size() - self.len).min(max_bytes);
        if spare == 0 {
            return Ok(0);
        }
        let start = self.len;
        let read = unsafe {
            let ptr = self.ptr.as_ptr().add(start).cast::<MaybeUninit<u8>>();
            let uninitialized = std::slice::from_raw_parts_mut(ptr, spare);
            let mut read_buffer = ReadBuf::uninit(uninitialized);
            std::future::poll_fn(|context| {
                match Pin::new(&mut *reader).poll_read(context, &mut read_buffer) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => Poll::Pending,
                }
            })
            .await?;
            read_buffer.filled().len()
        };
        self.len += read;
        Ok(read)
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if self.layout.size() > 0 {
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }
}

unsafe impl Send for AlignedBuffer {}
// The allocation is owned by the buffer. Shared access exposes only immutable
// bytes, while every mutation requires `&mut self`, so Rust's aliasing rules
// prevent concurrent mutation through this type.
unsafe impl Sync for AlignedBuffer {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_is_aligned_and_tail_zeroing_is_exact() {
        let mut buffer = AlignedBuffer::new(ALIGNMENT * 2);
        assert_eq!((buffer.ptr.as_ptr() as usize) % ALIGNMENT, 0);
        assert_eq!(buffer.extend_from_slice(&[0xA5; 17]), 17);
        buffer.fill_zeros();
        assert_eq!(buffer.as_full_slice().len(), ALIGNMENT * 2);
        assert_eq!(&buffer.as_full_slice()[..17], &[0xA5; 17]);
        assert!(buffer.as_full_slice()[17..].iter().all(|byte| *byte == 0));
        buffer.clear();
        assert!(buffer.as_slice().is_empty());
    }

    #[tokio::test]
    async fn uninitialized_tail_accepts_async_reads_without_exposure() {
        let mut buffer = AlignedBuffer::new(ALIGNMENT);
        let mut source: &[u8] = &[1, 2, 3, 4, 5];
        let read = buffer.read_from(&mut source, 3).await.unwrap();
        assert_eq!(read, 3);
        assert_eq!(buffer.as_slice(), &[1, 2, 3]);
        let read = buffer.read_from(&mut source, usize::MAX).await.unwrap();
        assert_eq!(read, 2);
        assert_eq!(buffer.as_slice(), &[1, 2, 3, 4, 5]);
    }
}
