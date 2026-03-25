use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// Tracks the exact operation that caused the error
#[derive(Debug)]
pub enum ErrorSource {
    None,
    Read,
    Write
}

// Wrapper for the stream to intercept errors
pub struct TrackedStream<T> {
    pub inner: T,
    pub error_source: ErrorSource,
}

impl<T> TrackedStream<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            error_source: ErrorSource::None,
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for TrackedStream<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        // Intercept read errors
        if let Poll::Ready(Err(_)) = &res {
            self.error_source = ErrorSource::Read;
        }
        res
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for TrackedStream<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        // Intercept write errors
        if let Poll::Ready(Err(_)) = &res {
            self.error_source = ErrorSource::Write;
        }
        res
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let res = Pin::new(&mut self.inner).poll_flush(cx);
        if let Poll::Ready(Err(_)) = &res {
            self.error_source = ErrorSource::Write;
        }
        res
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let res = Pin::new(&mut self.inner).poll_shutdown(cx);
        if let Poll::Ready(Err(_)) = &res {
            self.error_source = ErrorSource::Write;
        }
        res
    }
}
