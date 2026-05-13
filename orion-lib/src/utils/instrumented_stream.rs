use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use atomicoption::AtomicOption;
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{transport::AsyncReadWriteInstrumented, utils::rewindable_stream::RewindableHeadAsyncStream};

// Tracks the exact operation that caused the error
#[derive(Debug)]
pub enum ErrorSource {
    Read(io::Error),
    Write(io::Error),
}

pub struct StreamMetrics {
    total_bytes_read: AtomicU64,
    total_bytes_written: AtomicU64,
    txn_bytes_read_start: AtomicU64,
    txn_bytes_written_start: AtomicU64,
    requests_counter: AtomicU64,
    error: AtomicOption<ErrorSource>,
    drop_fn: AtomicOption<Box<dyn FnOnce(&StreamMetrics) + Send>>,
    txn_fn: AtomicOption<Box<dyn FnOnce(u64, u64) + Send>>,
}

impl Default for StreamMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for StreamMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let error = match self.error.as_ref(Ordering::Relaxed) {
            None => None,
            Some(ErrorSource::Read(err)) => Some(err.to_string()),
            Some(ErrorSource::Write(err)) => Some(err.to_string()),
        };
        f.debug_struct("StreamMetrics")
            .field("total_bytes_read", &self.total_bytes_read)
            .field("total_bytes_written", &self.total_bytes_written)
            .field("txn_bytes_read_start", &self.txn_bytes_read_start)
            .field("txn_bytes_written_start", &self.txn_bytes_written_start)
            .field("error", &error)
            .finish()
    }
}

impl Drop for StreamMetrics {
    fn drop(&mut self) {
        if let Some(log_fn) = self.drop_fn.take(Ordering::Acquire) {
            log_fn(self);
        }
    }
}

impl StreamMetrics {
    pub fn new() -> StreamMetrics {
        Self {
            total_bytes_read: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            txn_bytes_read_start: AtomicU64::new(0),
            txn_bytes_written_start: AtomicU64::new(0),
            requests_counter: AtomicU64::new(0),
            error: AtomicOption::none(),
            drop_fn: AtomicOption::none(),
            txn_fn: AtomicOption::none(),
        }
    }

    #[inline]
    pub fn with_drop_fn(&self, drop_fn: Box<dyn FnOnce(&StreamMetrics) + Send>) {
        self.drop_fn.store(Ordering::Release, drop_fn);
    }

    #[inline]
    pub fn on_flush(&self, flush_fn: Box<dyn FnOnce(u64, u64) + Send>) {
        self.txn_fn.store(Ordering::Release, flush_fn);
    }

    #[inline]
    pub fn txn_bytes_read(&self) -> u64 {
        self.total_bytes_read.load(Ordering::Relaxed) - self.txn_bytes_read_start.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn txn_bytes_written(&self) -> u64 {
        self.total_bytes_written.load(Ordering::Relaxed) - self.txn_bytes_written_start.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn bytes_read(&self) -> u64 {
        self.total_bytes_read.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn bytes_written(&self) -> u64 {
        self.total_bytes_written.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn requests_counter(&self) -> u64 {
        self.requests_counter.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn inc_requests(&self) -> u64 {
        self.requests_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub fn error(&self) -> Option<&io::Error> {
        let err = self.error.as_ref(Ordering::Relaxed);
        match err {
            None => None,
            Some(ErrorSource::Read(err)) => Some(err),
            Some(ErrorSource::Write(err)) => Some(err),
        }
    }

    fn reset_txn(&self) {
        self.txn_bytes_read_start.store(self.bytes_read(), Ordering::Relaxed);
        self.txn_bytes_written_start.store(self.bytes_written(), Ordering::Relaxed);
    }

    #[inline]
    fn on_read(&self, bytes: u64) {
        self.total_bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    fn on_write(&self, bytes: u64) {
        self.total_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    fn on_read_error(&self, err: &io::Error) {
        self.error.store(Ordering::Release, ErrorSource::Read(Self::clone_io_error(err)));
    }

    #[inline]
    fn on_write_error(&self, err: &io::Error) {
        self.error.store(Ordering::Release, ErrorSource::Write(Self::clone_io_error(err)));
    }

    #[inline]
    fn take_txn_fn(&self, ord: Ordering) -> Option<Box<dyn FnOnce(u64, u64) + Send>> {
        self.txn_fn.take(ord)
    }

    fn clone_io_error(err: &io::Error) -> io::Error {
        if let Some(code) = err.raw_os_error() {
            io::Error::from_raw_os_error(code)
        } else {
            io::Error::new(err.kind(), err.to_string())
        }
    }
}

#[pin_project]
pub struct InstrumentedStream<S> {
    #[pin]
    inner: S,
    metrics: Arc<StreamMetrics>,
}

impl<S> InstrumentedStream<S> {
    pub fn new(inner: S) -> InstrumentedStream<S> {
        Self { inner, metrics: Arc::new(StreamMetrics::new()) }
    }

    #[inline]
    #[allow(unused)]
    pub fn into_inner(self) -> S {
        self.inner
    }

    #[inline]
    #[allow(dead_code)]
    pub fn error(&self) -> Option<&io::Error> {
        self.metrics.as_ref().error()
    }

    #[inline]
    pub fn metrics(&self) -> &StreamMetrics {
        self.metrics.as_ref()
    }
}

impl<S: AsyncRead> AsyncRead for InstrumentedStream<S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        let before = buf.filled().len();
        match this.inner.poll_read(cx, buf) {
            res @ Poll::Ready(Ok(())) => {
                let bytes = buf.filled().len() - before;
                this.metrics.on_read(bytes as u64);
                res
            },
            Poll::Ready(Err(e)) => {
                this.metrics.on_read_error(&e);
                Poll::Ready(Err(e))
            },
            res => res,
        }
    }
}

impl<S: AsyncWrite> AsyncWrite for InstrumentedStream<S> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let this = self.project();
        match this.inner.poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.metrics.on_write(n as u64);
                Poll::Ready(Ok(n))
            },
            Poll::Ready(Err(e)) => {
                this.metrics.on_write_error(&e);
                Poll::Ready(Err(e))
            },
            res => res,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();

        match this.inner.poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                if let Some(log_access) = this.metrics.take_txn_fn(Ordering::Acquire) {
                    log_access(this.metrics.txn_bytes_read(), this.metrics.txn_bytes_written());
                    this.metrics.reset_txn();
                }
                Poll::Ready(Ok(()))
            },
            Poll::Ready(Err(e)) => {
                this.metrics.on_write_error(&e);
                Poll::Ready(Err(e))
            },
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        match this.inner.poll_shutdown(cx) {
            Poll::Ready(Err(e)) => {
                this.metrics.on_write_error(&e);
                Poll::Ready(Err(e))
            },
            res => res,
        }
    }
}

pub trait HasMetrics {
    fn metrics(&self) -> &StreamMetrics;
    fn shared_metrics(&self) -> Arc<StreamMetrics>;
}

impl<S> HasMetrics for InstrumentedStream<S> {
    fn metrics(&self) -> &StreamMetrics {
        self.metrics.as_ref()
    }

    fn shared_metrics(&self) -> Arc<StreamMetrics> {
        self.metrics.clone()
    }
}

impl HasMetrics for tokio_rustls::server::TlsStream<Box<dyn AsyncReadWriteInstrumented>> {
    fn metrics(&self) -> &StreamMetrics {
        self.get_ref().0.metrics()
    }

    fn shared_metrics(&self) -> Arc<StreamMetrics> {
        self.get_ref().0.shared_metrics()
    }
}

impl<R> HasMetrics for RewindableHeadAsyncStream<R>
where
    R: AsyncReadWriteInstrumented + HasMetrics + ?Sized,
{
    fn metrics(&self) -> &StreamMetrics {
        self.get_ref().metrics()
    }

    fn shared_metrics(&self) -> Arc<StreamMetrics> {
        self.get_ref().shared_metrics()
    }
}
