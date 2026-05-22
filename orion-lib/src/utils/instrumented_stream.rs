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
use parking_lot::Mutex;
use pin_project::pin_project;
use smallvec::SmallVec;
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
    #[allow(clippy::type_complexity)]
    drop_fn: AtomicOption<Box<dyn FnOnce(&StreamMetrics) + Send>>,
    #[allow(clippy::type_complexity)]
    flush_callbacks: Mutex<SmallVec<[Box<dyn FnOnce() + Send>; 4]>>,
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
            Some(ErrorSource::Read(err) | ErrorSource::Write(err)) => Some(err.to_string()),
        };
        f.debug_struct("StreamMetrics")
            .field("total_bytes_read", &self.total_bytes_read)
            .field("total_bytes_written", &self.total_bytes_written)
            .field("txn_bytes_read_start", &self.txn_bytes_read_start)
            .field("txn_bytes_written_start", &self.txn_bytes_written_start)
            .field("requests_counter", &self.requests_counter)
            .field("error", &error)
            .field("drop_fn", &self.drop_fn.is_some(Ordering::Relaxed))
            .field("flush_callbacks", &"...")
            .finish()
    }
}

impl Drop for StreamMetrics {
    fn drop(&mut self) {
        self.on_flush();
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
            flush_callbacks: Mutex::new(SmallVec::new()),
        }
    }

    pub fn add_flush_callback(&self, cb: Box<dyn FnOnce() + Send>) {
        let mut callbacks = self.flush_callbacks.lock();
        callbacks.push(cb);
    }

    pub fn on_flush(&self) {
        let mut callbacks = self.flush_callbacks.lock();
        for cb in callbacks.drain(..) {
            cb();
        }
    }

    #[inline]
    pub fn with_drop_fn(&self, drop_fn: Box<dyn FnOnce(&StreamMetrics) + Send>) {
        self.drop_fn.store(Ordering::Release, drop_fn);
    }

    #[inline]
    pub fn txn_take_metrics(&self) -> (u64, u64) {
        let read = self.txn_bytes_read();
        let written = self.txn_bytes_written();
        self.reset_txn();
        (read, written)
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
            Some(ErrorSource::Read(err) | ErrorSource::Write(err)) => Some(err),
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
        this.metrics.on_flush();
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
                this.metrics.on_flush();
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
            Poll::Ready(Ok(())) => {
                this.metrics.on_flush();
                Poll::Ready(Ok(()))
            },
            Poll::Ready(Err(e)) => {
                this.metrics.on_write_error(&e);
                Poll::Ready(Err(e))
            },
            Poll::Pending => Poll::Pending,
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
        Arc::clone(&self.metrics)
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
