use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use parking_lot::Mutex;
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{transport::AsyncReadWriteInstrumented, utils::rewindable_stream::RewindableHeadAsyncStream};

pub struct StreamMetrics {
    conn_bytes_read: AtomicU64,
    conn_bytes_written: AtomicU64,
    txn_bytes_read: AtomicU64,
    txn_bytes_written: AtomicU64,
    log_txn: Mutex<Option<Box<dyn FnOnce(u64, u64) + Send>>>,
    log_conn: Mutex<Option<Box<dyn FnOnce(&StreamMetrics) + Send>>>,
}

impl std::fmt::Debug for StreamMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamMetrics")
            .field("conn_bytes_read", &self.conn_bytes_read)
            .field("conn_bytes_written", &self.conn_bytes_written)
            .field("txn_bytes_read", &self.txn_bytes_read)
            .field("txn_bytes_written", &self.txn_bytes_written)
            .finish()
    }
}

impl Drop for StreamMetrics {
    fn drop(&mut self) {
        if let Some(log_fn) = self.log_conn.lock().take() {
            log_fn(self);
        }
    }
}

impl StreamMetrics {
    #[inline]
    pub fn new(log_fn: Option<Box<dyn FnOnce(&StreamMetrics) + Send>>) -> StreamMetrics {
        Self {
            conn_bytes_read: AtomicU64::new(0),
            conn_bytes_written: AtomicU64::new(0),
            txn_bytes_read: AtomicU64::new(0),
            txn_bytes_written: AtomicU64::new(0),
            log_txn: Mutex::new(None),
            log_conn: Mutex::new(log_fn),
        }
    }

    #[inline]
    pub fn log_and_reset(&self, log_fn: Box<dyn FnOnce(u64, u64) + Send>) {
        let mut self_log_fn = self.log_txn.lock();
        *self_log_fn = Some(log_fn);
    }

    #[inline]
    fn reset(&self) {
        // println!("RESET METRICS! @{:p}", self as * const StreamMetrics);
        self.txn_bytes_read.store(0, Ordering::Relaxed);
        self.txn_bytes_written.store(0, Ordering::Relaxed);
    }

    #[inline]
    pub fn txn_bytes_read(&self) -> u64 {
        self.txn_bytes_read.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn txn_bytes_written(&self) -> u64 {
        self.txn_bytes_written.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn conn_bytes_read(&self) -> u64 {
        self.conn_bytes_read.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn conn_bytes_written(&self) -> u64 {
        self.conn_bytes_written.load(Ordering::Relaxed)
    }
}

#[pin_project]
pub struct InstrumentedStream<S> {
    #[pin]
    inner: S,
    metrics: Arc<StreamMetrics>,
}

impl<S> InstrumentedStream<S> {
    pub fn new(inner: S, log_fn: Option<Box<dyn FnOnce(&StreamMetrics) + Send>>) -> InstrumentedStream<S> {
        Self { inner, metrics: Arc::new(StreamMetrics::new(log_fn)) }
    }

    #[inline]
    #[allow(unused)]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncRead> AsyncRead for InstrumentedStream<S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        let before = buf.filled().len();
        match this.inner.poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let bytes = buf.filled().len() - before;
                this.metrics.conn_bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
                this.metrics.txn_bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
                // println!("BYTES READ: {bytes} -> {} @{:p}", prev + bytes as u64, this.metrics.as_ref() as *const StreamMetrics);
                Poll::Ready(Ok(()))
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
                this.metrics.conn_bytes_written.fetch_add(n as u64, Ordering::Relaxed);
                this.metrics.txn_bytes_written.fetch_add(n as u64, Ordering::Relaxed);
                // println!("BYTES WRITTEN: {n} -> {} @{:p}", prev + n as u64, this.metrics.as_ref() as *const StreamMetrics);
                Poll::Ready(Ok(n))
            },
            res => res,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        // println!("POLL FLUSH! @{:p}", this.metrics.as_ref() as *const StreamMetrics);
        if let Some(log_access) = this.metrics.log_txn.lock().take() {
            // println!("=> FLUSHING LOG  @{:p}", this.metrics.as_ref() as *const StreamMetrics);
            log_access(this.metrics.txn_bytes_read(), this.metrics.txn_bytes_written());
            this.metrics.reset();
        }

        this.inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        // println!("SHUTDOWN -> @{:p}", this.metrics.as_ref() as *const StreamMetrics);
        this.inner.poll_shutdown(cx)
    }
}

pub trait Instrumented {
    fn metrics(&self) -> &StreamMetrics;
    fn shared_metrics(&self) -> Arc<StreamMetrics>;
}

impl<S> Instrumented for InstrumentedStream<S> {
    fn metrics(&self) -> &StreamMetrics {
        &self.metrics
    }

    fn shared_metrics(&self) -> Arc<StreamMetrics> {
        Arc::clone(&self.metrics)
    }
}

impl Instrumented for tokio_rustls::server::TlsStream<Box<dyn AsyncReadWriteInstrumented>> {
    fn metrics(&self) -> &StreamMetrics {
        self.get_ref().0.metrics()
    }

    fn shared_metrics(&self) -> Arc<StreamMetrics> {
        self.get_ref().0.shared_metrics()
    }
}

impl<R> Instrumented for RewindableHeadAsyncStream<R>
where
    R: AsyncReadWriteInstrumented + Instrumented + ?Sized,
{
    fn metrics(&self) -> &StreamMetrics {
        self.get_ref().metrics()
    }

    fn shared_metrics(&self) -> Arc<StreamMetrics> {
        self.get_ref().shared_metrics()
    }
}
