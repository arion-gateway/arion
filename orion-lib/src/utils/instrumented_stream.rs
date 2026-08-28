use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock,
    },
    task::{Context, Poll},
    time::Duration,
};

use atomicoption::AtomicOption;

#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;
#[cfg(feature = "metrics")]
use orion_metrics::metrics::user;
use parking_lot::Mutex;
use pin_project::pin_project;
use quanta::Clock;
use smallvec::SmallVec;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[cfg(feature = "metrics")]
use crate::{get_shard_id, metrics, with_metric};

use super::StreamMetrics as ConnMetrics;
use crate::utils::rewindable_stream::RewindableHeadAsyncStream;

// Tracks the exact operation that caused the error
#[derive(Debug)]
pub enum ErrorSource {
    Read(io::Error),
    Write(io::Error),
}

pub trait OnFlush: Send + Sized + 'static {
    fn run(self, metrics: &StreamMetrics<Self>);
}

impl OnFlush for () {
    fn run(self, _metrics: &StreamMetrics<Self>) {}
}

pub struct CallbackQueue<T> {
    has_pending: AtomicBool,
    queue: Mutex<SmallVec<[T; 4]>>,
}

impl<T> CallbackQueue<T> {
    pub fn new() -> Self {
        Self { has_pending: AtomicBool::new(false), queue: Mutex::new(SmallVec::new()) }
    }

    pub fn push(&self, cb: T) {
        let mut queue = self.queue.lock();
        queue.push(cb);
        self.has_pending.store(true, Ordering::Release);
    }

    pub fn drain(&self) -> Option<SmallVec<[T; 4]>> {
        if !self.has_pending.load(Ordering::Acquire) {
            return None;
        }
        let mut queue = self.queue.lock();
        if queue.is_empty() {
            self.has_pending.store(false, Ordering::Release);
            return None;
        }
        let taken = std::mem::take(&mut *queue);
        self.has_pending.store(false, Ordering::Release);
        Some(taken)
    }
}

static WALL_CLOCK: LazyLock<Clock> = LazyLock::new(Clock::new);

pub struct StreamMetrics<C: OnFlush = ()> {
    total_bytes_read: AtomicU64,
    total_bytes_written: AtomicU64,
    txn_bytes_read_start: AtomicU64,
    txn_bytes_written_start: AtomicU64,
    raw_clock: AtomicU64,
    requests_counter: AtomicU64,
    error: AtomicOption<ErrorSource>,
    #[allow(clippy::type_complexity)]
    drop_fn: AtomicOption<Box<dyn FnOnce(&StreamMetrics<C>, Duration) + Send>>,
    flush_callbacks: CallbackQueue<C>,
    user_partition_key: AtomicOption<&'static str>,
}

impl<C: OnFlush> Default for StreamMetrics<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: OnFlush> std::fmt::Debug for StreamMetrics<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let error = match self.error.as_ref(Ordering::Relaxed) {
            None => None,
            Some(ErrorSource::Read(err) | ErrorSource::Write(err)) => Some(err.to_string()),
        };
        let user_partition_key = self.user_partition_key.as_ref(Ordering::Relaxed).map(|s| *s);
        f.debug_struct("StreamMetrics")
            .field("total_bytes_read", &self.total_bytes_read)
            .field("total_bytes_written", &self.total_bytes_written)
            .field("txn_bytes_read_start", &self.txn_bytes_read_start)
            .field("txn_bytes_written_start", &self.txn_bytes_written_start)
            .field("raw_clock", &self.raw_clock)
            .field("requests_counter", &self.requests_counter)
            .field("error", &error)
            .field("drop_fn", &self.drop_fn.is_some(Ordering::Relaxed))
            .field("user_partition_key", &user_partition_key)
            .finish()
    }
}

impl<C: OnFlush> Drop for StreamMetrics<C> {
    fn drop(&mut self) {
        self.on_flush();
        if let Some(drop_fn) = self.drop_fn.take(Ordering::Acquire) {
            drop_fn(self, WALL_CLOCK.delta(self.raw_clock.load(Ordering::Relaxed), WALL_CLOCK.raw()));
        }

        #[cfg(feature = "metrics")]
        if let Some(user_partition_key) = self.user_partition_key.as_ref(Ordering::Relaxed) {
            with_metric!(
                user::CONNECTIONS_ACTIVE,
                sub,
                1,
                get_shard_id!(),
                &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), *user_partition_key)]
            );
        }
    }
}

impl<C: OnFlush> StreamMetrics<C> {
    pub fn new() -> StreamMetrics<C> {
        Self {
            total_bytes_read: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            txn_bytes_read_start: AtomicU64::new(0),
            txn_bytes_written_start: AtomicU64::new(0),
            raw_clock: AtomicU64::new(WALL_CLOCK.raw()),
            requests_counter: AtomicU64::new(0),
            error: AtomicOption::none(),
            drop_fn: AtomicOption::none(),
            flush_callbacks: CallbackQueue::new(),
            user_partition_key: AtomicOption::none(),
        }
    }

    #[inline]
    pub fn add_flush_callback(&self, cb: C) {
        self.flush_callbacks.push(cb);
    }

    #[inline]
    pub fn on_flush(&self) {
        if let Some(callbacks) = self.flush_callbacks.drain() {
            for cb in callbacks {
                cb.run(self);
            }
        }
    }

    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn with_drop_fn(&self, drop_fn: Box<dyn FnOnce(&StreamMetrics<C>, Duration) + Send>) {
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
    #[allow(dead_code)]
    pub fn requests_counter(&self) -> u64 {
        self.requests_counter.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn inc_requests(&self) -> u64 {
        self.requests_counter.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    pub fn update_raw_clock(&self) {
        self.raw_clock.store(WALL_CLOCK.raw(), Ordering::Relaxed)
    }

    #[inline]
    pub fn set_user_partition_key(&self, key: &'static str) {
        self.user_partition_key.store(Ordering::Release, key);
    }

    pub fn error(&self) -> Option<&io::Error> {
        let err = self.error.as_ref(Ordering::Relaxed);
        match err {
            None => None,
            Some(ErrorSource::Read(err) | ErrorSource::Write(err)) => Some(err),
        }
    }

    #[inline]
    fn reset_txn(&self) {
        self.txn_bytes_read_start.store(self.bytes_read(), Ordering::Relaxed);
        self.txn_bytes_written_start.store(self.bytes_written(), Ordering::Relaxed);
    }

    #[inline]
    fn on_read(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.total_bytes_read.fetch_add(bytes, Ordering::Relaxed);
        self.update_raw_clock();
    }

    #[inline]
    fn on_write(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.total_bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.update_raw_clock();
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
    metrics: Arc<ConnMetrics>,
}

impl<S> InstrumentedStream<S> {
    pub fn new(inner: S) -> InstrumentedStream<S> {
        Self { inner, metrics: Arc::new(ConnMetrics::new()) }
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
    pub(crate) fn metrics(&self) -> &ConnMetrics {
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
            Poll::Ready(Ok(bytes)) => {
                this.metrics.on_write(bytes as u64);
                Poll::Ready(Ok(bytes))
            },
            Poll::Ready(Err(e)) => {
                this.metrics.on_write_error(&e);
                Poll::Ready(Err(e))
            },
            res => res,
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.project();
        match this.inner.poll_write_vectored(cx, bufs) {
            Poll::Ready(Ok(bytes)) => {
                this.metrics.on_write(bytes as u64);
                Poll::Ready(Ok(bytes))
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

pub(crate) trait HasMetrics {
    fn metrics(&self) -> &ConnMetrics;
    fn shared_metrics(&self) -> Arc<ConnMetrics>;
}

impl<S> HasMetrics for InstrumentedStream<S> {
    fn metrics(&self) -> &ConnMetrics {
        self.metrics.as_ref()
    }

    fn shared_metrics(&self) -> Arc<ConnMetrics> {
        Arc::clone(&self.metrics)
    }
}

impl<R> HasMetrics for RewindableHeadAsyncStream<R>
where
    R: HasMetrics + AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    fn metrics(&self) -> &ConnMetrics {
        self.get_ref().metrics()
    }

    fn shared_metrics(&self) -> Arc<ConnMetrics> {
        self.get_ref().shared_metrics()
    }
}
