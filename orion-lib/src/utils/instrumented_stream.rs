use std::{
    pin::Pin, sync::{
        Arc, atomic::{AtomicU64, Ordering}
    }, task::{Context, Poll}
};

use pin_project::pin_project;
use tokio::{io::{AsyncRead, AsyncWrite, ReadBuf}};

use crate::{transport::AsyncReadWriteInstrumented, utils::rewindable_stream::RewindableHeadAsyncStream};

pub struct StreamMetrics {
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

impl StreamMetrics {
    pub fn new() -> StreamMetrics {
        Self { bytes_read: AtomicU64::new(0), bytes_written: AtomicU64::new(0) }
    }

    pub fn reset(&self) {
        self.bytes_read.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
    }

    #[inline]
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
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
}

impl<S: AsyncRead> AsyncRead for InstrumentedStream<S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        let before = buf.filled().len();
        match this.inner.poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                this.metrics.bytes_read.fetch_add((buf.filled().len() - before) as u64, Ordering::Relaxed);
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
                this.metrics.bytes_written.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(n))
            },
            res => res,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_shutdown(cx)
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
