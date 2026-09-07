use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};
use triomphe::Arc;

use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;

use crate::utils::instrumented_stream::{HasMetrics, InstrumentedStream};
use crate::utils::rewindable_stream::RewindableHeadAsyncStream;
use crate::utils::StreamMetrics;

pub enum AsyncInstrumentedStream {
    Tcp(InstrumentedStream<TcpStream>),
    Duplex(InstrumentedStream<DuplexStream>),
    Rewound(RewindableHeadAsyncStream<AsyncInstrumentedStream>),
    ServerTls(Box<ServerTlsStream<Box<AsyncInstrumentedStream>>>),
    ClientTls(Box<InstrumentedStream<ClientTlsStream<Box<AsyncInstrumentedStream>>>>),
}

impl From<InstrumentedStream<TcpStream>> for AsyncInstrumentedStream {
    fn from(stream: InstrumentedStream<TcpStream>) -> Self {
        Self::Tcp(stream)
    }
}

impl From<InstrumentedStream<DuplexStream>> for AsyncInstrumentedStream {
    fn from(stream: InstrumentedStream<DuplexStream>) -> Self {
        Self::Duplex(stream)
    }
}

impl AsyncInstrumentedStream {
    pub fn rewound(stream: RewindableHeadAsyncStream<Self>) -> Self {
        Self::Rewound(stream)
    }

    pub fn server_tls(stream: ServerTlsStream<Box<Self>>) -> Self {
        Self::ServerTls(Box::new(stream))
    }

    pub fn client_tls(stream: InstrumentedStream<ClientTlsStream<Box<Self>>>) -> Self {
        Self::ClientTls(Box::new(stream))
    }

    pub fn error(&self) -> Option<&io::Error> {
        self.metrics().error()
    }
}

impl HasMetrics for AsyncInstrumentedStream {
    fn metrics(&self) -> &StreamMetrics {
        match self {
            Self::Tcp(stream) => stream.metrics(),
            Self::Duplex(stream) => stream.metrics(),
            Self::Rewound(stream) => stream.metrics(),
            Self::ServerTls(stream) => stream.get_ref().0.metrics(),
            Self::ClientTls(stream) => stream.metrics(),
        }
    }

    fn shared_metrics(&self) -> Arc<StreamMetrics> {
        match self {
            Self::Tcp(stream) => stream.shared_metrics(),
            Self::Duplex(stream) => stream.shared_metrics(),
            Self::Rewound(stream) => stream.shared_metrics(),
            Self::ServerTls(stream) => stream.get_ref().0.shared_metrics(),
            Self::ClientTls(stream) => stream.shared_metrics(),
        }
    }
}

impl HasMetrics for Box<AsyncInstrumentedStream> {
    fn metrics(&self) -> &StreamMetrics {
        self.as_ref().metrics()
    }

    fn shared_metrics(&self) -> Arc<StreamMetrics> {
        self.as_ref().shared_metrics()
    }
}

macro_rules! poll_io {
    ($self:expr, $method:ident($($arg:expr),*)) => {
        match $self.get_mut() {
            AsyncInstrumentedStream::Tcp(stream) => Pin::new(stream).$method($($arg),*),
            AsyncInstrumentedStream::Duplex(stream) => Pin::new(stream).$method($($arg),*),
            AsyncInstrumentedStream::Rewound(stream) => Pin::new(stream).$method($($arg),*),
            AsyncInstrumentedStream::ServerTls(stream) => Pin::new(stream.as_mut()).$method($($arg),*),
            AsyncInstrumentedStream::ClientTls(stream) => Pin::new(stream.as_mut()).$method($($arg),*),
        }
    };
}

impl AsyncRead for AsyncInstrumentedStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        poll_io!(self, poll_read(cx, buf))
    }
}

impl AsyncWrite for AsyncInstrumentedStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        poll_io!(self, poll_write(cx, buf))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        poll_io!(self, poll_write_vectored(cx, bufs))
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tcp(stream) => stream.is_write_vectored(),
            Self::Duplex(stream) => stream.is_write_vectored(),
            Self::Rewound(stream) => stream.is_write_vectored(),
            Self::ServerTls(stream) => stream.is_write_vectored(),
            Self::ClientTls(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        poll_io!(self, poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        poll_io!(self, poll_shutdown(cx))
    }
}
