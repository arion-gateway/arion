// Copyright 2025 The kmesh Authors
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

pub mod bind_device;
pub mod connector;
mod grpc_channel;
pub(crate) mod http_channel;
mod resolver;
pub mod tcp_channel;
pub use resolver::resolve;
pub mod proxy_protocol;
pub mod timer;
pub mod tls_inspector;
pub mod transport_socket;

pub use self::{
    grpc_channel::{GrpcService, SimpleRoundRobinGrpcServiceLB},
    http_channel::{HttpChannel, HttpChannelBuilder, HttpChannels},
    proxy_protocol::ProxyProtocolReader,
    tcp_channel::TcpChannelConnector,
    transport_socket::UpstreamTransportSocketConfigurator,
};

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin {}

pub type AsyncStream = Box<dyn AsyncReadWrite>;

pub struct HttpConnection {
    inner: TokioIo<AsyncStream>,
    is_http2: bool,
}

impl HttpConnection {
    pub fn new(stream: TokioIo<AsyncStream>) -> Self {
        Self { inner: stream, is_http2: false }
    }

    pub fn new_http2(stream: TokioIo<AsyncStream>) -> Self {
        Self { inner: stream, is_http2: true }
    }
}

impl Connection for HttpConnection {
    fn connected(&self) -> Connected {
        let conn = Connected::new();
        if self.is_http2 {
            conn.negotiated_h2()
        } else {
            conn
        }
    }
}

impl Deref for HttpConnection {
    type Target = TokioIo<AsyncStream>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for HttpConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl hyper::rt::Read for HttpConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for HttpConnection {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
