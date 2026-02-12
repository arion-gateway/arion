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

use std::{
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::{Duration, Instant},
};

use http::uri::Authority;
use hyper::Uri;
use hyper_util::rt::TokioIo;
use orion_configuration::config::core::Address;
use orion_error::{Context, WithContext};
use orion_format::types::ResponseFlags;
use orion_interner::StringInterner;
use pingora_timeout::fast_timeout::fast_timeout;
use tokio::net::{TcpSocket, TcpStream};
use tower::Service;
use tracing::debug;

use crate::event_error::{elapsed, EventError};
use crate::listeners::internal_registry::{self, InternalConnection};
use crate::listeners::metadata::DownstreamConnectionMetadata;
use crate::transport::{AsyncStream, HttpConnection};

use super::{bind_device::BindDevice, resolve};

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Event(#[from] EventError),
}

#[derive(Clone, Debug)]
pub enum ConnectUsing {
    Socket { authority: Authority, bind_device: Option<BindDevice>, timeout: Option<Duration> },
    InternalListener { listener_name: &'static str, synthetic_authority: Authority },
}

impl ConnectUsing {
    pub fn authority(&self) -> &Authority {
        match self {
            ConnectUsing::Socket { authority, .. } => authority,
            ConnectUsing::InternalListener { synthetic_authority, .. } => synthetic_authority,
        }
    }

    pub fn with_bind_device(self, device: Option<BindDevice>) -> Self {
        match self {
            ConnectUsing::Socket { authority, timeout, .. } => {
                ConnectUsing::Socket { authority, bind_device: device, timeout }
            },
            internal @ ConnectUsing::InternalListener { .. } => internal,
        }
    }

    pub fn with_timeout(self, timeout: Option<Duration>) -> Self {
        match self {
            ConnectUsing::Socket { authority, bind_device, .. } => {
                ConnectUsing::Socket { authority, bind_device, timeout }
            },
            internal @ ConnectUsing::InternalListener { .. } => internal,
        }
    }

    pub fn from_address(
        address: &Address,
        bind_device: Option<BindDevice>,
        timeout: Option<Duration>,
    ) -> crate::Result<Self> {
        match address {
            Address::Socket(host, port) => {
                let authority = Authority::try_from(format!("{host}:{port}"))?;
                Ok(ConnectUsing::Socket { authority, bind_device, timeout })
            },
            Address::Internal(internal) => {
                let listener_name = internal.server_listener_name.to_static_str();
                let synthetic_authority = Authority::try_from(format!("{listener_name}.listener.internal:80"))?;
                Ok(ConnectUsing::InternalListener { listener_name, synthetic_authority })
            },
            Address::Pipe(path, _) => {
                Err(format!("Pipe addresses are not supported for upstream connections: {path}").into())
            },
        }
    }
}

pub struct TcpErrorContext {
    pub upstream_addr: SocketAddr,
    pub response_flags: ResponseFlags,
    pub cluster_name: &'static str,
}

#[derive(Clone, Debug)]
pub struct LocalConnectorWithDNSResolver {
    pub addr: Authority,
    pub cluster_name: &'static str,
    pub bind_device: Option<BindDevice>,
    pub timeout: Option<Duration>,
}

impl LocalConnectorWithDNSResolver {
    #[allow(clippy::too_many_lines)]
    pub fn connect(
        &self,
    ) -> impl Future<Output = std::result::Result<(TcpStream, &'static str), WithContext<ConnectError>>> + 'static {
        let addr = self.addr.clone();
        let device = self.bind_device.clone();
        let cluster_name = self.cluster_name;
        let connection_timeout = self.timeout;

        async move {
            let host = addr.host();
            let port = addr
                .port_u16()
                .ok_or(WithContext::new(io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    format!("Port has to be set {addr:?}"),
                )))
                .map_err(|e| {
                    // this error is difficult to categorize. It happens because the port is not set in the URI.
                    // The host has not been resolved yet, so we cannot provide a specific address.
                    e.with_context_data(TcpErrorContext {
                        upstream_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
                        response_flags: ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
                        cluster_name,
                    })
                    .map_into()
                })?;

            let addr = resolve(host, port).await.map_err(|e| {
                WithContext::new(e)
                    .with_context_data(TcpErrorContext {
                        upstream_addr: SocketAddr::from(([0, 0, 0, 0], port)),
                        response_flags: ResponseFlags::DNS_RESOLUTION_FAILED,
                        cluster_name,
                    })
                    .map_into()
            })?;

            let sock = match addr {
                std::net::SocketAddr::V4(_) => TcpSocket::new_v4().map_err(|e| {
                    WithContext::new(e)
                        .with_context_data(TcpErrorContext {
                            upstream_addr: addr,
                            response_flags: ResponseFlags::NO_HEALTHY_UPSTREAM
                                | ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
                            cluster_name,
                        })
                        .map_into()
                })?,
                std::net::SocketAddr::V6(_) => TcpSocket::new_v4().map_err(|e| {
                    WithContext::new(e)
                        .with_context_data(TcpErrorContext {
                            upstream_addr: addr,
                            response_flags: ResponseFlags::NO_HEALTHY_UPSTREAM
                                | ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
                            cluster_name,
                        })
                        .map_into()
                })?,
            };

            if let Some(device) = device {
                // binding might succeed here but still fail later
                // e.g. with an uncategorized error on connect
                debug!("Binding socket to: {:?}", device);
                super::bind_device::bind_device(&sock, &device).map_err(|e| {
                    WithContext::new(e)
                        .with_context_data(TcpErrorContext {
                            upstream_addr: addr,
                            response_flags: ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
                            cluster_name,
                        })
                        .map_into()
                })?;
            }

            let stream = if let Some(connection_timeout) = connection_timeout {
                fast_timeout(connection_timeout, sock.connect(addr))
                    .await // Result<Result<TcpStream, io::Error>>, Elapsed>
                    .map_err(|_| EventError::ConnectTimeout(elapsed()))
                    .map_err(|e| {
                        WithContext::new(e)
                            .with_context_data(TcpErrorContext {
                                upstream_addr: addr,
                                response_flags: ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
                                cluster_name,
                            })
                            .map_into()
                    })? // Result<TcpStream, io::Error>
                    .map_err(|orig| EventError::IoError(io::Error::new(orig.kind(), orig.to_string())))
                    .map_err(|e| {
                        WithContext::new(e)
                            .with_context_data(TcpErrorContext {
                                upstream_addr: addr,
                                response_flags: ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
                                cluster_name,
                            })
                            .map_into()
                    })?
            } else {
                sock.connect(addr)
                    .await
                    .map_err(|orig| EventError::IoError(io::Error::new(orig.kind(), orig.to_string())))
                    .map_err(|e| {
                        WithContext::new(e)
                            .with_context_data(TcpErrorContext {
                                upstream_addr: addr,
                                response_flags: ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
                                cluster_name,
                            })
                            .map_into()
                    })?
            };

            _ = stream.set_nodelay(true);
            _ = stream.set_quickack(true);

            Ok((stream, cluster_name))
        }
    }
}

impl Service<Uri> for LocalConnectorWithDNSResolver {
    type Response = TokioIo<TcpStream>;
    type Error = WithContext<ConnectError>;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // This connector is always ready, but others might not be.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Uri) -> Self::Future {
        let f = self.connect();
        Box::pin(async move { f.await.map(|(stream, _)| stream).map(TokioIo::new) })
    }
}

#[derive(Debug, Clone)]
pub struct InternalConnector {
    pub listener_name: &'static str,
    pub cluster_name: &'static str,
    pub is_http2: bool,
}

impl InternalConnector {
    pub async fn connect(
        &self,
        downstream_metadata: Option<Arc<DownstreamConnectionMetadata>>,
    ) -> std::result::Result<(AsyncStream, &'static str), WithContext<io::Error>> {
        debug!("Connecting to internal listener '{}' from cluster '{}'", self.listener_name, self.cluster_name);

        let sender = internal_registry::get_connection_sender_for_listener(self.listener_name).ok_or_else(|| {
            let err = io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("Internal listener '{}' not found or not ready", self.listener_name),
            );
            WithContext::new(err)
        })?;
        let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
        let downstream_metadata = downstream_metadata.unwrap_or_else(|| {
            Arc::new(DownstreamConnectionMetadata::FromSocket {
                peer_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
                local_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
            })
        });
        let internal_conn = InternalConnection {
            stream: Box::new(server_stream) as AsyncStream,
            downstream_metadata,
            start_instant: Instant::now(),
        };
        if let Err(e) = sender.send(internal_conn).await {
            let err = io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("Failed to send connection to internal listener '{}': {}", self.listener_name, e),
            );
            return Err(WithContext::new(err));
        }
        debug!("Successfully connected to internal listener '{}'", self.listener_name);

        Ok((Box::new(client_stream) as AsyncStream, self.cluster_name))
    }
}

#[derive(Clone, Debug)]
pub enum UnifiedConnector {
    Socket(LocalConnectorWithDNSResolver),
    Internal(InternalConnector),
}

impl From<(&ConnectUsing, &'static str, bool)> for UnifiedConnector {
    fn from((target, cluster_name, is_http2): (&ConnectUsing, &'static str, bool)) -> Self {
        match target {
            ConnectUsing::Socket { authority, bind_device, timeout } => {
                UnifiedConnector::Socket(LocalConnectorWithDNSResolver {
                    addr: authority.clone(),
                    cluster_name,
                    bind_device: bind_device.clone(),
                    timeout: *timeout,
                })
            },
            ConnectUsing::InternalListener { listener_name, .. } => {
                debug!("Creating InternalConnector for listener '{}' with is_http2={})", listener_name, is_http2,);
                UnifiedConnector::Internal(InternalConnector { listener_name, cluster_name, is_http2 })
            },
        }
    }
}

impl From<(&ConnectUsing, &'static str)> for UnifiedConnector {
    fn from((target, cluster_name): (&ConnectUsing, &'static str)) -> Self {
        Self::from((target, cluster_name, false))
    }
}

impl Service<Uri> for UnifiedConnector {
    type Response = HttpConnection;
    type Error = WithContext<ConnectError>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            UnifiedConnector::Socket(c) => c.poll_ready(cx),
            UnifiedConnector::Internal(_) => Poll::Ready(Ok(())),
        }
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        match self {
            UnifiedConnector::Socket(c) => {
                let fut = c.call(uri);
                Box::pin(async move {
                    let stream = fut.await?;
                    let tcp_stream = stream.into_inner();
                    Ok(HttpConnection::new(TokioIo::new(Box::new(tcp_stream) as AsyncStream)))
                })
            },
            UnifiedConnector::Internal(c) => {
                let connector = c.clone();
                let is_http2 = c.is_http2;
                debug!("UnifiedConnector::call for internal listener '{}' with is_http2={}", c.listener_name, is_http2);
                Box::pin(async move {
                    let (stream, _cluster_name) = connector.connect(None).await.map_err(|e| {
                        let (io_err, _info) = e.into_inner();
                        WithContext::new(ConnectError::Io(io_err))
                    })?;
                    debug!("Internal connection established, using is_http2={}", is_http2);
                    if is_http2 {
                        Ok(HttpConnection::new_http2(TokioIo::new(stream)))
                    } else {
                        Ok(HttpConnection::new(TokioIo::new(stream)))
                    }
                })
            },
        }
    }
}
