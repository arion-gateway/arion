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

use std::{net::SocketAddr, sync::Arc};

use super::{
    connector::{ConnectUsing, UnifiedConnector},
    AsyncInstrumentedStream, UpstreamTransportSocketConfigurator,
};
use crate::{
    listeners::metadata::DownstreamConnectionMetadata,
    secrets::{TlsConfigurator, WantsToBuildClient},
    utils::instrumented_stream::InstrumentedStream,
};
use futures::future::BoxFuture;
use rustls::ClientConfig;
use tokio_rustls::TlsConnector;
use webpki::types::ServerName;

#[derive(Debug, Clone)]
pub struct TcpChannelConnector {
    connector: UnifiedConnector,
    transport_socket: UpstreamTransportSocketConfigurator,
}

pub struct TcpChannel {
    pub stream: AsyncInstrumentedStream,
    pub cluster_name: &'static str,
    pub upstream_local_addr: Option<SocketAddr>,
    pub upstream_peer_addr: Option<SocketAddr>,
}

impl TcpChannelConnector {
    pub fn new(
        target: &ConnectUsing,
        cluster_name: &'static str,
        transport_socket: UpstreamTransportSocketConfigurator,
    ) -> Self {
        let connector = UnifiedConnector::from((target, cluster_name));
        Self { connector, transport_socket }
    }

    pub fn connect(
        &self,
        connection_metadata: Option<&DownstreamConnectionMetadata>,
    ) -> BoxFuture<'static, crate::Result<TcpChannel>> {
        let connector = self.connector.clone();
        let transport_socket = self.transport_socket.clone();
        let connection_metadata = connection_metadata.cloned();

        Box::pin(async move {
            let is_internal = matches!(connector, UnifiedConnector::Internal(_));

            let (mut base_stream, cluster_name, upstream_local_addr, upstream_peer_addr) = match connector {
                UnifiedConnector::Socket(socket_connector) => {
                    let (tcp_stream, cluster_name) = socket_connector
                        .connect()
                        .await
                        .map_err(|e| -> crate::Error { format!("TCP connection failed: {e}").into() })?;

                    let upstream_local_addr = tcp_stream.local_addr().ok();
                    let upstream_peer_addr = tcp_stream.peer_addr().ok();
                    let stream: AsyncInstrumentedStream = Box::new(InstrumentedStream::new(tcp_stream));

                    (stream, cluster_name, upstream_local_addr, upstream_peer_addr)
                },
                UnifiedConnector::Internal(internal_connector) => {
                    let (stream, cluster_name) = internal_connector
                        .connect(connection_metadata.clone().map(Arc::new))
                        .await
                        .map_err(|e| -> crate::Error { format!("Internal connection failed: {e}").into() })?;

                    (stream, cluster_name, None, None)
                },
            };

            let stream: AsyncInstrumentedStream = match &transport_socket {
                UpstreamTransportSocketConfigurator::Tls(tls_configurator) => {
                    configure_tls(tls_configurator, base_stream).await?
                },
                UpstreamTransportSocketConfigurator::ProxyProtocol(proxy_configurator) => {
                    if !is_internal {
                        if let Some(metadata) = &connection_metadata {
                            proxy_configurator.write_proxy_header(&mut base_stream, metadata).await.map_err(
                                |e| -> crate::Error { format!("Failed to write proxy protocol header: {e}").into() },
                            )?;
                        }
                    }
                    if let Some(inner_tls) = &proxy_configurator.inner_tls_configurator {
                        configure_tls(inner_tls, base_stream).await?
                    } else {
                        base_stream
                    }
                },
                UpstreamTransportSocketConfigurator::None => base_stream,
            };

            Ok(TcpChannel { stream, cluster_name, upstream_local_addr, upstream_peer_addr })
        })
    }
}

async fn configure_tls(
    tls_config: &TlsConfigurator<ClientConfig, WantsToBuildClient>,
    stream: AsyncInstrumentedStream,
) -> crate::Result<AsyncInstrumentedStream> {
    let client_config = tls_config.clone().into_inner();
    let server_name = ServerName::try_from(tls_config.sni())
        .map_err(|e| -> crate::Error { format!("Invalid server name: {e}").into() })?;
    let tls_connector = TlsConnector::from(Arc::new(client_config));
    let tls_stream = tls_connector
        .connect(server_name, stream)
        .await
        .map_err(|e| -> crate::Error { format!("TLS connection failed: {e}").into() })?;
    Ok(Box::new(InstrumentedStream::new(tls_stream)))
}
