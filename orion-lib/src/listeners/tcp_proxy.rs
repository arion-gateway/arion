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

#[cfg(feature = "access-log")]
use {
    crate::access_log::Target, crate::with_access_log,
    orion_format::LogFormatter,
};

use crate::{
    clusters::clusters_manager::{self, RoutingContext},
    event_error::{
        find_error_in_chain, ConnectionTerminationDetails, ResponseCodeDetails, UpstreamTransportEventError,
    },
    listeners::metadata::DownstreamMetadata,
    transport::connector::TcpErrorContext,
    utils::tracked_stream::{ErrorSource, TrackedStream},
    AsyncInstrumentedStream, Result,
};
use orion_configuration::config::{
    access_log::AccessLog, cluster::ClusterSpecifier as ClusterSpecifierConfig,
    network_filters::tcp_proxy::TcpProxy as TcpProxyConfig,
};

#[cfg(feature = "access-log")]
use orion_format::context::{FinishContext, InitContext, SocketAddrContext, TcpContext, WireContext};

#[cfg(feature = "access-log")]
use std::time::Instant;

use orion_format::types::ResponseFlags;

use std::{fmt, net::SocketAddr};
use tracing::{debug, error};

#[derive(Debug, Clone)]
pub struct TcpProxy {
    pub listener_name: &'static str,
    pub filterchain_id: u64,
    cluster: ClusterSpecifierConfig,
    pub access_log: Vec<AccessLog>,
}

#[derive(Debug, Clone)]
pub struct TcpProxyBuilder {
    listener_name: Option<&'static str>,
    filterchain_id: Option<u64>,
    tcp_proxy_config: TcpProxyConfig,
}

impl From<TcpProxyConfig> for TcpProxyBuilder {
    fn from(tcp_proxy_config: TcpProxyConfig) -> Self {
        Self { tcp_proxy_config, filterchain_id: None, listener_name: None }
    }
}

impl TcpProxyBuilder {
    #[inline]
    pub fn with_listener_name(self, name: &'static str) -> Self {
        TcpProxyBuilder { listener_name: Some(name), ..self }
    }

    #[inline]
    pub fn with_filterchain_id(self, value: u64) -> Self {
        TcpProxyBuilder { filterchain_id: Some(value), ..self }
    }

    #[inline]
    pub fn build(self) -> Result<TcpProxy> {
        let listener_name = self.listener_name.unwrap_or("listener name is not set");
        let filterchain_id = self.filterchain_id.unwrap_or(0 as u64);
        let TcpProxyConfig { cluster_specifier, access_log } = self.tcp_proxy_config;
        Ok(TcpProxy { listener_name, filterchain_id, access_log, cluster: cluster_specifier })
    }
}

impl fmt::Display for TcpProxy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TcpProxy").field("name", &self.listener_name).finish()
    }
}

impl TcpProxy {
    #[allow(clippy::too_many_lines)]
    pub async fn serve_connection(&self, stream: AsyncInstrumentedStream, metadata: DownstreamMetadata) -> Result<()> {
        #[cfg(feature = "access-log")]
        let start_instant = Instant::now();

        #[cfg(feature = "access-log")]
        let mut access_loggers = self.access_log.iter().map(|al| al.get_logger().clone()).collect::<Vec<_>>();

        #[cfg(feature = "access-log")]
        with_access_log!(&mut access_loggers, InitContext { start_time: std::time::SystemTime::now() });

        let cluster_selector = &self.cluster;
        let cluster_id = clusters_manager::resolve_cluster(cluster_selector, None)
            .ok_or("Failed to resolve cluster from specifier")?;
        let maybe_connector = clusters_manager::get_tcp_connection(cluster_id, RoutingContext::None);

        let mut _bytes_received = 0;
        let mut _bytes_sent = 0;
        let mut _response_flags = ResponseFlags::empty();
        let mut _maybe_upstream_transport_error: Option<UpstreamTransportEventError> = None;
        let mut _maybe_response_code_details: Option<ResponseCodeDetails> = None;
        let mut _maybe_connection_termination_details: Option<ConnectionTerminationDetails> = None;
        let _maybe_upstream_local_addr: Option<SocketAddr>;
        let _maybe_upstream_peer_addr: Option<SocketAddr>;
        let _cluster_name: &str;

        let res = match maybe_connector {
            Ok(connector) => {
                let channel_result = connector.connect(Some(&metadata.connection)).await;
                match channel_result {
                    Ok(channel) => {
                        _maybe_upstream_local_addr = channel.upstream_local_addr;
                        _maybe_upstream_peer_addr = channel.upstream_peer_addr;

                        let mut down_stream = TrackedStream::new(stream);
                        let mut up_stream = TrackedStream::new(channel.stream);

                        let res = tokio::io::copy_bidirectional(&mut down_stream, &mut up_stream).await;
                        match res {
                            Ok((received, sent)) => {
                                _bytes_received = received;
                                _bytes_sent = sent;
                            },
                            Err(ref e) => {
                                debug!("Error with TCP stream: {}", e);
                                if !matches!(down_stream.error_source, ErrorSource::None) {
                                    // downstream error
                                    _maybe_connection_termination_details = Some(ConnectionTerminationDetails::from(e));
                                } else if !matches!(up_stream.error_source, ErrorSource::None) {
                                    // upstream error
                                    _maybe_upstream_transport_error = Some(e.into());
                                }
                                // information related to both upstream and downstream (l7)
                                _maybe_response_code_details = Some(ResponseCodeDetails::from(e));
                                _response_flags.insert(ResponseFlags::UPSTREAM_CONNECTION_FAILURE);
                            },
                        }

                        #[cfg(feature = "access-log")]
                        with_access_log!(
                            &mut access_loggers,
                            TcpContext {
                                socket_address: SocketAddrContext {
                                    downstream_local_addr: Some(metadata.connection.local_address()),
                                    downstream_peer_addr: Some(metadata.connection.peer_address()),
                                    upstream_local_addr: _maybe_upstream_local_addr,
                                    upstream_peer_addr: _maybe_upstream_peer_addr,
                                },
                                cluster_name: channel.cluster_name,
                            }
                        );

                        Ok(())
                    },
                    Err(e) => {
                        _response_flags.insert(ResponseFlags::UPSTREAM_CONNECTION_FAILURE);

                        if let Some(tcp_error) = e.get_context_data::<TcpErrorContext>() {
                            _maybe_upstream_peer_addr = Some(tcp_error.upstream_addr);
                            _response_flags = tcp_error.response_flags.clone();
                            _cluster_name = tcp_error.cluster_name;
                        } else {
                            // impossible case to make the compiler happy...
                            _maybe_upstream_peer_addr = None;
                            _cluster_name = "-";
                        }

                        let io_err = find_error_in_chain::<std::io::Error>(e.inner());
                        _maybe_upstream_transport_error = io_err.map(UpstreamTransportEventError::from);
                        _maybe_response_code_details = io_err.map(ResponseCodeDetails::from);

                        #[cfg(feature = "access-log")]
                        with_access_log!(
                            &mut access_loggers,
                            TcpContext {
                                socket_address: SocketAddrContext {
                                    downstream_local_addr: Some(metadata.connection.local_address()),
                                    downstream_peer_addr: Some(metadata.connection.peer_address()),
                                    upstream_local_addr: None,
                                    upstream_peer_addr: _maybe_upstream_peer_addr,
                                },
                                cluster_name: _cluster_name,
                            }
                        );

                        Err(e)
                    },
                }
            },
            Err(e) => {
                error!("Failed to get TCP connection for cluster {:?}: {}", cluster_selector, e);
                _response_flags.insert(ResponseFlags::NO_ROUTE_FOUND);

                let io_err = find_error_in_chain::<std::io::Error>(e.inner());
                _maybe_upstream_transport_error = io_err.map(UpstreamTransportEventError::from);
                _maybe_response_code_details = io_err.map(ResponseCodeDetails::from);

                #[cfg(feature = "access-log")]
                with_access_log!(
                    &mut access_loggers,
                    TcpContext {
                        socket_address: SocketAddrContext {
                            downstream_local_addr: Some(metadata.connection.local_address()),
                            downstream_peer_addr: Some(metadata.connection.peer_address()),
                            upstream_local_addr: None,
                            upstream_peer_addr: None,
                        },
                        cluster_name: &cluster_selector.name(),
                    }
                );

                Err(e)
            },
        };

        #[cfg(feature = "access-log")]
        with_access_log!(
            &mut access_loggers,
            FinishContext {
                duration: start_instant.elapsed(),
                bytes_received: _bytes_received,
                bytes_sent: _bytes_sent,
                response_flags: _response_flags,
                upstream_failure: _maybe_upstream_transport_error.as_ref().map(|x| x.0),
                response_code_details: _maybe_response_code_details.as_ref().map(|x| x.0),
                connection_termination_details: _maybe_connection_termination_details.as_ref().map(|x| x.0),
            }
        );

        #[cfg(feature = "access-log")]
        with_access_log!(
            &mut access_loggers,
            WireContext { wire_bytes_received: _bytes_received, wire_bytes_sent: _bytes_sent }
        );

        #[cfg(feature = "access-log")]
        {
            use crate::access_log::log_access_blocking;
            let messages = access_loggers.into_iter().map(LogFormatter::into_message).collect::<Vec<_>>();
            log_access_blocking(
                Target::ListenerFilterChain(self.listener_name.into(), self.filterchain_id),
                messages,
            );
        }
        res
    }
}
