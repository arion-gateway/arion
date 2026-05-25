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
    crate::access_log::Target,
    crate::event_error::{
        find_error_in_chain, ConnectionTerminationDetails, ResponseCodeDetails, UpstreamTransportEventError,
    },
    crate::transport::connector::TcpErrorContext,
    crate::with_access_log,
    orion_format::context::{FinishContext, InitContext, SocketAddrContext, TcpContext, WireContext},
    orion_format::types::ResponseFlags,
    orion_format::LogFormatter,
    std::time::Instant,
};

use crate::{
    clusters::clusters_manager::{self, RoutingContext},
    listeners::metadata::DownstreamMetadata,
    utils::instrumented_stream::InstrumentedStream,
    with_metric, AsyncInstrumentedStream, Result,
};
use orion_configuration::config::{
    access_log::AccessLog, cluster::ClusterSpecifier as ClusterSpecifierConfig,
    network_filters::tcp_proxy::TcpProxy as TcpProxyConfig,
};

#[cfg(feature = "metrics")]
use {
    crate::get_shard_id,
    opentelemetry::KeyValue,
    orion_metrics::metrics::{clusters, tcp, user},
};

use std::{fmt, net::SocketAddr};
use tracing::error;

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
    pub fn build(self) -> TcpProxy {
        let listener_name = self.listener_name.unwrap_or("listener name is not set");
        let filterchain_id = self.filterchain_id.unwrap_or(0_u64);
        let TcpProxyConfig { cluster_specifier, access_log } = self.tcp_proxy_config;
        TcpProxy { listener_name, filterchain_id, access_log, cluster: cluster_specifier }
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

        #[allow(unused_variables, unused_mut, unused_assignments)]
        let mut bytes_received_down = 0;
        #[allow(unused_variables, unused_mut, unused_assignments)]
        let mut bytes_sent_down = 0;
        #[cfg(feature = "metrics")]
        let bytes_received_up;
        #[cfg(feature = "metrics")]
        let bytes_sent_up;

        #[cfg(feature = "access-log")]
        let mut response_flags = ResponseFlags::empty();
        #[cfg(feature = "access-log")]
        let mut maybe_upstream_transport_failure_reason: Option<UpstreamTransportEventError> = None;
        #[cfg(feature = "access-log")]
        let mut maybe_response_code_details: Option<ResponseCodeDetails> = None;
        #[cfg(feature = "access-log")]
        let mut maybe_connection_termination_details: Option<ConnectionTerminationDetails> = None;
        #[allow(unused_variables)]
        let maybe_upstream_local_addr: Option<SocketAddr>;
        #[allow(unused_variables)]
        let maybe_upstream_peer_addr: Option<SocketAddr>;
        #[allow(unused_variables)]
        let cluster_name: &str;

        #[cfg(feature = "metrics")]
        let shard_id = get_shard_id!();

        let res = match maybe_connector {
            Ok(connector) => {
                let channel_result = connector.connect(Some(&metadata.connection)).await;
                match channel_result {
                    Ok(channel) => {
                        #[cfg(feature = "access-log")]
                        {
                            maybe_upstream_local_addr = channel.upstream_local_addr;
                            maybe_upstream_peer_addr = channel.upstream_peer_addr
                        }

                        let mut downstream = InstrumentedStream::new(stream);
                        let mut upstream = InstrumentedStream::new(channel.stream);

                        #[allow(unused_variables)]
                        let res = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;

                        #[cfg(feature = "metrics")]
                        {
                            bytes_received_down = downstream.metrics().bytes_read();
                            bytes_sent_down = downstream.metrics().bytes_written();
                            bytes_received_up = upstream.metrics().bytes_read();
                            bytes_sent_up = upstream.metrics().bytes_written()
                        }

                        #[cfg(feature = "access-log")]
                        if let Err(ref e) = res {
                            if downstream.error().is_some() {
                                // downstream error
                                maybe_connection_termination_details = Some(ConnectionTerminationDetails::from(e));
                            } else if upstream.error().is_some() {
                                // upstream error
                                maybe_upstream_transport_failure_reason = Some(e.into());
                            }
                            // information related to both upstream and downstream (l7)
                            maybe_response_code_details = Some(ResponseCodeDetails::from(e));
                            response_flags.insert(ResponseFlags::UPSTREAM_CONNECTION_FAILURE);
                        }

                        with_metric!(
                            clusters::UPSTREAM_CX_RX_BYTES_TOTAL,
                            add,
                            bytes_received_up,
                            shard_id,
                            &[KeyValue::new("cluster", channel.cluster_name)]
                        );

                        with_metric!(
                            clusters::UPSTREAM_CX_TX_BYTES_TOTAL,
                            add,
                            bytes_sent_up,
                            shard_id,
                            &[KeyValue::new("cluster", channel.cluster_name)]
                        );

                        #[cfg(feature = "metrics")]
                        {
                            let user_partition_key = crate::metrics::extract_user_partition_key(
                                (&http::HeaderMap::new(), metadata.sni.as_ref()),
                                crate::metrics::USER_KEY.source(),
                            );

                            if let Some(user_partition_key) = user_partition_key {
                                with_metric!(
                                    user::BYTES_RX,
                                    add,
                                    bytes_received_down,
                                    shard_id,
                                    &[
                                        KeyValue::new(
                                            crate::metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                            user_partition_key
                                        ),
                                        KeyValue::new("listener", metadata.listener_name)
                                    ]
                                );
                                with_metric!(
                                    user::BYTES_TX,
                                    add,
                                    bytes_sent_down,
                                    shard_id,
                                    &[
                                        KeyValue::new(
                                            crate::metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                            user_partition_key
                                        ),
                                        KeyValue::new("listener", metadata.listener_name)
                                    ]
                                );
                            }
                        }

                        #[cfg(feature = "access-log")]
                        with_access_log!(
                            &mut access_loggers,
                            TcpContext {
                                socket_address: SocketAddrContext {
                                    downstream_local_addr: Some(metadata.connection.local_address()),
                                    downstream_peer_addr: Some(metadata.connection.peer_address()),
                                    upstream_local_addr: maybe_upstream_local_addr,
                                    upstream_peer_addr: maybe_upstream_peer_addr,
                                },
                                cluster_name: channel.cluster_name,
                            }
                        );

                        Ok(())
                    },
                    Err(e) => {
                        #[cfg(feature = "access-log")]
                        {
                            response_flags.insert(ResponseFlags::UPSTREAM_CONNECTION_FAILURE);

                            if let Some(tcp_error) = e.get_context_data::<TcpErrorContext>() {
                                maybe_upstream_peer_addr = Some(tcp_error.upstream_addr);
                                response_flags = tcp_error.response_flags;
                                cluster_name = tcp_error.cluster_name;
                            } else {
                                // impossible case to make the compiler happy...
                                maybe_upstream_peer_addr = None;
                                cluster_name = "-";
                            }

                            let io_err = find_error_in_chain::<std::io::Error>(e.inner());
                            maybe_upstream_transport_failure_reason = io_err.map(UpstreamTransportEventError::from);
                            maybe_response_code_details = io_err.map(ResponseCodeDetails::from);
                            with_access_log!(
                                &mut access_loggers,
                                TcpContext {
                                    socket_address: SocketAddrContext {
                                        downstream_local_addr: Some(metadata.connection.local_address()),
                                        downstream_peer_addr: Some(metadata.connection.peer_address()),
                                        upstream_local_addr: None,
                                        upstream_peer_addr: maybe_upstream_peer_addr,
                                    },
                                    cluster_name,
                                }
                            )
                        };

                        Err(e)
                    },
                }
            },
            Err(e) => {
                error!("Failed to get TCP connection for cluster {:?}: {}", cluster_selector, e);
                #[cfg(feature = "access-log")]
                {
                    response_flags.insert(ResponseFlags::NO_ROUTE_FOUND);

                    let io_err = find_error_in_chain::<std::io::Error>(e.inner());
                    maybe_upstream_transport_failure_reason = io_err.map(UpstreamTransportEventError::from);
                    maybe_response_code_details = io_err.map(ResponseCodeDetails::from);

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
                    )
                };

                Err(e)
            },
        };

        #[cfg(feature = "access-log")]
        with_access_log!(
            &mut access_loggers,
            FinishContext {
                duration: start_instant.elapsed(),
                bytes_received: bytes_received_down,
                bytes_sent: bytes_sent_down,
                response_flags,
                upstream_transport_failure_reason: maybe_upstream_transport_failure_reason.as_ref().map(|x| x.0),
                response_code_details: maybe_response_code_details.as_ref().map(|x| x.0),
                connection_termination_details: maybe_connection_termination_details.as_ref().map(|x| x.0),
            }
        );

        #[cfg(feature = "access-log")]
        with_access_log!(
            &mut access_loggers,
            WireContext { wire_bytes_received: bytes_received_down, wire_bytes_sent: bytes_sent_down }
        );

        #[cfg(feature = "access-log")]
        {
            use crate::access_log::log_access_blocking;
            let messages = access_loggers.into_iter().map(LogFormatter::into_message).collect::<Vec<_>>();
            log_access_blocking(Target::ListenerFilterChain(self.listener_name.into(), self.filterchain_id), messages)
        }
        res
    }
}
