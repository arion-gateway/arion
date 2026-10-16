// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

use super::{
    filterchain::{ConnectionHandler, FilterchainBuilder, FilterchainType},
    filterchain_matcher::FilterChains,
    internal_registry::{self, InternalConnection},
    listeners_manager::TlsContextChange,
};
#[cfg(feature = "instrumentation")]
use crate::instrumentation;

#[cfg(any(feature = "access-log", feature = "metrics"))]
use crate::utils::StreamMetrics;

#[cfg(feature = "access-log")]
use {
    crate::access_log::{Target, blocking_log_access},
    arion_format::context::SocketAddrContext,
    arion_format::{LogFormatter, context::ConnectionContext},
};

#[cfg(feature = "metrics")]
use crate::get_shard_id;

use crate::{
    AsyncInstrumentedStream, ConversionContext, Error, Result, RouteConfigurationChange,
    listeners::{
        http_connection_manager::mcp_gateway::mcp::McpGatewayListenerContext,
        metadata::{DownstreamConnectionMetadata, DownstreamMetadata},
        rate_limiter::local_rate_limiter::ListenerLocalRateLimit,
    },
    secrets::{TlsConfigurator, WantsToBuildServer},
    transport::{ProxyProtocolReader, bind_device::BindDevice, tls_inspector},
    utils::instrumented_stream::InstrumentedStream,
};

use arion_configuration::config::{
    access_log::AccessLog,
    listener::{Listener as ListenerConfig, ListenerType},
    listener_filters::{DownstreamProxyProtocolConfig, ListenerLocalRateLimitConfig},
};
use arion_interner::InternedStr;
#[cfg(test)]
use arion_interner::StringInterner;
#[cfg(feature = "metrics")]
use arion_metrics::metrics::filters;
use tokio::sync::mpsc;

#[cfg(feature = "access-log")]
use crate::with_access_log;

#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;
use owning_ref::ArcRef;

use crate::{with_histogram, with_metric};
#[cfg(feature = "metrics")]
use arion_metrics::metrics::{http, listeners, tcp};
use papaya::HashMap as PapayaMap;

use rustls::ServerConfig;
use scopeguard::defer;
#[cfg(any(feature = "access-log", feature = "metrics"))]
use std::time::Duration;
use std::{
    fmt::Debug,
    net::SocketAddr,
    sync::{
        Arc as StdArc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::{
    net::{TcpListener, TcpSocket},
    sync::broadcast::{self},
};
use tracing::{debug, info, warn};
use triomphe::Arc;

#[derive(Debug, Clone)]
enum ListenerBinding {
    Socket { address: SocketAddr, bind_device: Option<BindDevice>, tcp_backlog_size: u32 },
    Internal,
}

enum ConnectionSource {
    Socket {
        local_address: SocketAddr,
        peer_addr: SocketAddr,
        proxy_protocol_config: Option<Arc<DownstreamProxyProtocolConfig>>,
    },
    Internal {
        metadata: Arc<DownstreamConnectionMetadata>,
    },
}

#[derive(Debug, Clone)]
struct PartialListener {
    name: InternedStr,
    binding: ListenerBinding,
    filter_chains: FilterChains<FilterchainBuilder>,
    with_tls_inspector: bool,
    proxy_protocol_config: Option<DownstreamProxyProtocolConfig>,
    listener_local_rate_limit_config: Option<ListenerLocalRateLimitConfig>,
    access_log: Vec<AccessLog>,
}

#[derive(Debug, Clone)]
pub struct ListenerFactory {
    listener: PartialListener,
}

impl TryFrom<ConversionContext<'_, ListenerConfig>> for PartialListener {
    type Error = Error;
    fn try_from(ctx: ConversionContext<'_, ListenerConfig>) -> std::result::Result<Self, Self::Error> {
        let ConversionContext { envoy_object: listener, secret_manager } = ctx;
        let with_tls_inspector = listener.with_tls_inspector;
        let proxy_protocol_config = listener.proxy_protocol_config;
        let listener_local_rate_limit_config = listener.listener_local_rate_limit_config;
        let access_log = listener.access_log;
        debug!("Listener {} :TLS Inspector is {with_tls_inspector}", listener.name);

        let binding = match listener.listener_type {
            ListenerType::Socket { address, bind_device } => {
                ListenerBinding::Socket { address, bind_device, tcp_backlog_size: listener.tcp_backlog_size }
            },
            ListenerType::Internal { .. } => ListenerBinding::Internal,
        };

        let filter_chains: FilterChains<_> = listener
            .filter_chains
            .into_iter()
            .map(|f| FilterchainBuilder::try_from(ConversionContext::new((f.1, secret_manager))).map(|x| (f.0, x)))
            .collect::<Result<_>>()?;

        if !with_tls_inspector && filter_chains.has_server_names() {
            return Err((format!(
                "Listener '{}' has server_names in filter_chain_match, but no TLS inspector so matches would always fail",
                listener.name
            ))
            .into());
        }

        Ok(PartialListener {
            name: listener.name,
            binding,
            filter_chains,
            with_tls_inspector,
            proxy_protocol_config,
            listener_local_rate_limit_config,
            access_log,
        })
    }
}

impl ListenerFactory {
    pub fn into_listener(
        self,
        route_updates_receiver: broadcast::Receiver<RouteConfigurationChange>,
        secret_updates_receiver: broadcast::Receiver<TlsContextChange>,
    ) -> Result<Listener> {
        let PartialListener {
            name,
            binding,
            filter_chains,
            with_tls_inspector,
            proxy_protocol_config,
            listener_local_rate_limit_config,
            access_log,
        } = self.listener;

        let filter_chains = filter_chains.try_map(|fc| fc.with_listener_name(name).build())?;

        let listener_local_rate_limit = listener_local_rate_limit_config.map(Into::into);

        Ok(Listener {
            name,
            binding,
            filter_chains,
            with_tls_inspector,
            proxy_protocol_config,
            local_rate_limit: listener_local_rate_limit,
            route_updates_receiver,
            secret_updates_receiver,
            access_log,
        })
    }
}

impl TryFrom<ConversionContext<'_, ListenerConfig>> for ListenerFactory {
    type Error = Error;
    fn try_from(ctx: ConversionContext<'_, ListenerConfig>) -> std::result::Result<Self, Self::Error> {
        let listener = PartialListener::try_from(ctx)?;
        Ok(Self { listener })
    }
}

#[derive(Debug, Default)]
pub struct ListenerContext {
    pub mcp: McpGatewayListenerContext,
}

static LISTENERS_CONTEXT: OnceLock<PapayaMap<&'static str, StdArc<ListenerContext>, ahash::RandomState>> =
    OnceLock::new();

pub trait FilterListenerContext {
    fn get_filter_context(listener_name: InternedStr) -> ArcRef<ListenerContext, Self>;
}

impl FilterListenerContext for McpGatewayListenerContext {
    #[inline]
    fn get_filter_context(listener_name: InternedStr) -> ArcRef<ListenerContext, McpGatewayListenerContext> {
        let ctx = get_listener_context(listener_name);
        ArcRef::new(ctx).map(|ctx| &ctx.mcp)
    }
}

#[inline]
fn get_listener_context(listener_name: InternedStr) -> StdArc<ListenerContext> {
    let map = LISTENERS_CONTEXT.get_or_init(|| PapayaMap::with_hasher(ahash::RandomState::new()));
    StdArc::clone(map.pin().get_or_insert_with(listener_name.as_str(), || StdArc::new(ListenerContext::default())))
}

#[derive(Debug)]
pub struct Listener {
    name: InternedStr,
    binding: ListenerBinding,
    pub filter_chains: FilterChains<FilterchainType>,
    with_tls_inspector: bool,
    proxy_protocol_config: Option<DownstreamProxyProtocolConfig>,
    local_rate_limit: Option<ListenerLocalRateLimit>,
    route_updates_receiver: broadcast::Receiver<RouteConfigurationChange>,
    secret_updates_receiver: broadcast::Receiver<TlsContextChange>,
    access_log: Vec<AccessLog>,
}

impl Listener {
    #[cfg(test)]
    pub(crate) fn test_listener<T: StringInterner + ?Sized>(
        name: &T,
        route_rx: broadcast::Receiver<RouteConfigurationChange>,
        secret_rx: broadcast::Receiver<TlsContextChange>,
    ) -> Self {
        use std::net::{IpAddr, Ipv4Addr};
        Listener {
            name: name.to_interned_str(),
            binding: ListenerBinding::Socket {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                bind_device: None,
                tcp_backlog_size: 128,
            },
            filter_chains: FilterChains::default(),
            with_tls_inspector: false,
            proxy_protocol_config: None,
            local_rate_limit: None,
            route_updates_receiver: route_rx,
            secret_updates_receiver: secret_rx,
            access_log: vec![],
        }
    }

    pub fn get_name(&self) -> InternedStr {
        self.name
    }

    pub fn get_socket(&self) -> Option<(&std::net::SocketAddr, Option<&BindDevice>)> {
        match &self.binding {
            ListenerBinding::Socket { address, bind_device, .. } => Some((address, bind_device.as_ref())),
            ListenerBinding::Internal => None,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn start(self) -> Error {
        let Self {
            name,
            binding,
            filter_chains,
            with_tls_inspector,
            proxy_protocol_config,
            local_rate_limit: listener_local_rate_limit,
            mut route_updates_receiver,
            mut secret_updates_receiver,
            #[allow(unused_variables)]
            access_log,
        } = self;

        let mut filter_chains = Arc::new(filter_chains);
        let proxy_protocol_config = proxy_protocol_config.map(Arc::new);
        #[allow(unused_variables)]
        let listener_name = name;

        match binding {
            ListenerBinding::Socket { address, bind_device, tcp_backlog_size } => {
                let listener = match configure_and_start_tcp_listener(address, bind_device.as_ref(), tcp_backlog_size) {
                    Ok(x) => x,
                    Err(e) => return e,
                };

                let actual_address = listener.local_addr().unwrap_or(address);
                info!("listener '{name}' started: {actual_address}");

                #[cfg(feature = "instrumentation")]
                let clock = quanta::Clock::new();

                loop {
                    tokio::select! {
                        biased;
                        maybe_stream = listener.accept() => {
                            match maybe_stream {
                                Ok((stream, peer_addr)) => {

                                    if let Some(rate_limiter) = &listener_local_rate_limit {
                                        if !rate_limiter.allow() {
                                            debug!("Connection from {} rate-limited", peer_addr);
                                            #[cfg(feature = "metrics")]
                                            with_metric!(
                                                filters::CONNECTION_RATE_LIMIT,
                                                add,
                                                1,
                                                get_shard_id!(),
                                                &[
                                                    KeyValue::new("listener", listener_name.as_str()),
                                                    KeyValue::new("filter", rate_limiter.stat_prefix.as_str()),
                                                    KeyValue::new("result", filters::EVENT_RATE_LIMITED),
                                                ]
                                            );
                                            continue;
                                        }
                                        #[cfg(feature = "metrics")]
                                        with_metric!(
                                            filters::CONNECTION_RATE_LIMIT,
                                            add,
                                            1,
                                            get_shard_id!(),
                                            &[
                                                KeyValue::new("listener", listener_name.as_str()),
                                                KeyValue::new("filter", rate_limiter.stat_prefix.as_str()),
                                                KeyValue::new("result", filters::EVENT_OK)
                                            ]
                                        );
                                    }
                                    let local_address = stream.local_addr().ok();

                                    #[cfg(feature = "instrumentation")]
                                    instrumentation::metrics::CONNECTIONS.add(1);

                                    #[cfg(feature = "instrumentation")]
                                    let start_clock = clock.raw();

                                    let filter_chains = Arc::clone(&filter_chains);
                                    let proxy_protocol_config = proxy_protocol_config.clone();

                                    #[cfg(feature = "access-log")]
                                    let mut conn_formatters : Vec<_> = access_log.iter().map(AccessLog::get_logger).cloned().collect();

                                    tokio::spawn(async move {
                                        let start = Instant::now();

                                        #[cfg(feature = "access-log")]
                                        let start_time = std::time::SystemTime::now();

                                        #[cfg(target_os = "linux")]
                                        {
                                            _ = stream.set_nodelay(true);
                                            _ = stream.set_quickack(true);
                                        }

                                        #[cfg(feature = "metrics")]
                                        let shard_id = get_shard_id!();

                                        #[cfg(any(feature = "access-log", feature = "metrics"))]
                                        let drop_cb = {
                                            #[cfg(feature = "access-log")]
                                            let downstream_peer_addr = Some(peer_addr);
                                            #[cfg(feature = "access-log")]
                                            let downstream_local_addr = local_address;

                                            Box::new(
                                            move |metrics: &StreamMetrics, _idle: Duration| {
                                                #[cfg(feature = "metrics")]
                                                {
                                                    with_metric!(
                                                        tcp::CX_RX_BYTES_RECEIVED,
                                                        add,
                                                        metrics.bytes_read(),
                                                        shard_id,
                                                        &[KeyValue::new("listener", listener_name.as_str())]
                                                    );

                                                    with_metric!(
                                                        tcp::CX_TX_BYTES_SENT,
                                                        add,
                                                        metrics.bytes_written(),
                                                        shard_id,
                                                        &[KeyValue::new("listener", listener_name.as_str())]
                                                    );

                                                    ()
                                                }
                                                #[cfg(feature = "access-log")]
                                                {
                                                    let err_descr = metrics.error().map(ToString::to_string);
                                                    with_access_log!(&mut conn_formatters, ConnectionContext::<'_> {
                                                        start_time,
                                                        duration: start.elapsed(),
                                                        wire_bytes_received: metrics.bytes_read(),
                                                        wire_bytes_sent: metrics.bytes_written(),
                                                        connection_termination_details: err_descr.as_deref(),
                                                    });

                                                   with_access_log!(&mut conn_formatters, SocketAddrContext {
                                                       downstream_local_addr,
                                                       downstream_peer_addr,
                                                       upstream_local_addr: None,
                                                       upstream_peer_addr: None });

                                                   if !conn_formatters.is_empty() {
                                                       let messages = conn_formatters.into_iter().map(LogFormatter::into_message).collect::<Vec<_>>();
                                                       _ = blocking_log_access(Target::Listener(listener_name), messages);
                                                   }
                                               }
                                            })
                                        };

                                        let stream = InstrumentedStream::new(stream);
                                        #[cfg(any(feature = "access-log", feature = "metrics"))]
                                        {
                                           stream.metrics().with_drop_fn(drop_cb)
                                        }

                                        with_metric!(listeners::DOWNSTREAM_CX_TOTAL, add, 1, shard_id,&[KeyValue::new("listener", listener_name.as_str())]);
                                        with_metric!(listeners::DOWNSTREAM_CX_ACTIVE, add, 1, shard_id,&[KeyValue::new("listener", listener_name.as_str())]);

                                        _ = Self::process_connection(
                                            name,
                                            filter_chains,
                                            with_tls_inspector,
                                            ConnectionSource::Socket { local_address: local_address.unwrap_or(address), peer_addr, proxy_protocol_config },
                                            stream.into(),
                                            start,
                                        ).await;
                                    });

                                    #[cfg(feature = "instrumentation")]
                                    {
                                        let nanos = clock.delta_as_nanos(start_clock, clock.raw());
                                        #[allow(clippy::cast_possible_truncation)]
                                        instrumentation::metrics::CONNECTION_SETUP_TIME.observe(nanos as usize);
                                    }
                                },
                                Err(e) => {
                                    warn!("failed to accept tcp connection: {e}");
                                }
                            }
                        },
                        maybe_route_update = route_updates_receiver.recv() => {
                            match maybe_route_update {
                                Ok(route_update) => {Self::process_route_update(name.as_str(), &filter_chains, route_update)},
                                Err(e) => {return e.into();}
                            }
                        },
                        maybe_secret_update = secret_updates_receiver.recv() => {
                            match maybe_secret_update {
                                Ok(secret_update) => {
                                    let mut filter_chains_clone = Arc::unwrap_or_clone(filter_chains);
                                    Self::process_secret_update(name.as_str(), &mut filter_chains_clone, secret_update);
                                    filter_chains = Arc::new(filter_chains_clone);
                                }
                                Err(e) => {return e.into();}
                            }
                        }
                    }
                }
            },
            ListenerBinding::Internal => {
                let (tx, mut rx) = mpsc::channel::<InternalConnection>(128);

                internal_registry::register(name, tx);
                scopeguard::defer! {
                    internal_registry::unregister(name.as_str());
                }
                info!("internal listener '{name}' started");

                loop {
                    tokio::select! {
                        biased;
                        maybe_connection = rx.recv() => {
                            match maybe_connection {
                                Some(InternalConnection { stream, downstream_metadata, start_instant }) => {
                                    #[cfg(feature = "metrics")]
                                    let shard_id = get_shard_id!();
                                    with_metric!(listeners::DOWNSTREAM_CX_TOTAL, add, 1, shard_id, &[KeyValue::new("listener", listener_name.as_str())]);
                                    with_metric!(listeners::DOWNSTREAM_CX_ACTIVE, add, 1, shard_id, &[KeyValue::new("listener", listener_name.as_str())]);

                                    let filter_chains = Arc::clone(&filter_chains);
                                    tokio::spawn(Self::process_connection(
                                        name,
                                        filter_chains,
                                        with_tls_inspector,
                                        ConnectionSource::Internal { metadata: downstream_metadata },
                                        stream,
                                        start_instant,
                                    ));
                                },
                                None => {
                                    return "Internal listener channel closed".into();
                                }
                            }
                        },
                        maybe_route_update = route_updates_receiver.recv() => {
                            match maybe_route_update {
                                Ok(route_update) => {Self::process_route_update(name.as_str(), &filter_chains, route_update)},
                                Err(e) => {return e.into();}
                            }
                        },
                        maybe_secret_update = secret_updates_receiver.recv() => {
                            match maybe_secret_update {
                                Ok(secret_update) => {
                                    let mut filter_chains_clone = Arc::unwrap_or_clone(filter_chains);
                                    Self::process_secret_update(name.as_str(), &mut filter_chains_clone, secret_update);
                                    filter_chains = Arc::new(filter_chains_clone);
                                }
                                Err(e) => {return e.into();}
                            }
                        }
                    }
                }
            },
        }
    }

    #[inline]
    fn select_filterchain<'a, T>(
        filter_chains: &'a FilterChains<T>,
        connection_metadata: &DownstreamConnectionMetadata,
        server_name: Option<&str>,
    ) -> Result<Option<&'a T>> {
        filter_chains.select(connection_metadata, server_name)
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    async fn process_connection(
        listener_name: InternedStr,
        filter_chains: Arc<FilterChains<FilterchainType>>,
        with_tls_inspector: bool,
        source: ConnectionSource,
        mut stream: AsyncInstrumentedStream,
        start_instant: std::time::Instant,
    ) -> Result<()> {
        #[cfg(feature = "metrics")]
        let shard_id = get_shard_id!();
        let ssl = AtomicBool::new(false);
        defer! {
            with_metric!(listeners::DOWNSTREAM_CX_DESTROY, add, 1, shard_id, &[KeyValue::new("listener", listener_name.as_str())]);
            with_metric!(listeners::DOWNSTREAM_CX_ACTIVE, sub, 1, shard_id, &[KeyValue::new("listener", listener_name.as_str())]);
            if ssl.load(Ordering::Relaxed) {
                with_metric!(http::DOWNSTREAM_CX_SSL_ACTIVE, sub, 1, shard_id, &[KeyValue::new("listener", listener_name.as_str())]);
            }
            with_histogram!(listeners::DOWNSTREAM_CX_LENGTH_MS, record,
                u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX),
                shard_id, &[KeyValue::new("listener", listener_name.as_str())]);
        }

        let connection_metadata = match source {
            ConnectionSource::Socket { local_address, peer_addr, proxy_protocol_config } => {
                if let Some(config) = proxy_protocol_config.as_ref() {
                    let reader = ProxyProtocolReader::new(Arc::clone(config));
                    let (metadata, new_stream) = reader.try_read_proxy_header(stream, local_address, peer_addr).await?;
                    stream = new_stream;
                    metadata
                } else {
                    DownstreamConnectionMetadata::FromSocket { peer_address: peer_addr, local_address }
                }
            },
            ConnectionSource::Internal { metadata } => (*metadata).clone(),
        };

        let sni = if with_tls_inspector {
            let (tls_result, rewound_stream) = tls_inspector::inspect_client_hello(stream).await;
            stream = rewound_stream;
            match tls_result {
                crate::transport::tls_inspector::InspectorResult::Success(sni) => {
                    debug!("{listener_name} : Detected TLS server name: {sni}");
                    with_metric!(
                        http::DOWNSTREAM_CX_SSL_TOTAL,
                        add,
                        1,
                        shard_id,
                        &[KeyValue::new("listener", listener_name.as_str())]
                    );
                    with_metric!(
                        http::DOWNSTREAM_CX_SSL_ACTIVE,
                        add,
                        1,
                        shard_id,
                        &[KeyValue::new("listener", listener_name.as_str())]
                    );
                    ssl.store(true, Ordering::Relaxed);
                    Some(sni)
                },
                crate::transport::tls_inspector::InspectorResult::SuccessNoSni => {
                    debug!("{listener_name} : No TLS server name indication present");
                    with_metric!(
                        http::DOWNSTREAM_CX_SSL_TOTAL,
                        add,
                        1,
                        shard_id,
                        &[KeyValue::new("listener", listener_name.as_str())]
                    );
                    with_metric!(
                        http::DOWNSTREAM_CX_SSL_ACTIVE,
                        add,
                        1,
                        shard_id,
                        &[KeyValue::new("listener", listener_name.as_str())]
                    );
                    ssl.store(true, Ordering::Relaxed);
                    None
                },
                crate::transport::tls_inspector::InspectorResult::TlsError(e) => {
                    debug!("{listener_name} : TLS inspector did not detect a TLS handshake at byte 0: {e}");
                    None
                },
            }
        } else {
            None
        };

        let selected_filterchain = Self::select_filterchain(&filter_chains, &connection_metadata, sni.as_deref())?;

        if let Some(filterchain) = selected_filterchain {
            debug!(
                "{listener_name} : mapping connection from {} to filter chain {}",
                connection_metadata.peer_address(),
                filterchain.filter_chain().name
            );
            // when we are done processing the connection the guard is dropped
            // and the internal counter is decremented
            let _cx_guard = filterchain.apply_connection_limit().await?;
            filterchain.apply_network_rate_limit(sni.as_ref(), listener_name).await?;
            if let Some(stream) = filterchain.apply_rbac(stream, &connection_metadata, sni.as_deref()) {
                return filterchain
                    .start_filterchain(
                        stream,
                        Arc::new(DownstreamMetadata::new(connection_metadata, sni, listener_name)),
                        start_instant,
                    )
                    .await;
            }
            debug!("{listener_name} : dropped connection from {} due to rbac", connection_metadata.peer_address());
        } else {
            with_metric!(
                listeners::NO_FILTER_CHAIN_MATCH,
                add,
                1,
                shard_id,
                &[KeyValue::new("listener", listener_name.as_str())]
            );
            warn!(
                "{listener_name} : No match for {} {}",
                connection_metadata.peer_address(),
                connection_metadata.local_address()
            );
        }
        Ok(())
    }

    //could secrets and routes also be updated through a CachedWatch?
    // they only need to be updated when they're read after all and could work with
    fn process_secret_update(
        listener_name: &str,
        filter_chains: &mut FilterChains<FilterchainType>,
        secret_update: TlsContextChange,
    ) {
        match secret_update {
            TlsContextChange::Updated((secret_id, secret)) => {
                for chain in filter_chains.values_mut() {
                    let filterchain = &mut chain.config;
                    if let Some(tls_configurator) = filterchain.tls_configurator.clone() {
                        let maybe_configurator = TlsConfigurator::<ServerConfig, WantsToBuildServer>::update(
                            tls_configurator,
                            &secret_id,
                            &secret,
                        );
                        if let Ok(new_tls_configurator) = maybe_configurator {
                            filterchain.tls_configurator = Some(new_tls_configurator);
                        } else {
                            let msg = format!(
                                "{listener_name} Couldn't update a secret for filterchain {} {:?}",
                                filterchain.name,
                                maybe_configurator.err()
                            );
                            warn!("{msg}");
                        }
                    }
                }
            },
        }
    }

    fn process_route_update(
        listener_name: &str,
        filter_chains: &FilterChains<FilterchainType>,
        route_update: RouteConfigurationChange,
    ) {
        match route_update {
            RouteConfigurationChange::Added((id, route), notify) => {
                let mut applied = false;
                for chain in filter_chains.values() {
                    if let ConnectionHandler::Http(http_manager) = &chain.handler {
                        let route_id = http_manager.get_route_id();
                        if let Some(route_id) = route_id {
                            if route_id == &id {
                                debug!("{listener_name} Route updated {id} {route:?}");
                                http_manager.update_route(route.clone());
                                applied = true;
                            }
                        } else {
                            debug!("{listener_name} Got route update but id doesn't match {route_id:?} {id}");
                        }
                    }
                }
                if applied && let Some(notify) = notify {
                    notify.notify_one();
                }
            },
            RouteConfigurationChange::Removed(id, notify) => {
                let mut applied = false;
                for chain in filter_chains.values() {
                    if let ConnectionHandler::Http(http_manager) = &chain.handler
                        && let Some(route_id) = http_manager.get_route_id()
                        && route_id == &id
                    {
                        http_manager.remove_route();
                        applied = true;
                    }
                }
                if applied && let Some(notify) = notify {
                    notify.notify_one();
                }
            },
        }
    }
}

fn configure_and_start_tcp_listener(
    addr: SocketAddr,
    device: Option<&BindDevice>,
    tcp_backlog_size: u32,
) -> Result<TcpListener> {
    let socket = if addr.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
    socket.set_reuseaddr(true)?;
    socket.set_keepalive(true)?;

    if let Some(device) = device {
        crate::transport::bind_device::bind_device(&socket, device)?;
    }

    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    socket.set_reuseport(true)?;
    socket.bind(addr)?;

    Ok(socket.listen(tcp_backlog_size)?)
}

#[cfg(test)]
mod tests {
    use arion_configuration::config::listener::{FilterChainMatch, FilterChainMatch as FilterChainMatchConfig, ServerNameMatch};
    use arion_data_plane_api::{
        decode::from_yaml, envoy_data_plane_api::envoy::config::listener::v3::FilterChainMatch as EnvoyFilterChainMatch,
    };

    use crate::SecretManager;

    use super::*;
    use arion_data_plane_api::envoy_data_plane_api::envoy::config::listener::v3::Listener as EnvoyListener;

    use std::{net::Ipv4Addr, str::FromStr};

    #[test]
    fn listener_bind_device() {
        const LISTENER: &str = r#"
name: listener_https
address:
  socket_address: { address: 0.0.0.0, port_value: 8443 }
filter_chains:
  - name: filter_chain
    filters:
      - name: https_gateway
        typedConfig:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          codec_type: HTTP1
          stat_prefix: http
          httpFilters:
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
              start_child_span: false
          route_config:
            name: basic_https_route
            virtual_hosts:
              - name: backend_https
                domains: ["*"]
socket_options:
  - description: "bind to interface virt1"
    level: 1
    name: 25
    # utf8 string 'virt1' bytes encoded as base64
    buf_value: dmlydDE=
"#;

        let envoy_listener: EnvoyListener = from_yaml(LISTENER).unwrap();
        let listener = envoy_listener.try_into().unwrap();
        let secrets_manager = SecretManager::new();
        let ctx = ConversionContext::new((listener, &secrets_manager));
        let l = PartialListener::try_from(ctx).unwrap();
        let expected_bind_device = Some(BindDevice::from_str("virt1").unwrap());

        match &l.binding {
            ListenerBinding::Socket { bind_device, .. } => assert_eq!(bind_device, &expected_bind_device),
            ListenerBinding::Internal => panic!("Expected socket listener"),
        }
    }

    #[test]
    fn match_fallback_sni() {
        let fcm = [
            (
                FilterChainMatch {
                    destination_port: None,
                    destination_prefix_ranges: Vec::new(),
                    server_names: vec![
                        ServerNameMatch::from_str("host1.test").unwrap(),
                        ServerNameMatch::from_str("host2.test").unwrap(),
                    ],
                    source_prefix_ranges: Vec::new(),
                    source_ports: Vec::new(),
                },
                0,
            ),
            (FilterChainMatch::default(), 1),
        ];
        let filter_chains: FilterChains<_> = fcm.iter().cloned().collect();
        let metadata = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 33000).into(),
            local_address: (Ipv4Addr::LOCALHOST, 8443).into(),
        };
        let selected = Listener::select_filterchain(&filter_chains, &metadata, None).unwrap();
        assert_eq!(selected.copied(), Some(1));
    }

    #[test]
    fn sni_match_without_inspector_fails() {
        const LISTENER: &str = r#"
name: listener_https
address:
  socket_address: { address: 0.0.0.0, port_value: 8443 }
filter_chains:
  - name: filter_chain_https1
    filter_chain_match:
      server_names: [hostname.example]
    filters:
      - name: https_gateway
        typedConfig:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          codec_type: HTTP1
          stat_prefix: http
          httpFilters:
            - name: envoy.filters.http.router
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
                start_child_span: false
          route_config:
            name: basic_https_route
            virtual_hosts:
              - name: backend_https
                domains: ["*"]
"#;

        let envoy_listener: EnvoyListener = from_yaml(LISTENER).unwrap();
        let listener = envoy_listener.try_into().unwrap();
        let secrets_man = SecretManager::new();

        let conv = ConversionContext { envoy_object: listener, secret_manager: &secrets_man };
        let r = PartialListener::try_from(conv);
        let err = r.unwrap_err();
        assert!(
            err.to_string()
                .contains("has server_names in filter_chain_match, but no TLS inspector so matches would always fail")
        );
    }

    #[test]
    fn filter_chain_multiple() {
        let m: EnvoyFilterChainMatch = from_yaml(
            "
        server_names: [host.test, \"*.wildcard\"]
        destination_port: 443
        source_ports: [3300]
        prefix_ranges: [{address_prefix: 127.0.0.1, prefix_len: 32}]
        ",
        )
        .unwrap();
        let m: FilterChains<_> = std::iter::once((m.try_into().unwrap(), ())).collect();
        let metadata = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 3300).into(),
            local_address: (Ipv4Addr::LOCALHOST, 443).into(),
        };

        assert!(matches!(Listener::select_filterchain(&m, &metadata, Some("host.test")), Ok(Some(()))));
        assert!(matches!(Listener::select_filterchain(&m, &metadata, Some("a.wildcard")), Ok(Some(()))));
        assert!(matches!(Listener::select_filterchain(&m, &metadata, None), Ok(None)));
    }

    #[test]
    fn most_specific_wins() {
        let l: EnvoyListener = from_yaml(
            "
        name: listener
        filter_chains:
        - filter_chain_match:
            server_names: [this.is.more.specific]
        - filter_chain_match:
            server_names: [\"*.more.specific\"]
        - filter_chain_match:
            server_names: [\"*.specific\"]
        - filter_chain_match:
            server_names: []
        ",
        )
        .unwrap();
        let m = l
            .filter_chains
            .into_iter()
            .enumerate()
            .map(|(i, fc)| {
                fc.filter_chain_match
                    .map(FilterChainMatchConfig::try_from)
                    .transpose()
                    .map(|x| (x.unwrap_or_default(), i))
            })
            .collect::<std::result::Result<FilterChains<_>, _>>()
            .unwrap();
        let metadata = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 33000).into(),
            local_address: (Ipv4Addr::LOCALHOST, 8443).into(),
        };
        assert_eq!(Listener::select_filterchain(&m, &metadata, None).unwrap().copied(), Some(3));
        assert_eq!(
            Listener::select_filterchain(&m, &metadata, Some("this.is.more.specific")).unwrap().copied(),
            Some(0)
        );
        assert_eq!(
            Listener::select_filterchain(&m, &metadata, Some("not.this.is.more.specific")).unwrap().copied(),
            Some(1)
        );
        assert_eq!(Listener::select_filterchain(&m, &metadata, Some("is.more.specific")).unwrap().copied(), Some(1));

        assert_eq!(Listener::select_filterchain(&m, &metadata, Some("more.specific")).unwrap().copied(), Some(2));
        assert_eq!(
            Listener::select_filterchain(&m, &metadata, Some("this.is.less.specific")).unwrap().copied(),
            Some(2)
        );
        assert_eq!(Listener::select_filterchain(&m, &metadata, Some("hello.world")).unwrap().copied(), Some(3));
    }

    #[test]
    fn hierarchy_and_lpm_precedence() {
        let l: EnvoyListener = from_yaml(
            "
        name: listener
        filter_chains:
        - filter_chain_match:
            destination_port: 8443
            prefix_ranges: [{address_prefix: 10.0.0.0, prefix_len: 8}]
            server_names: [app.example.com]
            source_prefix_ranges: [{address_prefix: 192.168.1.0, prefix_len: 24}]
            source_ports: [5000]
        - filter_chain_match:
            destination_port: 8443
            prefix_ranges: [{address_prefix: 10.1.0.0, prefix_len: 16}]
            server_names: [app.example.com]
        - filter_chain_match:
            destination_port: 8443
            prefix_ranges: [{address_prefix: 10.0.0.0, prefix_len: 8}]
            server_names: [app.example.com]
        - filter_chain_match:
            destination_port: 8443
        - filter_chain_match: {}
        ",
        )
        .unwrap();
        let m = l
            .filter_chains
            .into_iter()
            .enumerate()
            .map(|(i, fc)| {
                fc.filter_chain_match
                    .map(FilterChainMatchConfig::try_from)
                    .transpose()
                    .map(|x| (x.unwrap_or_default(), i))
            })
            .collect::<std::result::Result<FilterChains<_>, _>>()
            .unwrap();

        // 10.1.2.3 matches /16 (chain 1) over /8 (chains 0 and 2)
        let meta_16 = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::new(192, 168, 1, 10), 5000).into(),
            local_address: (Ipv4Addr::new(10, 1, 2, 3), 8443).into(),
        };
        assert_eq!(Listener::select_filterchain(&m, &meta_16, Some("app.example.com")).unwrap().copied(), Some(1));

        // 10.2.2.3 matches /8 (chains 0 and 2), and peer 192.168.1.10:5000 selects chain 0
        let meta_8_specific_src = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::new(192, 168, 1, 10), 5000).into(),
            local_address: (Ipv4Addr::new(10, 2, 2, 3), 8443).into(),
        };
        assert_eq!(
            Listener::select_filterchain(&m, &meta_8_specific_src, Some("app.example.com")).unwrap().copied(),
            Some(0)
        );

        // 10.2.2.3 matches /8 (chains 0 and 2), peer 192.168.2.10 falls back to chain 2 (no source_prefix_ranges)
        let meta_8_fallback_src = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::new(192, 168, 2, 10), 5000).into(),
            local_address: (Ipv4Addr::new(10, 2, 2, 3), 8443).into(),
        };
        assert_eq!(
            Listener::select_filterchain(&m, &meta_8_fallback_src, Some("app.example.com")).unwrap().copied(),
            Some(2)
        );

        // Different destination IP on port 8443 falls back to chain 3
        let meta_port_8443_other_ip = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 5000).into(),
            local_address: (Ipv4Addr::LOCALHOST, 8443).into(),
        };
        assert_eq!(
            Listener::select_filterchain(&m, &meta_port_8443_other_ip, Some("app.example.com")).unwrap().copied(),
            Some(3)
        );

        // Different destination port falls back to chain 4
        let meta_other_port = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 5000).into(),
            local_address: (Ipv4Addr::LOCALHOST, 9000).into(),
        };
        assert_eq!(Listener::select_filterchain(&m, &meta_other_port, None).unwrap().copied(), Some(4));
    }
}
