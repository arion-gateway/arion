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

use super::{
    filterchain::{ConnectionHandler, FilterchainBuilder, FilterchainType},
    internal_registry::{self, InternalConnection},
    listeners_manager::TlsContextChange,
};
#[cfg(feature = "instrumentation")]
use crate::instrumentation;

use crate::{
    get_shard_id,
    listeners::{
        http_connection_manager::mcp_gateway::mcp::McpGatewayListenerContext,
        metadata::{DownstreamConnectionMetadata, DownstreamMetadata},
    },
    secrets::{TlsConfigurator, WantsToBuildServer},
    transport::{bind_device::BindDevice, tls_inspector, ProxyProtocolReader},
    utils::instrumented_stream::InstrumentedStream,
    AsyncInstrumentedStream, ConversionContext, Error, Result, RouteConfigurationChange,
};
use orion_configuration::config::{
    listener::{FilterChainMatch, Listener as ListenerConfig, ListenerType, MatchResult},
    listener_filters::DownstreamProxyProtocolConfig,
};
use orion_interner::StringInterner;
use tokio::sync::mpsc;

#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;
use owning_ref::ArcRef;

use crate::{with_histogram, with_metric};
use dashmap::DashMap;
#[cfg(feature = "metrics")]
use orion_metrics::metrics::{http, listeners};

use rustls::ServerConfig;
use scopeguard::defer;
use std::{
    collections::HashMap,
    fmt::Debug,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
};
use tokio::{
    net::{TcpListener, TcpSocket},
    sync::broadcast::{self},
};
use tracing::{debug, info, warn};

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
    name: &'static str,
    binding: ListenerBinding,
    filter_chains: HashMap<FilterChainMatch, FilterchainBuilder>,
    with_tls_inspector: bool,
    proxy_protocol_config: Option<DownstreamProxyProtocolConfig>,
}
#[derive(Debug, Clone)]
pub struct ListenerFactory {
    listener: PartialListener,
}

impl TryFrom<ConversionContext<'_, ListenerConfig>> for PartialListener {
    type Error = Error;
    fn try_from(ctx: ConversionContext<'_, ListenerConfig>) -> std::result::Result<Self, Self::Error> {
        let ConversionContext { envoy_object: listener, secret_manager } = ctx;
        let name = listener.name.to_static_str();
        let with_tls_inspector = listener.with_tls_inspector;
        let proxy_protocol_config = listener.proxy_protocol_config;
        debug!("Listener {name} :TLS Inspector is {with_tls_inspector}");

        let binding = match listener.listener_type {
            ListenerType::Socket { address, bind_device } => {
                ListenerBinding::Socket { address, bind_device, tcp_backlog_size: listener.tcp_backlog_size }
            },
            ListenerType::Internal { .. } => ListenerBinding::Internal,
        };

        let filter_chains: HashMap<FilterChainMatch, _> = listener
            .filter_chains
            .into_iter()
            .map(|f| FilterchainBuilder::try_from(ConversionContext::new((f.1, secret_manager))).map(|x| (f.0, x)))
            .collect::<Result<_>>()?;

        if !with_tls_inspector {
            let has_server_names = filter_chains.keys().any(|m| !m.server_names.is_empty());
            if has_server_names {
                return Err((format!(
                    "Listener '{name}' has server_names in filter_chain_match, but no TLS inspector so matches would always fail"
                )).into());
            }
        }

        Ok(PartialListener { name, binding, filter_chains, with_tls_inspector, proxy_protocol_config })
    }
}

impl ListenerFactory {
    pub fn make_listener(
        self,
        route_updates_receiver: broadcast::Receiver<RouteConfigurationChange>,
        secret_updates_receiver: broadcast::Receiver<TlsContextChange>,
    ) -> Result<Listener> {
        let PartialListener { name, binding, filter_chains, with_tls_inspector, proxy_protocol_config } = self.listener;

        let filter_chains = filter_chains
            .into_iter()
            .map(|fc| fc.1.with_listener_name(name).build().map(|x| (fc.0, x)))
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(Listener {
            name,
            binding,
            filter_chains,
            with_tls_inspector,
            proxy_protocol_config,
            route_updates_receiver,
            secret_updates_receiver,
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

static LISTENERS_CONTEXT: OnceLock<DashMap<&'static str, Arc<ListenerContext>>> = OnceLock::new();

pub trait FilterListenerContext {
    fn get_filter_context(listener_name: &'static str) -> ArcRef<ListenerContext, Self>;
}

impl FilterListenerContext for McpGatewayListenerContext {
    #[inline]
    fn get_filter_context(listener_name: &'static str) -> ArcRef<ListenerContext, McpGatewayListenerContext> {
        let ctx = get_listener_context(listener_name);
        ArcRef::new(ctx).map(|ctx| &ctx.mcp)
    }
}

#[inline]
fn get_listener_context(listener_name: &'static str) -> Arc<ListenerContext> {
    let dmap = LISTENERS_CONTEXT.get_or_init(|| DashMap::new());
    dmap.entry(listener_name).or_insert_with(|| Arc::new(ListenerContext::default())).value().clone()
}

#[derive(Debug)]
pub struct Listener {
    name: &'static str,
    binding: ListenerBinding,
    pub filter_chains: HashMap<FilterChainMatch, FilterchainType>,
    with_tls_inspector: bool,
    proxy_protocol_config: Option<DownstreamProxyProtocolConfig>,
    route_updates_receiver: broadcast::Receiver<RouteConfigurationChange>,
    secret_updates_receiver: broadcast::Receiver<TlsContextChange>,
}

impl Listener {
    #[cfg(test)]
    pub(crate) fn test_listener(
        name: &'static str,
        route_rx: broadcast::Receiver<RouteConfigurationChange>,
        secret_rx: broadcast::Receiver<TlsContextChange>,
    ) -> Self {
        use std::net::{IpAddr, Ipv4Addr};
        Listener {
            name,
            binding: ListenerBinding::Socket {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                bind_device: None,
                tcp_backlog_size: 128,
            },
            filter_chains: HashMap::new(),
            with_tls_inspector: false,
            proxy_protocol_config: None,
            route_updates_receiver: route_rx,
            secret_updates_receiver: secret_rx,
        }
    }

    pub fn get_name(&self) -> &'static str {
        self.name
    }

    pub fn get_socket(&self) -> Option<(&std::net::SocketAddr, Option<&BindDevice>)> {
        match &self.binding {
            ListenerBinding::Socket { address, bind_device, .. } => Some((address, bind_device.as_ref())),
            ListenerBinding::Internal => None,
        }
    }

    pub async fn start(self) -> Error {
        let Self {
            name,
            binding,
            filter_chains,
            with_tls_inspector,
            proxy_protocol_config,
            mut route_updates_receiver,
            mut secret_updates_receiver,
        } = self;

        let mut filter_chains = Arc::new(filter_chains);
        let proxy_protocol_config = proxy_protocol_config.map(Arc::new);
        let _listener_name = name;

        match binding {
            ListenerBinding::Socket { address: local_address, bind_device, tcp_backlog_size } => {
                let listener =
                    match configure_and_start_tcp_listener(local_address, bind_device.as_ref(), tcp_backlog_size) {
                        Ok(x) => x,
                        Err(e) => return e,
                    };

                let actual_address = listener.local_addr().unwrap_or(local_address);
                info!("listener '{name}' started: {actual_address}");

                #[cfg(feature = "instrumentation")]
                let clock = quanta::Clock::new();

                loop {
                    tokio::select! {
                        biased;
                        maybe_stream = listener.accept() => {
                            match maybe_stream {
                                Ok((stream, peer_addr)) => {
                                    #[cfg(feature = "instrumentation")]
                                    instrumentation::metrics::CONNECTIONS.add(1);

                                    #[cfg(feature = "instrumentation")]
                                    let start_clock = clock.raw();

                                    let filter_chains = Arc::clone(&filter_chains);
                                    let proxy_protocol_config = proxy_protocol_config.clone();
                                    tokio::spawn(async move {
                                        let start = std::time::Instant::now();

                                        _ = stream.set_nodelay(true);
                                        _ = stream.set_quickack(true);

                                        let stream = InstrumentedStream::new(stream, None);

                                        let _shard_id = get_shard_id!();
                                        with_metric!(listeners::DOWNSTREAM_CX_TOTAL, add, 1, _shard_id,&[KeyValue::new("listener", _listener_name)]);
                                        with_metric!(listeners::DOWNSTREAM_CX_ACTIVE, add, 1, _shard_id,&[KeyValue::new("listener", _listener_name)]);

                                        _ = tokio::spawn(Self::process_connection(
                                            name,
                                            filter_chains,
                                            with_tls_inspector,
                                            ConnectionSource::Socket { local_address, peer_addr, proxy_protocol_config },
                                            Box::new(stream),
                                            start,
                                        )).await;
                                    });

                                    #[cfg(feature = "instrumentation")]
                                    {
                                        let nanos = clock.delta_as_nanos(start_clock, clock.raw());
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
                                Ok(route_update) => {Self::process_route_update(name, &filter_chains, route_update)},
                                Err(e) => {return e.into();}
                            }
                        },
                        maybe_secret_update = secret_updates_receiver.recv() => {
                            match maybe_secret_update {
                                Ok(secret_update) => {
                                    let mut filter_chains_clone = Arc::unwrap_or_clone(filter_chains);
                                    Self::process_secret_update(name, &mut filter_chains_clone, secret_update);
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
                    internal_registry::unregister(name);
                }
                info!("internal listener '{name}' started");

                loop {
                    tokio::select! {
                        maybe_connection = rx.recv() => {
                            match maybe_connection {
                                Some(InternalConnection { stream, downstream_metadata, start_instant }) => {
                                    let _shard_id = get_shard_id!();
                                    with_metric!(listeners::DOWNSTREAM_CX_TOTAL, add, 1, _shard_id, &[KeyValue::new("listener", _listener_name)]);
                                    with_metric!(listeners::DOWNSTREAM_CX_ACTIVE, add, 1, _shard_id, &[KeyValue::new("listener", _listener_name)]);

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
                                Ok(route_update) => {Self::process_route_update(name, &filter_chains, route_update)},
                                Err(e) => {return e.into();}
                            }
                        },
                        maybe_secret_update = secret_updates_receiver.recv() => {
                            match maybe_secret_update {
                                Ok(secret_update) => {
                                    let mut filter_chains_clone = Arc::unwrap_or_clone(filter_chains);
                                    Self::process_secret_update(name, &mut filter_chains_clone, secret_update);
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

    fn select_filterchain<'a, T>(
        filter_chains: &'a HashMap<FilterChainMatch, T>,
        connection_metadata: &DownstreamConnectionMetadata,
        server_name: Option<&str>,
    ) -> Result<Option<&'a T>> {
        let source_addr = connection_metadata.peer_address();
        let destination_addr = connection_metadata.local_address();
        fn match_subitem<'a, F: Fn(&FilterChainMatch, T) -> MatchResult, T: Copy>(
            function: F,
            comparand: T,
            iter: impl Iterator<Item = &'a FilterChainMatch>,
            scratchpad: &mut [MatchResult],
            possible_filters: &mut [bool],
        ) {
            let mut best_match = MatchResult::FailedMatch;
            // check all filters still in the running, skipping over those already eliminated
            for (i, match_config) in iter.enumerate().filter(|(i, _)| possible_filters[*i]) {
                let match_result = function(match_config, comparand);
                //mark the outcome of this iteration, and keep track of the best result
                scratchpad[i] = match_result;
                if match_result > best_match {
                    best_match = match_result;
                }
            }
            // now trim all the results that failed to match, or were less specific than the best match
            for i in 0..scratchpad.len() {
                if scratchpad[i] != best_match || scratchpad[i] == MatchResult::FailedMatch {
                    possible_filters[i] = false;
                }
            }
        }

        //todo: smallvec? other optimization?
        let mut possible_filters = vec![true; filter_chains.len()];
        let mut scratchpad = vec![MatchResult::NoRule; filter_chains.len()];

        match_subitem(
            FilterChainMatch::matches_destination_port,
            destination_addr.port(),
            filter_chains.keys(),
            &mut scratchpad,
            &mut possible_filters,
        );

        match_subitem(
            FilterChainMatch::matches_destination_ip,
            destination_addr.ip(),
            filter_chains.keys(),
            &mut scratchpad,
            &mut possible_filters,
        );

        match_subitem(
            FilterChainMatch::matches_server_name,
            server_name.unwrap_or(""),
            filter_chains.keys(),
            &mut scratchpad,
            &mut possible_filters,
        );

        match_subitem(
            FilterChainMatch::matches_source_ip,
            source_addr.ip(),
            filter_chains.keys(),
            &mut scratchpad,
            &mut possible_filters,
        );

        match_subitem(
            FilterChainMatch::matches_source_port,
            source_addr.port(),
            filter_chains.keys(),
            &mut scratchpad,
            &mut possible_filters,
        );

        let mut possible_filters = possible_filters
            .into_iter()
            .zip(filter_chains.iter())
            .filter_map(|(include, item)| include.then_some(item.1));

        let first_match = possible_filters.next();
        if possible_filters.next().is_some() {
            Err("multiple filterchains matched a single connection. This is a bug in orion!".into())
        } else {
            Ok(first_match)
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn process_connection(
        listener_name: &'static str,
        filter_chains: Arc<HashMap<FilterChainMatch, FilterchainType>>,
        with_tls_inspector: bool,
        source: ConnectionSource,
        mut stream: AsyncInstrumentedStream,
        start_instant: std::time::Instant,
    ) -> Result<()> {
        let _shard_id = get_shard_id!();
        let ssl = AtomicBool::new(false);
        defer! {
            with_metric!(listeners::DOWNSTREAM_CX_DESTROY, add, 1, _shard_id, &[KeyValue::new("listener", listener_name)]);
            with_metric!(listeners::DOWNSTREAM_CX_ACTIVE, sub, 1, _shard_id, &[KeyValue::new("listener", listener_name)]);
            if ssl.load(Ordering::Relaxed) {
                with_metric!(http::DOWNSTREAM_CX_SSL_ACTIVE, add, 1, _shard_id, &[KeyValue::new("listener", listener_name)]);
            }
            let _ms = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
            with_histogram!(listeners::DOWNSTREAM_CX_LENGTH_MS, record, _ms, &[KeyValue::new("listener", listener_name)]);
        }

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
                        _shard_id,
                        &[KeyValue::new("listener", listener_name)]
                    );
                    with_metric!(
                        http::DOWNSTREAM_CX_SSL_ACTIVE,
                        add,
                        1,
                        _shard_id,
                        &[KeyValue::new("listener", listener_name)]
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
                        _shard_id,
                        &[KeyValue::new("listener", listener_name)]
                    );
                    with_metric!(
                        http::DOWNSTREAM_CX_SSL_ACTIVE,
                        add,
                        1,
                        _shard_id,
                        &[KeyValue::new("listener", listener_name)]
                    );
                    ssl.store(true, Ordering::Relaxed);
                    None
                },
                crate::transport::tls_inspector::InspectorResult::TlsError(e) => {
                    debug!("{listener_name} : No TLS handshake: Error: {e}");
                    None
                },
            }
        } else {
            None
        };

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

        let selected_filterchain = Self::select_filterchain(&filter_chains, &connection_metadata, sni.as_deref())?;

        if let Some(filterchain) = selected_filterchain {
            debug!(
                "{listener_name} : mapping connection from {} to filter chain {}",
                connection_metadata.peer_address(),
                filterchain.filter_chain().name
            );
            if let Some(stream) = filterchain.apply_rbac(stream, &connection_metadata, sni.as_deref()) {
                return filterchain
                    .start_filterchain(
                        stream,
                        DownstreamMetadata::new(connection_metadata, sni, listener_name),
                        listener_name,
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
                _shard_id,
                &[KeyValue::new("listener", listener_name)]
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
        filter_chains: &mut HashMap<FilterChainMatch, FilterchainType>,
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
        filter_chains: &HashMap<FilterChainMatch, FilterchainType>,
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
                if applied {
                    if let Some(notify) = notify {
                        notify.notify_one();
                    }
                }
            },
            RouteConfigurationChange::Removed(id, notify) => {
                let mut applied = false;
                for chain in filter_chains.values() {
                    if let ConnectionHandler::Http(http_manager) = &chain.handler {
                        if let Some(route_id) = http_manager.get_route_id() {
                            if route_id == &id {
                                http_manager.remove_route();
                                applied = true;
                            }
                        }
                    }
                }
                if applied {
                    if let Some(notify) = notify {
                        notify.notify_one();
                    }
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
    use orion_configuration::config::listener::{FilterChainMatch as FilterChainMatchConfig, ServerNameMatch};
    use orion_data_plane_api::{
        decode::from_yaml, envoy_data_plane_api::envoy::config::listener::v3::FilterChainMatch as EnvoyFilterChainMatch,
    };

    use crate::SecretManager;

    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::envoy::config::listener::v3::Listener as EnvoyListener;

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
        let hashmap: HashMap<_, _> = fcm.iter().cloned().collect();
        let metadata = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 33000).into(),
            local_address: (Ipv4Addr::LOCALHOST, 8443).into(),
        };
        let selected = Listener::select_filterchain(&hashmap, &metadata, None).unwrap();
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
        assert!(err
            .to_string()
            .contains("has server_names in filter_chain_match, but no TLS inspector so matches would always fail"));
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
        let m = std::iter::once((m.try_into().unwrap(), ())).collect();
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
        //     let listener : Listener = l.try_into().unwrap();
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
            .collect::<std::result::Result<HashMap<_, _>, _>>()
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
}
