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

pub(crate) mod cedar_policy;
pub mod cors;
mod direct_response;
pub mod ext_proc;
#[cfg(feature = "wasm")]
pub mod wasm;
//pub mod global_rate_limit;
pub mod http_modifiers;
pub mod jwt_authn;
pub mod mcp_gateway;
pub mod oauth2;
mod redirect;
mod route;
mod upgrades;
pub mod user_rate_limiter;

use ahash::AHashMap;
use arion_interner::InternedStr;
use smallvec::SmallVec;
#[cfg(any(feature = "tracing", feature = "metrics", feature = "access-log"))]
use std::sync::atomic::AtomicUsize;

#[cfg(feature = "access-log")]
use crate::event_error::EventErrorContext;

#[cfg(any(feature = "tracing", feature = "metrics"))]
use opentelemetry::KeyValue;

#[cfg(feature = "tracing")]
use {
    crate::tracing_attributes::HTTP_RESPONSE_STATUS_CODE,
    crate::tracing_attributes::set_attributes_from_request,
    arion_tracing::{
        http_tracer::{SpanKind, SpanName},
        span_state::SpanState,
        trace_context::TraceContext,
    },
    opentelemetry::global::BoxedSpan,
    opentelemetry::trace::Span,
    opentelemetry::trace::Status,
};

#[cfg(feature = "access-log")]
use crate::utils::http::{request_head_size, response_head_size};
#[cfg(feature = "access-log")]
use crate::with_access_log;
#[cfg(feature = "metrics")]
use crate::{metrics, with_histogram};

#[cfg(feature = "metrics")]
use arion_metrics::metrics::{custom::CUSTOM_METRICS, http, user};

#[cfg(feature = "access-log")]
use {
    crate::access_log::{Target, blocking_log_access, is_access_log_enabled},
    crate::event_error::UpstreamTransportEventError,
    arion_configuration::config::access_log::AccessLog,
    arion_format::LogFormatter,
    arion_format::context::{
        DownstreamResponseContext, FinishContext, HttpRequestDurationContext, HttpResponseDurationContext,
        InitHttpContext,
    },
};

#[cfg(any(feature = "access-log", feature = "metrics"))]
use {parking_lot::Mutex, std::time::Instant};

use ::http::HeaderValue;
use arc_swap::ArcSwapOption;
use arion_configuration::config::network_filters::http_connection_manager::{
    CodecType, ConfigSource, ConfigSourceSpecifier, HttpConnectionManager as HttpConnectionManagerConfig, MatchHost,
    MatchHostScoreLPM, RdsSpecifier, RetryPolicy, RouteSpecifier, UpgradeType,
    route::{Action, RouteMatchResult},
};
#[cfg(feature = "metrics")]
use arion_metrics::metrics::clusters;
use core::time::Duration;
use hyper::{Request, Response, StatusCode, body::Incoming, header::HOST};

use std::fmt::Write;

use crate::{
    ArionClientBody, ArionRequestBody, ArionResponseBody, ConversionContext, PolyBody, Result, RouteConfiguration,
    body::{
        instrumented_body::InstrumentedBody,
        response_flags::{BodyKind, ResponseFlags},
        timeout_body::TimeoutBody,
    },
    event_error::{EventFailure, EventKind, UpstreamError},
    get_shard_id,
    listeners::{
        http_connection_manager::http_modifiers::ModifiersExtractor,
        http_filters::{FilterDecision, FilterFactory, HttpFilter, HttpFilterValue, compile_route_http_filters},
        metadata::{ConnMeta, DownstreamMetadata},
        synthetic_http_response::SyntheticHttpResponse,
    },
    with_client_span, with_metric, with_server_span,
};

use crate::utils::StreamMetrics;
#[cfg(any(feature = "access-log", feature = "metrics"))]
use crate::utils::instrumented_stream::OnFlush;

use arion_configuration::config::network_filters::{
    early_header_mutation::EarlyHeaderMutation,
    http_connection_manager::{
        HeaderModifiersAdd, HeaderModifiersRemove, RequestHost, Route, VirtualHost, XffSettings,
    },
    tracing::{TracingConfig, TracingKey},
};
use arion_format::types::ResponseFlags as FmtResponseFlags;
use route::RouteContext;
use smol_str::SmolStr;
use std::cell::RefCell;
use std::{fmt, result::Result as StdResult, sync::Arc as StdArc};
use triomphe::{Arc, UniqueArc};

use tracing::{debug, error};
use upgrades as upgrade_utils;

use arion_tracing::http_tracer::{HttpTracer, ScopedClientSpan};
use arion_tracing::request_id::{RequestId, RequestIdManager};

use crate::listeners::http_connection_manager::http_modifiers::HeaderMapModifier;
#[cfg(feature = "metrics")]
use arion_metrics::metrics::custom::MetricsHook;

struct LengthCounter(usize);

impl Write for LengthCounter {
    // Accumulate the byte length of the string slices being formatted
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0 += s.len();
        Ok(())
    }
}

#[derive(Debug, Clone)]
// =================================================================================================
// HTTP Connection Manager - Request Pipeline Architecture
// =================================================================================================
//
// Hyper drives the pipeline through `hyper::service::service_fn` (see `filterchain.rs`):
// the per-request future is the monomorphized `async` state machine below, polled in
// place on the connection task. Nothing is boxed — no per-request heap allocation.
//
// +-----------------------------------------------------------------------------------------------+
// | Hyper Server (hyper::server::conn::http1 / http2)                                             |
// |   Provides: Request<Incoming>                                                                 |
// +-----------------------------------------------------------------------------------------------+
//                                |
//                                v
// +-----------------------------------------------------------------------------------------------+
// | 1. TransactionLifecycleSvc  (unboxed async fn, via `service_fn`)                           |
// |   Input:  Request<Incoming>                                                                   |
// |   Action: Attach ConnMeta, request ID, span, TransactionContext → RequestCtx.                 |
// |           Return crate::Result directly for Hyper.                                             |
// |   Output: HttpRequest<Incoming>                                                               |
// +-----------------------------------------------------------------------------------------------+
//                                |
//                                v
// +-----------------------------------------------------------------------------------------------+
// | 2. TransactionSvc  (async fn, unboxed)                                                        |
// |   Input:  HttpRequest<Incoming>                                                               |
// |   Action: Metrics, validation, instrumented body, resolve route_conf.                         |
// |   Output: RoutedHttpRequest<ArionRequestBody>                                                 |
// +-----------------------------------------------------------------------------------------------+
//                                |
//                                v
// +-----------------------------------------------------------------------------------------------+
// | 3. HttpPipelineSvc  (async fn, unboxed)                                                       |
// |   Input:  RoutedHttpRequest<ArionRequestBody>                                                 |
// |   Action: Filter chain + routing via RequestHandler (ctx passed explicitly).                  |
// |   Output: Response<ArionClientBody>                                                           |
// +-----------------------------------------------------------------------------------------------+
//
// =================================================================================================

pub struct HttpConnectionManagerBuilder {
    listener_name: Option<InternedStr>,
    filterchain_id: Option<u64>,
    downstream_tls: bool,
    connection_manager: PartialHttpConnectionManager,
}

impl TryFrom<ConversionContext<'_, HttpConnectionManagerConfig>> for HttpConnectionManagerBuilder {
    type Error = crate::Error;
    fn try_from(ctx: ConversionContext<HttpConnectionManagerConfig>) -> Result<Self> {
        let partial = PartialHttpConnectionManager::try_from(ctx)?;
        Ok(Self { listener_name: None, filterchain_id: None, downstream_tls: false, connection_manager: partial })
    }
}

impl HttpConnectionManagerBuilder {
    pub fn build(self) -> Result<HttpConnectionManager> {
        let listener_name = self.listener_name.ok_or("listener name is not set")?;
        let filterchain_id = self.filterchain_id.unwrap_or(0);
        let partial = self.connection_manager;
        let initial_routing_state =
            partial.router.map(|router| StdArc::new(RoutingState::new(router, &partial.http_filters_hcm)));
        let websocket_enabled_by_default = upgrade_utils::is_websocket_enabled_by_hcm(&partial.enabled_upgrades);

        Ok(HttpConnectionManager {
            listener_name,
            filterchain_id,
            downstream_tls: self.downstream_tls,
            routing_state: ArcSwapOption::new(initial_routing_state),
            codec_type: partial.codec_type,
            dynamic_route_name: partial.dynamic_route_name,
            http_filters_hcm: partial.http_filters_hcm,
            websocket_enabled_by_default,
            request_timeout: partial.request_timeout,
            xff_settings: partial.xff_settings,
            request_id_handler: RequestIdManager::new(
                partial.generate_request_id,
                partial.preserve_external_request_id,
                partial.always_set_request_id_in_response,
            ),
            http_tracer: match partial.tracing {
                Some(tracing) => HttpTracer::new().with_config(tracing),
                None => HttpTracer::new(),
            },
            early_header_mutation: partial.early_header_mutation,
            #[cfg(feature = "access-log")]
            access_log: partial.access_log,
        })
    }

    #[inline]
    pub fn with_listener_name(self, name: InternedStr) -> Self {
        HttpConnectionManagerBuilder { listener_name: Some(name), ..self }
    }

    #[inline]
    pub fn with_filterchain_id(self, value: u64) -> Self {
        HttpConnectionManagerBuilder { filterchain_id: Some(value), ..self }
    }

    #[inline]
    pub fn with_downstream_tls(self, value: bool) -> Self {
        HttpConnectionManagerBuilder { downstream_tls: value, ..self }
    }
}

#[derive(Debug, Clone)]
pub struct PartialHttpConnectionManager {
    router: Option<RouteConfiguration>,
    codec_type: CodecType,
    dynamic_route_name: Option<SmolStr>,
    http_filters_hcm: Vec<Arc<HttpFilter>>,
    enabled_upgrades: Vec<UpgradeType>,
    request_timeout: Option<Duration>,
    xff_settings: XffSettings,
    generate_request_id: bool,
    preserve_external_request_id: bool,
    always_set_request_id_in_response: bool,
    tracing: Option<TracingConfig>,
    early_header_mutation: Vec<EarlyHeaderMutation>,
    #[cfg(feature = "access-log")]
    access_log: Vec<AccessLog>,
}

impl TryFrom<ConversionContext<'_, HttpConnectionManagerConfig>> for PartialHttpConnectionManager {
    type Error = crate::Error;
    fn try_from(ctx: ConversionContext<HttpConnectionManagerConfig>) -> Result<Self> {
        let ConversionContext { envoy_object: configuration, .. } = ctx;
        let codec_type = configuration.codec_type;
        let enabled_upgrades = configuration.enabled_upgrades;
        let http_filters_hcm: Vec<_> = configuration
            .http_filters
            .into_iter()
            .map(|f| -> Result<Arc<HttpFilter>> { Ok(Arc::new(HttpFilter::try_from(f)?)) })
            .collect::<Result<Vec<Arc<HttpFilter>>>>()?;
        let request_timeout = configuration.request_timeout;
        let xff_settings = configuration.xff_settings;
        let generate_request_id = configuration.generate_request_id;
        let preserve_external_request_id = configuration.preserve_external_request_id;
        let always_set_request_id_in_response = configuration.always_set_request_id_in_response;

        let (dynamic_route_name, router) = match configuration.route_specifier {
            RouteSpecifier::Rds(RdsSpecifier {
                route_config_name,
                config_source: ConfigSource { config_source_specifier },
            }) => match config_source_specifier {
                ConfigSourceSpecifier::ADS => (Some(route_config_name), None),
            },
            RouteSpecifier::RouteConfig(config) => (None, Some(config)),
        };

        Ok(PartialHttpConnectionManager {
            router,
            codec_type,
            dynamic_route_name,
            http_filters_hcm,
            enabled_upgrades,
            request_timeout,
            xff_settings,
            generate_request_id,
            preserve_external_request_id,
            always_set_request_id_in_response,
            tracing: configuration.tracing,
            early_header_mutation: configuration.early_header_mutation,
            #[cfg(feature = "access-log")]
            access_log: configuration.access_log,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AlpnCodecs {
    Http1,
    Http2,
}

impl AsRef<[u8]> for AlpnCodecs {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Http2 => b"h2",
            Self::Http1 => b"http/1.1",
        }
    }
}

impl AlpnCodecs {
    pub fn from_codec(codec: CodecType) -> &'static [Self] {
        match codec {
            CodecType::Auto => &[AlpnCodecs::Http2, AlpnCodecs::Http1],
            CodecType::Http2 => &[AlpnCodecs::Http2],
            CodecType::Http1 => &[AlpnCodecs::Http1],
        }
    }
}

/// Runtime representation of a `Route` paired directly with its pre-resolved HTTP filter chain.
#[derive(Debug)]
pub struct RuntimeRoute {
    pub route: Route,
    pub http_filters: Box<[Arc<HttpFilter>]>,
}

/// Runtime representation of a `VirtualHost` with `domains` extracted into [`VirtualHostTable`]
/// and routes pre-compiled with their per-route HTTP filter chains.
#[derive(Debug)]
pub struct RuntimeVirtualHost {
    #[allow(dead_code)]
    pub name: InternedStr,
    pub routes: Box<[RuntimeRoute]>,
    pub request_headers_to_add: HeaderModifiersAdd,
    pub request_headers_to_remove: HeaderModifiersRemove,
    pub response_headers_to_add: HeaderModifiersAdd,
    pub response_headers_to_remove: HeaderModifiersRemove,
    pub retry_policy: Option<RetryPolicy>,
}

/// Pre-compiled domain lookup table mapping host patterns directly to [`Arc<RuntimeVirtualHost>`].
///
/// Eliminates linear $O(V \cdot D)$ virtual host scanning and any intermediate index lookups:
/// exact domains resolve in $O(1)$, suffix/prefix rules are sorted by descending length for
/// immediate longest-match early exit, and wildcard is an $O(1)$ fallback.
#[derive(Debug)]
pub struct VirtualHostTable {
    /// $O(1)$ lookup for `MatchHost::Exact` domains -> `Arc<RuntimeVirtualHost>`.
    exact: AHashMap<SmolStr, Arc<RuntimeVirtualHost>>,
    /// `MatchHost::Suffix` rules sorted by descending suffix length (longest match first).
    suffixes: Box<[(SmolStr, Arc<RuntimeVirtualHost>)]>,
    /// `MatchHost::Prefix` rules sorted by descending prefix length (longest match first).
    prefixes: Box<[(SmolStr, Arc<RuntimeVirtualHost>)]>,
    /// Fallback `MatchHost::Wildcard` (`*`) virtual host, if configured.
    wildcard: Option<Arc<RuntimeVirtualHost>>,
}

impl VirtualHostTable {
    pub fn compile(virtual_hosts: Vec<VirtualHost>, hcm_filters: &[Arc<HttpFilter>]) -> Self {
        let mut exact = AHashMap::new();
        let mut suffixes = Vec::new();
        let mut prefixes = Vec::new();
        let mut wildcard = None;

        for vh in virtual_hosts {
            let VirtualHost {
                name,
                domains,
                routes,
                request_headers_to_add,
                request_headers_to_remove,
                response_headers_to_add,
                response_headers_to_remove,
                retry_policy,
            } = vh;

            let runtime_routes = routes
                .into_iter()
                .map(|route| {
                    let http_filters = compile_route_http_filters(&route, hcm_filters);
                    RuntimeRoute { route, http_filters }
                })
                .collect();

            let runtime_vh = Arc::new(RuntimeVirtualHost {
                name: name.into(),
                routes: runtime_routes,
                request_headers_to_add,
                request_headers_to_remove,
                response_headers_to_add,
                response_headers_to_remove,
                retry_policy,
            });

            for domain in domains {
                match domain {
                    MatchHost::Exact(h) => {
                        exact.entry(h).or_insert_with(|| Arc::clone(&runtime_vh));
                    },
                    MatchHost::Suffix(s) => {
                        suffixes.push((s, Arc::clone(&runtime_vh)));
                    },
                    MatchHost::Prefix(p) => {
                        prefixes.push((p, Arc::clone(&runtime_vh)));
                    },
                    MatchHost::Wildcard => {
                        if wildcard.is_none() {
                            wildcard = Some(Arc::clone(&runtime_vh));
                        }
                    },
                }
            }
        }

        // Stable sort by descending length: the first matching suffix/prefix in slice order
        // is guaranteed to have the maximum length (and earliest virtual host on ties).
        suffixes.sort_by_key(|(a, _)| std::cmp::Reverse(a.len()));
        prefixes.sort_by_key(|(a, _)| std::cmp::Reverse(a.len()));

        Self { exact, suffixes: suffixes.into_boxed_slice(), prefixes: prefixes.into_boxed_slice(), wildcard }
    }

    #[inline]
    pub fn select(&self, host: RequestHost<'_>) -> Option<&RuntimeVirtualHost> {
        let full_match = self.match_single(host.full);
        // Short-circuit: if `host.full` matched an Exact rule, `without_port` is strictly shorter
        // and cannot produce a higher `MatchHostScoreLPM`.
        if let Some((MatchHostScoreLPM::Exact(_), full_vh)) = full_match {
            return Some(full_vh);
        }

        let matched = match (full_match, host.without_port.and_then(|wp| self.match_single(wp))) {
            (Some((full_score, full_vh)), Some((wp_score, wp_vh))) => {
                if wp_score > full_score {
                    Some(wp_vh)
                } else {
                    Some(full_vh)
                }
            },
            (Some((_, vh)), None) | (None, Some((_, vh))) => Some(vh),
            (None, None) => None,
        };
        matched.or(self.wildcard.as_deref())
    }

    #[inline]
    fn match_single(&self, raw_host: &str) -> Option<(MatchHostScoreLPM, &RuntimeVirtualHost)> {
        let stripped = raw_host.strip_suffix('.').unwrap_or(raw_host);

        // 1. Exact match: O(1) hash lookup (highest priority in MatchHostScoreLPM)
        if let Some(vh) = self.exact.get(stripped) {
            return Some((MatchHostScoreLPM::Exact(stripped.len()), vh.as_ref()));
        }

        // 2. Longest Suffix match: sorted by descending length, so the first match wins immediately
        for (suffix, vh) in &self.suffixes {
            if stripped.len() > suffix.len() && stripped.ends_with(suffix.as_str()) {
                return Some((MatchHostScoreLPM::Suffix(suffix.len()), vh.as_ref()));
            }
        }

        // 3. Longest Prefix match: sorted by descending length, so the first match wins immediately
        for (prefix, vh) in &self.prefixes {
            if raw_host.len() > prefix.len() && raw_host.starts_with(prefix.as_str()) {
                return Some((MatchHostScoreLPM::Prefix(prefix.len()), vh.as_ref()));
            }
        }

        None
    }
}

#[derive(Debug)]
pub struct RoutingState {
    #[allow(dead_code)]
    pub name: InternedStr,
    pub most_specific_header_mutations_wins: bool,
    pub request_headers_to_add: HeaderModifiersAdd,
    pub request_headers_to_remove: HeaderModifiersRemove,
    pub response_headers_to_add: HeaderModifiersAdd,
    pub response_headers_to_remove: HeaderModifiersRemove,
    pub has_request_mutations: bool,
    pub has_response_mutations: bool,
    pub virtual_hosts: VirtualHostTable,
}

impl RoutingState {
    pub fn new(mut route_configuration: RouteConfiguration, hcm_filters: &[Arc<HttpFilter>]) -> Self {
        // Belt-and-braces: the cached mutation flags must reflect the config actually
        // served, regardless of which construction path produced it (Envoy conversion,
        // struct literal, deserialization). Runs once per config load/update.
        route_configuration.recompute_mutation_flags();

        let virtual_hosts = VirtualHostTable::compile(route_configuration.virtual_hosts, hcm_filters);

        Self {
            name: route_configuration.name.into(),
            most_specific_header_mutations_wins: route_configuration.most_specific_header_mutations_wins,
            request_headers_to_add: route_configuration.request_headers_to_add,
            request_headers_to_remove: route_configuration.request_headers_to_remove,
            response_headers_to_add: route_configuration.response_headers_to_add,
            response_headers_to_remove: route_configuration.response_headers_to_remove,
            has_request_mutations: route_configuration.has_request_mutations,
            has_response_mutations: route_configuration.has_response_mutations,
            virtual_hosts,
        }
    }

    #[inline]
    pub fn select_virtual_host<T>(&self, request: &Request<T>) -> Option<&RuntimeVirtualHost> {
        let host = RequestHost::from_request(request)?;
        self.virtual_hosts.select(host)
    }

    #[inline]
    pub fn match_request_route<B>(&self, request: &Request<B>) -> Option<CachedRoute<'_>> {
        let chosen_vh = self.select_virtual_host(request)?;
        for runtime_route in &chosen_vh.routes {
            let route_match = runtime_route.route.route_match.match_request(request);
            if route_match.matched() {
                return Some(CachedRoute {
                    route: &runtime_route.route,
                    http_filters: &runtime_route.http_filters,
                    route_match,
                    vh: chosen_vh,
                });
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct HttpConnectionManager {
    pub listener_name: InternedStr,
    pub filterchain_id: u64,
    downstream_tls: bool,
    routing_state: ArcSwapOption<RoutingState>,
    pub codec_type: CodecType,
    dynamic_route_name: Option<SmolStr>,
    http_filters_hcm: Vec<Arc<HttpFilter>>,
    /// Cached at build time: whether the `enabled_upgrades` list contains websocket.
    /// The per-request hot path reads this single bool instead of scanning the `Vec`.
    /// (`enabled_upgrades` itself is not retained: this was its only reader.)
    websocket_enabled_by_default: bool,
    request_timeout: Option<Duration>,
    xff_settings: XffSettings,
    request_id_handler: RequestIdManager,
    pub http_tracer: HttpTracer,
    early_header_mutation: Vec<EarlyHeaderMutation>,
    #[cfg(feature = "access-log")]
    access_log: Vec<AccessLog>,
}

impl fmt::Display for HttpConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HttpConnectionManager {}", self.listener_name)
    }
}

impl HttpConnectionManager {
    #[inline]
    pub fn get_tracing_key(&self) -> TracingKey {
        TracingKey(self.listener_name, self.filterchain_id)
    }

    #[inline]
    pub fn get_route_id(&self) -> Option<&SmolStr> {
        self.dynamic_route_name.as_ref()
    }

    pub fn update_route(&self, route: RouteConfiguration) {
        let new_state = StdArc::new(RoutingState::new(route, &self.http_filters_hcm));
        self.routing_state.store(Some(new_state));
    }

    pub fn remove_route(&self) {
        self.routing_state.store(None);
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn transaction_context_svc(
        this: &Arc<Self>,
        conn: ConnMeta,
    ) -> StdArc<TransactionLifecycleSvc<TransactionSvc<HttpPipelineSvc>>> {
        StdArc::new(TransactionLifecycleSvc::new(Arc::clone(this), conn, TransactionSvc::new(HttpPipelineSvc::new())))
    }
}

#[derive(Debug)]
pub struct CachedRoute<'a> {
    pub route: &'a Route,
    pub http_filters: &'a [Arc<HttpFilter>],
    pub route_match: RouteMatchResult,
    pub vh: &'a RuntimeVirtualHost,
}

#[cfg(any(feature = "access-log", feature = "metrics"))]
#[derive(Debug, Default)]
pub struct TransactionState {
    bytes: u64, // either the request or response body size, depending which one has completed first
    flags: ResponseFlags,
    event: Option<EventKind>,
    pub upstream_start_instant: Option<std::time::Instant>,
    pub upstream_cluster_name: Option<InternedStr>,
    #[cfg(feature = "access-log")]
    pub(crate) loggers: Option<Box<[arion_format::LogFormatter]>>,
}

#[cfg(any(feature = "access-log", feature = "metrics"))]
impl TransactionState {
    pub fn new(#[cfg(feature = "access-log")] access_log: &[AccessLog]) -> Self {
        TransactionState {
            bytes: 0,
            flags: ResponseFlags::default(),
            event: None,
            upstream_start_instant: None,
            upstream_cluster_name: None,
            #[cfg(feature = "access-log")]
            loggers: if access_log.is_empty() {
                None
            } else {
                Some(access_log.iter().map(|al| al.get_logger().clone()).collect::<Vec<_>>().into_boxed_slice())
            },
        }
    }
}

#[cfg(feature = "metrics")]
pub type ShardId = std::thread::ThreadId;

#[cfg(not(feature = "metrics"))]
pub type ShardId = ();

thread_local! {
    static TX_CONTEXT_POOL: RefCell<Vec<UniqueArc<TransactionContext>>> = RefCell::new(Vec::with_capacity(128));
}

#[derive(Debug)]
pub struct PooledTxnCtx(Option<Arc<TransactionContext>>);

impl PooledTxnCtx {
    #[allow(clippy::too_many_arguments)]
    pub fn acquire(
        request_id: Option<RequestId>,
        user_partition_key: Option<InternedStr>,
        thread_id: ShardId,
        #[cfg(feature = "access-log")] access_log: &[AccessLog],
        #[cfg(feature = "tracing")] trace_ctx: Option<TraceContext>,
        #[cfg(feature = "tracing")] upstream_tracing_key: Option<TracingKey>,
        #[cfg(feature = "tracing")] server_span: Option<BoxedSpan>,
    ) -> Self {
        let unique = TX_CONTEXT_POOL.with(|pool| pool.borrow_mut().pop());
        let arc = if let Some(mut ctx) = unique {
            ctx.reset(
                request_id,
                user_partition_key,
                thread_id,
                #[cfg(feature = "access-log")]
                access_log,
                #[cfg(feature = "tracing")]
                trace_ctx,
                #[cfg(feature = "tracing")]
                upstream_tracing_key,
                #[cfg(feature = "tracing")]
                server_span,
            );
            UniqueArc::shareable(ctx)
        } else {
            Arc::new(TransactionContext::new(
                request_id,
                user_partition_key,
                thread_id,
                #[cfg(feature = "access-log")]
                access_log,
                #[cfg(feature = "tracing")]
                trace_ctx,
                #[cfg(feature = "tracing")]
                upstream_tracing_key,
                #[cfg(feature = "tracing")]
                server_span,
            ))
        };
        Self(Some(arc))
    }
}

impl Clone for PooledTxnCtx {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Default for PooledTxnCtx {
    fn default() -> Self {
        Self(Some(Arc::new(TransactionContext::default())))
    }
}

impl std::ops::Deref for PooledTxnCtx {
    type Target = TransactionContext;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl Drop for PooledTxnCtx {
    #[inline]
    fn drop(&mut self) {
        if let Some(arc) = self.0.take()
            && let Ok(unique) = Arc::try_unique(arc)
        {
            TX_CONTEXT_POOL.with(|pool| {
                let mut pool = pool.borrow_mut();
                if pool.len() < 1024 {
                    pool.push(unique);
                }
            });
        }
    }
}

#[derive(Debug)]
pub struct TransactionContext {
    #[cfg(feature = "access-log")]
    pub has_access_log: bool,
    #[allow(dead_code)]
    start_instant: std::time::Instant,
    request_id: Option<RequestId>,
    #[allow(dead_code)]
    pub user_partition_key: Option<InternedStr>,
    shard_id: ShardId,
    #[cfg(feature = "tracing")]
    trace_ctx: Option<TraceContext>,
    #[cfg(feature = "tracing")]
    upstream_tracing_key: Option<TracingKey>,
    #[cfg(feature = "tracing")]
    span_state: Option<Arc<SpanState>>,
    #[cfg(any(feature = "access-log", feature = "metrics"))]
    trans_state: Mutex<TransactionState>,
    #[cfg(any(feature = "access-log", feature = "metrics", feature = "tracing"))]
    trans_phase: TransactionPhase,
    #[cfg(feature = "instrumentation")]
    pub clock: quanta::Clock,
}

#[derive(Debug)]
#[cfg(any(feature = "access-log", feature = "metrics", feature = "tracing"))]
struct TransactionPhase(AtomicUsize);

#[cfg(any(feature = "access-log", feature = "metrics", feature = "tracing"))]
impl TransactionPhase {
    fn new() -> Self {
        TransactionPhase(AtomicUsize::new(0))
    }

    fn is_complete(&self) -> bool {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) > 0
    }
}

impl Default for TransactionContext {
    fn default() -> Self {
        TransactionContext {
            #[cfg(feature = "access-log")]
            has_access_log: false,
            start_instant: std::time::Instant::now(),
            request_id: None,
            user_partition_key: None,
            shard_id: get_shard_id!(),
            #[cfg(any(feature = "access-log", feature = "metrics"))]
            trans_state: Mutex::new(TransactionState::default()),
            #[cfg(feature = "tracing")]
            trace_ctx: None,
            #[cfg(feature = "tracing")]
            upstream_tracing_key: None,
            #[cfg(feature = "tracing")]
            span_state: None,
            #[cfg(any(feature = "access-log", feature = "metrics", feature = "tracing"))]
            trans_phase: TransactionPhase::new(),
            #[cfg(feature = "instrumentation")]
            clock: quanta::Clock::new(),
        }
    }
}

#[cfg(feature = "access-log")]
struct EventInfo {
    #[allow(dead_code)]
    body_kind: BodyKind,
    event_kind: Option<EventKind>,
    response_flags: ResponseFlags,
}

impl TransactionContext {
    #[allow(clippy::too_many_arguments, clippy::unnecessary_operation)]
    pub fn reset(
        &mut self,
        request_id: Option<RequestId>,
        user_partition_key: Option<InternedStr>,
        thread_id: ShardId,
        #[cfg(feature = "access-log")] access_log: &[AccessLog],
        #[cfg(feature = "tracing")] trace_ctx: Option<TraceContext>,
        #[cfg(feature = "tracing")] upstream_tracing_key: Option<TracingKey>,
        #[cfg(feature = "tracing")] server_span: Option<BoxedSpan>,
    ) {
        #[cfg(feature = "access-log")]
        {
            self.has_access_log = !access_log.is_empty()
        };

        self.start_instant = std::time::Instant::now();
        self.request_id = request_id;
        self.user_partition_key = user_partition_key;

        #[cfg(any(feature = "access-log", feature = "metrics"))]
        {
            *self.trans_state.get_mut() = TransactionState::new(
                #[cfg(feature = "access-log")]
                access_log,
            )
        };

        #[cfg(feature = "tracing")]
        {
            self.trace_ctx = trace_ctx;
            self.upstream_tracing_key = upstream_tracing_key;
            self.span_state = server_span.map(|span| Arc::new(SpanState::new(Some(span))))
        };

        self.shard_id = thread_id;

        #[cfg(any(feature = "access-log", feature = "metrics", feature = "tracing"))]
        {
            self.trans_phase = TransactionPhase::new()
        };

        #[cfg(feature = "instrumentation")]
        {
            self.clock = quanta::Clock::new()
        };
    }
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: Option<RequestId>,
        user_partition_key: Option<InternedStr>,
        thread_id: ShardId,
        #[cfg(feature = "access-log")] access_log: &[AccessLog],
        #[cfg(feature = "tracing")] trace_ctx: Option<TraceContext>,
        #[cfg(feature = "tracing")] upstream_tracing_key: Option<TracingKey>,
        #[cfg(feature = "tracing")] server_span: Option<BoxedSpan>,
    ) -> Self {
        TransactionContext {
            #[cfg(feature = "access-log")]
            has_access_log: !access_log.is_empty(),
            start_instant: std::time::Instant::now(),
            request_id,
            user_partition_key,
            #[cfg(any(feature = "access-log", feature = "metrics"))]
            trans_state: Mutex::new(TransactionState::new(
                #[cfg(feature = "access-log")]
                access_log,
            )),
            #[cfg(feature = "tracing")]
            trace_ctx,
            #[cfg(feature = "tracing")]
            upstream_tracing_key,
            #[cfg(feature = "tracing")]
            span_state: server_span.map(|span| Arc::new(SpanState::new(Some(span)))),
            shard_id: thread_id,
            #[cfg(any(feature = "access-log", feature = "metrics", feature = "tracing"))]
            trans_phase: TransactionPhase::new(),
            #[cfg(feature = "instrumentation")]
            clock: quanta::Clock::new(),
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    #[inline]
    pub(crate) fn propagated_request_id(&self) -> Option<&HeaderValue> {
        self.request_id.as_ref().and_then(RequestId::propagate_ref)
    }

    pub(crate) fn begin_upstream_span(&self, span_name: &str) -> ScopedClientSpan {
        #[cfg(feature = "tracing")]
        {
            HttpTracer::begin_scoped_client_span(self.trace_ctx.as_ref(), self.upstream_tracing_key.as_ref(), span_name)
        }

        #[cfg(not(feature = "tracing"))]
        {
            let _ = span_name;
            ScopedClientSpan::disabled()
        }
    }

    #[cfg(feature = "access-log")]
    pub fn with_loggers<F>(&self, mut f: F)
    where
        F: FnMut(&mut [arion_format::LogFormatter]),
    {
        if self.has_access_log {
            let mut state = self.trans_state.lock();
            if let Some(loggers) = state.loggers.as_mut() {
                f(loggers);
            }
        }
    }

    #[allow(unused_variables)]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::let_unit_value)]
    fn trace_status_code(&self, res: &Result<Response<ArionClientBody>>, listener_name: InternedStr) {
        if let Ok(response) = &res {
            let status_code = response.status().as_u16();

            with_server_span!(self.span_state, |srv_span: &mut BoxedSpan| srv_span
                .set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, i64::from(status_code))));

            #[cfg(feature = "metrics")]
            let listener_attr = [KeyValue::new("listener", listener_name.as_str())];

            #[cfg(feature = "metrics")]
            let user_attrs = self.user_partition_key.map(|user_key| {
                [KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_key.as_str())]
            });

            #[cfg(feature = "metrics")]
            if let Some(ref attrs) = user_attrs {
                with_metric!(user::INVOCATIONS, add, 1, self.shard_id(), attrs);
            }

            #[allow(clippy::match_same_arms)]
            match status_code {
                100..200 => {
                    with_metric!(http::DOWNSTREAM_RQ_1XX, add, 1, self.shard_id(), &listener_attr);
                    #[cfg(feature = "metrics")]
                    if let Some(ref a) = user_attrs {
                        {
                            with_metric!(user::HTTP_1XX_RESPONSES, add, 1, self.shard_id(), a);
                        }
                    }
                },
                200..300 => {
                    with_metric!(http::DOWNSTREAM_RQ_2XX, add, 1, self.shard_id(), &listener_attr);
                    #[cfg(feature = "metrics")]
                    if let Some(ref a) = user_attrs {
                        {
                            with_metric!(user::HTTP_2XX_RESPONSES, add, 1, self.shard_id(), a);
                        }
                    }
                },
                300..400 => {
                    with_metric!(http::DOWNSTREAM_RQ_3XX, add, 1, self.shard_id(), &listener_attr);
                    #[cfg(feature = "metrics")]
                    if let Some(ref a) = user_attrs {
                        {
                            with_metric!(user::HTTP_3XX_RESPONSES, add, 1, self.shard_id(), a);
                        }
                    }
                },
                400..500 => {
                    with_metric!(http::DOWNSTREAM_RQ_4XX, add, 1, self.shard_id(), &listener_attr);
                    #[cfg(feature = "metrics")]
                    if let Some(ref a) = user_attrs {
                        with_metric!(user::USER_ERRORS, add, 1, self.shard_id(), a);
                        with_metric!(user::TOTAL_ERRORS, add, 1, self.shard_id(), a);
                        with_metric!(user::HTTP_4XX_RESPONSES, add, 1, self.shard_id(), a);
                        match status_code {
                            404 => {
                                with_metric!(user::HTTP_404_RESPONSES, add, 1, self.shard_id(), a);
                            },
                            429 => {
                                with_metric!(user::THROTTLES, add, 1, self.shard_id(), a);
                            },
                            _ => {},
                        }
                    }
                },
                _ => {
                    with_metric!(http::DOWNSTREAM_RQ_5XX, add, 1, self.shard_id(), &listener_attr);
                    #[cfg(feature = "metrics")]
                    if let Some(ref a) = user_attrs {
                        with_metric!(user::SYSTEM_ERRORS, add, 1, self.shard_id(), a);
                        with_metric!(user::TOTAL_ERRORS, add, 1, self.shard_id(), a);
                        with_metric!(user::HTTP_5XX_RESPONSES, add, 1, self.shard_id(), a);
                        match status_code {
                            502 => {
                                with_metric!(user::HTTP_502_RESPONSES, add, 1, self.shard_id(), a);
                            },
                            504 => {
                                with_metric!(user::HTTP_504_RESPONSES, add, 1, self.shard_id(), a);
                            },
                            _ => (),
                        }
                    }

                    with_server_span!(self.span_state, |srv_span: &mut BoxedSpan| {
                        srv_span.set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, 500));
                        srv_span.set_status(Status::error("5xx"));
                    });

                    with_client_span!(self.span_state, |clt_span: &mut BoxedSpan| {
                        clt_span.set_status(Status::error("5xx"));
                    });
                },
            }
        } else {
            with_metric!(
                http::DOWNSTREAM_RQ_5XX,
                add,
                1,
                self.shard_id(),
                &[KeyValue::new("listener", listener_name.as_str())]
            );

            with_server_span!(self.span_state, |srv_span: &mut BoxedSpan| {
                srv_span.set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, 500));
                srv_span.set_status(Status::error("5xx"));
            });

            with_client_span!(self.span_state, |clt_span: &mut BoxedSpan| {
                clt_span.set_status(Status::error("5xx"));
            });
        }
    }
}

#[derive(Clone, Copy)]
pub struct HttpPipelineSvc;

impl HttpPipelineSvc {
    pub const fn new() -> Self {
        Self
    }

    #[allow(clippy::too_many_lines, clippy::large_futures)]
    async fn call(
        &self,
        manager: &HttpConnectionManager,
        req: RoutedHttpRequest<'_, ArionRequestBody>,
    ) -> StdResult<Response<ArionClientBody>, crate::Error> {
        let RoutedHttpRequest { http: HttpRequest { mut request, ctx }, routing_state } = req;

        #[allow(unused_variables)]
        let listener_name = manager.listener_name;
        #[allow(unused_variables)]
        let filterchain_id = manager.filterchain_id;
        let downstream_addr = ctx.conn.downstream_peer_address();

        if ctx.conn.stream_metrics.inc_requests() == 0 {
            #[cfg(feature = "metrics")]
            if let Some(user_key) = ctx.txn.user_partition_key {
                let user_attr_name = metrics::USER_KEY.attribute_name().unwrap_or("user");
                with_metric!(
                    user::CONNECTIONS,
                    add,
                    1,
                    ctx.txn.shard_id,
                    &[KeyValue::new(user_attr_name, user_key.as_str())]
                );
                with_metric!(
                    user::CONNECTIONS_ACTIVE,
                    add,
                    1,
                    ctx.txn.shard_id,
                    &[KeyValue::new(user_attr_name, user_key.as_str())]
                );

                // store the user_partition_key to decrement CONNECTIONS_ACTIVE later, when the stream is closed.
                ctx.conn.stream_metrics.set_user_partition_key(user_key);
            }
        }

        // apply the request header modifiers: early mutations first (Envoy parity),
        // then XFF and the rest of the prerouting functions.
        http_modifiers::apply_prerouting_functions(
            &mut request,
            downstream_addr,
            manager.xff_settings,
            &ctx.conn,
            &manager.early_header_mutation,
        );

        // process request, get the response..
        let result = routing_state.to_response(&ctx, request, manager).await;

        // calculate the time to first byte..
        #[cfg(any(feature = "access-log", feature = "metrics"))]
        let first_byte_instant = Instant::now();

        result.map(|mut response| {
            // set the request id on the response...
            manager
                .request_id_handler
                .apply_to(&mut response, ctx.txn.request_id.as_ref().and_then(|x| x.propagate_ref()));

            #[cfg(feature = "access-log")]
            let (initial_flags, initial_event) = {
                let ec = response.extensions().get::<EventErrorContext>();
                (ec.map(|ec| ec.response_flags).unwrap_or_default(), ec.and_then(|ec| ec.event_kind.clone()))
            };

            #[cfg(feature = "access-log")]
            {
                use crate::with_access_log;

                ctx.txn.with_loggers(|loggers| {
                    with_access_log!(
                        loggers,
                        DownstreamResponseContext {
                            response: &response,
                            response_head_size: response_head_size(&response)
                        }
                    )
                })
            };

            #[cfg(any(feature = "access-log", feature = "tracing", feature = "metrics"))]
            let trans_ctx = ctx.txn;
            let stream_metrics = ctx.conn.stream_metrics;

            response.map(move |body| {
                InstrumentedBody::new(
                    BodyKind::Response,
                    body,
                    Some(stream_metrics),
                    #[allow(unused_variables)]
                    move |body_bytes, stream_metrics, body_error, body_flags| {
                        #[cfg(any(feature = "access-log", feature = "metrics"))]
                        {
                            let mut trans_state = trans_ctx.trans_state.lock();
                            #[allow(unused_variables)]
                            let duration = first_byte_instant.saturating_duration_since(trans_ctx.start_instant);
                            #[allow(unused_variables)]
                            let tx_duration = Instant::now().saturating_duration_since(first_byte_instant);

                            #[cfg(feature = "metrics")]
                            {
                                if let (Some(start_time), Some(cluster)) =
                                    (trans_state.upstream_start_instant, trans_state.upstream_cluster_name)
                                {
                                    let elapsed_ms = u64::try_from(start_time.elapsed().as_millis()).unwrap_or(0);
                                    let shard_id = trans_ctx.shard_id();
                                    crate::with_histogram!(
                                        clusters::UPSTREAM_RQ_TIME,
                                        record,
                                        elapsed_ms,
                                        shard_id,
                                        &[opentelemetry::KeyValue::new("cluster", cluster.as_str())]
                                    );
                                    if let Some(user_key) = trans_ctx.user_partition_key {
                                        let user_attr_name =
                                            crate::metrics::USER_KEY.attribute_name().unwrap_or("user");
                                        crate::with_histogram!(
                                            user::UPSTREAM_RQ_TIME,
                                            record,
                                            elapsed_ms,
                                            shard_id,
                                            &[opentelemetry::KeyValue::new(user_attr_name, user_key.as_str())]
                                        );
                                    }
                                }
                            }

                            #[cfg(feature = "access-log")]
                            if let Some(loggers) = trans_state.loggers.as_mut() {
                                with_access_log!(
                                    loggers.as_mut(),
                                    HttpResponseDurationContext { duration, tx_duration }
                                );
                            }

                            if trans_ctx.trans_phase.is_complete() {
                                drop(trans_state);
                                stream_metrics.add_flush_callback(HttpTxnFlush {
                                    trans_ctx: trans_ctx.clone(),
                                    listener_name,
                                    filterchain_id,
                                    body_bytes,
                                    body_is_response: true,
                                    #[cfg(feature = "access-log")]
                                    extra_event: initial_event.clone().or(body_error),
                                    #[cfg(feature = "access-log")]
                                    extra_flags: initial_flags | body_flags,
                                });
                            } else {
                                trans_state.bytes = body_bytes;
                                #[cfg(feature = "access-log")]
                                {
                                    trans_state.flags = initial_flags | body_flags;
                                    trans_state.event = initial_event.or(body_error);
                                }
                            }
                        }

                        #[cfg(feature = "tracing")]
                        if trans_ctx.trans_phase.is_complete()
                            && let Some(span) = trans_ctx.span_state.as_ref()
                        {
                            span.end();
                        }
                    },
                )
            })
        })
    }
}

// has to be a trait due to foreign impl rules.
pub trait RequestHandler<R, A>: Sized {
    fn to_response(
        self,
        ctx: &RequestCtx,
        request: R,
        arg: A,
    ) -> impl Future<Output = Result<Response<ArionResponseBody>>> + Send;
}

impl RequestHandler<Request<ArionRequestBody>, &HttpConnectionManager> for &RoutingState {
    #[allow(clippy::too_many_lines)]
    async fn to_response(
        self,
        ctx: &RequestCtx,
        mut request: Request<ArionRequestBody>,
        arg: &HttpConnectionManager,
    ) -> Result<Response<ArionResponseBody>> {
        let mut cached_route = self.match_request_route(&request);
        let mut active_filters: SmallVec<[HttpFilterValue; 4]> = SmallVec::new();
        let connection_manager = arg;

        let mut filter_start_idx = 0;
        let filter_response = 'filter_loop: loop {
            let Some(ref chosen_route) = cached_route else {
                break 'filter_loop FilterDecision::DirectResponse(Box::new(
                    SyntheticHttpResponse::not_found(
                        EventFailure::RouteNotFound.into(),
                        ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
                    )
                    .into_response(request.version()),
                ));
            };

            let route_filters = chosen_route.http_filters;
            let mut reroute = false;

            for filter in route_filters.iter().skip(filter_start_idx) {
                filter_start_idx += 1;
                if filter.disabled {
                    continue;
                }

                let Some(filter_config) = &filter.filter else {
                    continue;
                };

                let mut filter_value = filter_config.new_from();
                let filter_res = filter_value.apply_request(&mut request, ctx).await;

                match filter_res {
                    FilterDecision::Continue => {
                        active_filters.push(filter_value);
                    },
                    FilterDecision::DirectResponse(_) => {
                        break 'filter_loop filter_res;
                    },
                    FilterDecision::Reroute => {
                        active_filters.push(filter_value);
                        reroute = true;
                        break;
                    },
                }
            }

            if reroute {
                debug!("rerouting request...");
                cached_route = self.match_request_route(&request);
            } else {
                break 'filter_loop FilterDecision::Continue;
            }
        };

        #[allow(clippy::single_match_else)]
        let mut response = match cached_route {
            None => SyntheticHttpResponse::not_found(
                EventFailure::RouteNotFound.into(),
                ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
            )
            .into_response(request.version()),
            Some(cached_route) => match filter_response {
                FilterDecision::DirectResponse(response) => {
                    let mut response = *response;
                    apply_mutations_on_response(
                        &mut response,
                        self,
                        &cached_route,
                        self.most_specific_header_mutations_wins,
                        &ctx.conn,
                    );
                    response
                },
                _ => {
                    let websocket_enabled_by_default = connection_manager.websocket_enabled_by_default;

                    let mut response = match &cached_route.route.action {
                        Action::DirectResponse(dr) => dr.to_response(ctx, request, &cached_route.route.name).await,
                        Action::Redirect(rd) => {
                            let downstream_scheme = if connection_manager.downstream_tls {
                                ::http::uri::Scheme::HTTPS
                            } else {
                                ::http::uri::Scheme::HTTP
                            };
                            rd.to_response(
                                ctx,
                                request,
                                (&cached_route.route_match, &cached_route.route.name, downstream_scheme),
                            )
                            .await
                        },
                        Action::Route(route) => {
                            apply_mutations_on_request(
                                &mut request,
                                self,
                                &cached_route,
                                self.most_specific_header_mutations_wins,
                                &ctx.conn,
                            );

                            let remote_address = ctx.conn.downstream_peer_address();
                            route
                                .to_response(
                                    ctx,
                                    request,
                                    (
                                        RouteContext {
                                            route_name: &cached_route.route.name,
                                            retry_policy: route
                                                .retry_policy
                                                .as_ref()
                                                .or(cached_route.vh.retry_policy.as_ref()),
                                            route_match: &cached_route.route_match,
                                            remote_address,
                                            websocket_enabled_by_default,
                                        },
                                        connection_manager,
                                    ),
                                )
                                .await
                        },
                    }?;

                    #[cfg(feature = "metrics")]
                    if let Some(custom_metrics) = CUSTOM_METRICS.get() {
                        let mut attrs = SmallVec::<[KeyValue; 2]>::new();
                        if let Some(custom_keys) = metrics::CUSTOM_KEYS.get() {
                            for key in custom_keys {
                                if let Some(source) = key.source()
                                    && let Some(custom_id) =
                                        metrics::extract_custom_partition_key(response.headers(), Some(source))
                                {
                                    attrs.push(KeyValue::new(
                                        key.attribute_name().unwrap_or("custom"),
                                        custom_id.as_str(),
                                    ));
                                }
                            }
                        }
                        custom_metrics.with_headers(
                            MetricsHook::IncomingResponse,
                            response.headers(),
                            attrs.as_slice(),
                        );
                    }

                    #[cfg(feature = "access-log")]
                    ctx.txn.with_loggers(|loggers| {
                        if let Err(err) = crate::access_log::evaluate_base64_access_log_hook(
                            crate::access_log::AccessLogHook::IncomingResponse,
                            response.headers(),
                            loggers,
                        ) {
                            tracing::warn!("Failed to process access log header for IncomingResponse: {err}");
                        }
                    });

                    apply_mutations_on_response(
                        &mut response,
                        self,
                        &cached_route,
                        self.most_specific_header_mutations_wins,
                        &ctx.conn,
                    );
                    response
                },
            },
        };

        for filter in active_filters.iter_mut().rev() {
            let filter_res = filter.apply_response(&mut response, ctx).await;
            if let FilterDecision::DirectResponse(direct_response) = filter_res {
                response = *direct_response;
            }
        }

        Ok(response)
    }
}

fn apply_mutations_on_request<B>(
    target: &mut Request<B>,
    routing_state: &RoutingState,
    cached_route: &CachedRoute<'_>,
    most_specific_header_mutations_wins: bool,
    conn: &ConnMeta,
) where
    Route: ModifiersExtractor<Request<B>>,
    RuntimeVirtualHost: ModifiersExtractor<Request<B>>,
    RoutingState: ModifiersExtractor<Request<B>>,
{
    // Fast path: single cached bool, computed once per config load. Covers the common
    // case of no mutations configured anywhere.
    if !routing_state.has_request_mutations {
        return;
    }
    let (rc_remove, rc_add) = ModifiersExtractor::<Request<B>>::extract(routing_state);
    let (vh_remove, vh_add) = ModifiersExtractor::<Request<B>>::extract(cached_route.vh);
    let (route_remove, route_add) = ModifiersExtractor::<Request<B>>::extract(cached_route.route);

    // Second layer: skip levels with no mutations (e.g. only route-level configured).
    let apply_pair = |target: &mut Request<B>, remove: &HeaderModifiersRemove, add: &HeaderModifiersAdd| {
        if !remove.0.is_empty() {
            target.apply_mutation(remove);
        }
        if !add.0.is_empty() {
            target.apply_mutation((add, conn));
        }
    };

    if most_specific_header_mutations_wins {
        apply_pair(target, rc_remove, rc_add);
        apply_pair(target, vh_remove, vh_add);
        apply_pair(target, route_remove, route_add);
    } else {
        apply_pair(target, route_remove, route_add);
        apply_pair(target, vh_remove, vh_add);
        apply_pair(target, rc_remove, rc_add);
    }
}

fn apply_mutations_on_response<B>(
    target: &mut Response<B>,
    routing_state: &RoutingState,
    cached_route: &CachedRoute<'_>,
    most_specific_header_mutations_wins: bool,
    conn: &ConnMeta,
) where
    Route: ModifiersExtractor<Response<B>>,
    RuntimeVirtualHost: ModifiersExtractor<Response<B>>,
    RoutingState: ModifiersExtractor<Response<B>>,
{
    // Fast path: see `apply_mutations_on_request`.
    if !routing_state.has_response_mutations {
        return;
    }
    let (rc_remove, rc_add) = ModifiersExtractor::<Response<B>>::extract(routing_state);
    let (vh_remove, vh_add) = ModifiersExtractor::<Response<B>>::extract(cached_route.vh);
    let (route_remove, route_add) = ModifiersExtractor::<Response<B>>::extract(cached_route.route);

    // Second layer: skip levels with no mutations.
    let apply_pair = |target: &mut Response<B>, remove: &HeaderModifiersRemove, add: &HeaderModifiersAdd| {
        if !remove.0.is_empty() {
            target.apply_mutation(remove);
        }
        if !add.0.is_empty() {
            target.apply_mutation((add, conn));
        }
    };

    if most_specific_header_mutations_wins {
        apply_pair(target, rc_remove, rc_add);
        apply_pair(target, vh_remove, vh_add);
        apply_pair(target, route_remove, route_add);
    } else {
        apply_pair(target, route_remove, route_add);
        apply_pair(target, vh_remove, vh_add);
        apply_pair(target, rc_remove, rc_add);
    }
}

// --- Typed request envelope (Service stack) ---

/// Per-request context always present after `TransactionLifecycleSvc`.
#[derive(Clone, Debug, Default)]
pub struct RequestCtx {
    pub conn: ConnMeta,
    pub txn: PooledTxnCtx,
}

impl RequestCtx {
    #[inline]
    pub fn new(conn: ConnMeta, txn: PooledTxnCtx) -> Self {
        Self { conn, txn }
    }

    #[inline]
    pub(crate) fn propagated_request_id(&self) -> Option<&HeaderValue> {
        self.txn.propagated_request_id()
    }

    #[inline]
    pub(crate) fn begin_upstream_span(&self, span_name: &str) -> ScopedClientSpan {
        self.txn.begin_upstream_span(span_name)
    }
}

/// HTTP request + full request context (conn + transaction).
pub struct HttpRequest<B> {
    pub request: Request<B>,
    pub ctx: RequestCtx,
}

impl<B> HttpRequest<B> {
    #[inline]
    pub fn map_body<B2>(self, f: impl FnOnce(B) -> B2) -> HttpRequest<B2> {
        HttpRequest { request: self.request.map(f), ctx: self.ctx }
    }
}

/// `HttpRequest` after route configuration has been resolved.
///
/// Holds a borrow of the `ArcSwap` guard's `RoutingState` instead of a cloned `StdArc`:
/// inner stages are unboxed futures on the same task, so nothing requires owned
/// (`'static`) access here — the guard keeps the config alive across `.await`
/// exactly like the cloned `Arc` did.
pub struct RoutedHttpRequest<'a, B> {
    pub http: HttpRequest<B>,
    pub routing_state: &'a RoutingState,
}

#[derive(Clone)]
pub struct TransactionLifecycleSvc<S> {
    conn: ConnMeta,
    manager: Arc<HttpConnectionManager>,
    inner: S,
}

impl<S> TransactionLifecycleSvc<S> {
    pub fn new(manager: Arc<HttpConnectionManager>, conn: ConnMeta, inner: S) -> Self {
        Self { conn, manager, inner }
    }
}

impl TransactionLifecycleSvc<TransactionSvc<HttpPipelineSvc>> {
    // NOTE: `self` is taken as an 8-byte `StdArc` on purpose. hyper moves this (~18 KB) future into
    // its `in_flight` box on every request, and only writes it in place when the arguments sit next
    // to the state byte. rustc orders fields by the largest power of two dividing their size: an
    // 8-byte `StdArc` can be placed before the inner future (whose size is any multiple of 16), away
    // from `incoming_request`, which turns the in-place write into a copy of the whole future.
    // `StdArc` (rather than `&self`) is also required: hyper-util bounds the per-request future
    // with `S::Future: 'static`, so it must own — not borrow — the connection state. Cloning the
    // `Arc` (one atomic inc) is cheaper than cloning the whole struct (three: conn x2 + manager).
    #[allow(clippy::large_futures)]
    pub async fn handle_request(
        self: StdArc<Self>,
        incoming_request: Request<Incoming>,
    ) -> crate::Result<Response<ArionClientBody>> {
        let conn = self.conn.clone();
        let manager = &self.manager;
        let incoming_request_id = RequestId::from_request(&incoming_request);
        let incoming_version = incoming_request.version();
        let listener_name = manager.listener_name;

        let access_log_enabled = {
            #[cfg(feature = "access-log")]
            {
                is_access_log_enabled()
            }
            #[cfg(not(feature = "access-log"))]
            false
        };

        let is_internal = http_modifiers::is_internal_ip(conn.downstream_peer_address().ip());

        #[allow(unused_mut)]
        let (mut request, request_id) = manager.request_id_handler.apply_policy(
            incoming_request,
            access_log_enabled,
            incoming_request_id.as_ref(),
            is_internal,
        );

        #[cfg(feature = "tracing")]
        let trace_context =
            manager.http_tracer.try_build_trace_context(&request, incoming_request_id.or(request_id.clone()));

        #[cfg(feature = "tracing")]
        let tracing_key = manager.get_tracing_key();

        #[cfg(feature = "tracing")]
        let mut server_span = manager.http_tracer.try_create_span(
            trace_context.as_ref(),
            &tracing_key,
            SpanKind::Server,
            SpanName::Host(&request),
        );

        #[cfg(feature = "tracing")]
        let upstream_tracing_key = (trace_context.as_ref().is_some_and(TraceContext::should_sample)
            && manager.http_tracer.upstream_spans_enabled())
        .then_some(tracing_key);

        #[cfg(feature = "tracing")]
        if let Some(span) = server_span.as_mut() {
            set_attributes_from_request(span, &request);
        }

        #[cfg(feature = "metrics")]
        let user_partition_key = metrics::extract_user_partition_key(
            (request.headers(), conn.downstream.sni.as_ref()),
            metrics::USER_KEY.source(),
        );

        #[cfg(not(feature = "metrics"))]
        let user_partition_key = None;

        #[allow(clippy::let_unit_value)]
        let shard_id = get_shard_id!();

        let trans_ctx = PooledTxnCtx::acquire(
            request_id,
            user_partition_key,
            shard_id,
            #[cfg(feature = "access-log")]
            &manager.access_log,
            #[cfg(feature = "tracing")]
            trace_context,
            #[cfg(feature = "tracing")]
            upstream_tracing_key,
            #[cfg(feature = "tracing")]
            server_span,
        );

        #[cfg(feature = "tracing")]
        if let Some(trace_ctx) = trans_ctx.trace_ctx.as_ref() {
            manager.http_tracer.update_tracing_headers(trace_ctx, &mut request);
        }

        let ctx = RequestCtx::new(conn, trans_ctx.clone());
        let http_req = HttpRequest { request, ctx };

        let response = self.inner.call(manager, http_req).await;

        trans_ctx.trace_status_code(&response, listener_name);
        if let Err(err) = response {
            error!("Error during handling HTTP transaction: {}", err);
            let msg = err.to_string();
            let event_error = err.as_upstream_error().cloned().unwrap_or_else(|| UpstreamError::Other(msg.clone()));
            let response = SyntheticHttpResponse::internal_server_error(
                EventKind::Upstream(event_error),
                ResponseFlags(arion_format::types::ResponseFlags::LOCAL_RESET),
            )
            .with_body(msg)
            .into_response(incoming_version);
            Ok(response.map(|body| InstrumentedBody::new(BodyKind::Response, body, None, |_, _, _, _| {})))
        } else {
            response
        }
    }
}

#[derive(Clone)]
pub struct TransactionSvc<S> {
    inner: S,
}

impl<S> TransactionSvc<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl TransactionSvc<HttpPipelineSvc> {
    #[allow(clippy::too_many_lines, clippy::large_futures)]
    async fn call(
        &self,
        manager: &HttpConnectionManager,
        req: HttpRequest<Incoming>,
    ) -> StdResult<Response<ArionClientBody>, crate::Error> {
        let HttpRequest { request, ctx } = req;
        let listener_name = manager.listener_name;
        let routing_state_guard = manager.routing_state.load();

        with_metric!(
            http::DOWNSTREAM_RQ_TOTAL,
            add,
            1,
            ctx.txn.shard_id(),
            &[KeyValue::new("listener", listener_name.as_str())]
        );
        with_metric!(
            http::DOWNSTREAM_RQ_ACTIVE,
            add,
            1,
            ctx.txn.shard_id(),
            &[KeyValue::new("listener", listener_name.as_str())]
        );

        #[cfg(feature = "metrics")]
        if let Some(custom_metrics) = CUSTOM_METRICS.get() {
            let mut attrs = SmallVec::<[KeyValue; 2]>::new();
            if let Some(custom_keys) = metrics::CUSTOM_KEYS.get() {
                for key in custom_keys {
                    if let Some(source) = key.source()
                        && let Some(custom_id) = metrics::extract_custom_partition_key(request.headers(), Some(source))
                    {
                        attrs.push(KeyValue::new(key.attribute_name().unwrap_or("custom"), custom_id.as_str()));
                    }
                }
            }
            custom_metrics.with_headers(MetricsHook::IncomingRequest, request.headers(), attrs.as_slice());
        }

        #[cfg(feature = "access-log")]
        ctx.txn.with_loggers(|loggers| {
            if let Err(err) = crate::access_log::evaluate_base64_access_log_hook(
                crate::access_log::AccessLogHook::IncomingRequest,
                request.headers(),
                loggers,
            ) {
                tracing::warn!("Failed to process access log header for IncomingRequest: {err}");
            }
        });

        #[allow(unused_variables)]
        let shard_id = ctx.txn.shard_id();
        scopeguard::defer! {
            with_metric!(http::DOWNSTREAM_RQ_ACTIVE, sub, 1, shard_id, &[KeyValue::new("listener", listener_name.as_str())]);
        }

        let req_timeout = manager.request_timeout;
        let filterchain_id = manager.filterchain_id;

        eval_http_init_context(&request, &ctx.txn, Some(ctx.conn.downstream.as_ref()));

        let Some(routing_state) = routing_state_guard.as_deref() else {
            return Ok(handle_route_conf_not_found(
                request.version(),
                &ctx.txn,
                Some(&ctx.conn.stream_metrics),
                listener_name,
                ctx.txn.user_partition_key,
                filterchain_id,
            ));
        };

        if let Some(response_error) = reject_request_if_invalid(
            &request,
            &ctx.txn,
            Some(&ctx.conn.stream_metrics),
            listener_name,
            ctx.txn.user_partition_key,
            filterchain_id,
        ) {
            return Ok(response_error);
        }

        let stream_metrics = Arc::clone(&ctx.conn.stream_metrics);
        #[cfg(any(feature = "access-log", feature = "tracing", feature = "metrics"))]
        let trans_ctx = ctx.txn.clone();
        let http = HttpRequest { request, ctx }.map_body(|body| {
            let body = TimeoutBody::new(req_timeout, PolyBody::from(body));
            InstrumentedBody::new(
                BodyKind::Request,
                body,
                Some(stream_metrics),
                #[allow(unused_variables)]
                move |body_bytes, stream_metrics, body_error, body_flags| {
                    #[cfg(any(feature = "access-log", feature = "metrics"))]
                    {
                        let mut trans_state = trans_ctx.trans_state.lock();
                        #[allow(unused_variables)]
                        let duration = trans_ctx.start_instant.elapsed();

                        #[cfg(feature = "access-log")]
                        if let Some(loggers) = trans_state.loggers.as_mut() {
                            with_access_log!(
                                loggers.as_mut(),
                                HttpRequestDurationContext { duration, tx_duration: duration }
                            );
                        }

                        if trans_ctx.trans_phase.is_complete() {
                            drop(trans_state);
                            stream_metrics.add_flush_callback(HttpTxnFlush {
                                trans_ctx: trans_ctx.clone(),
                                listener_name,
                                filterchain_id,
                                body_bytes,
                                body_is_response: false,
                                #[cfg(feature = "access-log")]
                                extra_event: body_error,
                                #[cfg(feature = "access-log")]
                                extra_flags: body_flags,
                            });
                        } else {
                            trans_state.bytes = body_bytes;
                            #[cfg(feature = "access-log")]
                            {
                                trans_state.event = body_error;
                                trans_state.flags = body_flags;
                            }
                        }
                    }

                    #[cfg(feature = "tracing")]
                    if trans_ctx.trans_phase.is_complete()
                        && let Some(span) = trans_ctx.span_state.as_ref()
                    {
                        span.end();
                    }
                },
            )
        });

        #[cfg(feature = "access-log")]
        let trans_ctx_clone = http.ctx.txn.clone();
        let response = self.inner.call(manager, RoutedHttpRequest { http, routing_state }).await;

        #[cfg(feature = "metrics")]
        if let Ok(response) = &response
            && let Some(custom_metrics) = CUSTOM_METRICS.get()
        {
            let mut attrs = SmallVec::<[KeyValue; 2]>::new();
            if let Some(custom_keys) = metrics::CUSTOM_KEYS.get() {
                for key in custom_keys {
                    if let Some(source) = key.source()
                        && let Some(custom_id) = metrics::extract_custom_partition_key(response.headers(), Some(source))
                    {
                        attrs.push(KeyValue::new(key.attribute_name().unwrap_or("custom"), custom_id.as_str()));
                    }
                }
            }
            custom_metrics.with_headers(MetricsHook::DownstreamResponse, response.headers(), attrs.as_slice());
        }

        #[cfg(feature = "access-log")]
        if let Ok(response) = &response {
            trans_ctx_clone.with_loggers(|loggers| {
                if let Err(err) = crate::access_log::evaluate_base64_access_log_hook(
                    crate::access_log::AccessLogHook::DownstreamResponse,
                    response.headers(),
                    loggers,
                ) {
                    tracing::warn!("Failed to process access log header for DownstreamResponse: {err}");
                }
            });
        }
        response
    }
}

fn eval_http_init_context<R>(
    #[allow(unused_variables)] request: &Request<R>,
    #[allow(unused_variables)] trans_ctx: &TransactionContext,
    #[allow(unused_variables)] metadata: Option<&DownstreamMetadata>,
) {
    #[cfg(feature = "tracing")]
    #[allow(unused_variables)]
    let trace_id =
        trans_ctx.trace_ctx.as_ref().and_then(|t| t.map_child(arion_tracing::trace_info::TraceInfo::trace_id));

    #[cfg(not(feature = "tracing"))]
    #[allow(unused_variables)]
    let trace_id: Option<u128> = None;

    #[cfg(feature = "access-log")]
    {
        use crate::with_access_log;
        use arion_format::context::SocketAddrContext;

        let server_name = metadata.and_then(|md| md.sni.as_ref().map(SmolStr::as_str));

        #[cfg(feature = "access-log")]
        trans_ctx.with_loggers(|loggers| {
            with_access_log!(
                loggers,
                InitHttpContext {
                    start_time: std::time::SystemTime::now(),
                    downstream_request: request,
                    request_head_size: request_head_size(request),
                    trace_id,
                    server_name,
                    socket_address: SocketAddrContext {
                        downstream_local_addr: metadata.map(|md| md.connection.local_address()),
                        downstream_peer_addr: metadata.map(|md| md.connection.peer_address()),
                        upstream_local_addr: None,
                        upstream_peer_addr: None,
                    }
                }
            );
        });
    }
}

#[cfg(feature = "access-log")]
struct AccessLogFinishContext<'a> {
    event: EventInfo,
    access_loggers: Option<&'a mut [LogFormatter]>,
}

#[cfg(feature = "metrics")]
struct MetricsFinishContext {
    shard_id: ShardId,
}

#[cfg(any(feature = "access-log", feature = "metrics"))]
pub(crate) struct HttpTxnFlush {
    trans_ctx: PooledTxnCtx,
    listener_name: InternedStr,
    filterchain_id: u64,
    body_bytes: u64,
    body_is_response: bool,
    #[cfg(feature = "access-log")]
    extra_event: Option<EventKind>,
    #[cfg(feature = "access-log")]
    extra_flags: ResponseFlags,
}

#[cfg(any(feature = "access-log", feature = "metrics"))]
impl OnFlush for HttpTxnFlush {
    fn run(self, metrics: &StreamMetrics) {
        let mut trans_state = self.trans_ctx.trans_state.lock();
        let stream_metrics = metrics;
        let ctx_bytes = trans_state.bytes;
        #[cfg(feature = "access-log")]
        let ctx_flags = trans_state.flags;
        #[cfg(feature = "access-log")]
        let ctx_event = trans_state.event.clone();

        let (bytes_received, bytes_sent) =
            if self.body_is_response { (ctx_bytes, self.body_bytes) } else { (self.body_bytes, ctx_bytes) };

        eval_http_finish_context(FinishContextParams {
            stream_metrics,
            listener_name: self.listener_name,
            user_partition_key: self.trans_ctx.user_partition_key,
            filterchain_id: self.filterchain_id,
            bytes_received,
            bytes_sent,
            trans_start_time: self.trans_ctx.start_instant,
            #[cfg(feature = "metrics")]
            m_ctx: MetricsFinishContext { shard_id: self.trans_ctx.shard_id() },
            #[cfg(feature = "access-log")]
            al_ctx: AccessLogFinishContext {
                event: EventInfo {
                    body_kind: if self.body_is_response { BodyKind::Response } else { BodyKind::Request },
                    event_kind: ctx_event.or(self.extra_event),
                    response_flags: ctx_flags | self.extra_flags,
                },
                access_loggers: trans_state.loggers.as_deref_mut(),
            },
        });
    }
}

#[cfg(any(feature = "access-log", feature = "metrics"))]
struct FinishContextParams<'a> {
    stream_metrics: &'a StreamMetrics,
    listener_name: InternedStr,
    #[allow(dead_code)]
    user_partition_key: Option<InternedStr>,
    #[allow(dead_code)]
    filterchain_id: u64,
    #[allow(dead_code)]
    bytes_received: u64,
    #[allow(dead_code)]
    bytes_sent: u64,
    trans_start_time: Instant,
    #[cfg(feature = "metrics")]
    m_ctx: MetricsFinishContext,
    #[cfg(feature = "access-log")]
    al_ctx: AccessLogFinishContext<'a>,
}

#[allow(unused_mut)]
#[cfg(any(feature = "access-log", feature = "metrics"))]
#[allow(clippy::too_many_lines)]
fn eval_http_finish_context(mut params: FinishContextParams<'_>) {
    #[cfg(feature = "access-log")]
    use arion_format::context::WireContext;

    let latency = params.trans_start_time.elapsed();

    #[cfg(feature = "metrics")]
    if let Some(user_key) = params.user_partition_key {
        let user_attr_name = metrics::USER_KEY.attribute_name().unwrap_or("user");
        with_histogram!(
            user::LATENCY,
            record,
            #[allow(clippy::cast_possible_truncation)]
            {
                latency.as_millis() as u64
            },
            params.m_ctx.shard_id,
            &[KeyValue::new(user_attr_name, user_key.as_str())]
        );
    }

    #[cfg(feature = "access-log")]
    let duration = latency;

    #[cfg(feature = "access-log")]
    with_access_log!(
        params.al_ctx.access_loggers.as_deref_mut().unwrap_or(&mut []),
        FinishContext {
            duration,
            bytes_received: params.bytes_received,
            bytes_sent: params.bytes_sent,
            response_flags: params.al_ctx.event.response_flags.0,
            upstream_transport_failure_reason: params.al_ctx.event.event_kind.as_ref().and_then(|ev| {
                let EventKind::Upstream(err) = ev else {
                    return None;
                };
                UpstreamTransportEventError::try_from(err).ok().map(|e| e.0)
            }),
            response_code_details: params
                .al_ctx
                .event
                .event_kind
                .as_ref()
                .map_or(EventKind::Failure(EventFailure::ViaUpstream).code_details(), EventKind::code_details)
                .map(|d| d.0),
            connection_termination_details: params
                .al_ctx
                .event
                .event_kind
                .as_ref()
                .and_then(EventKind::termination_details)
                .map(|d| d.0),
        }
    );

    #[cfg(feature = "access-log")]
    let mut loggers: Option<&mut [LogFormatter]> = params.al_ctx.access_loggers;

    #[cfg(feature = "metrics")]
    let user_partition_key = params.user_partition_key;

    let (wire_bytes_received, wire_bytes_sent) = params.stream_metrics.txn_take_metrics();

    #[allow(unused_variables)]
    #[cfg(feature = "metrics")]
    let shard_id = params.m_ctx.shard_id;

    with_metric!(
        http::DOWNSTREAM_CX_RX_BYTES_TOTAL,
        add,
        wire_bytes_received,
        shard_id,
        &[KeyValue::new("listener", params.listener_name.as_str())]
    );

    with_metric!(
        http::DOWNSTREAM_CX_TX_BYTES_TOTAL,
        add,
        wire_bytes_sent,
        shard_id,
        &[KeyValue::new("listener", params.listener_name.as_str())]
    );

    #[cfg(feature = "metrics")]
    if let Some(user_key) = user_partition_key {
        let user_attr_name = metrics::USER_KEY.attribute_name().unwrap_or("user");
        with_metric!(
            user::BYTES_TX,
            add,
            wire_bytes_received,
            shard_id,
            &[
                KeyValue::new(user_attr_name, user_key.as_str()),
                KeyValue::new("listener", params.listener_name.as_str())
            ]
        );
        with_metric!(
            user::BYTES_RX,
            add,
            wire_bytes_sent,
            shard_id,
            &[
                KeyValue::new(user_attr_name, user_key.as_str()),
                KeyValue::new("listener", params.listener_name.as_str())
            ]
        );
    }

    #[cfg(feature = "access-log")]
    if let Some(loggers) = loggers
        && !loggers.is_empty()
    {
        with_access_log!(&mut *loggers, WireContext { wire_bytes_received, wire_bytes_sent });
        let messages = loggers.iter_mut().map(|l| l.clone().into_message()).collect::<Vec<_>>();
        _ = blocking_log_access(Target::ListenerFilterChain(params.listener_name, params.filterchain_id), messages);
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
#[allow(unused_variables)]
fn instrument_early_failure_response(
    response: Response<crate::ArionResponseBody>,
    trans_ctx: &PooledTxnCtx,
    stream_metrics: Option<&Arc<StreamMetrics>>,
    listener_name: InternedStr,
    user_partition_key: Option<InternedStr>,
    filterchain_id: u64,
) -> Response<ArionClientBody> {
    #[cfg(feature = "access-log")]
    let first_byte_instant = Instant::now();

    with_metric!(
        http::DOWNSTREAM_RQ_4XX,
        add,
        1,
        trans_ctx.shard_id(),
        &[KeyValue::new("listener", listener_name.as_str())]
    );

    #[cfg(feature = "tracing")]
    if let Some(state) = trans_ctx.span_state.as_ref()
        && let Some(ref mut span) = *state.server_span.lock()
    {
        span.set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, 400));
    }

    #[cfg(feature = "access-log")]
    trans_ctx.with_loggers(|loggers| {
        with_access_log!(
            loggers,
            DownstreamResponseContext { response: &response, response_head_size: response_head_size(&response) }
        );
    });

    #[cfg(feature = "access-log")]
    let (initial_flags, initial_event) = {
        let ec = response.extensions().get::<EventErrorContext>();
        (ec.map(|ec| ec.response_flags).unwrap_or_default(), ec.and_then(|ec| ec.event_kind.clone()))
    };

    #[cfg(any(feature = "access-log", feature = "tracing", feature = "metrics"))]
    let trans_ctx = trans_ctx.clone();

    let stream_metrics = stream_metrics.cloned();
    response.map(|body| {
        InstrumentedBody::new(
            BodyKind::Response,
            body,
            stream_metrics,
            #[allow(unused_variables)]
            move |body_bytes, stream_metrics, body_error, body_flags| {
                #[cfg(any(feature = "access-log", feature = "metrics"))]
                {
                    let mut trans_state = trans_ctx.trans_state.lock();

                    #[cfg(feature = "access-log")]
                    {
                        use crate::with_access_log;

                        #[allow(unused_variables)]
                        let duration = first_byte_instant.saturating_duration_since(trans_ctx.start_instant);
                        #[allow(unused_variables)]
                        let tx_duration = Instant::now().saturating_duration_since(first_byte_instant);

                        if let Some(loggers) = trans_state.loggers.as_mut() {
                            with_access_log!(loggers.as_mut(), HttpResponseDurationContext { duration, tx_duration })
                        }
                    };

                    if trans_ctx.trans_phase.is_complete() {
                        drop(trans_state);
                        stream_metrics.add_flush_callback(HttpTxnFlush {
                            trans_ctx: trans_ctx.clone(),
                            listener_name,
                            filterchain_id,
                            body_bytes,
                            body_is_response: true,
                            #[cfg(feature = "access-log")]
                            extra_event: initial_event.clone().or(body_error),
                            #[cfg(feature = "access-log")]
                            extra_flags: initial_flags | body_flags,
                        });
                    } else {
                        trans_state.bytes = body_bytes;
                        #[cfg(feature = "access-log")]
                        {
                            trans_state.flags = initial_flags | body_flags;
                            trans_state.event = initial_event.or(body_error);
                        }
                    }
                }

                #[cfg(feature = "tracing")]
                if trans_ctx.trans_phase.is_complete()
                    && let Some(span) = trans_ctx.span_state.as_ref()
                {
                    span.end();
                }
            },
        )
    })
}

/// Maximum allowed length for an HTTP method string.
const MAX_METHOD_LENGTH: usize = 1024;

/// Maximum allowed length for an HTTP URI string.
const MAX_URI_LENGTH: usize = 2048;

#[allow(clippy::too_many_arguments)]
fn reject_request_if_invalid(
    request: &Request<Incoming>,
    trans_ctx: &PooledTxnCtx,
    stream_metrics: Option<&Arc<StreamMetrics>>,
    listener_name: InternedStr,
    user_partition_key: Option<InternedStr>,
    filterchain_id: u64,
) -> Option<Response<crate::ArionClientBody>> {
    // check if request has no host header, or if it has multiple ones (invalid for http1.1)
    //
    if matches!(request.version(), ::http::Version::HTTP_11) {
        let count = request.headers().get_all(HOST).iter().count();
        if count != 1 {
            debug!("Invalid number of host headers: {}", count);
            let r = SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into())
                .with_close_connection(true)
                .into_response(request.version());
            return Some(instrument_early_failure_response(
                r,
                trans_ctx,
                stream_metrics,
                listener_name,
                user_partition_key,
                filterchain_id,
            ));
        }
    }

    // check if method is too long...
    //
    if request.method().as_str().len() > MAX_METHOD_LENGTH {
        debug!("Too long method: {} bytes", request.method().as_str());
        let r = SyntheticHttpResponse::custom_error(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            None,
            EventFailure::DirectResponse.into(),
            ResponseFlags::default(),
        )
        .into_response(request.version());
        return Some(instrument_early_failure_response(
            r,
            trans_ctx,
            stream_metrics,
            listener_name,
            user_partition_key,
            filterchain_id,
        ));
    }

    // check if uri/line is too long...
    //
    let mut counter = LengthCounter(0);
    _ = write!(&mut counter, "{}", request.uri());
    if counter.0 > MAX_URI_LENGTH {
        debug!("Too long uri: {} bytes", counter.0);
        let r = SyntheticHttpResponse::custom_error(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            None,
            EventFailure::DirectResponse.into(),
            ResponseFlags::default(),
        )
        .into_response(request.version());
        return Some(instrument_early_failure_response(
            r,
            trans_ctx,
            stream_metrics,
            listener_name,
            user_partition_key,
            filterchain_id,
        ));
    }

    None
}

#[allow(clippy::too_many_arguments)]
fn handle_route_conf_not_found(
    version: ::http::Version,
    trans_ctx: &PooledTxnCtx,
    stream_metrics: Option<&Arc<StreamMetrics>>,
    listener_name: InternedStr,
    user_partition_key: Option<InternedStr>,
    filterchain_id: u64,
) -> Response<crate::ArionClientBody> {
    // immediately return a SyntheticHttpResponse, and calculate the first byte instant
    let response = SyntheticHttpResponse::not_found(
        EventFailure::RouteNotFound.into(),
        ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
    )
    .into_response(version);

    instrument_early_failure_response(
        response,
        trans_ctx,
        stream_metrics,
        listener_name,
        user_partition_key,
        filterchain_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arion_configuration::config::network_filters::http_connection_manager::{
        header_modifier::{HeaderAppendAction, HeaderKeyValue, HeaderValueOption},
        http_filters::{
            FilterConfigOverride, FilterOverride, HttpFilter as HttpFilterConfig, HttpFilterType,
            local_rate_limit::LocalRateLimit,
        },
        route::{DirectResponseAction, PathMatcher, PathSpecifier, RouteMatch},
    };
    use arion_format::header_formatter::HeaderFormatter;
    use std::collections::HashMap;

    fn make_vh(name: &str, domains: &[&str]) -> VirtualHost {
        VirtualHost {
            name: name.into(),
            domains: domains.iter().copied().flat_map(MatchHost::try_from).collect(),
            ..Default::default()
        }
    }

    fn select_vh_name<'a>(table: &'a VirtualHostTable, host_header: &str) -> Option<&'a str> {
        let req = Request::builder().header("host", host_header).body(()).unwrap();
        let host = RequestHost::from_request(&req)?;
        table.select(host).map(|vh| vh.name.as_str())
    }

    /// Reference oracle based on `MatchHost::eval_lpm` with first-declaration-wins tie-breaking.
    fn reference_select_vh_name<'a>(virtual_hosts: &'a [VirtualHost], host: RequestHost<'_>) -> Option<&'a str> {
        let mut best: Option<(MatchHostScoreLPM, &'a str)> = None;
        for vh in virtual_hosts {
            if let Some(vh_score) = vh.domains.iter().filter_map(|d| d.eval_lpm(host)).max()
                && best.as_ref().is_none_or(|(best_score, _)| vh_score > *best_score)
            {
                best = Some((vh_score, vh.name.as_str()));
            }
        }
        best.map(|(_, name)| name)
    }

    #[test]
    fn test_select_virtual_hosts() {
        let vh1 = make_vh("vh1", &["domain1.com:8000", "domain1.com"]);
        let vh2 = make_vh("vh2", &["domain2.com"]);
        let vh3 = make_vh("vh3", &["*.domain3.com", "domain3.com"]);

        let table = VirtualHostTable::compile(vec![vh1.clone(), vh2.clone(), vh3.clone()], &[]);

        assert_eq!(select_vh_name(&table, "127.0.0.1:8000"), None);
        assert_eq!(select_vh_name(&table, "domain1.com:8000"), Some("vh1"));
        assert_eq!(select_vh_name(&table, "domain1.com"), Some("vh1"));
        assert_eq!(select_vh_name(&table, "domain3.com"), Some("vh3"));
        assert_eq!(select_vh_name(&table, "blah.domain3.com"), Some("vh3"));
        assert_eq!(select_vh_name(&table, "blah.domain3.com:8000"), Some("vh3"));
        assert_eq!(select_vh_name(&table, "domain2.com:8000"), Some("vh2"));
        assert_eq!(select_vh_name(&table, "domain1.com:9000"), Some("vh1"));

        let vh2_port = make_vh("vh2", &["domain2.com:8000"]);
        let table_port = VirtualHostTable::compile(vec![vh1, vh2_port.clone(), vh3], &[]);
        assert_eq!(select_vh_name(&table_port, "domain2.com"), None);

        let vh4 = make_vh("vh4", &["domain2.com"]);
        let table_both = VirtualHostTable::compile(vec![vh2_port, vh4], &[]);
        assert_eq!(select_vh_name(&table_both, "domain2.com:8000"), Some("vh2"));
        assert_eq!(select_vh_name(&table_both, "domain2.com:9000"), Some("vh4"));
    }

    #[test]
    fn test_select_virtual_hosts_precedence_and_lpm() {
        // Verify Envoy precedence: Exact > Longest Suffix > Longest Prefix > Wildcard
        let vh_wildcard = make_vh("vh_wildcard", &["*"]);
        let vh_prefix_short = make_vh("vh_prefix_short", &["api.*"]);
        let vh_prefix_long = make_vh("vh_prefix_long", &["api.v1.*"]);
        let vh_suffix_short = make_vh("vh_suffix_short", &["*.example.com"]);
        let vh_suffix_long = make_vh("vh_suffix_long", &["*.internal.example.com"]);
        let vh_exact = make_vh("vh_exact", &["api.v1.internal.example.com"]);

        let table = VirtualHostTable::compile(
            vec![vh_wildcard, vh_prefix_short, vh_prefix_long, vh_suffix_short, vh_suffix_long, vh_exact],
            &[],
        );

        // Exact beats everything (even when suffix and prefix also match)
        assert_eq!(select_vh_name(&table, "api.v1.internal.example.com"), Some("vh_exact"));
        assert_eq!(select_vh_name(&table, "api.v1.internal.example.com:8443"), Some("vh_exact"));

        // Longest suffix beats shorter suffix, prefixes, and wildcard
        assert_eq!(select_vh_name(&table, "api.v2.internal.example.com"), Some("vh_suffix_long"));
        assert_eq!(select_vh_name(&table, "svc.example.com"), Some("vh_suffix_short"));
        // Suffix beats prefix even when prefix also matches (`api.v1.other.example.com`)
        assert_eq!(select_vh_name(&table, "api.v1.other.example.com"), Some("vh_suffix_short"));

        // Longest prefix beats shorter prefix and wildcard
        assert_eq!(select_vh_name(&table, "api.v1.service.local"), Some("vh_prefix_long"));
        assert_eq!(select_vh_name(&table, "api.v2.service.local"), Some("vh_prefix_short"));

        // Wildcard catches unmatched hosts
        assert_eq!(select_vh_name(&table, "unmatched.org"), Some("vh_wildcard"));
    }

    #[test]
    fn test_select_virtual_hosts_full_vs_without_port_competition() {
        // Case 1: without_port Exact beats full Prefix (e.g. `foo.com:*` vs `foo.com`)
        let vh_prefix_port = make_vh("vh_prefix_port", &["foo.com:*"]);
        let vh_exact_noport = make_vh("vh_exact_noport", &["foo.com"]);
        let table = VirtualHostTable::compile(vec![vh_prefix_port, vh_exact_noport], &[]);
        assert_eq!(select_vh_name(&table, "foo.com:8080"), Some("vh_exact_noport"));

        // Case 2: full Exact (`foo.com:8080`) beats without_port Exact (`foo.com`) because len 12 > 7
        let vh_exact_noport = make_vh("vh_exact_noport", &["foo.com"]);
        let vh_exact_port = make_vh("vh_exact_port", &["foo.com:8080"]);
        let table = VirtualHostTable::compile(vec![vh_exact_noport, vh_exact_port], &[]);
        assert_eq!(select_vh_name(&table, "foo.com:8080"), Some("vh_exact_port"));
        assert_eq!(select_vh_name(&table, "foo.com:9090"), Some("vh_exact_noport"));

        // Case 3: full Suffix (`*.foo.com:8080`) beats without_port Suffix (`*.foo.com`) because len 13 > 8
        let vh_suffix_noport = make_vh("vh_suffix_noport", &["*.foo.com"]);
        let vh_suffix_port = make_vh("vh_suffix_port", &["*.foo.com:8080"]);
        let table = VirtualHostTable::compile(vec![vh_suffix_noport, vh_suffix_port], &[]);
        assert_eq!(select_vh_name(&table, "sub.foo.com:8080"), Some("vh_suffix_port"));
        assert_eq!(select_vh_name(&table, "sub.foo.com:9090"), Some("vh_suffix_noport"));
    }

    #[test]
    fn test_select_virtual_hosts_trailing_dot_and_boundaries() {
        let vh_exact = make_vh("vh_exact", &["example.com"]);
        let vh_suffix = make_vh("vh_suffix", &["*.sub.example.com"]);
        let vh_prefix = make_vh("vh_prefix", &["prefix.*"]);
        let table = VirtualHostTable::compile(vec![vh_exact, vh_suffix, vh_prefix], &[]);

        // Trailing dot on FQDN is stripped for exact and suffix matches
        assert_eq!(select_vh_name(&table, "example.com."), Some("vh_exact"));
        assert_eq!(select_vh_name(&table, "a.sub.example.com."), Some("vh_suffix"));

        // Suffix `.sub.example.com` must NOT match bare `sub.example.com` (requires strictly longer host)
        assert_eq!(select_vh_name(&table, "sub.example.com"), None);

        // Prefix `prefix.` must NOT match bare `prefix.` (requires strictly longer host)
        assert_eq!(select_vh_name(&table, "prefix."), None);
        assert_eq!(select_vh_name(&table, "prefix.x"), Some("vh_prefix"));
    }

    #[test]
    fn test_select_virtual_hosts_tie_breaking_preserves_declaration_order() {
        // First virtual host in declaration order wins when patterns are identical
        let vh_first = make_vh("vh_first", &["dup.example.com", "*.dup.com", "dup.*", "*"]);
        let vh_second = make_vh("vh_second", &["dup.example.com", "*.dup.com", "dup.*", "*"]);
        let table = VirtualHostTable::compile(vec![vh_first, vh_second], &[]);

        assert_eq!(select_vh_name(&table, "dup.example.com"), Some("vh_first"));
        assert_eq!(select_vh_name(&table, "a.dup.com"), Some("vh_first"));
        assert_eq!(select_vh_name(&table, "dup.org"), Some("vh_first"));
        assert_eq!(select_vh_name(&table, "anything.else"), Some("vh_first"));
    }

    #[test]
    fn test_select_virtual_hosts_http2_uri_authority_and_ipv6() {
        let vh_h2 = make_vh("vh_h2", &["h2.example.com"]);
        let vh_ipv6 = make_vh("vh_ipv6", &["[::1]"]);
        let vh_ipv6_port = make_vh("vh_ipv6_port", &["[::1]:8080"]);
        let table = VirtualHostTable::compile(vec![vh_h2, vh_ipv6, vh_ipv6_port], &[]);

        // HTTP/2 request with authority in URI and no Host header
        let req_h2 = Request::builder().uri("https://h2.example.com:443/path").body(()).unwrap();
        let host_h2 = RequestHost::from_request(&req_h2).unwrap();
        assert_eq!(table.select(host_h2).map(|vh| vh.name.as_str()), Some("vh_h2"));

        // IPv6 host with port and without port
        assert_eq!(select_vh_name(&table, "[::1]:8080"), Some("vh_ipv6_port"));
        assert_eq!(select_vh_name(&table, "[::1]:9090"), Some("vh_ipv6"));
        assert_eq!(select_vh_name(&table, "[::1]"), Some("vh_ipv6"));

        // Request without Host header and without URI authority returns None
        let req_empty = Request::builder().uri("/only-path").body(()).unwrap();
        assert!(RequestHost::from_request(&req_empty).is_none());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_virtual_host_single_rule_cartesian_product_all_rule_and_request_combinations() {
        // Every combination of filter rule type x port specification:
        // - Exact: host-only, host+port, IPv4-only, IPv4+port, IPv6-only, IPv6+port
        // - Suffix: subdomain without port, subdomain with port, dashed suffix without/with port, port-only (`*:8080`)
        // - Prefix: host prefix without port, host + wildcard port (`host:*`), IPv6 + wildcard port (`[::1]:*`), port prefix (`host:80*`)
        // - Wildcard: `*`
        let filter_rules = [
            "app.sub.example.com",
            "app.sub.example.com:8080",
            "127.0.0.1",
            "127.0.0.1:8080",
            "[::1]",
            "[::1]:8080",
            "*.sub.example.com",
            "*.sub.example.com:8080",
            "*.example.com",
            "*.example.com:8080",
            "*-api.example.com",
            "*-api.example.com:8080",
            "*:8080",
            "app.sub.*",
            "app.*",
            "app.sub.example.com:*",
            "app.sub.example.com:80*",
            "[::1]:*",
            "*",
        ];

        // Full Cartesian product of request host shapes:
        // (host-only, host:8080, host:8081, host:9090, trailing-dot, trailing-dot:port, malformed ports, IPv4, IPv6)
        let request_hosts = [
            "app.sub.example.com",
            "app.sub.example.com:8080",
            "app.sub.example.com:8081",
            "app.sub.example.com:9090",
            "app.sub.example.com.",
            "app.sub.example.com.:8080",
            "app.sub.example.com.:9090",
            "app.sub.example.com:",
            "app.sub.example.com:http",
            "other.sub.example.com",
            "other.sub.example.com:8080",
            "other.sub.example.com:9090",
            "other.sub.example.com.",
            "other.sub.example.com.:8080",
            "sub.example.com",
            "sub.example.com:8080",
            "v1-api.example.com",
            "v1-api.example.com:8080",
            "v1-api.example.com:9090",
            "-api.example.com",
            "-api.example.com:8080",
            "app.other.org",
            "app.other.org:8080",
            "127.0.0.1",
            "127.0.0.1:8080",
            "127.0.0.1:9090",
            "[::1]",
            "[::1]:8080",
            "[::1]:9090",
            "::1",
            ":8080",
            "unmatched.org",
            "unmatched.org:8080",
        ];

        for &rule_str in &filter_rules {
            let vhs = vec![make_vh("vh_single", &[rule_str])];
            let table = VirtualHostTable::compile(vhs.clone(), &[]);

            for &req_host_str in &request_hosts {
                // 1. Via `Host` header
                let req_header = Request::builder().header("host", req_host_str).body(()).unwrap();
                let parsed_host = RequestHost::from_request(&req_header).unwrap();
                let actual = table.select(parsed_host).map(|vh| vh.name.as_str());
                let expected = reference_select_vh_name(&vhs, parsed_host);
                assert_eq!(actual, expected, "single-rule mismatch for rule={rule_str:?}, req_host={req_host_str:?}");

                // 2. Via HTTP/2 URI `:authority` (for valid authority strings)
                if let Ok(req_uri) = Request::builder().uri(format!("http://{req_host_str}/path")).body(())
                    && let Some(uri_host) = RequestHost::from_request(&req_uri)
                {
                    assert_eq!(
                        table.select(uri_host).map(|vh| vh.name.as_str()),
                        reference_select_vh_name(&vhs, uri_host),
                        "URI authority mismatch for rule={rule_str:?}, req_host={req_host_str:?}"
                    );
                }
            }
        }

        // Verify `Host` header takes priority over URI `:authority` when both are present
        let table = VirtualHostTable::compile(
            vec![make_vh("vh_header", &["header.example.com:8080"]), make_vh("vh_uri", &["uri.example.com:8080"])],
            &[],
        );
        let req_both = Request::builder()
            .uri("http://uri.example.com:8080/path")
            .header("host", "header.example.com:8080")
            .body(())
            .unwrap();
        let host_both = RequestHost::from_request(&req_both).unwrap();
        assert_eq!(table.select(host_both).map(|vh| vh.name.as_str()), Some("vh_header"));
    }

    #[test]
    fn test_virtual_host_power_set_512_combinations_differential_oracle() {
        // 9 orthogonal rule archetypes covering all combinations of:
        // (Exact / Suffix / Prefix / Wildcard) x (host+port / host-only / port-only / host+wildcard-port)
        let archetypes: [(&str, &str); 9] = [
            ("vh_exact_port", "app.sub.example.com:8080"),
            ("vh_exact_host", "app.sub.example.com"),
            ("vh_suffix_long_port", "*.sub.example.com:8080"),
            ("vh_suffix_long_host", "*.sub.example.com"),
            ("vh_suffix_short_host", "*.example.com"),
            ("vh_suffix_port_only", "*:8080"),
            ("vh_prefix_wildcard_port", "app.sub.example.com:*"),
            ("vh_prefix_host", "app.*"),
            ("vh_wildcard", "*"),
        ];

        let request_hosts = [
            "app.sub.example.com:8080",
            "app.sub.example.com:9090",
            "app.sub.example.com",
            "app.sub.example.com.",
            "app.sub.example.com.:8080",
            "app.sub.example.com.:9090",
            "app.sub.example.com:",
            "app.sub.example.com:http",
            "other.sub.example.com:8080",
            "other.sub.example.com:9090",
            "other.sub.example.com",
            "other.sub.example.com.",
            "other.example.com:8080",
            "other.example.com:9090",
            "other.example.com",
            "app.other.org:8080",
            "app.other.org:9090",
            "app.other.org",
            "unrelated.org:8080",
            "unrelated.org:9090",
            "unrelated.org",
        ];

        // Iterate over all 2^9 = 512 subsets of rules!
        for mask in 0..(1usize << archetypes.len()) {
            let vhs: Vec<VirtualHost> = archetypes
                .iter()
                .enumerate()
                .filter_map(|(bit, &(name, pattern))| ((mask & (1 << bit)) != 0).then(|| make_vh(name, &[pattern])))
                .collect();

            let table = VirtualHostTable::compile(vhs.clone(), &[]);

            for &req_host_str in &request_hosts {
                let req = Request::builder().header("host", req_host_str).body(()).unwrap();
                let parsed_host = RequestHost::from_request(&req).unwrap();
                let actual = table.select(parsed_host).map(|vh| vh.name.as_str());
                let expected = reference_select_vh_name(&vhs, parsed_host);
                assert_eq!(actual, expected, "power-set mismatch for mask={mask:#011b}, req_host={req_host_str:?}");
            }
        }
    }

    #[test]
    fn test_routing_state_match_request_route_and_per_route_filters_cartesian() {
        let base_rl =
            LocalRateLimit { stat_prefix: "base".into(), token_bucket: None, status: StatusCode::TOO_MANY_REQUESTS };
        let override_rl = LocalRateLimit { stat_prefix: "override".into(), ..base_rl.clone() };

        let hcm_filters = vec![
            Arc::new(
                HttpFilter::try_from(HttpFilterConfig {
                    name: "f_override".into(),
                    disabled: false,
                    filter: HttpFilterType::RateLimit(base_rl.clone()),
                })
                .unwrap(),
            ),
            Arc::new(
                HttpFilter::try_from(HttpFilterConfig {
                    name: "f_keep".into(),
                    disabled: false,
                    filter: HttpFilterType::RateLimit(base_rl),
                })
                .unwrap(),
            ),
        ];

        let make_route = |name: &str, prefix: &str, overrides: HashMap<SmolStr, FilterOverride>| Route {
            name: name.into(),
            request_headers_to_add: HeaderModifiersAdd::default(),
            request_headers_to_remove: HeaderModifiersRemove::default(),
            response_headers_to_add: HeaderModifiersAdd::default(),
            response_headers_to_remove: HeaderModifiersRemove::default(),
            route_match: RouteMatch {
                path_matcher: Some(PathMatcher { specifier: PathSpecifier::Prefix(prefix.into()), ignore_case: false }),
                headers: vec![],
                query_parameters: vec![],
            },
            typed_per_filter_config: overrides,
            action: Action::DirectResponse(DirectResponseAction { status: StatusCode::OK, body: None }),
        };

        let mut overrides_custom = HashMap::new();
        overrides_custom.insert(
            "f_override".into(),
            FilterOverride {
                disabled: false,
                filter_settings: Some(FilterConfigOverride::LocalRateLimit(override_rl)),
            },
        );

        let mut overrides_disabled = HashMap::new();
        overrides_disabled.insert("f_override".into(), FilterOverride { disabled: true, filter_settings: None });

        let vh_api = VirtualHost {
            name: "vh_api".into(),
            domains: vec![MatchHost::try_from("api.example.com:8443").unwrap()],
            routes: vec![
                make_route("r_custom", "/v1", overrides_custom),
                make_route("r_disabled", "/v2", overrides_disabled),
                make_route("r_base", "/", HashMap::new()),
            ],
            ..Default::default()
        };

        let vh_fallback = VirtualHost {
            name: "vh_fallback".into(),
            domains: vec![MatchHost::try_from("*.example.com").unwrap()],
            routes: vec![make_route("r_fallback", "/v1", HashMap::new())],
            ..Default::default()
        };

        let rc = RouteConfiguration {
            name: "rc".into(),
            most_specific_header_mutations_wins: false,
            request_headers_to_add: HeaderModifiersAdd::default(),
            request_headers_to_remove: HeaderModifiersRemove::default(),
            response_headers_to_add: HeaderModifiersAdd::default(),
            response_headers_to_remove: HeaderModifiersRemove::default(),
            virtual_hosts: vec![vh_api, vh_fallback],
            has_request_mutations: false,
            has_response_mutations: false,
        };

        let state = RoutingState::new(rc, &hcm_filters);

        // 1. Exact host+port selects `vh_api`, `/v1` selects `r_custom` with overridden filter
        let req1 = Request::builder().header("host", "api.example.com:8443").uri("/v1/users").body(()).unwrap();
        let cached1 = state.match_request_route(&req1).unwrap();
        assert_eq!(cached1.vh.name.as_str(), "vh_api");
        assert_eq!(cached1.route.name, "r_custom");
        assert_eq!(cached1.http_filters.len(), 2);
        assert!(!cached1.http_filters[0].disabled);
        assert!(!Arc::ptr_eq(&cached1.http_filters[0], &hcm_filters[0]));
        assert!(Arc::ptr_eq(&cached1.http_filters[1], &hcm_filters[1]));

        // 2. `/v2` on `vh_api` selects `r_disabled` where `f_override` is disabled
        let req2 = Request::builder().header("host", "api.example.com:8443").uri("/v2/items").body(()).unwrap();
        let cached2 = state.match_request_route(&req2).unwrap();
        assert_eq!(cached2.vh.name.as_str(), "vh_api");
        assert_eq!(cached2.route.name, "r_disabled");
        assert!(cached2.http_filters[0].disabled);
        assert!(Arc::ptr_eq(&cached2.http_filters[1], &hcm_filters[1]));

        // 3. `/other` on `vh_api` falls back to `r_base` reusing `Arc` pointers from `hcm_filters`
        let req3 = Request::builder().header("host", "api.example.com:8443").uri("/other").body(()).unwrap();
        let cached3 = state.match_request_route(&req3).unwrap();
        assert_eq!(cached3.route.name, "r_base");
        assert!(Arc::ptr_eq(&cached3.http_filters[0], &hcm_filters[0]));
        assert!(Arc::ptr_eq(&cached3.http_filters[1], &hcm_filters[1]));

        // 4. Different port `api.example.com:9000` strips port and matches `*.example.com` (`vh_fallback`)
        let req4 = Request::builder().header("host", "api.example.com:9000").uri("/v1/users").body(()).unwrap();
        let cached4 = state.match_request_route(&req4).unwrap();
        assert_eq!(cached4.vh.name.as_str(), "vh_fallback");
        assert_eq!(cached4.route.name, "r_fallback");

        // 5. Unmatched route path on `vh_fallback` returns `None`
        let req5 = Request::builder().header("host", "api.example.com:9000").uri("/unmatched").body(()).unwrap();
        assert!(state.match_request_route(&req5).is_none());
    }

    #[test]
    fn test_header_mutations_cartesian_all_8_level_combinations_and_precedence() {
        let make_overwrite = |key: &'static str, val: &str| {
            HeaderModifiersAdd(vec![HeaderValueOption {
                header: HeaderKeyValue {
                    key: ::http::HeaderName::from_static(key),
                    value: HeaderFormatter::try_new(val).unwrap(),
                },
                append_action: HeaderAppendAction::OverwriteIfExistsOrAdd,
                keep_empty_value: false,
            }])
        };

        // Test all 2^3 = 8 combinations of (RC level, VH level, Route level) mutations
        // x 2 settings of `most_specific_header_mutations_wins` (false vs true) = 16 combinations.
        for mask in 0..8usize {
            for most_specific_wins in [false, true] {
                let rc_has = (mask & 1) != 0;
                let vh_has = (mask & 2) != 0;
                let route_has = (mask & 4) != 0;

                let route = Route {
                    name: "r".into(),
                    request_headers_to_add: if route_has {
                        make_overwrite("x-winner", "route")
                    } else {
                        HeaderModifiersAdd::default()
                    },
                    request_headers_to_remove: HeaderModifiersRemove::default(),
                    response_headers_to_add: if route_has {
                        make_overwrite("x-resp-winner", "route")
                    } else {
                        HeaderModifiersAdd::default()
                    },
                    response_headers_to_remove: HeaderModifiersRemove::default(),
                    route_match: RouteMatch {
                        path_matcher: Some(PathMatcher {
                            specifier: PathSpecifier::Prefix("/".into()),
                            ignore_case: false,
                        }),
                        headers: vec![],
                        query_parameters: vec![],
                    },
                    typed_per_filter_config: HashMap::new(),
                    action: Action::DirectResponse(DirectResponseAction { status: StatusCode::OK, body: None }),
                };

                let vh = VirtualHost {
                    name: "vh".into(),
                    domains: vec![MatchHost::Wildcard],
                    routes: vec![route],
                    request_headers_to_add: if vh_has {
                        make_overwrite("x-winner", "vh")
                    } else {
                        HeaderModifiersAdd::default()
                    },
                    request_headers_to_remove: HeaderModifiersRemove::default(),
                    response_headers_to_add: if vh_has {
                        make_overwrite("x-resp-winner", "vh")
                    } else {
                        HeaderModifiersAdd::default()
                    },
                    response_headers_to_remove: HeaderModifiersRemove::default(),
                    retry_policy: None,
                };

                let rc = RouteConfiguration {
                    name: "rc".into(),
                    most_specific_header_mutations_wins: most_specific_wins,
                    request_headers_to_add: if rc_has {
                        make_overwrite("x-winner", "rc")
                    } else {
                        HeaderModifiersAdd::default()
                    },
                    request_headers_to_remove: HeaderModifiersRemove::default(),
                    response_headers_to_add: if rc_has {
                        make_overwrite("x-resp-winner", "rc")
                    } else {
                        HeaderModifiersAdd::default()
                    },
                    response_headers_to_remove: HeaderModifiersRemove::default(),
                    virtual_hosts: vec![vh],
                    has_request_mutations: false,
                    has_response_mutations: false,
                };

                let state = RoutingState::new(rc, &[]);
                assert_eq!(state.has_request_mutations, mask != 0);
                assert_eq!(state.has_response_mutations, mask != 0);

                let mut req = Request::builder().header("host", "example.com").uri("/").body(()).unwrap();
                let mut resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
                let conn = ConnMeta::default();

                {
                    let cached = state.match_request_route(&req).unwrap();
                    apply_mutations_on_request(&mut req, &state, &cached, most_specific_wins, &conn);
                    apply_mutations_on_response(&mut resp, &state, &cached, most_specific_wins, &conn);
                }

                let expected = if most_specific_wins {
                    if route_has {
                        Some("route")
                    } else if vh_has {
                        Some("vh")
                    } else if rc_has {
                        Some("rc")
                    } else {
                        None
                    }
                } else if rc_has {
                    Some("rc")
                } else if vh_has {
                    Some("vh")
                } else if route_has {
                    Some("route")
                } else {
                    None
                };

                assert_eq!(
                    req.headers().get("x-winner").and_then(|v| v.to_str().ok()),
                    expected,
                    "request mutation mismatch for mask={mask:#05b}, most_specific_wins={most_specific_wins}"
                );
                assert_eq!(
                    resp.headers().get("x-resp-winner").and_then(|v| v.to_str().ok()),
                    expected,
                    "response mutation mismatch for mask={mask:#05b}, most_specific_wins={most_specific_wins}"
                );
            }
        }
    }

    #[test]
    fn propagated_request_id_exposes_only_authoritative_propagating_ids() {
        let propagated = HeaderValue::from_static("propagated-id");
        let ctx = TransactionContext {
            request_id: Some(RequestId::Propagate(propagated.clone())),
            ..TransactionContext::default()
        };
        assert_eq!(ctx.propagated_request_id(), Some(&propagated));

        let ctx = TransactionContext {
            request_id: Some(RequestId::Internal(HeaderValue::from_static("internal-id"))),
            ..TransactionContext::default()
        };
        assert!(ctx.propagated_request_id().is_none());
    }
}
