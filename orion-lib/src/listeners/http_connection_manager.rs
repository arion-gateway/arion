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

// This is to remove linter warning on HashMap<RouteMatch, Vec<HttpFilter>>
// which is a false positive since the Hasher of RouteMatch does not use mutable
// keys for the string/pattern matchers Regex field.
//
// This false positive is a known issue in the clippy linter:
// https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type
#![allow(clippy::mutable_key_type)]

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
mod redirect;
mod route;
mod upgrades;
pub mod user_rate_limiter;

use smallvec::SmallVec;
#[cfg(any(feature = "tracing", feature = "metrics", feature = "access-log"))]
use std::sync::atomic::AtomicUsize;

#[cfg(feature = "access-log")]
use crate::event_error::EventErrorContext;

#[cfg(any(feature = "tracing", feature = "metrics"))]
use opentelemetry::KeyValue;

#[cfg(feature = "tracing")]
use {
    crate::tracing_attributes::set_attributes_from_request,
    crate::tracing_attributes::HTTP_RESPONSE_STATUS_CODE,
    opentelemetry::global::BoxedSpan,
    opentelemetry::trace::Span,
    opentelemetry::trace::Status,
    orion_tracing::{
        http_tracer::{SpanKind, SpanName},
        span_state::SpanState,
        trace_context::TraceContext,
    },
};

#[cfg(feature = "access-log")]
use crate::utils::http::{request_head_size, response_head_size};
#[cfg(feature = "access-log")]
use crate::with_access_log;
#[cfg(feature = "metrics")]
use crate::{metrics, with_histogram};

#[cfg(feature = "metrics")]
use orion_metrics::metrics::{custom::CUSTOM_METRICS, http, user};

#[cfg(feature = "access-log")]
use {
    crate::access_log::{is_access_log_enabled, log_access_blocking, Target},
    crate::event_error::UpstreamTransportEventError,
    orion_configuration::config::access_log::AccessLog,
    orion_format::context::{
        DownstreamResponseContext, FinishContext, HttpRequestDurationContext, HttpResponseDurationContext,
        InitHttpContext,
    },
    orion_format::LogFormatter,
};

#[cfg(any(feature = "access-log", feature = "metrics"))]
use {parking_lot::Mutex, std::time::Instant};

use ::http::HeaderValue;
use arc_swap::ArcSwap;
use core::time::Duration;
use futures::future::BoxFuture;
use hyper::{body::Incoming, header::HOST, service::Service, Request, Response, StatusCode};
use orion_configuration::config::network_filters::http_connection_manager::route::RouteMatch;
use orion_configuration::config::network_filters::http_connection_manager::{
    route::{Action, RouteMatchResult},
    CodecType, ConfigSource, ConfigSourceSpecifier, HttpConnectionManager as HttpConnectionManagerConfig, RdsSpecifier,
    RouteSpecifier, UpgradeType,
};
#[cfg(feature = "metrics")]
use orion_metrics::metrics::clusters;

use std::fmt::Write;

use crate::{
    body::{
        instrumented_body::InstrumentedBody,
        response_flags::{BodyKind, ResponseFlags},
        timeout_body::TimeoutBody,
    },
    event_error::{EventFailure, EventKind},
    get_shard_id,
    listeners::{
        http_connection_manager::http_modifiers::ModifiersExtractor,
        http_filters::{per_route_http_filters, FilterDecision, FilterFactory, HttpFilter, HttpFilterValue},
        metadata::{ConnMeta, DownstreamMetadata},
        synthetic_http_response::SyntheticHttpResponse,
    },
    with_client_span, with_metric, with_server_span, ConversionContext, OrionRequestBody, OrionResponseBody, PolyBody,
    Result, RouteConfiguration,
};

use crate::utils::instrumented_stream::StreamMetrics;

use orion_configuration::config::network_filters::{
    http_connection_manager::{Route, VirtualHost, XffSettings},
    tracing::{TracingConfig, TracingKey},
};
use orion_format::types::ResponseFlags as FmtResponseFlags;
use route::RouteContext;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::{fmt, future::Future, result::Result as StdResult, sync::Arc};
use tokio::sync::watch;
use tracing::{debug, error};
use upgrades as upgrade_utils;

use orion_tracing::http_tracer::{HttpTracer, ScopedClientSpan};
use orion_tracing::request_id::{RequestId, RequestIdManager};

use crate::listeners::http_connection_manager::http_modifiers::HeaderMapModifier;
#[cfg(feature = "metrics")]
use orion_metrics::metrics::custom::MetricsHook;

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
// HTTP Connection Manager - Service Stack Architecture
// =================================================================================================
//
// The HTTP request processing is structured as a stack of strongly-typed `hyper::Service` layers.
// Each layer wraps the request in a specific type, adding context as it moves down the stack,
// and performs pre-processing (before calling the inner service) and post-processing (after).
//
// +-----------------------------------------------------------------------------------------------+
// | Hyper Server (hyper::server::conn::http1 / http2)                                             |
// |   Provides: Request<Incoming>                                                                 |
// +-----------------------------------------------------------------------------------------------+
//                                |
//                                v
// +-----------------------------------------------------------------------------------------------+
// | 1. MetadataSvc                                                                                |
// |   Input:  Request<Incoming>                                                                   |
// |   Action: Attaches connection-scoped ConnMeta (Arc downstream + stream metrics).              |
// |   Output: IncomingHttpRequest<Incoming>                                                       |
// +-----------------------------------------------------------------------------------------------+
//                                |
//                                v
// +-----------------------------------------------------------------------------------------------+
// | 2. TransactionLifecycleSvc                                                                  |
// |   Input:  IncomingHttpRequest<Incoming>                                                       |
// |   Action: Request ID, span, TransactionContext → RequestCtx.                                  |
// |           Bridge (temporary): also inserts ctx into request.extensions for filters/WASM.      |
// |   Output: HttpRequest<Incoming>                                                               |
// +-----------------------------------------------------------------------------------------------+
//                                |
//                                v
// +-----------------------------------------------------------------------------------------------+
// | 3. TransactionSvc                                                                         |
// |   Input:  HttpRequest<Incoming>                                                               |
// |   Action: Metrics, validation, instrumented body, resolve route_conf.                         |
// |   Output: RoutedHttpRequest<OrionRequestBody>                                                 |
// +-----------------------------------------------------------------------------------------------+
//                                |
//                                v
// +-----------------------------------------------------------------------------------------------+
// | 4. HttpPipelineSvc (Terminal Service)                                                     |
// |   Input:  RoutedHttpRequest<OrionRequestBody>                                                 |
// |   Action: Filter chain + routing via RequestHandler (ctx passed explicitly).                  |
// |   Output: Response<OrionRequestBody>                                                          |
// +-----------------------------------------------------------------------------------------------+
//
// =================================================================================================

pub struct HttpConnectionManagerBuilder {
    listener_name: Option<&'static str>,
    filterchain_id: Option<u64>,
    connection_manager: PartialHttpConnectionManager,
}

impl TryFrom<ConversionContext<'_, HttpConnectionManagerConfig>> for HttpConnectionManagerBuilder {
    type Error = crate::Error;
    fn try_from(ctx: ConversionContext<HttpConnectionManagerConfig>) -> Result<Self> {
        let partial = PartialHttpConnectionManager::try_from(ctx)?;
        Ok(Self { listener_name: None, filterchain_id: None, connection_manager: partial })
    }
}

impl HttpConnectionManagerBuilder {
    pub fn build(self) -> Result<HttpConnectionManager> {
        let listener_name = self.listener_name.ok_or("listener name is not set")?;
        let filterchain_id = self.filterchain_id.unwrap_or(0);
        let partial = self.connection_manager;
        let router_sender = watch::Sender::new(partial.router.map(Arc::new));

        Ok(HttpConnectionManager {
            listener_name,
            filterchain_id,
            router_sender,
            codec_type: partial.codec_type,
            dynamic_route_name: partial.dynamic_route_name,
            http_filters_hcm: partial.http_filters_hcm,
            http_filters_per_route: ArcSwap::new(Arc::new(partial.http_filters_per_route)),
            enabled_upgrades: partial.enabled_upgrades,
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
            #[cfg(feature = "access-log")]
            access_log: partial.access_log,
        })
    }

    #[inline]
    pub fn with_listener_name(self, name: &'static str) -> Self {
        HttpConnectionManagerBuilder { listener_name: Some(name), ..self }
    }

    #[inline]
    pub fn with_filterchain_id(self, value: u64) -> Self {
        HttpConnectionManagerBuilder { filterchain_id: Some(value), ..self }
    }
}

#[derive(Debug, Clone)]
pub struct PartialHttpConnectionManager {
    router: Option<RouteConfiguration>,
    codec_type: CodecType,
    dynamic_route_name: Option<SmolStr>,
    http_filters_hcm: Vec<Arc<HttpFilter>>,
    http_filters_per_route: HashMap<RouteMatch, Vec<Arc<HttpFilter>>>,
    enabled_upgrades: Vec<UpgradeType>,
    request_timeout: Option<Duration>,
    xff_settings: XffSettings,
    generate_request_id: bool,
    preserve_external_request_id: bool,
    always_set_request_id_in_response: bool,
    tracing: Option<TracingConfig>,
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

        let mut http_filters_per_route = HashMap::new();
        let (dynamic_route_name, router) = match configuration.route_specifier {
            RouteSpecifier::Rds(RdsSpecifier {
                route_config_name,
                config_source: ConfigSource { config_source_specifier },
            }) => match config_source_specifier {
                ConfigSourceSpecifier::ADS => (Some(route_config_name), None),
            },
            RouteSpecifier::RouteConfig(config) => {
                http_filters_per_route = per_route_http_filters(&config, &http_filters_hcm);
                (None, Some(config))
            },
        };

        Ok(PartialHttpConnectionManager {
            router,
            codec_type,
            dynamic_route_name,
            http_filters_hcm,
            http_filters_per_route,
            enabled_upgrades,
            request_timeout,
            xff_settings,
            generate_request_id,
            preserve_external_request_id,
            always_set_request_id_in_response,
            tracing: configuration.tracing,
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

#[derive(Debug)]
pub struct HttpConnectionManager {
    pub listener_name: &'static str,
    pub filterchain_id: u64,
    router_sender: watch::Sender<Option<Arc<RouteConfiguration>>>,
    pub codec_type: CodecType,
    dynamic_route_name: Option<SmolStr>,
    http_filters_hcm: Vec<Arc<HttpFilter>>,
    http_filters_per_route: ArcSwap<HashMap<RouteMatch, Vec<Arc<HttpFilter>>>>,
    enabled_upgrades: Vec<UpgradeType>,
    request_timeout: Option<Duration>,
    xff_settings: XffSettings,
    request_id_handler: RequestIdManager,
    pub http_tracer: HttpTracer,
    #[cfg(feature = "access-log")]
    access_log: Vec<AccessLog>,
}

impl fmt::Display for HttpConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HttpConnectionManager {}", &self.listener_name)
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
        self.http_filters_per_route.swap(Arc::new(per_route_http_filters(&route, &self.http_filters_hcm)));
        let _ = self.router_sender.send_replace(Some(Arc::new(route)));
    }

    pub fn remove_route(&self) {
        let _ = self.router_sender.send_replace(None);
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn transaction_context_svc(
        self: &Arc<Self>,
    ) -> TransactionLifecycleSvc<TransactionSvc<HttpPipelineSvc>> {
        let pipeline_service = HttpPipelineSvc::new(Arc::clone(self));
        let transaction_service =
            TransactionSvc::new(Arc::clone(self), self.router_sender.subscribe(), pipeline_service);
        TransactionLifecycleSvc::new(Arc::clone(self), transaction_service)
    }
}

#[derive(Debug)]
pub struct CachedRoute<'a> {
    route: &'a Route,
    route_match: RouteMatchResult,
    vh: &'a VirtualHost,
}

#[cfg(any(feature = "access-log", feature = "metrics"))]
#[derive(Debug, Default)]
pub struct TransactionState {
    bytes: u64, // either the request or response body size, depending which one has completed first
    flags: ResponseFlags,
    event: Option<EventKind>,
    pub upstream_start_instant: Option<std::time::Instant>,
    pub upstream_cluster_name: Option<&'static str>,
    #[cfg(feature = "access-log")]
    loggers: Vec<LogFormatter>,
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
            loggers: access_log.iter().map(|al| al.get_logger().clone()).collect::<Vec<_>>(),
        }
    }
}

#[cfg(feature = "metrics")]
pub type ShardId = std::thread::ThreadId;

#[cfg(not(feature = "metrics"))]
pub type ShardId = ();

#[derive(Debug)]
pub struct TransactionContext {
    #[allow(dead_code)]
    start_instant: std::time::Instant,
    request_id: Option<RequestId>,
    #[allow(dead_code)]
    pub user_partition_key: Option<&'static str>,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: Option<RequestId>,
        user_partition_key: Option<&'static str>,
        thread_id: ShardId,
        #[cfg(feature = "access-log")] access_log: &[AccessLog],
        #[cfg(feature = "tracing")] trace_ctx: Option<TraceContext>,
        #[cfg(feature = "tracing")] upstream_tracing_key: Option<TracingKey>,
        #[cfg(feature = "tracing")] server_span: Option<BoxedSpan>,
    ) -> Self {
        TransactionContext {
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
    pub fn with_loggers<F>(&self, f: F)
    where
        F: FnOnce(&mut [orion_format::LogFormatter]),
    {
        let mut state = self.trans_state.lock();
        f(&mut state.loggers);
    }

    #[allow(unused_variables)]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::let_unit_value)]
    #[allow(clippy::unused_self)]
    fn trace_status_code(self: Arc<Self>, res: &Result<Response<OrionRequestBody>>, listener_name: &'static str) {
        if let Ok(response) = &res {
            let status_code = response.status().as_u16();

            with_server_span!(self.span_state, |srv_span: &mut BoxedSpan| srv_span
                .set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, i64::from(status_code))));

            #[cfg(feature = "metrics")]
            if let Some(user_partition_key) = self.user_partition_key {
                with_metric!(
                    user::INVOCATIONS,
                    add,
                    1,
                    self.shard_id(),
                    &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                );
            }

            #[allow(clippy::match_same_arms)]
            match status_code {
                100..200 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_1XX,
                        add,
                        1,
                        self.shard_id(),
                        &[KeyValue::new("listener", listener_name)]
                    );
                    #[cfg(feature = "metrics")]
                    if let Some(user_partition_key) = self.user_partition_key {
                        with_metric!(
                            user::HTTP_1XX_RESPONSES,
                            add,
                            1,
                            self.shard_id(),
                            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                        );
                    }
                },
                200..300 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_2XX,
                        add,
                        1,
                        self.shard_id(),
                        &[KeyValue::new("listener", listener_name)]
                    );
                    #[cfg(feature = "metrics")]
                    if let Some(user_partition_key) = self.user_partition_key {
                        with_metric!(
                            user::HTTP_2XX_RESPONSES,
                            add,
                            1,
                            self.shard_id(),
                            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                        );
                    }
                },
                300..400 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_3XX,
                        add,
                        1,
                        self.shard_id(),
                        &[KeyValue::new("listener", listener_name)]
                    );
                    #[cfg(feature = "metrics")]
                    if let Some(user_partition_key) = self.user_partition_key {
                        with_metric!(
                            user::HTTP_3XX_RESPONSES,
                            add,
                            1,
                            self.shard_id(),
                            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                        );
                    }
                },
                400..500 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_4XX,
                        add,
                        1,
                        self.shard_id(),
                        &[KeyValue::new("listener", listener_name)]
                    );

                    #[cfg(feature = "metrics")]
                    if let Some(user_partition_key) = self.user_partition_key {
                        with_metric!(
                            user::USER_ERRORS,
                            add,
                            1,
                            self.shard_id(),
                            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                        );
                        with_metric!(
                            user::TOTAL_ERRORS,
                            add,
                            1,
                            self.shard_id(),
                            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                        );
                        with_metric!(
                            user::HTTP_4XX_RESPONSES,
                            add,
                            1,
                            self.shard_id(),
                            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                        );

                        match status_code {
                            404 => {
                                with_metric!(
                                    user::HTTP_404_RESPONSES,
                                    add,
                                    1,
                                    self.shard_id(),
                                    &[KeyValue::new(
                                        metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                        user_partition_key
                                    )]
                                );
                            },
                            429 => {
                                with_metric!(
                                    user::THROTTLES,
                                    add,
                                    1,
                                    self.shard_id(),
                                    &[KeyValue::new(
                                        metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                        user_partition_key
                                    )]
                                );
                            },
                            _ => (),
                        }
                    }
                },
                500..600 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_5XX,
                        add,
                        1,
                        self.shard_id(),
                        &[KeyValue::new("listener", listener_name)]
                    );

                    #[cfg(feature = "metrics")]
                    if let Some(user_partition_key) = self.user_partition_key {
                        with_metric!(
                            user::SYSTEM_ERRORS,
                            add,
                            1,
                            self.shard_id(),
                            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                        );
                        with_metric!(
                            user::TOTAL_ERRORS,
                            add,
                            1,
                            self.shard_id(),
                            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                        );
                        with_metric!(
                            user::HTTP_5XX_RESPONSES,
                            add,
                            1,
                            self.shard_id(),
                            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                        );
                        match status_code {
                            502 => {
                                with_metric!(
                                    user::HTTP_502_RESPONSES,
                                    add,
                                    1,
                                    self.shard_id(),
                                    &[KeyValue::new(
                                        metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                        user_partition_key
                                    )]
                                );
                            },
                            504 => {
                                with_metric!(
                                    user::HTTP_504_RESPONSES,
                                    add,
                                    1,
                                    self.shard_id(),
                                    &[KeyValue::new(
                                        metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                        user_partition_key
                                    )]
                                );
                            },
                            _ => (),
                        }
                    }

                    with_server_span!(self.span_state, |srv_span: &mut BoxedSpan| {
                        srv_span.set_status(Status::error("5xx"));
                    });

                    with_client_span!(self.span_state, |clt_span: &mut BoxedSpan| {
                        clt_span.set_status(Status::error("5xx"));
                    });
                },
                _ => {},
            }
        } else {
            with_metric!(http::DOWNSTREAM_RQ_5XX, add, 1, self.shard_id(), &[KeyValue::new("listener", listener_name)]);

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

#[derive(Clone)]
pub struct HttpPipelineSvc {
    manager: Arc<HttpConnectionManager>,
}

impl HttpPipelineSvc {
    pub fn new(manager: Arc<HttpConnectionManager>) -> Self {
        Self { manager }
    }
}

impl Service<RoutedHttpRequest<OrionRequestBody>> for HttpPipelineSvc {
    type Response = Response<OrionRequestBody>;
    type Error = crate::Error;
    type Future = BoxFuture<'static, StdResult<Self::Response, Self::Error>>;

    #[allow(clippy::too_many_lines)]
    fn call(&self, req: RoutedHttpRequest<OrionRequestBody>) -> Self::Future {
        let manager = Arc::clone(&self.manager);
        Box::pin(async move {
            let RoutedHttpRequest { http: HttpRequest { mut request, ctx }, route_conf } = req;
            let trans_ctx = Arc::clone(&ctx.tx);
            let stream_metrics = Arc::clone(&ctx.conn.stream_metrics);

            #[allow(unused_variables)]
            let listener_name = manager.listener_name;
            #[allow(unused_variables)]
            let filterchain_id = manager.filterchain_id;
            let downstream_addr = ctx.conn.downstream_peer_address();
            let stream_metrics_clone = Arc::clone(&stream_metrics);

            // check if this is the first request on the stream, and if so, record it in the metrics.
            if stream_metrics.inc_requests() == 0 {
                #[cfg(feature = "metrics")]
                if let Some(user_partition_key) = trans_ctx.user_partition_key {
                    with_metric!(
                        user::CONNECTIONS,
                        add,
                        1,
                        trans_ctx.shard_id,
                        &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                    );
                    with_metric!(
                        user::CONNECTIONS_ACTIVE,
                        add,
                        1,
                        trans_ctx.shard_id,
                        &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                    );

                    // store the user_partition_key to decrement CONNECTIONS_ACTIVE later, when the stream is closed.
                    stream_metrics.set_user_partition_key(user_partition_key);
                }
            }

            // apply the request header modifiers
            http_modifiers::apply_prerouting_functions(&mut request, downstream_addr, manager.xff_settings);

            // process request, get the response..
            let result = route_conf.to_response(&ctx, request, Arc::clone(&manager)).await;

            // calculate the time to first byte..
            #[cfg(any(feature = "access-log", feature = "metrics"))]
            let first_byte_instant = Instant::now();

            result.map(|mut response| {
                // set the request id on the response...
                manager
                    .request_id_handler
                    .apply_to(&mut response, trans_ctx.request_id.as_ref().and_then(|x| x.propagate_ref()));

                #[cfg(feature = "access-log")]
                let (initial_flags, initial_event) = {
                    let ec = response.extensions().get::<EventErrorContext>();
                    (ec.map(|ec| ec.response_flags).unwrap_or_default(), ec.and_then(|ec| ec.event_kind.clone()))
                };

                #[cfg(feature = "access-log")]
                {
                    use crate::with_access_log;

                    with_access_log!(
                        &mut trans_ctx.trans_state.lock().loggers,
                        DownstreamResponseContext {
                            response: &response,
                            response_head_size: response_head_size(&response)
                        }
                    )
                }

                response.map(move |body| {
                    #[allow(unused_variables)]
                    let stream_metrics_clone = stream_metrics_clone;

                    #[allow(unused_variables)]
                    let sm_for_cb2 = Arc::clone(&stream_metrics_clone);

                    InstrumentedBody::new(
                        BodyKind::Response,
                        body,
                        Some(stream_metrics_clone),
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
                                            &[opentelemetry::KeyValue::new("cluster", cluster)]
                                        );
                                        if let Some(user_key) = trans_ctx.user_partition_key {
                                            crate::with_histogram!(
                                                user::UPSTREAM_RQ_TIME,
                                                record,
                                                elapsed_ms,
                                                shard_id,
                                                &[opentelemetry::KeyValue::new(
                                                    crate::metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                                    user_key
                                                )]
                                            );
                                        }
                                    }
                                }

                                #[cfg(feature = "access-log")]
                                with_access_log!(
                                    &mut trans_state.loggers,
                                    HttpResponseDurationContext { duration, tx_duration }
                                );

                                if trans_ctx.trans_phase.is_complete() {
                                    let sm_arc = Arc::clone(&sm_for_cb2);
                                    let trans_ctx_cb = Arc::clone(&trans_ctx);
                                    #[cfg(feature = "access-log")]
                                    let initial_event_cb = initial_event.clone();
                                    let body_error_cb = body_error.clone();

                                    stream_metrics.add_flush_callback(Box::new(move || {
                                        #[allow(unused_mut)]
                                        let mut trans_ctx = trans_ctx_cb.trans_state.lock();
                                        #[allow(unused_variables)]
                                        let ctx_bytes = trans_ctx.bytes;
                                        #[allow(unused_variables)]
                                        let ctx_flags = trans_ctx.flags;
                                        #[allow(unused_variables)]
                                        let ctx_event = trans_ctx.event.clone();
                                        eval_http_finish_context(FinishContextParams {
                                            stream_metrics: &sm_arc,
                                            listener_name,
                                            user_partition_key: trans_ctx_cb.user_partition_key,
                                            filterchain_id,
                                            bytes_received: ctx_bytes,
                                            bytes_sent: body_bytes,
                                            trans_start_time: trans_ctx_cb.start_instant,
                                            #[cfg(feature = "metrics")]
                                            m_ctx: MetricsFinishContext { shard_id: trans_ctx_cb.shard_id() },
                                            #[cfg(feature = "access-log")]
                                            al_ctx: AccessLogFinishContext {
                                                event: EventInfo {
                                                    body_kind: BodyKind::Response,
                                                    event_kind: ctx_event.or(initial_event_cb).or(body_error_cb),
                                                    response_flags: ctx_flags | initial_flags | body_flags,
                                                },
                                                access_loggers: trans_ctx.loggers.as_mut(),
                                            },
                                        });
                                    }));
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
                            if trans_ctx.trans_phase.is_complete() {
                                if let Some(span) = trans_ctx.span_state.as_ref() {
                                    span.end();
                                }
                            }
                        },
                    )
                })
            })
        })
    }
}

fn select_virtual_host<'a, T>(request: &Request<T>, virtual_hosts: &'a [VirtualHost]) -> Option<&'a VirtualHost> {
    let mapped_vhs = virtual_hosts.iter().filter_map(|vh| {
        let maybe_score = vh.domains.iter().map(|domain| domain.eval_lpm_request(request)).max().flatten();
        maybe_score.map(|score| (vh, score))
    });

    let virtual_host_with_max_score = mapped_vhs.max_by_key(|(_, score)| score.clone());
    virtual_host_with_max_score.map(|(vh, _)| vh)
}

// has to be a trait due to foreign impl rules.
pub trait RequestHandler<R, A>: Sized {
    fn to_response(
        self,
        ctx: &RequestCtx,
        request: R,
        arg: A,
    ) -> impl Future<Output = Result<Response<OrionResponseBody>>> + Send;
}

#[inline]
fn match_request_route<'a, B>(request: &Request<B>, route_config: &'a RouteConfiguration) -> Option<CachedRoute<'a>> {
    let chosen_vh = select_virtual_host(request, &route_config.virtual_hosts)?;
    let (chosen_route, route_match_result) = chosen_vh
        .routes
        .iter()
        .map(|route| (route, route.route_match.match_request(request)))
        .find(|(_, match_result)| match_result.matched())?;
    Some(CachedRoute { route: chosen_route, route_match: route_match_result, vh: chosen_vh })
}

impl RequestHandler<Request<OrionRequestBody>, Arc<HttpConnectionManager>> for Arc<RouteConfiguration> {
    #[allow(clippy::too_many_lines)]
    async fn to_response(
        self,
        ctx: &RequestCtx,
        mut request: Request<OrionRequestBody>,
        arg: Arc<HttpConnectionManager>,
    ) -> Result<Response<OrionResponseBody>> {
        let route_conf = &self;
        let connection_manager = &arg;
        let mut cached_route = match_request_route(&request, route_conf);
        let mut active_filters: SmallVec<[HttpFilterValue; 4]> = SmallVec::new();

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

            let guard = connection_manager.http_filters_per_route.load();
            let route_filters = guard.get(&chosen_route.route.route_match);

            let Some(route_filters) = route_filters else {
                break 'filter_loop FilterDecision::Continue;
            };

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
                cached_route = match_request_route(&request, route_conf);
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
                        route_conf,
                        &cached_route,
                        route_conf.most_specific_header_mutations_wins,
                        &ctx.conn,
                    );
                    response
                },
                _ => {
                    let websocket_enabled_by_default =
                        upgrade_utils::is_websocket_enabled_by_hcm(&connection_manager.enabled_upgrades);

                    let mut response = match &cached_route.route.action {
                        Action::DirectResponse(dr) => dr.to_response(ctx, request, &cached_route.route.name).await,
                        Action::Redirect(rd) => {
                            rd.to_response(ctx, request, (&cached_route.route_match, &cached_route.route.name)).await
                        },
                        Action::Route(route) => {
                            apply_mutations_on_request(
                                &mut request,
                                route_conf,
                                &cached_route,
                                route_conf.most_specific_header_mutations_wins,
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
                                            retry_policy: cached_route.vh.retry_policy.as_ref(),
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
                                if let Some(source) = key.source() {
                                    if let Some(id) =
                                        metrics::extract_custom_partition_key(response.headers(), Some(source))
                                    {
                                        attrs.push(KeyValue::new(key.attribute_name().unwrap_or("custom"), id));
                                    }
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
                    if let Err(err) = crate::access_log::evaluate_base64_access_log_hook(
                        crate::access_log::AccessLogHook::IncomingResponse,
                        response.headers(),
                        &mut ctx.tx.trans_state.lock().loggers,
                    ) {
                        tracing::warn!("Failed to process access log header for IncomingResponse: {err}");
                    }

                    apply_mutations_on_response(
                        &mut response,
                        route_conf,
                        &cached_route,
                        route_conf.most_specific_header_mutations_wins,
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
    route_config: &RouteConfiguration,
    cached_route: &CachedRoute<'_>,
    most_specific_header_mutations_wins: bool,
    conn: &ConnMeta,
) where
    Route: ModifiersExtractor<Request<B>>,
    VirtualHost: ModifiersExtractor<Request<B>>,
    RouteConfiguration: ModifiersExtractor<Request<B>>,
{
    let apply_pair = |target: &mut Request<B>, (remove, add)| {
        target.apply_mutation(remove);
        target.apply_mutation((add, conn));
    };

    if most_specific_header_mutations_wins {
        apply_pair(target, ModifiersExtractor::<Request<B>>::extract(route_config));
        apply_pair(target, ModifiersExtractor::<Request<B>>::extract(cached_route.vh));
        apply_pair(target, ModifiersExtractor::<Request<B>>::extract(cached_route.route));
    } else {
        apply_pair(target, ModifiersExtractor::<Request<B>>::extract(cached_route.route));
        apply_pair(target, ModifiersExtractor::<Request<B>>::extract(cached_route.vh));
        apply_pair(target, ModifiersExtractor::<Request<B>>::extract(route_config));
    }
}

fn apply_mutations_on_response<B>(
    target: &mut Response<B>,
    route_config: &RouteConfiguration,
    cached_route: &CachedRoute<'_>,
    most_specific_header_mutations_wins: bool,
    conn: &ConnMeta,
) where
    Route: ModifiersExtractor<Response<B>>,
    VirtualHost: ModifiersExtractor<Response<B>>,
    RouteConfiguration: ModifiersExtractor<Response<B>>,
{
    let apply_pair = |target: &mut Response<B>, (remove, add)| {
        target.apply_mutation(remove);
        target.apply_mutation((add, conn));
    };

    if most_specific_header_mutations_wins {
        apply_pair(target, ModifiersExtractor::<Response<B>>::extract(route_config));
        apply_pair(target, ModifiersExtractor::<Response<B>>::extract(cached_route.vh));
        apply_pair(target, ModifiersExtractor::<Response<B>>::extract(cached_route.route));
    } else {
        apply_pair(target, ModifiersExtractor::<Response<B>>::extract(cached_route.route));
        apply_pair(target, ModifiersExtractor::<Response<B>>::extract(cached_route.vh));
        apply_pair(target, ModifiersExtractor::<Response<B>>::extract(route_config));
    }
}

// --- Typed request envelope (Service stack) ---

/// Per-request context always present after `TransactionLifecycleSvc`.
#[derive(Clone, Debug)]
pub struct RequestCtx {
    pub conn: ConnMeta,
    pub tx: Arc<TransactionContext>,
}

impl RequestCtx {
    #[inline]
    pub fn new(conn: ConnMeta, tx: Arc<TransactionContext>) -> Self {
        Self { conn, tx }
    }

    #[inline]
    pub(crate) fn propagated_request_id(&self) -> Option<&HeaderValue> {
        self.tx.propagated_request_id()
    }

    #[inline]
    pub(crate) fn begin_upstream_span(&self, span_name: &str) -> ScopedClientSpan {
        self.tx.begin_upstream_span(span_name)
    }
}

impl Default for RequestCtx {
    fn default() -> Self {
        Self { conn: ConnMeta::default(), tx: Arc::new(TransactionContext::default()) }
    }
}

/// HTTP request + connection meta (before transaction is created).
pub struct IncomingHttpRequest<B> {
    pub request: Request<B>,
    pub conn: ConnMeta,
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
pub struct RoutedHttpRequest<B> {
    pub http: HttpRequest<B>,
    pub route_conf: Arc<RouteConfiguration>,
}

#[derive(Clone)]
pub struct MetadataSvc<S> {
    conn: ConnMeta,
    inner: S,
}

impl<S> MetadataSvc<S> {
    pub fn new(downstream: Arc<DownstreamMetadata>, stream_metrics: Arc<StreamMetrics>, inner: S) -> Self {
        Self { conn: ConnMeta::new(downstream, stream_metrics), inner }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for MetadataSvc<S>
where
    S: Service<IncomingHttpRequest<ReqBody>, Error = crate::Error> + Clone,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = futures::future::BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<ReqBody>) -> Self::Future {
        let fut = self.inner.call(IncomingHttpRequest { request: req, conn: self.conn.clone() });
        Box::pin(
            async move { fut.await.map_err(|e| Box::new(e.into_inner()) as Box<dyn std::error::Error + Send + Sync>) },
        )
    }
}

#[derive(Clone)]
pub struct TransactionLifecycleSvc<S> {
    manager: Arc<HttpConnectionManager>,
    inner: S,
}

impl<S> TransactionLifecycleSvc<S> {
    pub fn new(manager: Arc<HttpConnectionManager>, inner: S) -> Self {
        Self { manager, inner }
    }
}

impl<S> Service<IncomingHttpRequest<Incoming>> for TransactionLifecycleSvc<S>
where
    S: Service<HttpRequest<Incoming>, Response = Response<OrionRequestBody>, Error = crate::Error>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, StdResult<Self::Response, Self::Error>>;

    fn call(&self, req: IncomingHttpRequest<Incoming>) -> Self::Future {
        let IncomingHttpRequest { request: incoming_request, conn } = req;
        let incoming_request_id = RequestId::from_request(&incoming_request);
        let incoming_version = incoming_request.version();
        let listener_name = self.manager.listener_name;

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
        let (mut request, request_id) = self.manager.request_id_handler.apply_policy(
            incoming_request,
            access_log_enabled,
            incoming_request_id.as_ref(),
            is_internal,
        );

        #[cfg(feature = "tracing")]
        let trace_context =
            self.manager.http_tracer.try_build_trace_context(&request, incoming_request_id.or(request_id.clone()));

        #[cfg(feature = "tracing")]
        let tracing_key = self.manager.get_tracing_key();

        #[cfg(feature = "tracing")]
        let mut server_span = self.manager.http_tracer.try_create_span(
            trace_context.as_ref(),
            &tracing_key,
            SpanKind::Server,
            SpanName::Host(&request),
        );

        #[cfg(feature = "tracing")]
        let upstream_tracing_key = (trace_context.as_ref().is_some_and(TraceContext::should_sample)
            && self.manager.http_tracer.upstream_spans_enabled())
        .then_some(tracing_key);

        #[cfg(feature = "tracing")]
        if let Some(span) = server_span.as_mut() {
            set_attributes_from_request(span, &request);
        }

        #[cfg(feature = "metrics")]
        let sni = conn.downstream.sni.clone();

        #[cfg(feature = "metrics")]
        let user_partition_key =
            metrics::extract_user_partition_key((request.headers(), sni.as_ref()), metrics::USER_KEY.source());

        #[cfg(not(feature = "metrics"))]
        let user_partition_key = None;

        #[allow(clippy::let_unit_value)]
        let shard_id = get_shard_id!();

        let trans_ctx = Arc::new(TransactionContext::new(
            request_id,
            user_partition_key,
            shard_id,
            #[cfg(feature = "access-log")]
            &self.manager.access_log,
            #[cfg(feature = "tracing")]
            trace_context,
            #[cfg(feature = "tracing")]
            upstream_tracing_key,
            #[cfg(feature = "tracing")]
            server_span,
        ));

        #[cfg(feature = "tracing")]
        if let Some(trace_ctx) = trans_ctx.trace_ctx.as_ref() {
            self.manager.http_tracer.update_tracing_headers(trace_ctx, &mut request);
        }

        let ctx = RequestCtx::new(conn, Arc::clone(&trans_ctx));
        let http_req = HttpRequest { request, ctx };

        let inner = self.inner.clone();
        Box::pin(async move {
            let response = inner.call(http_req).await;

            trans_ctx.trace_status_code(&response, listener_name);
            if let Err(err) = response {
                error!("Error during handling HTTP transaction: {}", err);
                let msg = err.to_string();
                let response = SyntheticHttpResponse::internal_server_error(
                    EventKind::Upstream(err.into()),
                    ResponseFlags(orion_format::types::ResponseFlags::LOCAL_RESET),
                )
                .with_body(msg)
                .into_response(incoming_version);
                Ok(response.map(|body| InstrumentedBody::new(BodyKind::Response, body, None, |_, _, _, _| {})))
            } else {
                response
            }
        })
    }
}

#[derive(Clone)]
pub struct TransactionSvc<S> {
    manager: Arc<HttpConnectionManager>,
    router: tokio::sync::watch::Receiver<Option<Arc<RouteConfiguration>>>,
    inner: S,
}

impl<S> TransactionSvc<S> {
    pub fn new(
        manager: Arc<HttpConnectionManager>,
        router: tokio::sync::watch::Receiver<Option<Arc<RouteConfiguration>>>,
        inner: S,
    ) -> Self {
        Self { manager, router, inner }
    }
}

impl<S> Service<HttpRequest<Incoming>> for TransactionSvc<S>
where
    S: Service<RoutedHttpRequest<OrionRequestBody>, Response = Response<OrionRequestBody>, Error = crate::Error>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, StdResult<Self::Response, Self::Error>>;

    #[allow(clippy::too_many_lines)]
    fn call(&self, req: HttpRequest<Incoming>) -> Self::Future {
        let HttpRequest { request, ctx } = req;
        let trans_ctx = Arc::clone(&ctx.tx);
        let stream_metrics = Arc::clone(&ctx.conn.stream_metrics);

        let manager = Arc::clone(&self.manager);
        let listener_name = manager.listener_name;
        let route_conf = self.router.borrow().clone();

        with_metric!(
            http::DOWNSTREAM_RQ_TOTAL,
            add,
            1,
            trans_ctx.shard_id(),
            &[KeyValue::new("listener", listener_name)]
        );
        with_metric!(
            http::DOWNSTREAM_RQ_ACTIVE,
            add,
            1,
            trans_ctx.shard_id(),
            &[KeyValue::new("listener", listener_name)]
        );

        #[cfg(feature = "metrics")]
        if let Some(custom_metrics) = CUSTOM_METRICS.get() {
            let mut attrs = SmallVec::<[KeyValue; 2]>::new();
            if let Some(custom_keys) = metrics::CUSTOM_KEYS.get() {
                for key in custom_keys {
                    if let Some(source) = key.source() {
                        if let Some(id) = metrics::extract_custom_partition_key(request.headers(), Some(source)) {
                            attrs.push(KeyValue::new(key.attribute_name().unwrap_or("custom"), id));
                        }
                    }
                }
            }
            custom_metrics.with_headers(MetricsHook::IncomingRequest, request.headers(), attrs.as_slice());
        }

        #[cfg(feature = "access-log")]
        if let Err(err) = crate::access_log::evaluate_base64_access_log_hook(
            crate::access_log::AccessLogHook::IncomingRequest,
            request.headers(),
            &mut trans_ctx.trans_state.lock().loggers,
        ) {
            tracing::warn!("Failed to process access log header for IncomingRequest: {err}");
        }

        let inner = self.inner.clone();

        Box::pin(async move {
            #[allow(unused_variables)]
            let trans_ctx_for_defer = Arc::clone(&trans_ctx);
            scopeguard::defer! {
                with_metric!(http::DOWNSTREAM_RQ_ACTIVE, sub, 1, trans_ctx_for_defer.shard_id(), &[KeyValue::new("listener", listener_name)]);
            }

            let req_timeout = manager.request_timeout;
            let filterchain_id = manager.filterchain_id;

            eval_http_init_context(&request, &trans_ctx, Some(ctx.conn.downstream.as_ref()));

            let Some(route_conf) = route_conf else {
                return Ok(handle_route_conf_not_found(
                    request.version(),
                    &trans_ctx,
                    Some(&stream_metrics),
                    listener_name,
                    trans_ctx.user_partition_key,
                    filterchain_id,
                ));
            };

            if let Some(response_error) = reject_request_if_invalid(
                &request,
                &trans_ctx,
                Some(&stream_metrics),
                listener_name,
                trans_ctx.user_partition_key,
                filterchain_id,
            ) {
                return Ok(response_error);
            }

            let sm_for_cb = Arc::clone(&stream_metrics);
            let http = HttpRequest { request, ctx }.map_body(|body| {
                #[cfg(any(feature = "access-log", feature = "tracing", feature = "metrics"))]
                let trans_ctx = Arc::clone(&trans_ctx);
                let body = TimeoutBody::new(req_timeout, PolyBody::from(body));

                #[allow(unused_variables)]
                let stream_metrics_clone = Arc::clone(&stream_metrics);
                InstrumentedBody::new(
                    BodyKind::Request,
                    body,
                    Some(sm_for_cb),
                    #[allow(unused_variables)]
                    move |body_bytes, _stream_metrics, body_error, body_flags| {
                        #[cfg(any(feature = "access-log", feature = "metrics"))]
                        {
                            let mut trans_state = trans_ctx.trans_state.lock();
                            #[allow(unused_variables)]
                            let duration = trans_ctx.start_instant.elapsed();

                            #[cfg(feature = "access-log")]
                            with_access_log!(
                                &mut trans_state.loggers,
                                HttpRequestDurationContext { duration, tx_duration: duration }
                            );

                            if trans_ctx.trans_phase.is_complete() {
                                let trans_ctx_cb = Arc::clone(&trans_ctx);
                                let body_error_cb = body_error.clone();

                                let stream_metrics_cb = Arc::clone(&stream_metrics_clone);
                                stream_metrics_clone.add_flush_callback(Box::new(move || {
                                    #[allow(unused_mut)]
                                    let mut trans_state = trans_ctx_cb.trans_state.lock();
                                    #[allow(unused_variables)]
                                    let ctx_bytes = trans_state.bytes;
                                    #[allow(unused_variables)]
                                    let ctx_flags = trans_state.flags;
                                    #[allow(unused_variables)]
                                    let ctx_event = trans_state.event.clone();

                                    eval_http_finish_context(FinishContextParams {
                                        stream_metrics: &stream_metrics_cb,
                                        listener_name,
                                        user_partition_key: trans_ctx_cb.user_partition_key,
                                        filterchain_id,
                                        bytes_received: body_bytes,
                                        bytes_sent: ctx_bytes,
                                        trans_start_time: trans_ctx_cb.start_instant,
                                        #[cfg(feature = "metrics")]
                                        m_ctx: MetricsFinishContext { shard_id: trans_ctx_cb.shard_id() },
                                        #[cfg(feature = "access-log")]
                                        al_ctx: AccessLogFinishContext {
                                            event: EventInfo {
                                                body_kind: BodyKind::Request,
                                                event_kind: ctx_event.or(body_error_cb),
                                                response_flags: ctx_flags | body_flags,
                                            },
                                            access_loggers: trans_state.loggers.as_mut(),
                                        },
                                    });
                                }));
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
                        if trans_ctx.trans_phase.is_complete() {
                            if let Some(span) = trans_ctx.span_state.as_ref() {
                                span.end();
                            }
                        }
                    },
                )
            });

            #[cfg(feature = "access-log")]
            let trans_ctx_clone = Arc::clone(&http.ctx.tx);
            let response = inner.call(RoutedHttpRequest { http, route_conf }).await;

            #[cfg(feature = "metrics")]
            if let Ok(response) = &response {
                if let Some(custom_metrics) = CUSTOM_METRICS.get() {
                    let mut attrs = SmallVec::<[KeyValue; 2]>::new();
                    if let Some(custom_keys) = metrics::CUSTOM_KEYS.get() {
                        for key in custom_keys {
                            if let Some(source) = key.source() {
                                if let Some(id) =
                                    metrics::extract_custom_partition_key(response.headers(), Some(source))
                                {
                                    attrs.push(KeyValue::new(key.attribute_name().unwrap_or("custom"), id));
                                }
                            }
                        }
                    }
                    custom_metrics.with_headers(MetricsHook::DownstreamResponse, response.headers(), attrs.as_slice());
                }
            }

            #[cfg(feature = "access-log")]
            if let Ok(response) = &response {
                if let Err(err) = crate::access_log::evaluate_base64_access_log_hook(
                    crate::access_log::AccessLogHook::DownstreamResponse,
                    response.headers(),
                    &mut trans_ctx_clone.trans_state.lock().loggers,
                ) {
                    tracing::warn!("Failed to process access log header for DownstreamResponse: {err}");
                }
            }
            response
        })
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
        trans_ctx.trace_ctx.as_ref().and_then(|t| t.map_child(orion_tracing::trace_info::TraceInfo::trace_id));

    #[cfg(not(feature = "tracing"))]
    #[allow(unused_variables)]
    let trace_id: Option<u128> = None;

    #[cfg(feature = "access-log")]
    {
        use crate::with_access_log;
        use orion_format::context::SocketAddrContext;

        let server_name = metadata.and_then(|md| md.sni.as_ref().map(SmolStr::as_str));

        #[cfg(feature = "access-log")]
        with_access_log!(
            &mut trans_ctx.trans_state.lock().loggers,
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
    }
}

#[cfg(feature = "access-log")]
struct AccessLogFinishContext<'a> {
    event: EventInfo,
    access_loggers: &'a mut Vec<LogFormatter>,
}

#[cfg(feature = "metrics")]
struct MetricsFinishContext {
    shard_id: ShardId,
}

#[cfg(any(feature = "access-log", feature = "metrics"))]
struct FinishContextParams<'a> {
    stream_metrics: &'a StreamMetrics,
    listener_name: &'static str,
    #[allow(dead_code)]
    user_partition_key: Option<&'static str>,
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
    use orion_format::context::WireContext;

    let latency = params.trans_start_time.elapsed();

    #[cfg(feature = "metrics")]
    if let Some(user_partition_key) = params.user_partition_key {
        with_histogram!(
            user::LATENCY,
            record,
            #[allow(clippy::cast_possible_truncation)]
            {
                latency.as_millis() as u64
            },
            params.m_ctx.shard_id,
            &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
        );
    }

    #[cfg(feature = "access-log")]
    let duration = latency;

    #[cfg(feature = "access-log")]
    with_access_log!(
        &mut *params.al_ctx.access_loggers,
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
    let mut loggers: Vec<LogFormatter> = std::mem::take(params.al_ctx.access_loggers);

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
        &[KeyValue::new("listener", params.listener_name)]
    );

    with_metric!(
        http::DOWNSTREAM_CX_TX_BYTES_TOTAL,
        add,
        wire_bytes_sent,
        shard_id,
        &[KeyValue::new("listener", params.listener_name)]
    );

    #[cfg(feature = "metrics")]
    if let Some(user_partition_key) = user_partition_key {
        with_metric!(
            user::BYTES_TX,
            add,
            wire_bytes_received,
            shard_id,
            &[
                KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key),
                KeyValue::new("listener", params.listener_name)
            ]
        );
        with_metric!(
            user::BYTES_RX,
            add,
            wire_bytes_sent,
            shard_id,
            &[
                KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key),
                KeyValue::new("listener", params.listener_name)
            ]
        );
    }

    #[cfg(feature = "access-log")]
    {
        with_access_log!(&mut loggers, WireContext { wire_bytes_received, wire_bytes_sent });
        let messages = loggers.into_iter().map(LogFormatter::into_message).collect::<Vec<_>>();
        log_access_blocking(Target::ListenerFilterChain(params.listener_name.into(), params.filterchain_id), messages);
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
#[allow(unused_variables)]
fn instrument_early_failure_response(
    response: Response<crate::OrionResponseBody>,
    trans_ctx: &Arc<TransactionContext>,
    stream_metrics: Option<&Arc<StreamMetrics>>,
    listener_name: &'static str,
    user_partition_key: Option<&'static str>,
    filterchain_id: u64,
) -> Response<OrionRequestBody> {
    #[cfg(feature = "access-log")]
    let first_byte_instant = Instant::now();

    with_metric!(http::DOWNSTREAM_RQ_4XX, add, 1, trans_ctx.shard_id(), &[KeyValue::new("listener", listener_name)]);

    #[cfg(feature = "tracing")]
    if let Some(state) = trans_ctx.span_state.as_ref() {
        if let Some(ref mut span) = *state.server_span.lock() {
            span.set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, 400));
        }
    }

    #[cfg(feature = "access-log")]
    with_access_log!(
        &mut trans_ctx.trans_state.lock().loggers,
        DownstreamResponseContext { response: &response, response_head_size: response_head_size(&response) }
    );

    #[cfg(feature = "access-log")]
    let (initial_flags, initial_event) = {
        let ec = response.extensions().get::<EventErrorContext>();
        (ec.map(|ec| ec.response_flags).unwrap_or_default(), ec.and_then(|ec| ec.event_kind.clone()))
    };

    #[cfg(any(feature = "access-log", feature = "tracing", feature = "metrics"))]
    let trans_ctx = Arc::clone(trans_ctx);

    let sm_for_cb = stream_metrics.cloned();
    response.map(|body| {
        let sm_for_cb2 = sm_for_cb.clone();
        InstrumentedBody::new(
            BodyKind::Response,
            body,
            sm_for_cb,
            #[allow(unused_variables)]
            move |body_bytes, stream_metrics, body_error, body_flags| {
                #[cfg(any(feature = "access-log", feature = "metrics"))]
                {
                    let mut log_ctx = trans_ctx.trans_state.lock();

                    #[cfg(feature = "access-log")]
                    {
                        use crate::with_access_log;

                        #[allow(unused_variables)]
                        let duration = first_byte_instant.saturating_duration_since(trans_ctx.start_instant);
                        #[allow(unused_variables)]
                        let tx_duration = Instant::now().saturating_duration_since(first_byte_instant);

                        with_access_log!(&mut log_ctx.loggers, HttpResponseDurationContext { duration, tx_duration })
                    };

                    if trans_ctx.trans_phase.is_complete() {
                        let sm_arc = sm_for_cb2.clone().unwrap();
                        let trans_ctx_cb = Arc::clone(&trans_ctx);
                        #[cfg(feature = "access-log")]
                        let initial_event_cb = initial_event.clone();
                        let body_error_cb = body_error.clone();

                        stream_metrics.add_flush_callback(Box::new(move || {
                            #[allow(unused_mut)]
                            let mut log_ctx = trans_ctx_cb.trans_state.lock();
                            #[allow(unused_variables)]
                            let ctx_bytes = log_ctx.bytes;
                            #[allow(unused_variables)]
                            let ctx_flags = log_ctx.flags;
                            #[allow(unused_variables)]
                            let ctx_event = log_ctx.event.clone();

                            eval_http_finish_context(FinishContextParams {
                                stream_metrics: &sm_arc,
                                listener_name,
                                user_partition_key: trans_ctx_cb.user_partition_key,
                                filterchain_id,
                                bytes_received: ctx_bytes,
                                bytes_sent: body_bytes,
                                trans_start_time: trans_ctx_cb.start_instant,
                                #[cfg(feature = "metrics")]
                                m_ctx: MetricsFinishContext { shard_id: trans_ctx_cb.shard_id() },
                                #[cfg(feature = "access-log")]
                                al_ctx: AccessLogFinishContext {
                                    event: EventInfo {
                                        body_kind: BodyKind::Response,
                                        event_kind: ctx_event.or(initial_event_cb).or(body_error_cb),
                                        response_flags: ctx_flags | initial_flags | body_flags,
                                    },
                                    access_loggers: log_ctx.loggers.as_mut(),
                                },
                            });
                        }));
                    } else {
                        log_ctx.bytes = body_bytes;
                        #[cfg(feature = "access-log")]
                        {
                            log_ctx.flags = initial_flags | body_flags;
                            log_ctx.event = initial_event.or(body_error);
                        }
                    }
                }

                #[cfg(feature = "tracing")]
                if trans_ctx.trans_phase.is_complete() {
                    if let Some(span) = trans_ctx.span_state.as_ref() {
                        span.end();
                    }
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
    trans_ctx: &Arc<TransactionContext>,
    stream_metrics: Option<&Arc<StreamMetrics>>,
    listener_name: &'static str,
    user_partition_key: Option<&'static str>,
    filterchain_id: u64,
) -> Option<Response<crate::OrionRequestBody>> {
    // check if request has no host header, or if it has multiple ones (invalid for http1.1)
    //
    let response = if matches!(request.version(), ::http::Version::HTTP_11) {
        match request.headers().get_all(HOST).iter().count() {
            1 => None,
            n => {
                debug!("Invalid number of host headers: {}", n);
                Some(
                    SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into())
                        .with_close_connection(true)
                        .into_response(request.version()),
                )
            },
        }
    } else {
        None
    };

    // check if method is too long...
    //
    let response = response.or_else(|| {
        (request.method().as_str().len() > MAX_METHOD_LENGTH).then(|| {
            debug!("Too long method: {} bytes", request.method().as_str());
            SyntheticHttpResponse::custom_error(
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                None,
                EventFailure::DirectResponse.into(),
                ResponseFlags::default(),
            )
            .into_response(request.version())
        })
    });

    //check if uri/line is too long...
    //
    let response = response.or_else(|| {
        let mut counter = LengthCounter(0);
        _ = write!(&mut counter, "{}", request.uri());
        (counter.0 > MAX_URI_LENGTH).then(|| {
            debug!("Too long uri: {} bytes", counter.0);
            SyntheticHttpResponse::custom_error(
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                None,
                EventFailure::DirectResponse.into(),
                ResponseFlags::default(),
            )
            .into_response(request.version())
        })
    });

    response.map(|r| {
        instrument_early_failure_response(
            r,
            trans_ctx,
            stream_metrics,
            listener_name,
            user_partition_key,
            filterchain_id,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn handle_route_conf_not_found(
    version: ::http::Version,
    trans_ctx: &Arc<TransactionContext>,
    stream_metrics: Option<&Arc<StreamMetrics>>,
    listener_name: &'static str,
    user_partition_key: Option<&'static str>,
    filterchain_id: u64,
) -> Response<crate::OrionRequestBody> {
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
    use orion_configuration::config::network_filters::http_connection_manager::MatchHost;

    use super::*;

    #[test]
    fn test_select_virtual_hosts() {
        let domains1 = vec!["domain1.com:8000", "domain1.com"].into_iter().flat_map(MatchHost::try_from).collect();
        let domains2 = vec!["domain2.com"].into_iter().flat_map(MatchHost::try_from).collect();
        let domains3 = vec!["*.domain3.com", "domain3.com"].into_iter().flat_map(MatchHost::try_from).collect();
        let vh1 = VirtualHost { domains: domains1, ..Default::default() };
        let vh2 = VirtualHost { domains: domains2, ..Default::default() };
        let vh3 = VirtualHost { domains: domains3, ..Default::default() };

        let request = Request::builder().header("host", "127.0.0.1:8000").body(()).unwrap();
        assert_eq!(select_virtual_host(&request, &[vh1.clone(), vh2.clone(), vh3.clone()]), None);

        let request = Request::builder().header("host", "domain1.com:8000").body(()).unwrap();
        assert_eq!(select_virtual_host(&request, &[vh1.clone(), vh2.clone(), vh3.clone()]), Some(&vh1));

        let request = Request::builder().header("host", "domain1.com").body(()).unwrap();
        assert_eq!(select_virtual_host(&request, &[vh1.clone(), vh2.clone(), vh3.clone()]), Some(&vh1));

        let request = Request::builder().header("host", "domain3.com").body(()).unwrap();
        assert_eq!(select_virtual_host(&request, &[vh1.clone(), vh2.clone(), vh3.clone()]), Some(&vh3));

        let request = Request::builder().header("host", "blah.domain3.com").body(()).unwrap();
        assert_eq!(select_virtual_host(&request, &[vh1.clone(), vh2.clone(), vh3.clone()]), Some(&vh3));

        let request = Request::builder().header("host", "blah.domain3.com:8000").body(()).unwrap();
        assert_eq!(select_virtual_host(&request, &[vh1.clone(), vh2.clone(), vh3.clone()]), None);

        let request = Request::builder().header("host", "domain2.com:8000").body(()).unwrap();
        assert_eq!(select_virtual_host(&request, &[vh1.clone(), vh2.clone(), vh3.clone()]), None);

        let domains2 = vec!["domain2.com:8000"].into_iter().flat_map(MatchHost::try_from).collect();
        let vh2 = VirtualHost { domains: domains2, ..Default::default() };
        let request = Request::builder().header("host", "domain2.com").body(()).unwrap();
        assert_eq!(select_virtual_host(&request, &[vh1.clone(), vh2.clone(), vh3.clone()]), None);
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
