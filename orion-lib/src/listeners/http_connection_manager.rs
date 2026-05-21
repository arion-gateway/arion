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

pub mod cors;
mod direct_response;
pub mod ext_proc;
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
use crate::extensions_context::EventContext;

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

use std::fmt::Write;

use crate::{
    body::{
        instrumented_body::InstrumentedBody,
        response_flags::{BodyKind, ResponseFlags},
        timeout_body::TimeoutBody,
    },
    event_error::{EventFailure, EventKind},
    extensions_context::MetadataContext,
    get_shard_id,
    listeners::{
        http_connection_manager::http_modifiers::ModifiersExtractor,
        http_filters::{per_route_http_filters, FilterDecision, FilterFactory, HttpFilter, HttpFilterValue},
        metadata::DownstreamMetadata,
        synthetic_http_response::SyntheticHttpResponse,
    },
    with_client_span, with_metric, with_server_span, ConversionContext, OrionRequestBody, OrionResponseBody, PolyBody,
    Result, RouteConfiguration,
};

#[cfg(any(feature = "access-log", feature = "metrics"))]
use crate::utils::instrumented_stream::StreamMetrics;

use orion_configuration::config::network_filters::{
    http_connection_manager::{Route, VirtualHost, XffSettings},
    tracing::{TracingConfig, TracingKey},
};
use orion_format::types::ResponseFlags as FmtResponseFlags;
use route::RouteContext;
use scopeguard::defer;
use smol_str::SmolStr;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use std::{fmt, future::Future, result::Result as StdResult, sync::Arc};
use tokio::sync::watch;
use tracing::{debug, error};
use upgrades as upgrade_utils;

use orion_tracing::http_tracer::HttpTracer;
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
        let http_filters_hcm = configuration
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
    pub(crate) fn request_handler(
        self: &Arc<Self>,
    ) -> Box<
        dyn Service<
                Request<Incoming>,
                Response = Response<OrionRequestBody>,
                Error = crate::Error,
                Future = BoxFuture<'static, StdResult<Response<OrionRequestBody>, crate::Error>>,
            > + Send
            + Sync,
    > {
        Box::new(HttpRequestHandler { manager: Arc::clone(self), router: self.router_sender.subscribe() })
            as Box<
                dyn Service<
                        Request<Incoming>,
                        Response = Response<OrionRequestBody>,
                        Error = crate::Error,
                        Future = BoxFuture<'static, StdResult<Response<OrionRequestBody>, crate::Error>>,
                    > + Send
                    + Sync,
            >
    }
}

#[derive(Debug)]
pub struct CachedRoute<'a> {
    route: &'a Route,
    route_match: RouteMatchResult,
    vh: &'a VirtualHost,
}

pub(crate) struct HttpRequestHandler {
    manager: Arc<HttpConnectionManager>,
    router: watch::Receiver<Option<Arc<RouteConfiguration>>>,
}

#[cfg(any(feature = "access-log", feature = "metrics"))]
#[derive(Debug, Default)]
pub struct TransactionState {
    bytes: u64, // either the request or response body size, depending which one has completed first
    flags: ResponseFlags,
    event: Option<EventKind>,
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
    span_state: Option<Arc<SpanState>>,
    #[cfg(any(feature = "access-log", feature = "metrics"))]
    trans_ctx: Mutex<TransactionState>,
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
            trans_ctx: Mutex::new(TransactionState::default()),
            #[cfg(feature = "tracing")]
            trace_ctx: None,
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
        #[cfg(feature = "tracing")] server_span: Option<BoxedSpan>,
    ) -> Self {
        TransactionContext {
            start_instant: std::time::Instant::now(),
            request_id,
            user_partition_key,
            #[cfg(any(feature = "access-log", feature = "metrics"))]
            trans_ctx: Mutex::new(TransactionState::new(
                #[cfg(feature = "access-log")]
                access_log,
            )),
            #[cfg(feature = "tracing")]
            trace_ctx,
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
                if status_code == 429 {
                    with_metric!(
                        user::THROTTLES,
                        add,
                        1,
                        self.shard_id(),
                        &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                    );
                } else {
                    with_metric!(
                        user::INVOCATIONS,
                        add,
                        1,
                        self.shard_id(),
                        &[KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key)]
                    );
                }
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

struct TransactionPipeline<RC> {
    route_conf: RC,
}

#[allow(clippy::wrong_self_convention)]
impl<RC> TransactionPipeline<RC>
where
    RC: RequestHandler<Request<OrionRequestBody>, Arc<HttpConnectionManager>> + Clone,
{
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_lines)]
    async fn to_response(
        self,
        trans_handler: Arc<TransactionContext>,
        manager: Arc<HttpConnectionManager>,
        mut request: Request<OrionRequestBody>,
    ) -> Result<Response<OrionRequestBody>> {
        #[allow(unused_variables)]
        let listener_name = manager.listener_name;
        #[allow(unused_variables)]
        let filterchain_id = manager.filterchain_id;
        let metadata = request.extensions().get::<MetadataContext>();
        let downstream_addr = metadata
            .map(|md| md.downstream.connection.peer_address())
            .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0));

        let stream_metrics = metadata.map(|md| Arc::clone(&md.stream_metrics));

        #[allow(clippy::unwrap_used)]
        stream_metrics.as_ref().unwrap().inc_requests();

        // apply the request header modifiers
        http_modifiers::apply_prerouting_functions(&mut request, downstream_addr, manager.xff_settings);

        // process request, get the response..
        let result = self.route_conf.to_response(&trans_handler, request, Arc::clone(&manager)).await;

        // calculate the time to first byte..
        #[cfg(any(feature = "access-log", feature = "metrics"))]
        let first_byte_instant = Instant::now();

        result.map(|mut response| {
            // set the request id on the response...
            manager
                .request_id_handler
                .apply_to(&mut response, trans_handler.request_id.as_ref().and_then(|x| x.propagate_ref()));

            #[cfg(feature = "access-log")]
            let (initial_flags, initial_event) = {
                let ec = response.extensions().get::<EventContext>();
                (ec.map(|ec| ec.response_flags).unwrap_or_default(), ec.and_then(|ec| ec.event_kind.clone()))
            };

            #[cfg(feature = "access-log")]
            {
                use crate::with_access_log;

                with_access_log!(
                    &mut trans_handler.trans_ctx.lock().loggers,
                    DownstreamResponseContext {
                        response: &response,
                        response_head_size: response_head_size(&response)
                    }
                )
            }

            response.map(move |body| {
                InstrumentedBody::new(
                    BodyKind::Response,
                    body,
                    stream_metrics,
                    #[allow(unused_variables)]
                    move |body_bytes, stream_metrics, body_error, body_flags| {
                        #[cfg(any(feature = "access-log", feature = "metrics"))]
                        {
                            let mut trans_ctx = trans_handler.trans_ctx.lock();
                            #[allow(unused_variables)]
                            let duration = first_byte_instant.saturating_duration_since(trans_handler.start_instant);
                            #[allow(unused_variables)]
                            let tx_duration = Instant::now().saturating_duration_since(first_byte_instant);

                            #[cfg(feature = "access-log")]
                            with_access_log!(
                                &mut trans_ctx.loggers,
                                HttpResponseDurationContext { duration, tx_duration }
                            );

                            if trans_handler.trans_phase.is_complete() {
                                #[allow(unused_variables)]
                                let ctx_bytes = trans_ctx.bytes;
                                #[allow(unused_variables)]
                                let ctx_flags = trans_ctx.flags;
                                #[allow(unused_variables)]
                                let ctx_event = trans_ctx.event.clone();
                                eval_http_finish_context(FinishContextParams {
                                    stream_metrics,
                                    listener_name,
                                    user_partition_key: trans_handler.user_partition_key,
                                    filterchain_id,
                                    bytes_received: ctx_bytes,
                                    bytes_sent: body_bytes,
                                    trans_start_time: trans_handler.start_instant,
                                    #[cfg(feature = "metrics")]
                                    m_ctx: MetricsFinishContext { shard_id: trans_handler.shard_id() },
                                    #[cfg(feature = "access-log")]
                                    al_ctx: AccessLogFinishContext {
                                        event: EventInfo {
                                            body_kind: BodyKind::Response,
                                            event_kind: ctx_event.or(initial_event).or(body_error),
                                            response_flags: ctx_flags | initial_flags | body_flags,
                                        },
                                        access_loggers: trans_ctx.loggers.as_mut(),
                                    },
                                });
                            } else {
                                trans_ctx.bytes = body_bytes;
                                #[cfg(feature = "access-log")]
                                {
                                    trans_ctx.flags = initial_flags | body_flags;
                                    trans_ctx.event = initial_event.or(body_error);
                                }
                            }
                        }

                        #[cfg(feature = "tracing")]
                        if trans_handler.trans_phase.is_complete() {
                            if let Some(span) = trans_handler.span_state.as_ref() {
                                span.end();
                            }
                        }
                    },
                )
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
        trans_context: &TransactionContext,
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

struct AsyncExecution(Arc<RouteConfiguration>);

impl RequestHandler<Request<OrionRequestBody>, (Arc<HttpConnectionManager>, usize, HttpFilterValue)>
    for AsyncExecution
{
    #[allow(clippy::too_many_lines)]
    async fn to_response(
        self,
        trans_context: &TransactionContext,
        mut request: Request<OrionRequestBody>,
        (connection_manager, mut filter_idx, http_filter): (Arc<HttpConnectionManager>, usize, HttpFilterValue),
    ) -> Result<Response<OrionResponseBody>> {
        let mut cached_route = match_request_route(&request, &self.0);
        let mut active_filters: SmallVec<[HttpFilterValue; 4]> = SmallVec::new();

        active_filters.push(http_filter);

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

            // Semantic Match: We keep the exact same while logic
            while filter_idx < route_filters.len() {
                // Safe Access: Replaces route_filters[filter_idx] without changing logic flow
                let Some(filter) = route_filters.get(filter_idx) else {
                    break;
                };

                filter_idx += 1;

                if filter.disabled {
                    continue;
                }

                if let Some(filter_config) = &filter.filter {
                    let mut filter_value = filter_config.new_from();
                    let filter_res = filter_value.apply_request(&mut request).await;

                    match filter_res {
                        FilterDecision::Continue => {
                            active_filters.push(filter_value);
                        },
                        FilterDecision::DirectResponse(_) => {
                            break 'filter_loop filter_res;
                        },
                        FilterDecision::AsyncRequest(_, _) => {
                            unimplemented!()
                        },
                        FilterDecision::Reroute => {
                            active_filters.push(filter_value);
                            reroute = true;
                            break;
                        },
                    }
                }
            }

            if reroute {
                debug!("rerouting request...");
                cached_route = match_request_route(&request, &self.0);
                // filter_idx remains incremented as in your original code
            } else {
                break 'filter_loop FilterDecision::Continue;
            }
        };

        // Rest of the function remains identical to your provided source
        let mut response = match cached_route {
            None => SyntheticHttpResponse::not_found(
                EventFailure::RouteNotFound.into(),
                ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
            )
            .into_response(request.version()),
            Some(cached_route) => match filter_response {
                FilterDecision::DirectResponse(response) | FilterDecision::AsyncRequest(response, _) => {
                    let mut response = *response;
                    apply_mutations_on_response(
                        &mut response,
                        &self.0,
                        &cached_route,
                        self.0.most_specific_header_mutations_wins,
                    );
                    response
                },
                _ => {
                    let websocket_enabled_by_default =
                        upgrade_utils::is_websocket_enabled_by_hcm(&connection_manager.enabled_upgrades);

                    let mut response = match &cached_route.route.action {
                        Action::DirectResponse(dr) => {
                            dr.to_response(trans_context, request, &cached_route.route.name).await
                        },
                        Action::Redirect(rd) => {
                            rd.to_response(
                                trans_context,
                                request,
                                (&cached_route.route_match, &cached_route.route.name),
                            )
                            .await
                        },
                        Action::Route(route) => {
                            apply_mutations_on_request(
                                &mut request,
                                &self.0,
                                &cached_route,
                                self.0.most_specific_header_mutations_wins,
                            );

                            let remote_address = request
                                .extensions()
                                .get::<MetadataContext>()
                                .map(|md| md.downstream.connection.peer_address())
                                .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0));

                            route
                                .to_response(
                                    trans_context,
                                    request,
                                    (
                                        RouteContext {
                                            route_name: &cached_route.route.name,
                                            retry_policy: cached_route.vh.retry_policy.as_ref(),
                                            route_match: &cached_route.route_match,
                                            remote_address,
                                            websocket_enabled_by_default,
                                        },
                                        &connection_manager,
                                    ),
                                )
                                .await
                        },
                    }?;

                    apply_mutations_on_response(
                        &mut response,
                        &self.0,
                        &cached_route,
                        self.0.most_specific_header_mutations_wins,
                    );
                    response
                },
            },
        };

        for filter in active_filters.iter_mut().rev() {
            let filter_res = filter.apply_response(&mut response).await;
            if let FilterDecision::DirectResponse(direct_response) = filter_res {
                response = *direct_response;
            }
        }

        Ok(response)
    }
}

impl RequestHandler<Request<OrionRequestBody>, Arc<HttpConnectionManager>> for Arc<RouteConfiguration> {
    #[allow(clippy::too_many_lines)]
    async fn to_response(
        self,
        trans_context: &TransactionContext,
        mut request: Request<OrionRequestBody>,
        arg: Arc<HttpConnectionManager>,
    ) -> Result<Response<OrionResponseBody>> {
        let connection_manager = arg;
        let mut cached_route = match_request_route(&request, &self);
        // let mut request: Request<HttpBody> = request.map(|body| body.map_inner(TimeoutBody::map_into));
        let mut active_filters: SmallVec<[HttpFilterValue; 4]> = SmallVec::new();

        let mut filter_idx = 0;

        let filter_response = 'filter_loop: loop {
            let Some(ref chosen_route) = cached_route else {
                // No route found - return 404 immediately
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
                // No filters to process
                break 'filter_loop FilterDecision::Continue;
            };

            let mut reroute = false;

            for (current_idx, filter) in route_filters.iter().enumerate().skip(filter_idx) {
                if filter.disabled {
                    filter_idx += 1;
                    continue;
                }

                if let Some(filter_config) = &filter.filter {
                    let mut filter_value = filter_config.new_from();
                    let filter_res = filter_value.apply_request(&mut request).await;

                    match filter_res {
                        FilterDecision::Continue => {
                            active_filters.push(filter_value);
                            filter_idx += 1;
                        },
                        FilterDecision::DirectResponse(_) => {
                            break 'filter_loop filter_res;
                        },
                        FilterDecision::AsyncRequest(resp, Some(req)) => {
                            let async_exec = AsyncExecution(Arc::clone(&self));
                            let conn_manager = Arc::clone(&connection_manager);

                            // Use current_idx + 1 to ensure the next task starts from the correct filter
                            let next_idx = current_idx + 1;

                            tokio::spawn(async move {
                                let trans_handler = TransactionContext::default();
                                _ = async_exec
                                    .to_response(&trans_handler, *req, (conn_manager, next_idx, filter_value))
                                    .await;
                            });

                            break 'filter_loop FilterDecision::AsyncRequest(resp, None);
                        },
                        FilterDecision::AsyncRequest(resp, None) => {
                            break 'filter_loop FilterDecision::AsyncRequest(resp, None);
                        },
                        FilterDecision::Reroute => {
                            active_filters.push(filter_value);
                            filter_idx += 1;
                            reroute = true;
                            break;
                        },
                    }
                } else {
                    filter_idx += 1;
                }
            }

            if reroute {
                debug!("rerouting request...");
                cached_route = match_request_route(&request, &self);
            } else {
                // All filters processed successfully
                break 'filter_loop FilterDecision::Continue;
            }
        };

        let mut response = match cached_route {
            None => SyntheticHttpResponse::not_found(
                EventFailure::RouteNotFound.into(),
                ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
            )
            .into_response(request.version()),
            Some(cached_route) => match filter_response {
                FilterDecision::DirectResponse(response) | FilterDecision::AsyncRequest(response, _) => {
                    let mut response = *response;
                    apply_mutations_on_response(
                        &mut response,
                        &self,
                        &cached_route,
                        self.most_specific_header_mutations_wins,
                    );
                    response
                },
                _ => {
                    let websocket_enabled_by_default =
                        upgrade_utils::is_websocket_enabled_by_hcm(&connection_manager.enabled_upgrades);

                    let mut response = match &cached_route.route.action {
                        Action::DirectResponse(dr) => {
                            dr.to_response(trans_context, request, &cached_route.route.name).await
                        },
                        Action::Redirect(rd) => {
                            rd.to_response(
                                trans_context,
                                request,
                                (&cached_route.route_match, &cached_route.route.name),
                            )
                            .await
                        },
                        Action::Route(route) => {
                            apply_mutations_on_request(
                                &mut request,
                                &self,
                                &cached_route,
                                self.most_specific_header_mutations_wins,
                            );

                            let remote_address = request
                                .extensions()
                                .get::<MetadataContext>()
                                .map(|md| md.downstream.connection.peer_address())
                                .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0));
                            route
                                .to_response(
                                    trans_context,
                                    request,
                                    (
                                        RouteContext {
                                            route_name: &cached_route.route.name,
                                            retry_policy: cached_route.vh.retry_policy.as_ref(),
                                            route_match: &cached_route.route_match,
                                            remote_address,
                                            websocket_enabled_by_default,
                                        },
                                        &connection_manager,
                                    ),
                                )
                                .await
                        },
                    }?;

                    #[cfg(feature = "metrics")]
                    if let Some(custom_metrics) = CUSTOM_METRICS.get() {
                        let attr =
                            metrics::extract_custom_partition_key(response.headers(), metrics::CUSTOM_KEY.source())
                                .map(|id| KeyValue::new(metrics::CUSTOM_KEY.attribute_name().unwrap_or("custom"), id));
                        custom_metrics.with_headers(MetricsHook::IncomingResponse, response.headers(), attr.as_slice());
                    }

                    apply_mutations_on_response(
                        &mut response,
                        &self,
                        &cached_route,
                        self.most_specific_header_mutations_wins,
                    );
                    response
                },
            },
        };

        // let's process the active filters on response in the reverse order...
        //
        for filter in &mut active_filters.iter_mut().rev() {
            let filter_res = filter.apply_response(&mut response).await;
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
) where
    Route: ModifiersExtractor<Request<B>>,
    VirtualHost: ModifiersExtractor<Request<B>>,
    RouteConfiguration: ModifiersExtractor<Request<B>>,
{
    if most_specific_header_mutations_wins {
        target.apply_mutation(ModifiersExtractor::<Request<B>>::extract(route_config));
        target.apply_mutation(ModifiersExtractor::<Request<B>>::extract(cached_route.vh));
        target.apply_mutation(ModifiersExtractor::<Request<B>>::extract(cached_route.route));
    } else {
        target.apply_mutation(ModifiersExtractor::<Request<B>>::extract(cached_route.route));
        target.apply_mutation(ModifiersExtractor::<Request<B>>::extract(cached_route.vh));
        target.apply_mutation(ModifiersExtractor::<Request<B>>::extract(route_config));
    }
}

fn apply_mutations_on_response<B>(
    target: &mut Response<B>,
    route_config: &RouteConfiguration,
    cached_route: &CachedRoute<'_>,
    most_specific_header_mutations_wins: bool,
) where
    Route: ModifiersExtractor<Response<B>>,
    VirtualHost: ModifiersExtractor<Response<B>>,
    RouteConfiguration: ModifiersExtractor<Response<B>>,
{
    if most_specific_header_mutations_wins {
        target.apply_mutation(ModifiersExtractor::<Response<B>>::extract(route_config));
        target.apply_mutation(ModifiersExtractor::<Response<B>>::extract(cached_route.vh));
        target.apply_mutation(ModifiersExtractor::<Response<B>>::extract(cached_route.route));
    } else {
        target.apply_mutation(ModifiersExtractor::<Response<B>>::extract(cached_route.route));
        target.apply_mutation(ModifiersExtractor::<Response<B>>::extract(cached_route.vh));
        target.apply_mutation(ModifiersExtractor::<Response<B>>::extract(route_config));
    }
}

impl Service<Request<Incoming>> for HttpRequestHandler {
    type Response = Response<OrionRequestBody>;
    type Error = crate::Error;
    type Future = BoxFuture<'static, StdResult<Self::Response, Self::Error>>;

    #[allow(clippy::too_many_lines)]
    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let incoming_request = req;
        // destructure the Request to get the request and addresses
        let incoming_request_id = RequestId::from_request(&incoming_request);
        let incoming_version = incoming_request.version();
        let metadata_context = incoming_request.extensions().get::<MetadataContext>();
        let stream_metrics = metadata_context.map(|md| Arc::clone(&md.stream_metrics));
        #[cfg(feature = "metrics")]
        let sni = metadata_context.and_then(|md| md.downstream.sni.clone());

        let access_log_enabled = {
            #[cfg(feature = "access-log")]
            {
                is_access_log_enabled()
            }
            #[cfg(not(feature = "access-log"))]
            false
        };

        // apply x_request_id policy...
        #[allow(unused_mut)]
        let (mut request, request_id) = self.manager.request_id_handler.apply_policy(
            incoming_request,
            access_log_enabled,
            incoming_request_id.as_ref(),
        );

        // create a trace context and SERVER span, if enabled...
        #[cfg(feature = "tracing")]
        let trace_context =
            self.manager.http_tracer.try_build_trace_context(&request, incoming_request_id.or(request_id.clone()));

        #[cfg(feature = "tracing")]
        let mut server_span = self.manager.http_tracer.try_create_span(
            trace_context.as_ref(),
            &self.manager.get_tracing_key(),
            SpanKind::Server,
            SpanName::Host(&request),
        );

        // set default attributes to span, using downstream request information...
        #[cfg(feature = "tracing")]
        if let Some(span) = server_span.as_mut() {
            set_attributes_from_request(span, &request);
        }

        // get user_partition_key, to be used with user metrics...
        //
        #[cfg(feature = "metrics")]
        let user_partition_key =
            metrics::extract_user_partition_key((request.headers(), sni.as_ref()), metrics::USER_KEY.source());

        #[cfg(not(feature = "metrics"))]
        let user_partition_key = None;

        // create the transaction context
        #[allow(clippy::let_unit_value)]
        let shard_id = get_shard_id!();

        let trans_handler = Arc::new(TransactionContext::new(
            request_id,
            user_partition_key,
            shard_id,
            #[cfg(feature = "access-log")]
            &self.manager.access_log,
            #[cfg(feature = "tracing")]
            trace_context,
            #[cfg(feature = "tracing")]
            server_span,
        ));

        // update tracing headers...
        #[cfg(feature = "tracing")]
        if let Some(trace_ctx) = trans_handler.trace_ctx.as_ref() {
            self.manager.http_tracer.update_tracing_headers(trace_ctx, &mut request);
        }

        let req_timeout = self.manager.request_timeout;
        let listener_name = self.manager.listener_name;
        #[allow(unused_variables)]
        let filterchain_id = self.manager.filterchain_id;
        let route_conf = self.router.borrow().clone();
        let manager = Arc::clone(&self.manager);

        with_metric!(
            http::DOWNSTREAM_RQ_TOTAL,
            add,
            1,
            trans_handler.shard_id(),
            &[KeyValue::new("listener", listener_name)]
        );
        with_metric!(
            http::DOWNSTREAM_RQ_ACTIVE,
            add,
            1,
            trans_handler.shard_id(),
            &[KeyValue::new("listener", listener_name)]
        );

        #[cfg(feature = "metrics")]
        let shard_id = get_shard_id!();

        defer! {
            with_metric!(http::DOWNSTREAM_RQ_ACTIVE, sub, 1, shard_id, &[KeyValue::new("listener", listener_name)]);
        }

        #[cfg(feature = "metrics")]
        if let Some(custom_metrics) = CUSTOM_METRICS.get() {
            let attr = metrics::extract_custom_partition_key(request.headers(), metrics::CUSTOM_KEY.source())
                .map(|id| KeyValue::new(metrics::CUSTOM_KEY.attribute_name().unwrap_or("custom"), id));
            custom_metrics.with_headers(MetricsHook::IncomingRequest, request.headers(), attr.as_slice());
        }

        Box::pin(async move {
            // optionally apply a timeout to the body.
            // envoy says this timeout is started when the request is initiated. This is relatively vague, but because at this point we will
            // already have the headers, it seems like a fair start.
            //  note that we can still time-out a request due to e.g. the filters taking a long time to compute, or the proxy being overwhelmed
            // not just due to the downstream being slow.
            // todo(hayley): this timeout is incorrect (checks for time between frames not total time), and doesn't seem to get converted into
            // http response

            //
            // evaluate InitHttpContext...

            let metadata = request.extensions().get::<MetadataContext>();
            eval_http_init_context(&request, &trans_handler, metadata.map(|md| &md.downstream));

            //
            // create the InstrumentedBody which will track the size of the request body

            #[cfg(feature = "access-log")]
            let (initial_flags, initial_event) = {
                let ec = request.extensions().get::<EventContext>();
                (ec.map(|ec| ec.response_flags).unwrap_or_default(), ec.and_then(|ec| ec.event_kind.clone()))
            };

            // check if route config is available....
            //

            let Some(route_conf) = route_conf else {
                return Ok(handle_route_conf_not_found(
                    request.version(),
                    &trans_handler,
                    stream_metrics.as_ref(),
                    listener_name,
                    user_partition_key,
                    filterchain_id,
                ));
            };

            // check if the request is valid....
            //

            if let Some(response_error) = reject_request_if_invalid(
                &request,
                &trans_handler,
                stream_metrics.as_ref(),
                listener_name,
                user_partition_key,
                filterchain_id,
            ) {
                return Ok(response_error);
            }

            let request = request.map(|body| {
                #[cfg(any(feature = "access-log", feature = "tracing", feature = "metrics"))]
                let trans_handler = Arc::clone(&trans_handler);
                let body = TimeoutBody::new(req_timeout, PolyBody::from(body));

                InstrumentedBody::new(
                    BodyKind::Request,
                    body,
                    stream_metrics.clone(),
                    #[allow(unused_variables)]
                    move |body_bytes, stream_metrics, body_error, body_flags| {
                        // emit the access log, if the transaction is completed..
                        #[cfg(any(feature = "access-log", feature = "metrics"))]
                        {
                            let mut trans_ctx = trans_handler.trans_ctx.lock();
                            #[allow(unused_variables)]
                            let duration = trans_handler.start_instant.elapsed();

                            #[cfg(feature = "access-log")]
                            with_access_log!(
                                &mut trans_ctx.loggers,
                                HttpRequestDurationContext { duration, tx_duration: duration }
                            );

                            if trans_handler.trans_phase.is_complete() {
                                #[allow(unused_variables)]
                                let ctx_bytes = trans_ctx.bytes;
                                #[allow(unused_variables)]
                                let ctx_flags = trans_ctx.flags;
                                #[allow(unused_variables)]
                                let ctx_event = trans_ctx.event.clone();

                                eval_http_finish_context(FinishContextParams {
                                    stream_metrics,
                                    listener_name,
                                    user_partition_key,
                                    filterchain_id,
                                    bytes_received: body_bytes,
                                    bytes_sent: ctx_bytes,
                                    trans_start_time: trans_handler.start_instant,
                                    #[cfg(feature = "metrics")]
                                    m_ctx: MetricsFinishContext { shard_id: trans_handler.shard_id() },
                                    #[cfg(feature = "access-log")]
                                    al_ctx: AccessLogFinishContext {
                                        event: EventInfo {
                                            body_kind: BodyKind::Request,
                                            event_kind: ctx_event.or(initial_event).or(body_error),
                                            response_flags: ctx_flags | initial_flags | body_flags,
                                        },
                                        access_loggers: trans_ctx.loggers.as_mut(),
                                    },
                                });
                            } else {
                                trans_ctx.bytes = body_bytes;
                                #[cfg(feature = "access-log")]
                                {
                                    trans_ctx.event = initial_event.or(body_error);
                                    trans_ctx.flags = initial_flags | body_flags;
                                }
                            }
                        }

                        #[cfg(feature = "tracing")]
                        if trans_handler.trans_phase.is_complete() {
                            if let Some(span) = trans_handler.span_state.as_ref() {
                                span.end();
                            }
                        }
                    },
                )
            });

            // proxy the request to the upstream...
            //

            let pipeline = TransactionPipeline { route_conf };
            let response = pipeline.to_response(Arc::clone(&trans_handler), manager, request).await;

            #[cfg(feature = "metrics")]
            if let Ok(response) = &response {
                if let Some(custom_metrics) = CUSTOM_METRICS.get() {
                    let attr = metrics::extract_custom_partition_key(response.headers(), metrics::CUSTOM_KEY.source())
                        .map(|id| KeyValue::new(metrics::CUSTOM_KEY.attribute_name().unwrap_or("custom"), id));
                    custom_metrics.with_headers(MetricsHook::DownstreamResponse, response.headers(), attr.as_slice());
                }
            }

            trans_handler.trace_status_code(&response, listener_name);
            if let Err(err) = response {
                error!("Error during handling HTTP transaction: {}", err);
                let msg = err.to_string();
                let response = SyntheticHttpResponse::internal_server_error(
                    EventKind::Upstream(err.into()),
                    ResponseFlags(FmtResponseFlags::LOCAL_RESET),
                    &msg,
                )
                .into_response(incoming_version);

                Ok(response.map(|body| InstrumentedBody::new(BodyKind::Response, body, None, |_, _, _, _| {})))
            } else {
                response
            }
        })
    }
}

#[allow(unused_variables)]
fn eval_http_init_context<R>(
    request: &Request<R>,
    trans_handler: &TransactionContext,
    metadata: Option<&DownstreamMetadata>,
) {
    #[cfg(feature = "tracing")]
    let trace_id =
        trans_handler.trace_ctx.as_ref().and_then(|t| t.map_child(orion_tracing::trace_info::TraceInfo::trace_id));
    #[cfg(not(feature = "tracing"))]
    let trace_id: Option<u128> = None;

    #[cfg(feature = "access-log")]
    {
        use crate::with_access_log;
        use orion_format::context::SocketAddrContext;

        let server_name = metadata.and_then(|md| md.sni.as_ref().map(SmolStr::as_str));

        #[cfg(feature = "access-log")]
        with_access_log!(
            &mut trans_handler.trans_ctx.lock().loggers,
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

    let log_fn: Box<dyn FnOnce(u64, u64) + Send> = Box::new(move |wire_bytes_received, wire_bytes_sent| {
        #[cfg(feature = "access-log")]
        use orion_format::context::WireContext;

        #[allow(unused_variables)]
        #[cfg(feature = "metrics")]
        let shard_id = params.m_ctx.shard_id;
        #[allow(unused_variables)]
        #[cfg(not(feature = "metrics"))]
        let shard_id = crate::get_shard_id!();

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
                user::BYTES_RX,
                add,
                wire_bytes_received,
                shard_id,
                &[
                    KeyValue::new(metrics::USER_KEY.attribute_name().unwrap_or("user"), user_partition_key),
                    KeyValue::new("listener", params.listener_name)
                ]
            );
            with_metric!(
                user::BYTES_TX,
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
            log_access_blocking(
                Target::ListenerFilterChain(params.listener_name.into(), params.filterchain_id),
                messages,
            );
        }
    });

    params.stream_metrics.on_flush(log_fn);
}

#[allow(clippy::too_many_arguments)]
#[allow(unused_variables)]
fn instrument_early_failure_response(
    response: Response<crate::OrionResponseBody>,
    trans_handler: &Arc<TransactionContext>,
    stream_metrics: Option<&Arc<crate::utils::instrumented_stream::StreamMetrics>>,
    listener_name: &'static str,
    user_partition_key: Option<&'static str>,
    filterchain_id: u64,
) -> Response<OrionRequestBody> {
    #[cfg(feature = "access-log")]
    let first_byte_instant = Instant::now();

    with_metric!(
        http::DOWNSTREAM_RQ_4XX,
        add,
        1,
        trans_handler.shard_id(),
        &[KeyValue::new("listener", listener_name)]
    );

    #[cfg(feature = "tracing")]
    if let Some(state) = trans_handler.span_state.as_ref() {
        if let Some(ref mut span) = *state.server_span.lock() {
            span.set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, 400));
        }
    }

    #[cfg(feature = "access-log")]
    with_access_log!(
        &mut trans_handler.trans_ctx.lock().loggers,
        DownstreamResponseContext { response: &response, response_head_size: response_head_size(&response) }
    );

    #[cfg(feature = "access-log")]
    let (initial_flags, initial_event) = {
        let ec = response.extensions().get::<EventContext>();
        (ec.map(|ec| ec.response_flags).unwrap_or_default(), ec.and_then(|ec| ec.event_kind.clone()))
    };

    #[cfg(any(feature = "access-log", feature = "tracing", feature = "metrics"))]
    let trans_handler = Arc::clone(trans_handler);

    response.map(|body| {
        InstrumentedBody::new(
            BodyKind::Response,
            body,
            stream_metrics.cloned(),
            #[allow(unused_variables)]
            move |body_bytes, stream_metrics, body_error, body_flags| {
                #[cfg(any(feature = "access-log", feature = "metrics"))]
                {
                    let mut log_ctx = trans_handler.trans_ctx.lock();

                    #[cfg(feature = "access-log")]
                    {
                        use crate::with_access_log;

                        #[allow(unused_variables)]
                        let duration = first_byte_instant.saturating_duration_since(trans_handler.start_instant);
                        #[allow(unused_variables)]
                        let tx_duration = Instant::now().saturating_duration_since(first_byte_instant);
                        with_access_log!(&mut log_ctx.loggers, HttpResponseDurationContext { duration, tx_duration })
                    }

                    if trans_handler.trans_phase.is_complete() {
                        #[allow(unused_variables)]
                        let ctx_bytes = log_ctx.bytes;
                        #[allow(unused_variables)]
                        let ctx_flags = log_ctx.flags;
                        #[allow(unused_variables)]
                        let ctx_event = log_ctx.event.clone();

                        eval_http_finish_context(FinishContextParams {
                            stream_metrics,
                            listener_name,
                            user_partition_key,
                            filterchain_id,
                            bytes_received: ctx_bytes,
                            bytes_sent: body_bytes,
                            trans_start_time: trans_handler.start_instant,
                            #[cfg(feature = "metrics")]
                            m_ctx: MetricsFinishContext { shard_id: trans_handler.shard_id() },
                            #[cfg(feature = "access-log")]
                            al_ctx: AccessLogFinishContext {
                                event: EventInfo {
                                    body_kind: BodyKind::Response,
                                    event_kind: ctx_event.or(initial_event).or(body_error),
                                    response_flags: ctx_flags | initial_flags | body_flags,
                                },
                                access_loggers: log_ctx.loggers.as_mut(),
                            },
                        });
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
                if trans_handler.trans_phase.is_complete() {
                    if let Some(span) = trans_handler.span_state.as_ref() {
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
    trans_handler: &Arc<TransactionContext>,
    stream_metrics: Option<&Arc<crate::utils::instrumented_stream::StreamMetrics>>,
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
            trans_handler,
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
    trans_handler: &Arc<TransactionContext>,
    stream_metrics: Option<&Arc<crate::utils::instrumented_stream::StreamMetrics>>,
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
        trans_handler,
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
}
