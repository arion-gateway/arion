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

mod direct_response;
mod ext_proc;
use ext_proc::ExternalProcessor;
use smallvec::SmallVec;
mod http_modifiers;
mod redirect;
mod route;
mod upgrades;

#[cfg(any(feature = "tracing", feature = "access-log"))]
use std::sync::atomic::AtomicUsize;

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
use crate::event_error::EventKind;
#[cfg(any(feature = "access-log", feature = "metrics"))]
use crate::utils::http::{request_head_size, response_head_size};

#[cfg(feature = "metrics")]
use orion_metrics::metrics::http;

#[cfg(feature = "access-log")]
use {
    crate::access_log::{
        is_access_log_enabled, log_access, log_access_reserve_balanced, ShareableAccessLogPermit, Target,
    },
    crate::event_error::UpstreamTransportEventError,
    crate::listeners::access_log::AccessLogContext,
    orion_configuration::config::network_filters::access_log::AccessLog,
    orion_format::context::{
        DownstreamResponse, FinishContext, HttpRequestDuration, HttpResponseDuration, InitHttpContext,
    },
    orion_format::LogFormatterLocal,
    parking_lot::Mutex,
    std::time::Instant,
};

use arc_swap::ArcSwap;
use core::time::Duration;
use futures::future::BoxFuture;
use hyper::{body::Incoming, service::Service, Request, Response};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::{
    FilterConfigOverride, FilterOverride,
};
use orion_configuration::config::network_filters::http_connection_manager::route::RouteMatch;
use orion_configuration::config::network_filters::http_connection_manager::{
    http_filters::{http_rbac::HttpRbac, HttpFilter as HttpFilterConfig, HttpFilterType},
    route::{Action, RouteMatchResult},
    CodecType, ConfigSource, ConfigSourceSpecifier, HttpConnectionManager as HttpConnectionManagerConfig, RdsSpecifier,
    RouteSpecifier, UpgradeType,
};
use orion_configuration::config::network_filters::http_connection_manager::{Route, VirtualHost, XffSettings};
use orion_configuration::config::network_filters::tracing::{TracingConfig, TracingKey};
use orion_configuration::config::GenericError;
use orion_format::types::ResponseFlags as FmtResponseFlags;
use route::MatchedRequest;
use scopeguard::defer;
use smol_str::SmolStr;
use std::collections::{HashMap, HashSet};
use std::thread::ThreadId;
use std::{fmt, future::Future, result::Result as StdResult, sync::Arc};
use tokio::sync::watch;
use tracing::debug;
use upgrades as upgrade_utils;

use crate::{
    body::{
        body_with_metrics::BodyWithMetrics,
        body_with_timeout::BodyWithTimeout,
        response_flags::{BodyKind, ResponseFlags},
    },
    event_error::EventFailure,
    listeners::{
        filter_state::DownstreamMetadata, rate_limiter::LocalRateLimit, synthetic_http_response::SyntheticHttpResponse,
    },
    with_client_span, with_metric, with_server_span, ConversionContext, PolyBody, Result, RouteConfiguration,
};

use orion_tracing::http_tracer::HttpTracer;
use orion_tracing::request_id::{RequestId, RequestIdManager};

#[derive(Debug, Clone)]
pub struct HttpConnectionManagerBuilder {
    listener_name: Option<&'static str>,
    filter_chain_match_hash: Option<u64>,
    connection_manager: PartialHttpConnectionManager,
}

impl TryFrom<ConversionContext<'_, HttpConnectionManagerConfig>> for HttpConnectionManagerBuilder {
    type Error = crate::Error;
    fn try_from(ctx: ConversionContext<HttpConnectionManagerConfig>) -> Result<Self> {
        let partial = PartialHttpConnectionManager::try_from(ctx)?;
        Ok(Self { listener_name: None, filter_chain_match_hash: None, connection_manager: partial })
    }
}

impl HttpConnectionManagerBuilder {
    pub fn build(self) -> Result<HttpConnectionManager> {
        let listener_name = self.listener_name.ok_or("listener name is not set")?;
        let filter_chain_match_hash = self.filter_chain_match_hash.unwrap_or(0);
        let partial = self.connection_manager;
        let router_sender = watch::Sender::new(partial.router.map(Arc::new));

        Ok(HttpConnectionManager {
            listener_name,
            filter_chain_match_hash,
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

    pub fn with_listener_name(self, name: &'static str) -> Self {
        HttpConnectionManagerBuilder { listener_name: Some(name), ..self }
    }

    pub fn with_filter_chain_match_hash(self, value: u64) -> Self {
        HttpConnectionManagerBuilder { filter_chain_match_hash: Some(value), ..self }
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

#[derive(Debug, Clone)]
pub struct HttpFilter {
    pub name: SmolStr,
    pub disabled: bool,
    pub filter: Option<HttpFilterValue>,
    pub base_config: Option<HttpFilterConfig>,
}

#[derive(Debug, Clone)]
pub enum HttpFilterValue {
    // todo(francesco): In this enum the RateLimit variant uses a runtime type
    // while Rbac uses a configuration type - we might want to revisit this
    RateLimit(LocalRateLimit),
    Rbac(HttpRbac),
    ExternalProcessor(ExternalProcessor),
}

impl From<HttpFilterConfig> for HttpFilter {
    fn from(value: HttpFilterConfig) -> Self {
        let hcm_config = match &value.filter {
            HttpFilterType::ExternalProcessor(_) => Some(value.clone()),
            _ => None,
        };

        let HttpFilterConfig { name, disabled, filter } = value;

        let filter = match filter {
            HttpFilterType::RateLimit(r) => HttpFilterValue::RateLimit(r.into()),
            HttpFilterType::Rbac(rbac) => HttpFilterValue::Rbac(rbac),
            HttpFilterType::ExternalProcessor(ext_proc) => HttpFilterValue::ExternalProcessor(ext_proc.into()),
        };
        Self { name, disabled, filter: Some(filter), base_config: hcm_config }
    }
}

impl HttpFilterValue {
    pub async fn apply_request(&mut self, request: &mut Request<BodyWithMetrics<PolyBody>>) -> FilterDecision {
        match self {
            HttpFilterValue::Rbac(rbac) => apply_authorization_rules(rbac, request),
            HttpFilterValue::RateLimit(rl) => rl.run(request),
            HttpFilterValue::ExternalProcessor(ext_proc) => ext_proc.apply_request(request).await,
        }
    }
    pub async fn apply_response(&mut self, response: &mut Response<PolyBody>) -> FilterDecision {
        match self {
            // RBAC and RateLimit do not apply on the response path
            HttpFilterValue::Rbac(_) | HttpFilterValue::RateLimit(_) => FilterDecision::Continue,
            HttpFilterValue::ExternalProcessor(ext_proc) => ext_proc.apply_response(response).await,
        }
    }
    fn from_filter_override(value: &FilterOverride, base_config: Option<&HttpFilterConfig>) -> Option<Self> {
        match &value.filter_settings {
            Some(filter_settings) => match filter_settings {
                FilterConfigOverride::LocalRateLimit(rl) => Some(HttpFilterValue::RateLimit((*rl).into())),
                FilterConfigOverride::Rbac(Some(rbac)) => Some(HttpFilterValue::Rbac(rbac.clone())),
                FilterConfigOverride::Rbac(None) => None,
                FilterConfigOverride::ExternalProcessor(ext_proc_per_route) => {
                    if let Some(HttpFilterConfig { filter: HttpFilterType::ExternalProcessor(base_config), .. }) =
                        base_config
                    {
                        let filter_value = HttpFilterValue::ExternalProcessor(
                            (base_config.clone(), Some(ext_proc_per_route.clone())).into(),
                        );
                        Some(filter_value)
                    } else {
                        None
                    }
                },
            },
            None => None,
        }
    }
}

fn per_route_http_filters(
    route_config: &RouteConfiguration,
    hcm_filters: &[Arc<HttpFilter>],
) -> HashMap<RouteMatch, Vec<Arc<HttpFilter>>> {
    let mut per_route_filters: HashMap<RouteMatch, Vec<Arc<HttpFilter>>> = HashMap::new();
    for vh in &route_config.virtual_hosts {
        for route in &vh.routes {
            for hcm_filter in hcm_filters {
                let effective_filter = match route.typed_per_filter_config.get(&hcm_filter.name) {
                    Some(override_config) => Arc::new(HttpFilter {
                        name: hcm_filter.name.clone(),
                        disabled: override_config.disabled,
                        filter: HttpFilterValue::from_filter_override(override_config, hcm_filter.base_config.as_ref()),
                        base_config: hcm_filter.base_config.clone(),
                    }),
                    None => Arc::clone(hcm_filter),
                };
                per_route_filters.entry(route.route_match.clone()).or_default().push(effective_filter);
            }
        }
    }
    per_route_filters
}

impl TryFrom<ConversionContext<'_, HttpConnectionManagerConfig>> for PartialHttpConnectionManager {
    type Error = crate::Error;
    fn try_from(ctx: ConversionContext<HttpConnectionManagerConfig>) -> Result<Self> {
        let ConversionContext { envoy_object: configuration, secret_manager: _ } = ctx;
        let codec_type = configuration.codec_type;
        let enabled_upgrades = configuration.enabled_upgrades;
        let http_filters_hcm = configuration
            .http_filters
            .into_iter()
            .map(|f| Arc::new(HttpFilter::from(f)))
            .collect::<Vec<Arc<HttpFilter>>>();
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
    pub filter_chain_match_hash: u64,
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
        write!(f, "HttpConnectionManager {}", &self.listener_name,)
    }
}

impl HttpConnectionManager {
    #[inline]
    pub fn get_tracing_key(&self) -> TracingKey {
        TracingKey(self.listener_name, self.filter_chain_match_hash)
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

    pub(crate) fn request_handler(
        self: &Arc<Self>,
    ) -> Box<
        dyn Service<
                ExtendedRequest<Incoming>,
                Response = Response<BodyWithMetrics<PolyBody>>,
                Error = crate::Error,
                Future = BoxFuture<'static, StdResult<Response<BodyWithMetrics<PolyBody>>, crate::Error>>,
            > + Send
            + Sync,
    > {
        Box::new(HttpRequestHandler { manager: Arc::clone(self), router: self.router_sender.subscribe() })
            as Box<
                dyn Service<
                        ExtendedRequest<Incoming>,
                        Response = Response<BodyWithMetrics<PolyBody>>,
                        Error = crate::Error,
                        Future = BoxFuture<'static, StdResult<Response<BodyWithMetrics<PolyBody>>, crate::Error>>,
                    > + Send
                    + Sync,
            >
    }
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub enum FilterDecision {
    #[default]
    Continue,
    Reroute,
    DirectResponse(Response<PolyBody>),
}

pub struct CachedRoute<'a> {
    route: &'a Route,
    route_match: RouteMatchResult,
    vh: &'a VirtualHost,
}

pub(crate) struct HttpRequestHandler {
    manager: Arc<HttpConnectionManager>,
    router: watch::Receiver<Option<Arc<RouteConfiguration>>>,
}

pub struct ExtendedRequest<B> {
    pub request: Request<B>,
    pub downstream_metadata: Arc<DownstreamMetadata>,
}

#[cfg(feature = "access-log")]
#[derive(Debug)]
pub struct AccessLoggersContext {
    bytes: u64, // either the request or response body size, depending which one has completed first
    flags: ResponseFlags,
    event: Option<EventKind>,
    loggers: Vec<LogFormatterLocal>,
}

#[cfg(feature = "access-log")]
impl AccessLoggersContext {
    pub fn new(access_log: &[AccessLog]) -> Self {
        AccessLoggersContext {
            loggers: access_log.iter().map(|al| al.logger.local_clone()).collect::<Vec<_>>(),
            bytes: 0,
            flags: ResponseFlags::default(),
            event: None,
        }
    }
}

#[derive(Debug)]
pub struct TransactionHandler {
    #[allow(dead_code)]
    start_instant: std::time::Instant,
    request_id: Option<RequestId>,
    thread_id: ThreadId,
    #[cfg(feature = "access-log")]
    access_log_ctx: Option<Mutex<AccessLoggersContext>>,
    #[cfg(feature = "tracing")]
    trace_ctx: Option<TraceContext>,
    #[cfg(feature = "tracing")]
    span_state: Option<Arc<SpanState>>,
    #[cfg(any(feature = "access-log", feature = "tracing"))]
    trans_state: TransactionPhases,
}

#[derive(Debug)]
#[cfg(any(feature = "access-log", feature = "tracing"))]
struct TransactionPhases {
    phase: AtomicUsize,
}

#[cfg(any(feature = "access-log", feature = "tracing"))]
impl TransactionPhases {
    fn new() -> Self {
        TransactionPhases { phase: AtomicUsize::new(0) }
    }

    fn message_complete(&self) -> TransactionComplete {
        TransactionComplete(self.phase.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0)
    }
}

#[cfg(any(feature = "access-log", feature = "tracing"))]
struct TransactionComplete(bool);

impl Default for TransactionHandler {
    fn default() -> Self {
        TransactionHandler {
            start_instant: std::time::Instant::now(),
            request_id: None,
            thread_id: std::thread::current().id(),
            #[cfg(feature = "access-log")]
            access_log_ctx: None,
            #[cfg(feature = "tracing")]
            trace_ctx: None,
            #[cfg(feature = "tracing")]
            span_state: None,
            #[cfg(any(feature = "access-log", feature = "tracing"))]
            trans_state: TransactionPhases::new(),
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

impl TransactionHandler {
    pub fn new(
        request_id: Option<RequestId>,
        thread_id: ThreadId,
        #[cfg(feature = "access-log")] access_log: &[AccessLog],
        #[cfg(feature = "tracing")] trace_ctx: Option<TraceContext>,
        #[cfg(feature = "tracing")] server_span: Option<BoxedSpan>,
    ) -> Self {
        TransactionHandler {
            start_instant: std::time::Instant::now(),
            request_id,
            #[cfg(feature = "access-log")]
            access_log_ctx: is_access_log_enabled().then(|| Mutex::new(AccessLoggersContext::new(access_log))),
            #[cfg(feature = "tracing")]
            trace_ctx,
            #[cfg(feature = "tracing")]
            span_state: server_span.map(|span| Arc::new(SpanState::new(Some(span)))),
            thread_id,
            #[cfg(any(feature = "access-log", feature = "tracing"))]
            trans_state: TransactionPhases::new(),
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_transaction<RC>(
        self: Arc<Self>,
        route_conf: RC,
        manager: Arc<HttpConnectionManager>,
        mut request: Request<BodyWithMetrics<BodyWithTimeout<Incoming>>>,
        downstream_metadata: Arc<DownstreamMetadata>,
        #[cfg(feature = "access-log")] permit: Option<ShareableAccessLogPermit>,
    ) -> Result<Response<BodyWithMetrics<PolyBody>>>
    where
        RC: RequestHandler<(
                Request<BodyWithMetrics<BodyWithTimeout<Incoming>>>,
                Arc<HttpConnectionManager>,
                Arc<DownstreamMetadata>,
            )> + Clone,
    {
        let _listener_name = manager.listener_name;

        // apply the request header modifiers
        http_modifiers::apply_prerouting_functions(
            &mut request,
            downstream_metadata.connection.peer_address(),
            manager.xff_settings,
        );

        // process request, get the response..
        let result = route_conf.to_response(&self, (request, manager.clone(), downstream_metadata.clone())).await;

        // calculate the time to first byte..
        #[cfg(feature = "access-log")]
        let first_byte_instant = Instant::now();

        result.map(|mut response| {
            // set the request id on the response...
            manager
                .request_id_handler
                .apply_to(&mut response, self.request_id.as_ref().and_then(|x| x.propagate_ref()));

            #[cfg(feature = "access-log")]
            let initial_flags = response.extensions().get::<ResponseFlags>().cloned().unwrap_or_default();
            #[cfg(feature = "access-log")]
            let initial_event = response.extensions().get::<Option<EventKind>>().cloned().unwrap_or_default();
            #[cfg(feature = "access-log")]
            if let Some(ctx) = self.access_log_ctx.as_ref() {
                let response_head_size = response_head_size(&response);
                ctx.lock().loggers.with_context(&DownstreamResponse { response: &response, response_head_size })
            }

            #[cfg(feature = "metrics")]
            let resp_head_size = response_head_size(&response);

            response.map(move |body| {
                BodyWithMetrics::new(BodyKind::Response, body, move |_nbytes, _body_error, _body_flags| {
                    with_metric!(
                        http::DOWNSTREAM_CX_TX_BYTES_TOTAL,
                        add,
                        _nbytes + resp_head_size as u64,
                        self.thread_id(),
                        &[KeyValue::new("listener", _listener_name)]
                    );

                    #[cfg(feature = "access-log")]
                    let _is_transaction_complete = if let Some(ctx) = self.access_log_ctx.as_ref() {
                        let mut log_ctx = ctx.lock();
                        let duration = first_byte_instant.saturating_duration_since(self.start_instant);
                        let tx_duration = Instant::now().saturating_duration_since(first_byte_instant);
                        log_ctx.loggers.with_context(&HttpResponseDuration { duration, tx_duration });

                        let is_transaction_complete = self.trans_state.message_complete();
                        if is_transaction_complete.0 {
                            let ctx_bytes = log_ctx.bytes;
                            let ctx_flags = log_ctx.flags.clone();
                            let ctx_event = log_ctx.event.clone();
                            eval_http_finish_context(
                                ctx_bytes, // bytes received
                                _nbytes,   // bytes sent
                                _listener_name,
                                EventInfo {
                                    body_kind: BodyKind::Response,
                                    event_kind: ctx_event.or(initial_event).or(_body_error.map(EventKind::Error)),
                                    response_flags: ctx_flags | initial_flags | _body_flags,
                                },
                                permit,
                                log_ctx.loggers.as_mut(),
                                self.start_instant,
                            );
                        } else {
                            log_ctx.bytes = _nbytes;
                            log_ctx.flags = initial_flags | _body_flags;
                            log_ctx.event = initial_event.or(_body_error.map(EventKind::Error));
                        }
                        is_transaction_complete
                    } else {
                        self.trans_state.message_complete()
                    };

                    #[cfg(all(not(feature = "access-log"), feature = "tracing"))]
                    let _is_transaction_complete = self.trans_state.message_complete();

                    #[cfg(feature = "tracing")]
                    if _is_transaction_complete.0 {
                        if let Some(span) = self.span_state.as_ref() {
                            span.end();
                        }
                    }
                })
            })
        })
    }

    fn trace_status_code(
        self: Arc<Self>,
        res: Result<Response<BodyWithMetrics<PolyBody>>>,
        _listener_name: &'static str,
    ) -> Result<Response<BodyWithMetrics<PolyBody>>> {
        if let Ok(response) = &res {
            let status_code = response.status().as_u16();

            with_server_span!(self.span_state, |srv_span: &mut BoxedSpan| srv_span
                .set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, i64::from(status_code))));

            match status_code {
                100..200 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_1XX,
                        add,
                        1,
                        self.thread_id(),
                        &[KeyValue::new("listener", _listener_name)]
                    );
                },
                200..300 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_2XX,
                        add,
                        1,
                        self.thread_id(),
                        &[KeyValue::new("listener", _listener_name)]
                    );
                },
                300..400 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_3XX,
                        add,
                        1,
                        self.thread_id(),
                        &[KeyValue::new("listener", _listener_name)]
                    );
                },
                400..500 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_4XX,
                        add,
                        1,
                        self.thread_id(),
                        &[KeyValue::new("listener", _listener_name)]
                    );
                },
                500..600 => {
                    with_metric!(
                        http::DOWNSTREAM_RQ_5XX,
                        add,
                        1,
                        self.thread_id(),
                        &[KeyValue::new("listener", _listener_name)]
                    );

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
            with_metric!(
                http::DOWNSTREAM_RQ_5XX,
                add,
                1,
                self.thread_id(),
                &[KeyValue::new("listener", _listener_name)]
            );

            with_server_span!(self.span_state, |srv_span: &mut BoxedSpan| {
                srv_span.set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, 500));
                srv_span.set_status(Status::error("5xx"));
            });

            with_client_span!(self.span_state, |clt_span: &mut BoxedSpan| {
                clt_span.set_status(Status::error("5xx"));
            });
        }
        res
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
pub trait RequestHandler<R>: Sized {
    fn to_response(
        self,
        trans_handler: &TransactionHandler,
        request: R,
    ) -> impl Future<Output = Result<Response<PolyBody>>> + Send;
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

impl
    RequestHandler<(
        Request<BodyWithMetrics<BodyWithTimeout<Incoming>>>,
        Arc<HttpConnectionManager>,
        Arc<DownstreamMetadata>,
    )> for Arc<RouteConfiguration>
{
    #[allow(clippy::too_many_lines)]
    async fn to_response(
        self,
        trans_handler: &TransactionHandler,
        (request, connection_manager, downstream_metadata): (
            Request<BodyWithMetrics<BodyWithTimeout<Incoming>>>,
            Arc<HttpConnectionManager>,
            Arc<DownstreamMetadata>,
        ),
    ) -> Result<Response<PolyBody>> {
        let mut processed_routes: HashSet<&RouteMatch> = HashSet::new();
        let mut cached_route = match_request_route(&request, &self);
        let mut request: Request<BodyWithMetrics<PolyBody>> = request.map(BodyWithMetrics::map_into::<PolyBody>);
        let mut active_filters: SmallVec<[HttpFilterValue; 2]> = SmallVec::new();
        loop {
            if let Some(ref chosen_route) = cached_route {
                if processed_routes.contains(&chosen_route.route.route_match) {
                    // we are in routing loop, processing the same route twice is not permitted
                    return Err(GenericError::from_msg("Routing loop detected").into());
                }

                let guard = connection_manager.http_filters_per_route.load();
                let route_filters = guard.get(&chosen_route.route.route_match);
                if let Some(route_filters) = route_filters {
                    let mut is_reroute = false;
                    for filter in route_filters {
                        if filter.disabled {
                            continue;
                        }
                        if let Some(filter_value) = &filter.filter {
                            let mut filter_value = filter_value.clone();
                            let filter_res = filter_value.apply_request(&mut request).await;
                            active_filters.push(filter_value);
                            if matches!(filter_res, FilterDecision::Reroute) {
                                // stop processing filters and re-evaluate the route
                                is_reroute = true;
                                break;
                            }
                            if let FilterDecision::DirectResponse(response) = filter_res {
                                return Ok(response);
                            }
                        }
                    }
                    if !is_reroute {
                        break;
                    }
                    debug!("rerouting enabled; active_filters dropped!");
                    active_filters.clear();
                    processed_routes.insert(&chosen_route.route.route_match);
                    cached_route = match_request_route(&request, &self);
                } else {
                    // there are no filters to process
                    break;
                }
            } else {
                return Ok(SyntheticHttpResponse::not_found(
                    EventFailure::RouteNotFound.into(),
                    ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
                )
                .into_response(request.version()));
            }
        }

        if let Some(chosen_route) = cached_route {
            let websocket_enabled_by_default =
                upgrade_utils::is_websocket_enabled_by_hcm(&connection_manager.enabled_upgrades);

            let mut response = match &chosen_route.route.action {
                Action::DirectResponse(dr) => dr.to_response(trans_handler, (request, &chosen_route.route.name)).await,
                Action::Redirect(rd) => {
                    rd.to_response(trans_handler, (request, chosen_route.route_match, &chosen_route.route.name)).await
                },
                Action::Route(route) => {
                    route
                        .to_response(
                            trans_handler,
                            (
                                MatchedRequest {
                                    request,
                                    route_name: &chosen_route.route.name,
                                    retry_policy: chosen_route.vh.retry_policy.as_ref(),
                                    route_match: chosen_route.route_match,
                                    remote_address: downstream_metadata.connection.peer_address(),
                                    websocket_enabled_by_default,
                                },
                                &connection_manager,
                            ),
                        )
                        .await
                },
            }?;

            // Process filters on response...
            //
            for filter in active_filters.iter_mut() {
                let filter_res = filter.apply_response(&mut response).await;
                if let FilterDecision::DirectResponse(direct_response) = filter_res {
                    response = direct_response;
                    break;
                }
            }

            // Extend the lifetime of active_filters to the response.
            // This is needed for filters to survive until the Response is completely streamed.
            //
            if !active_filters.is_empty() {
                response.extensions_mut().insert(active_filters);
            }

            let resp_headers = response.headers_mut();
            if self.most_specific_header_mutations_wins {
                self.response_header_modifier.modify(resp_headers);
                chosen_route.vh.response_header_modifier.modify(resp_headers);
                chosen_route.route.response_header_modifier.modify(resp_headers);
            } else {
                chosen_route.route.response_header_modifier.modify(resp_headers);
                chosen_route.vh.response_header_modifier.modify(resp_headers);
                self.response_header_modifier.modify(resp_headers);
            }

            Ok(response)
        } else {
            // We should not be here
            Ok(SyntheticHttpResponse::not_found(
                EventFailure::RouteNotFound.into(),
                ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
            )
            .into_response(request.version()))
        }
    }
}

impl Service<ExtendedRequest<Incoming>> for HttpRequestHandler {
    type Response = Response<BodyWithMetrics<PolyBody>>;
    type Error = crate::Error;
    type Future = BoxFuture<'static, StdResult<Self::Response, Self::Error>>;

    #[allow(clippy::too_many_lines)]
    fn call(&self, req: ExtendedRequest<Incoming>) -> Self::Future {
        // destructure the ExtendedRequest to get the request and addresses
        let ExtendedRequest { request, downstream_metadata } = req;
        let incoming_request_id = RequestId::from_request(&request);

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
        let (mut updated_request, request_id) =
            self.manager.request_id_handler.apply_policy(request, access_log_enabled, incoming_request_id.as_ref());

        // create a trace context and SERVER span, if enabled...
        #[cfg(feature = "tracing")]
        let trace_context = self
            .manager
            .http_tracer
            .try_build_trace_context(&updated_request, incoming_request_id.or(request_id.clone()));

        #[cfg(feature = "tracing")]
        let mut server_span = self.manager.http_tracer.try_create_span(
            trace_context.as_ref(),
            &self.manager.get_tracing_key(),
            SpanKind::Server,
            SpanName::Host(&updated_request),
        );

        // set default attributes to span, using downstream request information...
        #[cfg(feature = "tracing")]
        if let Some(span) = server_span.as_mut() {
            set_attributes_from_request(span, &updated_request);
        }

        // create the transaction context
        let trans_handler = Arc::new(TransactionHandler::new(
            request_id,
            std::thread::current().id(),
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
            self.manager.http_tracer.update_tracing_headers(trace_ctx, &mut updated_request);
        }

        // update the incoming request...
        let req = ExtendedRequest { request: updated_request, downstream_metadata };

        let req_timeout = self.manager.request_timeout;
        let listener_name = self.manager.listener_name;
        let route_conf = self.router.borrow().clone();
        let manager = Arc::clone(&self.manager);

        with_metric!(
            http::DOWNSTREAM_RQ_TOTAL,
            add,
            1,
            trans_handler.thread_id(),
            &[KeyValue::new("listener", listener_name)]
        );
        with_metric!(
            http::DOWNSTREAM_RQ_ACTIVE,
            add,
            1,
            trans_handler.thread_id(),
            &[KeyValue::new("listener", listener_name)]
        );

        #[cfg(feature = "metrics")]
        let thread_id = trans_handler.thread_id();
        defer! {
            with_metric!(http::DOWNSTREAM_RQ_ACTIVE, sub, 1, thread_id, &[KeyValue::new("listener", listener_name)]);
        }

        Box::pin(async move {
            let ExtendedRequest { request, downstream_metadata } = req;
            let (parts, body) = request.into_parts();
            let request = Request::from_parts(parts, BodyWithTimeout::new(req_timeout, body));

            #[cfg(feature = "access-log")]
            #[allow(clippy::if_then_some_else_none)] // avoid clippy false positive
            let permit: Option<ShareableAccessLogPermit> = {
                if is_access_log_enabled() {
                    Some(log_access_reserve_balanced().await)
                } else {
                    None
                }
            };

            #[cfg(feature = "access-log")]
            let permit_clone = permit.as_ref().map(Arc::clone);

            // optionally apply a timeout to the body.
            // envoy says this timeout is started when the request is initiated. This is relatively vague, but because at this point we will
            // already have the headers, it seems like a fair start.
            //  note that we can still time-out a request due to e.g. the filters taking a long time to compute, or the proxy being overwhelmed
            // not just due to the downstream being slow.
            // todo(hayley): this timeout is incorrect (checks for time between frames not total time), and doesn't seem to get converted into
            //  http response

            //
            // evaluate InitHttpContext...
            eval_http_init_context(&request, &trans_handler, downstream_metadata.server_name.as_deref());

            //
            // create the BodyWithMetrics which will track the size of the request body

            #[cfg(feature = "access-log")]
            let initial_flags = request.extensions().get::<ResponseFlags>().cloned().unwrap_or_default();
            #[cfg(feature = "access-log")]
            let initial_event = request.extensions().get::<Option<EventKind>>().cloned().unwrap_or_default();
            #[cfg(feature = "metrics")]
            let req_head_size = request_head_size(&request);

            let request = request.map(|body| {
                #[cfg(any(feature = "access-log", feature = "tracing", feature = "metrics"))]
                let trans_handler = Arc::clone(&trans_handler);

                BodyWithMetrics::new(BodyKind::Request, body, move |_nbytes, _body_error, _body_flags| {
                    with_metric!(
                        http::DOWNSTREAM_CX_RX_BYTES_TOTAL,
                        add,
                        _nbytes + req_head_size as u64,
                        trans_handler.thread_id(),
                        &[KeyValue::new("listener", listener_name)]
                    );

                    // emit the access log, if the transaction is completed..
                    #[cfg(feature = "access-log")]
                    let _is_transaction_complete = if let Some(ctx) = trans_handler.access_log_ctx.as_ref() {
                        let mut log_ctx = ctx.lock();
                        let duration = trans_handler.start_instant.elapsed();
                        log_ctx.loggers.with_context(&HttpRequestDuration { duration, tx_duration: duration });

                        let is_transaction_complete = trans_handler.trans_state.message_complete();
                        if is_transaction_complete.0 {
                            let ctx_bytes = log_ctx.bytes;
                            let ctx_flags = log_ctx.flags.clone();
                            let ctx_event = log_ctx.event.clone();

                            // if this happens is because the stream of body response finished before the request one!
                            eval_http_finish_context(
                                _nbytes,   // bytes received
                                ctx_bytes, // bytes sent
                                listener_name,
                                EventInfo {
                                    body_kind: BodyKind::Request,
                                    event_kind: ctx_event.or(initial_event).or(_body_error.map(EventKind::Error)),
                                    response_flags: ctx_flags | initial_flags | _body_flags,
                                },
                                permit_clone,
                                log_ctx.loggers.as_mut(),
                                trans_handler.start_instant,
                            );
                        } else {
                            log_ctx.bytes = _nbytes;
                            log_ctx.flags = initial_flags | _body_flags;
                            log_ctx.event = initial_event.or(_body_error.map(EventKind::Error));
                        }

                        is_transaction_complete
                    } else {
                        trans_handler.trans_state.message_complete()
                    };
                    #[cfg(all(not(feature = "access-log"), feature = "tracing"))]
                    let _is_transaction_complete = trans_handler.trans_state.message_complete();

                    #[cfg(feature = "tracing")]
                    if _is_transaction_complete.0 {
                        if let Some(span) = trans_handler.span_state.as_ref() {
                            span.end();
                        }
                    }
                })
            });

            let Some(route_conf) = route_conf else {
                // immediately return a SyntheticHttpResponse, and calcuate the first byte instant
                let resp = SyntheticHttpResponse::not_found(
                    EventFailure::RouteNotFound.into(),
                    ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
                )
                .into_response(request.version());

                #[cfg(feature = "access-log")]
                let first_byte_instant = Instant::now();

                with_metric!(
                    http::DOWNSTREAM_RQ_4XX,
                    add,
                    1,
                    trans_handler.thread_id(),
                    &[KeyValue::new("listener", listener_name)]
                );

                #[cfg(feature = "tracing")]
                if let Some(state) = trans_handler.span_state.as_ref() {
                    if let Some(ref mut span) = *state.server_span.lock() {
                        span.set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, 400));
                    }
                }

                #[cfg(feature = "access-log")]
                if let Some(log_ctx) = trans_handler.access_log_ctx.as_ref() {
                    let response_head_size = response_head_size(&resp);
                    log_ctx.lock().loggers.with_context(&DownstreamResponse { response: &resp, response_head_size })
                }

                #[cfg(feature = "access-log")]
                let initial_flags = resp.extensions().get::<ResponseFlags>().cloned().unwrap_or_default();
                #[cfg(feature = "access-log")]
                let initial_event = resp.extensions().get::<Option<EventKind>>().cloned().unwrap_or_default();
                #[cfg(feature = "metrics")]
                let resp_head_size = response_head_size(&resp);

                let response = resp.map(|body| {
                    BodyWithMetrics::new(BodyKind::Response, body, move |_nbytes, _body_error, _body_flags| {
                        with_metric!(
                            http::DOWNSTREAM_CX_TX_BYTES_TOTAL,
                            add,
                            _nbytes + resp_head_size as u64,
                            trans_handler.thread_id(),
                            &[KeyValue::new("listener", listener_name)]
                        );

                        #[cfg(feature = "access-log")]
                        let _is_transaction_complete = if let Some(ctx) = trans_handler.access_log_ctx.as_ref() {
                            let mut log_ctx = ctx.lock();
                            let duration = first_byte_instant.saturating_duration_since(trans_handler.start_instant);
                            let tx_duration = Instant::now().saturating_duration_since(first_byte_instant);
                            log_ctx.loggers.with_context(&HttpResponseDuration { duration, tx_duration });

                            let is_transaction_complete = trans_handler.trans_state.message_complete();
                            if is_transaction_complete.0 {
                                let ctx_bytes = log_ctx.bytes;
                                let ctx_flags = log_ctx.flags.clone();
                                let ctx_event = log_ctx.event.clone();

                                eval_http_finish_context(
                                    ctx_bytes, // bytes received
                                    _nbytes,   // bytes sent
                                    listener_name,
                                    EventInfo {
                                        body_kind: BodyKind::Response,
                                        event_kind: ctx_event.or(initial_event).or(_body_error.map(EventKind::Error)),
                                        response_flags: ctx_flags | initial_flags | _body_flags,
                                    },
                                    permit,
                                    log_ctx.loggers.as_mut(),
                                    trans_handler.start_instant,
                                );
                            } else {
                                log_ctx.bytes = _nbytes;
                                log_ctx.flags = initial_flags | _body_flags;
                                log_ctx.event = initial_event.or(_body_error.map(EventKind::Error));
                            }

                            is_transaction_complete
                        } else {
                            trans_handler.trans_state.message_complete()
                        };
                        #[cfg(all(not(feature = "access-log"), feature = "tracing"))]
                        let _is_transaction_complete = trans_handler.trans_state.message_complete();

                        #[cfg(feature = "tracing")]
                        if _is_transaction_complete.0 {
                            if let Some(span) = trans_handler.span_state.as_ref() {
                                span.end();
                            }
                        }
                    })
                });
                return Ok(response);
            };

            let response = trans_handler
                .clone()
                .handle_transaction(
                    route_conf,
                    manager,
                    request,
                    downstream_metadata,
                    #[cfg(feature = "access-log")]
                    permit,
                )
                .await;

            trans_handler.trace_status_code(response, listener_name)
        })
    }
}

fn eval_http_init_context<R>(_request: &Request<R>, _trans_handler: &TransactionHandler, _server_name: Option<&str>) {
    #[cfg(feature = "tracing")]
    let _trace_id =
        _trans_handler.trace_ctx.as_ref().and_then(|t| t.map_child(orion_tracing::trace_info::TraceInfo::trace_id));
    #[cfg(not(feature = "tracing"))]
    let _trace_id: Option<u128> = None;

    #[cfg(feature = "access-log")]
    if let Some(ctx) = _trans_handler.access_log_ctx.as_ref() {
        let request_head_size = request_head_size(_request);
        ctx.lock().loggers.with_context_fn(|| InitHttpContext {
            start_time: std::time::SystemTime::now(),
            downstream_request: _request,
            request_head_size,
            trace_id: _trace_id,
            server_name: _server_name,
        })
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "access-log")]
fn eval_http_finish_context(
    _bytes_received: u64,
    _bytes_sent: u64,
    _listener_name: &'static str,
    #[cfg(feature = "access-log")] event: EventInfo,
    #[cfg(feature = "access-log")] permit: Option<ShareableAccessLogPermit>,
    #[cfg(feature = "access-log")] access_loggers: &mut Vec<LogFormatterLocal>,
    #[cfg(feature = "access-log")] trans_start_time: Instant,
) {
    #[cfg(feature = "access-log")]
    if let Some(permit) = permit {
        access_loggers.with_context(&FinishContext {
            duration: trans_start_time.elapsed(),
            bytes_received: _bytes_received,
            bytes_sent: _bytes_sent,
            response_flags: event.response_flags.0,
            upstream_failure: event.event_kind.as_ref().and_then(|ev| {
                let EventKind::Error(err) = ev else {
                    return None;
                };
                UpstreamTransportEventError::try_from(err).ok().map(|e| e.0)
            }),
            response_code_details: event
                .event_kind
                .as_ref()
                .map_or(EventKind::Failure(EventFailure::ViaUpstream).code_details(), EventKind::code_details)
                .map(|d| d.0),
            connection_termination_details: event
                .event_kind
                .as_ref()
                .and_then(EventKind::termination_details)
                .map(|d| d.0),
        });

        let loggers: Vec<LogFormatterLocal> = std::mem::take(access_loggers);
        let messages = loggers.into_iter().map(LogFormatterLocal::into_message).collect::<Vec<_>>();
        log_access(permit, Target::Listener(_listener_name.into()), messages);
    }
}

fn apply_authorization_rules<B>(rbac: &HttpRbac, req: &Request<B>) -> FilterDecision {
    debug!("Applying authorization rules {rbac:?} {:?}", &req.headers());
    if rbac.is_permitted(req) {
        FilterDecision::Continue
    } else {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::forbidden(EventFailure::RbacAccessDenied.into(), "RBAC: access denied")
                .into_response(req.version()),
        )
    }
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
