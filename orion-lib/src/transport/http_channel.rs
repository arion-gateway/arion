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

use super::connector::{ConnectUsing, UnifiedConnector};
use crate::{
    body::{instrumented_body::InstrumentedBody, response_flags::ResponseFlags, timeout_body::TimeoutBody},
    clusters::{
        decrement_retries, retry_policy::RetryCondition, try_increment_retries, CircuitBreakerDenial, RoutingPriority,
    },
    event_error::{EventKind, TryInferFrom, UpstreamError},
    instrument_block, instrument_function,
    listeners::{
        http_connection_manager::{http_modifiers::strip_trailers_headers, RequestCtx, RequestHandler},
        synthetic_http_response::SyntheticHttpResponse,
    },
    secrets::{TlsConfigurator, WantsToBuildClient},
    transport::http1_pool::Http1ClientExt,
    transport::http2_pool::Http2ClientExt,
    Error, OrionRequestBody, OrionResponseBody, Result, UpstreamCallOpts,
};
use http::{
    uri::{Authority, Parts},
    HeaderValue, Response, Version,
};
use http_body_util::BodyExt;
use hyper::{Request, Uri};
use hyper_rustls::FixedServerNameResolver;
use orion_configuration::config::{
    cluster::http_protocol_options::{Codec, HttpProtocolOptions},
    network_filters::http_connection_manager::RetryPolicy,
};
use orion_format::types::{ResponseFlagsLong, ResponseFlagsShort};
#[cfg(feature = "metrics")]
use orion_metrics::metrics::custom::CUSTOM_METRICS;

use crate::with_metric;

#[cfg(feature = "metrics")]
use {crate::get_shard_id, opentelemetry::KeyValue, orion_metrics::metrics::clusters};

use pingora_timeout::fast_timeout::fast_timeout;
use pretty_duration::pretty_duration;
use rustls::ClientConfig;
#[cfg(feature = "metrics")]
use smallvec::SmallVec;
use smol_str::ToSmolStr;
use std::{future::Future, io, mem, time::Duration};
use tracing::debug;
use webpki::types::ServerName;

#[cfg(feature = "metrics")]
use scopeguard::defer;
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[must_use = "dropping the permit releases the retry circuit-breaker slot"]
struct RetryCircuitBreakerPermit {
    cluster_name: &'static str,
    priority: RoutingPriority,
}

impl RetryCircuitBreakerPermit {
    fn try_acquire(
        cluster_name: &'static str,
        priority: RoutingPriority,
    ) -> std::result::Result<Self, CircuitBreakerDenial> {
        try_increment_retries(cluster_name, priority)?;
        Ok(Self { cluster_name, priority })
    }
}

impl Drop for RetryCircuitBreakerPermit {
    fn drop(&mut self) {
        decrement_retries(self.cluster_name, self.priority);
    }
}

#[derive(Clone, Debug)]
pub enum HttpChannels {
    Single(HttpChannel),
    MultiWithFailover { channel: HttpChannel, failover_channels: Vec<HttpChannel> },
}

impl HttpChannels {
    pub fn channel(&self) -> &HttpChannel {
        match self {
            HttpChannels::Single(channel) | HttpChannels::MultiWithFailover { channel, .. } => channel,
        }
    }

    pub fn upstream_authority(&self) -> &Authority {
        &self.channel().upstream_authority
    }

    pub fn cluster_name(&self) -> &'static str {
        self.channel().cluster_name
    }

    pub fn http_version(&self) -> Codec {
        self.channel().http_version
    }
}

#[derive(Clone, Debug)]
pub struct HttpChannel {
    pub channel_client: HttpChannelClient,
    pub http_version: Codec,
    pub enable_trailers: bool,
    pub upstream_authority: Authority, // upstream authority
    pub cluster_name: &'static str,
}

#[derive(Clone, Debug)]
pub enum HttpChannelClient {
    Plain(PlainChannelClient),
    Tls(TlsChannelClient),
}

#[derive(Clone, Debug)]
pub enum PlainChannelClient {
    Http1(Http1ClientExt),
    Http2(Http2ClientExt),
}

#[derive(Clone, Debug)]
pub enum TlsChannelClient {
    Http1(Http1ClientExt),
    Http2(Http2ClientExt),
}

impl HttpChannelClient {
    #[inline]
    pub fn is_tls(&self) -> bool {
        matches!(self, HttpChannelClient::Tls(_))
    }
}

pub struct HttpChannelBuilder {
    connect_using: ConnectUsing,
    tls: Option<TlsConfigurator<ClientConfig, WantsToBuildClient>>,
    server_name: Option<ServerName<'static>>,
    http_protocol_options: HttpProtocolOptions,
    cluster_name: Option<&'static str>,
}

impl HttpChannelBuilder {
    pub fn new(connect_using: ConnectUsing) -> Self {
        Self {
            connect_using,
            tls: None,
            server_name: None,
            http_protocol_options: HttpProtocolOptions::default(),
            cluster_name: None,
        }
    }

    pub fn with_tls(self, tls_configurator: Option<TlsConfigurator<ClientConfig, WantsToBuildClient>>) -> Self {
        Self { tls: tls_configurator, ..self }
    }

    pub fn with_cluster_name(self, cluster_name: &'static str) -> Self {
        Self { cluster_name: Some(cluster_name), ..self }
    }

    pub fn with_server_name(self, server_name: ServerName<'static>) -> Self {
        Self { server_name: Some(server_name), ..self }
    }

    pub fn with_http_protocol_options(self, http_protocol_options: HttpProtocolOptions) -> Self {
        Self { http_protocol_options, ..self }
    }

    pub fn build(self) -> crate::Result<HttpChannel> {
        self.build_channel()
    }

    fn build_channel(self) -> crate::Result<HttpChannel> {
        let authority = self.connect_using.authority().clone();
        let is_http2 = matches!(self.http_protocol_options.codec, Codec::Http2);

        let enable_trailers = match self.http_protocol_options.codec {
            Codec::Http1 => self.http_protocol_options.http1_options.enable_trailers,
            Codec::Http2 => false,
        };
        let idle_timeout = self.http_protocol_options.common.idle_timeout.unwrap_or(DEFAULT_IDLE_TIMEOUT);
        let cluster_name = self.cluster_name.unwrap_or_default();
        let http2_options = self.http_protocol_options.http2_options.clone();

        let channel_client = if let Some(tls_context) = self.tls {
            let mut builder =
                hyper_rustls::HttpsConnectorBuilder::new().with_tls_config(tls_context.into_inner()).https_only();

            builder = if let Some(server_name) = self.server_name {
                builder.with_server_name_resolver(FixedServerNameResolver::new(server_name))
            } else {
                let server_name = ServerName::try_from(authority.host().to_owned())?;
                debug!("Server name is not configured in bootstrap.. using endpoint authority {:?}", server_name);
                builder.with_server_name_resolver(FixedServerNameResolver::new(server_name))
            };

            let connector = UnifiedConnector::from((&self.connect_using, cluster_name, is_http2));

            if is_http2 {
                let http_connector = builder.enable_http2().wrap_connector(connector);
                let ext = Http2ClientExt::tls(http_connector, &authority, idle_timeout, http2_options)?;
                HttpChannelClient::Tls(TlsChannelClient::Http2(ext))
            } else {
                let http_connector = builder.enable_http1().wrap_connector(connector);
                let ext = Http1ClientExt::tls(http_connector, &authority, idle_timeout)?;
                HttpChannelClient::Tls(TlsChannelClient::Http1(ext))
            }
        } else {
            let connector = UnifiedConnector::from((&self.connect_using, cluster_name, is_http2));

            if is_http2 {
                let ext = Http2ClientExt::plain(connector, &authority, idle_timeout, http2_options)?;
                HttpChannelClient::Plain(PlainChannelClient::Http2(ext))
            } else {
                let ext = Http1ClientExt::plain(connector, &authority, idle_timeout)?;
                HttpChannelClient::Plain(PlainChannelClient::Http1(ext))
            }
        };

        Ok(HttpChannel {
            channel_client,
            http_version: self.http_protocol_options.codec,
            enable_trailers,
            upstream_authority: authority,
            cluster_name,
        })
    }

    #[allow(dead_code)]
    fn build_channel_from_pipe(self) -> crate::Result<HttpChannel> {
        match &self.connect_using {
            ConnectUsing::Socket { .. } | ConnectUsing::InternalListener { .. } => {
                Err(Error::from("Pipe channel requires a pipe address"))
            },
        }
    }
}

impl<'a> RequestHandler<Request<OrionRequestBody>, UpstreamCallOpts<'a>> for &HttpChannels {
    async fn to_response(
        self,
        ctx: &RequestCtx,
        request: Request<OrionRequestBody>,
        arg: UpstreamCallOpts<'a>,
    ) -> Result<Response<OrionResponseBody>> {
        match self {
            HttpChannels::Single(channel) => channel.to_response(ctx, request, arg).await,
            HttpChannels::MultiWithFailover { channel, failover_channels } => {
                let UpstreamCallOpts { route_timeout, priority, .. } = arg;
                let (parts, body) = request.into_parts();
                let body_kind = body.body_kind;
                let stream_metrics = Clone::clone(&body.stream_metrics);
                let on_complete = Clone::clone(&body.on_complete);

                let body_timeout = body.inner.timeout;
                let collected = body.collect().await.map_err(Error::from)?;
                let replay_body = http_body_util::Full::new(collected.to_bytes());

                let mut last_error: Option<Error> = None;

                let total_attempts = 1 + failover_channels.len();
                for (attempt, channel) in std::iter::once(channel).chain(failover_channels.iter()).enumerate() {
                    let cloned_body = InstrumentedBody {
                        inner: TimeoutBody::new(body_timeout, replay_body.clone().into()),
                        body_kind,
                        body_bytes: 0,
                        stream_metrics: Clone::clone(&stream_metrics),
                        on_complete: Clone::clone(&on_complete),
                    };
                    let rebuilt_req = Request::from_parts(parts.clone(), cloned_body);
                    let attempt_ctx = UpstreamCallOpts { route_timeout, retry_policy: None, priority };

                    match channel.to_response(ctx, rebuilt_req, attempt_ctx).await {
                        Ok(response) => {
                            if response.status().is_server_error() && (attempt + 1) < total_attempts {
                                debug!(
                                    attempt,
                                    status = %response.status(),
                                    cluster = channel.cluster_name,
                                    upstream = %channel.upstream_authority,
                                    "Server error response, trying next failover"
                                );
                                continue;
                            }
                            return Ok(response);
                        },
                        Err(err) => {
                            debug!(
                                attempt,
                                cluster = channel.cluster_name,
                                upstream = %channel.upstream_authority,
                                error = %err,
                                "Failed to forward request upstream, trying next failover"
                            );
                            last_error = Some(err);
                        },
                    }
                }

                Err(last_error.unwrap_or_else(|| Error::new("Failed to forward request to any upstream channel")))
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Retries {
    pub requests: u32,
    pub timeouts: u32,
}

impl<'a> RequestHandler<Request<OrionRequestBody>, UpstreamCallOpts<'a>> for &HttpChannel {
    async fn to_response(
        self,
        #[allow(unused_variables)] ctx: &RequestCtx,
        request: Request<OrionRequestBody>,
        arg: UpstreamCallOpts<'a>,
    ) -> Result<Response<OrionResponseBody>> {
        instrument_function!(ctx.tx.clock, |nanos| {
            #[allow(clippy::cast_possible_truncation)]
            crate::instrumentation::metrics::REQUEST_TO_RESPONSE_TIME.observe(nanos as usize)
        });

        let version = request.version();
        #[cfg(feature = "metrics")]
        let shard_id = get_shard_id!();

        with_metric!(clusters::UPSTREAM_RQ_ACTIVE, add, 1, shard_id, &[KeyValue::new("cluster", self.cluster_name)]);

        #[cfg(feature = "metrics")]
        defer! {
            with_metric!(clusters::UPSTREAM_RQ_ACTIVE, sub, 1, shard_id, &[KeyValue::new("cluster", self.cluster_name)]);
        }

        #[cfg(feature = "metrics")]
        if let Some(custom_metrics) = CUSTOM_METRICS.get() {
            use crate::metrics;
            use orion_metrics::metrics::custom::MetricsHook;

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
            custom_metrics.with_headers(MetricsHook::UpstreamRequest, request.headers(), attrs.as_slice());
        }

        #[cfg(feature = "access-log")]
        ctx.tx.with_loggers(|loggers| {
            if let Err(err) = crate::access_log::evaluate_base64_access_log_hook(
                crate::access_log::AccessLogHook::UpstreamRequest,
                request.headers(),
                loggers,
            ) {
                tracing::warn!("Failed to process access log header for UpstreamRequest: {err}");
            }
        });

        let UpstreamCallOpts { route_timeout, retry_policy, priority } = arg;

        let mut retries = Retries::default();
        let start_time = std::time::Instant::now();
        let result = instrument_block!(
            ctx.tx.clock,
            |nanos| {
                #[allow(clippy::cast_possible_truncation)]
                crate::instrumentation::metrics::SEND_REQUEST_WAIT_RESPONSE.observe(nanos as usize);
            },
            {
                self.send_request(
                    request,
                    route_timeout,
                    retry_policy,
                    priority,
                    Some(&mut retries),
                    #[cfg(feature = "instrumentation")]
                    &ctx.tx.clock,
                )
                .await
            }
        );

        if result.is_ok() {
            with_metric!(clusters::UPSTREAM_RQ_TOTAL, add, 1, shard_id, &[KeyValue::new("cluster", self.cluster_name)]);
        }

        if retries.requests > 0 {
            with_metric!(
                clusters::UPSTREAM_RQ_RETRY,
                add,
                u64::from(retries.requests),
                shard_id,
                &[KeyValue::new("cluster", self.cluster_name)]
            );
        }

        if retries.timeouts > 0 {
            with_metric!(
                clusters::UPSTREAM_RQ_PER_TRY_TIMEOUT,
                add,
                u64::from(retries.timeouts),
                shard_id,
                &[KeyValue::new("cluster", self.cluster_name)]
            );
        }

        if let Err(ref err) = result {
            if let Some(UpstreamError::RouteTimeout) = UpstreamError::try_infer_from(err.as_ref()) {
                with_metric!(
                    clusters::UPSTREAM_RQ_TIMEOUT,
                    add,
                    1,
                    shard_id,
                    &[KeyValue::new("cluster", self.cluster_name)]
                );
            }
        }

        HttpChannel::map_upstream_result(result, start_time.elapsed(), route_timeout, version)
    }
}

impl HttpChannel {
    #[allow(clippy::too_many_arguments)]
    pub async fn send_request(
        &self,
        mut request: Request<OrionRequestBody>,
        timeout: Option<Duration>,
        retry_policy: Option<&RetryPolicy>,
        priority: RoutingPriority,
        output: Option<&mut Retries>,
        #[cfg(feature = "instrumentation")] clock: &quanta::Clock,
    ) -> Result<Response<OrionResponseBody>> {
        match &self.channel_client {
            HttpChannelClient::Plain(plain) => match plain {
                PlainChannelClient::Http1(ext) => {
                    prepare_http1_request(&mut request)?;
                    let pool_ref = ext.local_pool_ref();
                    self.send_with_policy(
                        request,
                        timeout,
                        retry_policy,
                        priority,
                        |req| async move { pool_ref.send(req).await },
                        output,
                        #[cfg(feature = "instrumentation")]
                        clock,
                    )
                    .await
                },
                PlainChannelClient::Http2(ext) => {
                    prepare_http2_request(&mut request, false)?;
                    let pool_ref = ext.local_pool_ref();
                    self.send_with_policy(
                        request,
                        timeout,
                        retry_policy,
                        priority,
                        |req| async move { pool_ref.send(req).await },
                        output,
                        #[cfg(feature = "instrumentation")]
                        clock,
                    )
                    .await
                },
            },
            HttpChannelClient::Tls(tls) => match tls {
                TlsChannelClient::Http1(ext) => {
                    prepare_http1_request(&mut request)?;
                    let pool_ref = ext.local_pool_ref();
                    self.send_with_policy(
                        request,
                        timeout,
                        retry_policy,
                        priority,
                        |req| async move { pool_ref.send(req).await },
                        output,
                        #[cfg(feature = "instrumentation")]
                        clock,
                    )
                    .await
                },
                TlsChannelClient::Http2(ext) => {
                    prepare_http2_request(&mut request, true)?;
                    let pool_ref = ext.local_pool_ref();
                    self.send_with_policy(
                        request,
                        timeout,
                        retry_policy,
                        priority,
                        |req| async move { pool_ref.send(req).await },
                        output,
                        #[cfg(feature = "instrumentation")]
                        clock,
                    )
                    .await
                },
            },
        }
    }

    /// Send the request and return the Result, either the Response or an Error,
    /// along with the time spent for possible retransmissions. Note: the returned
    /// duration does not include the time spent receiving the Body of the Response.
    #[allow(clippy::too_many_arguments)]
    async fn send_with_policy<F, Fut>(
        &self,
        mut req: Request<OrionRequestBody>,
        timeout: Option<Duration>,
        retry_policy: Option<&RetryPolicy>,
        priority: RoutingPriority,
        send: F,
        output: Option<&mut Retries>,
        #[cfg(feature = "instrumentation")] clock: &quanta::Clock,
    ) -> Result<Response<OrionResponseBody>>
    where
        F: Fn(Request<OrionRequestBody>) -> Fut,
        Fut: Future<Output = Result<Response<OrionResponseBody>>>,
    {
        if !self.enable_trailers {
            strip_trailers_headers(self.http_version, req.headers_mut());
        }

        if let Some(policy) = retry_policy.filter(|policy| policy.is_retriable(&req)) {
            let fut = self.send_with_retry(
                req,
                policy,
                priority,
                send,
                output,
                #[cfg(feature = "instrumentation")]
                clock,
            );
            if let Some(t) = timeout {
                fast_timeout(t, fut).await.map_err(|_e| Error::from(UpstreamError::RouteTimeout))?
            } else {
                fut.await
            }
        } else if let Some(t) = timeout {
            // Keep instrumentation inside the timed future so a route timeout
            // cancels `sender.request` without recording SEND_REQUEST.
            fast_timeout(t, async {
                instrument_block!(
                    clock,
                    |nanos| {
                        #[allow(clippy::cast_possible_truncation)]
                        crate::instrumentation::metrics::SEND_REQUEST.observe(nanos as usize);
                    },
                    { send(req).await }
                )
            })
            .await
            .map_err(|_e| Error::from(UpstreamError::RouteTimeout))?
        } else {
            instrument_block!(
                clock,
                |nanos| {
                    #[allow(clippy::cast_possible_truncation)]
                    crate::instrumentation::metrics::SEND_REQUEST.observe(nanos as usize);
                },
                { send(req).await }
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_with_retry<F, Fut>(
        &self,
        req: Request<OrionRequestBody>,
        retry_policy: &RetryPolicy,
        priority: RoutingPriority,
        send: F,
        mut output: Option<&mut Retries>,
        #[cfg(feature = "instrumentation")] clock: &quanta::Clock,
    ) -> Result<Response<OrionResponseBody>>
    where
        F: Fn(Request<OrionRequestBody>) -> Fut,
        Fut: Future<Output = Result<Response<OrionResponseBody>>>,
    {
        instrument_function!(clock, |nanos| {
            #[allow(clippy::cast_possible_truncation)]
            crate::instrumentation::metrics::SEND_REQUEST_WITH_RETRY.observe(nanos as usize)
        });

        let (parts, body) = req.into_parts();
        let body_kind = body.body_kind;
        let stream_metrics = Clone::clone(&body.stream_metrics);
        let on_complete = Clone::clone(&body.on_complete);

        let collected_bytes = if http_body::Body::size_hint(&body).exact() == Some(0) {
            bytes::Bytes::new()
        } else {
            body.collect().await.map_err(Error::from)?.to_bytes()
        };

        let body = http_body_util::Full::new(collected_bytes);

        let max_retries = retry_policy.num_retries() as usize;
        let mut parts_opt = Some(parts);
        let mut retry_permit = None;
        let mut last_result: Option<Result<Response<OrionResponseBody>>> = None;

        for (index, back_off) in retry_policy.exponential_back_off().iter().enumerate() {
            let back_off = back_off.unwrap_or(Duration::from_secs(1));

            let cloned_body = InstrumentedBody {
                inner: TimeoutBody::new(None, body.clone().into()),
                body_kind,
                body_bytes: 0,
                stream_metrics: Clone::clone(&stream_metrics),
                on_complete: Clone::clone(&on_complete),
            };

            // avoid to clone parts on the last attempt
            #[allow(clippy::unwrap_used)]
            let current_parts =
                if index == max_retries { parts_opt.take().unwrap() } else { parts_opt.as_ref().unwrap().clone() };

            let cloned_req: Request<OrionRequestBody> = Request::from_parts(current_parts, cloned_body);

            // actually send the request and wait for the response...
            let result: Result<Response<OrionResponseBody>> = if let Some(t) = retry_policy.per_try_timeout() {
                match fast_timeout(t, send(cloned_req)).await.map_err(|_e| UpstreamError::PerTryTimeout) {
                    Ok(result) => result,
                    Err(err) => Err(err.into()),
                }
            } else {
                send(cloned_req).await
            };

            // generate a possible retry condition...
            let Some((is_per_try_timeout, should_retry)) = RetryCondition::try_infer_from(&result)
                .map(|condition| (condition.is_per_try_timeout(), condition.should_retry(retry_policy)))
            else {
                return result;
            };

            if is_per_try_timeout {
                if let Some(retries) = output.as_deref_mut() {
                    // Increment the timeout counter
                    retries.timeouts += 1;
                }
            }

            // check for a possible retry...
            if !should_retry {
                return result;
            }

            if let Some(output) = output.as_deref_mut() {
                output.requests += 1;
            }

            // take an exponential back off break and retry...
            if index < retry_policy.num_retries() as usize {
                if retry_permit.is_none() {
                    let Ok(permit) = RetryCircuitBreakerPermit::try_acquire(self.cluster_name, priority) else {
                        debug!(
                            "retry_policy: circuit breaker denied retry #{}/{} for cluster {}",
                            index + 1,
                            retry_policy.num_retries(),
                            self.cluster_name
                        );
                        #[cfg(feature = "metrics")]
                        {
                            let shard_id = get_shard_id!();
                            with_metric!(
                                clusters::UPSTREAM_RQ_RETRY_OVERFLOW,
                                add,
                                1,
                                shard_id,
                                &[KeyValue::new("cluster", self.cluster_name)]
                            );
                        };
                        return result;
                    };
                    retry_permit = Some(permit);
                }

                debug!(
                    "retry_policy: retrying request #{}/{} in {}...",
                    index + 1,
                    retry_policy.num_retries(),
                    pretty_duration(&back_off, None)
                );

                pingora_timeout::sleep(back_off).await;
            }

            last_result = Some(result);
        }

        match last_result {
            Some(result) => result,
            None => {
                Err(io::Error::new(io::ErrorKind::InvalidData, "retry loop completed without producing a result")
                    .into())
            },
        }
    }

    fn map_upstream_result(
        result: Result<Response<OrionResponseBody>>,
        elapsed: Duration,
        route_timeout: Option<Duration>,
        version: http::Version,
    ) -> Result<hyper::Response<OrionResponseBody>> {
        match (result, elapsed) {
            (Ok(response), elapsed) => {
                // calculate the remaining timeout (relative to the route timeout) for receiving
                // the body of the incoming response...
                let (parts, mut body) = response.into_parts();
                body.timeout = route_timeout.map(|dur| dur.checked_sub(elapsed).unwrap_or_default());
                Ok(Response::from_parts(parts, body))
            },
            (Err(err), dur) => {
                if let Some(event_error) = UpstreamError::try_infer_from(err.as_ref()) {
                    let response_flags: ResponseFlags = event_error.clone().into();
                    debug!(
                        "Event ({event_error}) occurred after {:?}: {} ({})",
                        pretty_duration(&dur, None),
                        ResponseFlagsLong(&response_flags.0).to_smolstr(),
                        ResponseFlagsShort(&response_flags.0).to_smolstr()
                    );

                    match event_error {
                        UpstreamError::RefusedStream | UpstreamError::Io(_) | UpstreamError::ConnectTimeout(_) => {
                            Ok(SyntheticHttpResponse::service_unavailable(
                                EventKind::Upstream(event_error),
                                response_flags,
                            )
                            .into_response(version))
                        },
                        UpstreamError::PerTryTimeout | UpstreamError::RouteTimeout => {
                            Ok(SyntheticHttpResponse::gateway_timeout(EventKind::Upstream(event_error), response_flags)
                                .into_response(version))
                        },
                        UpstreamError::Reset | UpstreamError::Http3PostConnectFailure => {
                            Ok(SyntheticHttpResponse::bad_gateway(EventKind::Upstream(event_error), response_flags)
                                .into_response(version))
                        },
                        UpstreamError::Error(_) => Ok(SyntheticHttpResponse::internal_server_error(
                            EventKind::Upstream(event_error),
                            response_flags,
                        )
                        .with_body("internal server error")
                        .into_response(version)),
                    }
                } else {
                    debug!("Route: error occurred after {:?}: {err}", pretty_duration(&dur, None));
                    Err(err)
                }
            },
        }
    }

    pub fn is_https(&self) -> bool {
        self.channel_client.is_tls()
    }

    pub fn http_version(&self) -> Codec {
        self.http_version
    }

    pub fn load(&self) -> u32 {
        let load = match &self.channel_client {
            HttpChannelClient::Plain(plain) => match plain {
                PlainChannelClient::Http1(ext) => ext.strong_count(),
                PlainChannelClient::Http2(ext) => ext.strong_count(),
            },
            HttpChannelClient::Tls(tls) => match tls {
                TlsChannelClient::Http1(ext) => ext.strong_count(),
                TlsChannelClient::Http2(ext) => ext.strong_count(),
            },
        };
        u32::try_from(load).unwrap_or(u32::MAX)
    }
}

#[inline]
fn prepare_http1_request(request: &mut Request<OrionRequestBody>) -> Result<()> {
    // Fast-path for H1 -> H1 origin-form requests (the dominant hot path):
    if request.version() == Version::HTTP_11 {
        let uri = request.uri();
        if uri.scheme().is_none() && uri.authority().is_none() {
            if let Some(host) = request.headers().get(http::header::HOST) {
                if host.as_bytes().is_empty() {
                    return Err(format!("Empty Host header").into());
                }
            }
            return Ok(());
        }
    }

    // Cross-protocol or non-origin-form:
    if !request.headers().contains_key(http::header::HOST) {
        if let Some(authority) = request.uri().authority() {
            let val = HeaderValue::try_from(authority.as_str())
                .map_err(|e| format!("Invalid authority for Host header: {e}"))?;
            request.headers_mut().insert(http::header::HOST, val);
        }
    } else if let Some(host) = request.headers().get(http::header::HOST) {
        if host.as_bytes().is_empty() {
            return Err(format!("Empty Host header").into());
        }
    }

    if request.version() == Version::HTTP_2 {
        *request.version_mut() = Version::HTTP_11;
    }

    // Ensure origin-form URI:
    let uri = request.uri_mut();
    if uri.scheme().is_none() && uri.authority().is_none() {
        if uri.path_and_query().is_none() {
            *uri = Uri::from_static("/");
        }
        return Ok(());
    }

    let mut parts = Parts::from(mem::take(uri));
    parts.scheme = None;
    parts.authority = None;
    if parts.path_and_query.is_none() {
        parts.path_and_query = Some(http::uri::PathAndQuery::from_static("/"));
    }
    *uri = Uri::from_parts(parts).map_err(|e| format!("Invalid uri: {e}"))?;
    Ok(())
}

#[inline]
fn prepare_http2_request(request: &mut Request<OrionRequestBody>, is_tls: bool) -> Result<()> {
    // 1. Version rewrite
    *request.version_mut() = Version::HTTP_2;

    // 2. Ensure absolute URI with scheme and authority
    let uri = request.uri();
    let has_scheme = uri.scheme().is_some();
    let has_authority = uri.authority().is_some();

    if !has_scheme || !has_authority {
        let authority_from_host = if !has_authority {
            if let Some(host_header) = request.headers().get(http::header::HOST) {
                let authority_str = host_header.to_str().map_err(|e| format!("Can't parse Host header: {e}"))?;
                Some(authority_str.parse::<Authority>().map_err(|e| format!("Can't parse authority: {e}"))?)
            } else {
                None
            }
        } else {
            None
        };

        let uri = request.uri_mut();
        let mut parts = Parts::from(mem::take(uri));

        if parts.scheme.is_none() {
            parts.scheme = if is_tls { Some(http::uri::Scheme::HTTPS) } else { Some(http::uri::Scheme::HTTP) };
        }

        if parts.authority.is_none() {
            parts.authority = authority_from_host;
        }

        *uri = Uri::from_parts(parts).map_err(|e| format!("Can't normalize uri: {e}"))?;
    }

    // 3. Remove Host header as HTTP/2 uses :authority
    request.headers_mut().remove(http::header::HOST);

    Ok(())
}
