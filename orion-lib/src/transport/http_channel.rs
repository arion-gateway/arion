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

use super::{bind_device::BindDevice, connector::LocalConnectorWithDNSResolver};
use crate::{
    body::{
        instrumented_body::InstrumentedBody, poly_body::PolyBody, response_flags::ResponseFlags,
        timeout_body::TimeoutBody,
    },
    clusters::retry_policy::RetryCondition,
    event_error::{EventError, EventKind, TryInferFrom},
    get_shard_id, instrument_block, instrument_function,
    listeners::{
        http_connection_manager::{http_modifiers::strip_trailers_headers, RequestHandler, TransactionHandler},
        synthetic_http_response::SyntheticHttpResponse,
    },
    secrets::{TlsConfigurator, WantsToBuildClient},
    thread_local::{LocalBuilder, LocalObject},
    transport::timer::PingoraTimer,
    Error, OrionRequestBody, OrionResponseBody, RequestContext, Result,
};
use http::{
    uri::{Authority, Parts},
    HeaderValue, Response, Version,
};
use http_body_util::BodyExt;
use hyper::{body::Incoming, Request, Uri};
use hyper_rustls::{FixedServerNameResolver, HttpsConnector};
use hyper_util::{
    client::legacy::{connect::Connect, Builder, Client},
    rt::tokio::TokioExecutor,
};
use orion_configuration::config::{
    cluster::http_protocol_options::{Codec, HttpProtocolOptions},
    core::envoy_conversions::Address,
    network_filters::http_connection_manager::RetryPolicy,
};
use orion_format::types::{ResponseFlagsLong, ResponseFlagsShort};

use crate::with_metric;
use hyperlocal::UnixConnector;
#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;
#[cfg(feature = "metrics")]
use orion_metrics::metrics::clusters;

use pingora_timeout::fast_timeout::fast_timeout;
use pretty_duration::pretty_duration;
use rustls::ClientConfig;
use scopeguard::defer;
use smol_str::ToSmolStr;
use std::{io::ErrorKind, mem, result::Result as StdResult, sync::Arc, time::Duration};
use tracing::debug;
use webpki::types::ServerName;

#[cfg(feature = "metrics")]
use {
    hyper_util::client::legacy::pool::{EventHandler, PoolEvent},
    hyper_util::client::legacy::PoolKey,
    std::any::Any,
};
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

type HttpClient = Client<LocalConnectorWithDNSResolver, OrionRequestBody>;
type HttpsClient = Client<HttpsConnector<LocalConnectorWithDNSResolver>, OrionRequestBody>;

// Rationale: The outer Arc is necessary to avoid building a new Client when cloning the HttpChannel.
// The inner Arc, instead, is used to pass the client to async code, so it's already wrapped by the Arc.

#[derive(Clone, Debug)]
pub struct ClientContext {
    configured_upstream_http_version: Codec,
    client: Arc<LocalObject<Arc<HttpsClient>, Builder, HttpsConnector<LocalConnectorWithDNSResolver>>>,
}
impl ClientContext {
    fn new(
        configured_upstream_http_version: Codec,
        client: Arc<LocalObject<Arc<HttpsClient>, Builder, HttpsConnector<LocalConnectorWithDNSResolver>>>,
    ) -> Self {
        Self { configured_upstream_http_version, client }
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
    Plain(Arc<LocalObject<Arc<HttpClient>, Builder, LocalConnectorWithDNSResolver>>),
    Tls(ClientContext),
    Unix(hyper::Uri, Arc<Client<UnixConnector, InstrumentedBody<TimeoutBody<PolyBody>>>>),
}

impl HttpChannelClient {
    pub fn is_tls(&self) -> bool {
        matches!(self, HttpChannelClient::Tls(_))
    }
}

#[derive(Default)]
pub struct HttpChannelBuilder {
    tls: Option<TlsConfigurator<ClientConfig, WantsToBuildClient>>,
    address: Option<Address>,
    authority: Option<Authority>,
    bind_device: Option<BindDevice>,
    server_name: Option<ServerName<'static>>,
    http_protocol_options: HttpProtocolOptions,
    connection_timeout: Option<Duration>,
    cluster_name: Option<&'static str>,
}

impl LocalBuilder<LocalConnectorWithDNSResolver, Arc<HttpClient>> for Builder {
    fn build(&self, arg: LocalConnectorWithDNSResolver) -> Arc<HttpClient> {
        Arc::new(self.build(arg))
    }
}

impl LocalBuilder<HttpsConnector<LocalConnectorWithDNSResolver>, Arc<HttpsClient>> for Builder {
    fn build(&self, arg: HttpsConnector<LocalConnectorWithDNSResolver>) -> Arc<HttpsClient> {
        Arc::new(self.build(arg))
    }
}

impl HttpChannelBuilder {
    pub fn new(bind_device: Option<BindDevice>) -> Self {
        Self { bind_device, ..Default::default() }
    }

    pub fn with_tls(self, tls_configurator: Option<TlsConfigurator<ClientConfig, WantsToBuildClient>>) -> Self {
        Self { tls: tls_configurator, ..self }
    }

    pub fn with_timeout(self, timeout: Option<Duration>) -> Self {
        Self { connection_timeout: timeout, ..self }
    }

    pub fn with_authority(self, authority: Authority) -> Self {
        Self { authority: Some(authority), ..self }
    }

    pub fn with_address(self, address: Address) -> Self {
        Self { address: Some(address), ..self }
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

    #[allow(clippy::cast_sign_loss)]
    pub fn build(self) -> crate::Result<HttpChannel> {
        match self.address {
            Some(Address::Socket(_, _)) => self.build_channel_from_authority(),
            Some(Address::Pipe(_, _)) => self.build_channel_from_pipe(),
            None => Err(Error::from("Address is mandatory")),
        }
    }

    #[allow(clippy::cast_sign_loss)]
    pub fn build_with_no_address(self) -> crate::Result<HttpChannel> {
        self.build_channel_from_authority()
    }

    fn configure_hyper_client(&self) -> Builder {
        let mut client_builder = Client::builder(TokioExecutor::new());
        client_builder
            .timer(PingoraTimer)
            .pool_idle_timeout(self.http_protocol_options.common.idle_timeout.unwrap_or(DEFAULT_IDLE_TIMEOUT))
            .pool_timer(PingoraTimer)
            .pool_max_idle_per_host(usize::MAX)
            .set_host(false);

        let configured_upstream_http_version = self.http_protocol_options.codec;

        self.configure_http2_if_needed(&mut client_builder, configured_upstream_http_version);

        #[cfg(feature = "metrics")]
        {
            let cluster_name = self.cluster_name.unwrap_or_default();
            client_builder.pool_event_handler(EventHandler::new(update_upstream_stats, cluster_name));
        }

        client_builder
    }

    fn configure_http2_if_needed(&self, client_builder: &mut Builder, version: Codec) {
        if matches!(version, Codec::Http2) {
            client_builder.http2_only(true);
            let http2_options = &self.http_protocol_options.http2_options;

            if let Some(settings) = &http2_options.keep_alive_settings {
                client_builder.http2_keep_alive_interval(settings.keep_alive_interval);
                if let Some(timeout) = settings.keep_alive_timeout {
                    client_builder.http2_keep_alive_timeout(timeout);
                }
                client_builder.http2_keep_alive_while_idle(true);
            }

            client_builder.http2_initial_connection_window_size(http2_options.initial_connection_window_size());
            client_builder.http2_initial_stream_window_size(http2_options.initial_stream_window_size());
            client_builder.http2_connection_sharing(true);

            if let Some(max) = http2_options.max_concurrent_streams() {
                client_builder.http2_initial_max_send_streams(max);
                if let Ok(max) = u32::try_from(max) {
                    client_builder.http2_max_concurrent_streams(max);
                }
            }
        }
    }

    fn build_channel_from_authority(self) -> crate::Result<HttpChannel> {
        let authority = self.authority.clone().ok_or_else(|| Error::from("Authority is mandatory"))?;
        let client_builder = self.configure_hyper_client();

        // enable_trailers is only valid for HTTP1 and the flag is used to
        // include the TE and Trailer headers if they were missing from the
        // original request
        let enable_trailers = match self.http_protocol_options.codec {
            Codec::Http1 => self.http_protocol_options.http1_options.enable_trailers,
            Codec::Http2 => false,
        };

        if let Some(tls_context) = self.tls {
            // Build TLS client inline to avoid ownership issues
            let mut builder =
                hyper_rustls::HttpsConnectorBuilder::new().with_tls_config(tls_context.into_inner()).https_only();

            builder = if let Some(server_name) = self.server_name {
                builder.with_server_name_resolver(FixedServerNameResolver::new(server_name))
            } else {
                let server_name = ServerName::try_from(authority.host().to_owned())?;
                debug!("Server name is not configured in bootstrap.. using endpoint authority {:?}", server_name);
                builder.with_server_name_resolver(FixedServerNameResolver::new(server_name))
            };

            let connector = LocalConnectorWithDNSResolver {
                addr: authority.clone(),
                cluster_name: self.cluster_name.unwrap_or_default(),
                bind_device: self.bind_device,
                timeout: self.connection_timeout,
            };

            let http_connector = match self.http_protocol_options.codec {
                Codec::Http2 => builder.enable_http2().wrap_connector(connector),
                Codec::Http1 => builder.enable_http1().wrap_connector(connector),
            };

            Ok(HttpChannel {
                channel_client: HttpChannelClient::Tls(ClientContext::new(
                    self.http_protocol_options.codec,
                    Arc::new(LocalObject::new(client_builder, http_connector)),
                )),
                http_version: self.http_protocol_options.codec,
                enable_trailers,
                upstream_authority: authority,
                cluster_name: self.cluster_name.unwrap_or_default(),
            })
        } else {
            // Build plain client inline
            let connector = LocalConnectorWithDNSResolver {
                addr: authority.clone(),
                bind_device: self.bind_device,
                timeout: self.connection_timeout,
                cluster_name: self.cluster_name.unwrap_or_default(),
            };

            Ok(HttpChannel {
                channel_client: HttpChannelClient::Plain(Arc::new(LocalObject::new(client_builder, connector))),
                http_version: self.http_protocol_options.codec,
                enable_trailers,
                upstream_authority: authority,
                cluster_name: self.cluster_name.unwrap_or_default(),
            })
        }
    }

    fn build_channel_from_pipe(self) -> crate::Result<HttpChannel> {
        use hyperlocal::{UnixClientExt, Uri};

        match self.address {
            Some(Address::Pipe(name, _)) => {
                debug!("Building address from a pipe {name}");
                let uri: hyper::Uri = Uri::new(name.clone(), "/").into();
                let authority = uri.authority().cloned().unwrap_or(Authority::from_static("none"));
                debug!("Building address from a pipe {uri:?}");
                Ok(HttpChannel {
                    channel_client: HttpChannelClient::Unix(uri, Arc::new(Client::unix())),
                    http_version: self.http_protocol_options.codec,
                    enable_trailers: self.http_protocol_options.http1_options.enable_trailers,
                    upstream_authority: authority,
                    cluster_name: self.cluster_name.unwrap_or_default(),
                })
            },
            _ => Err(Error::from("Trying to build a pipe address from invalid address")),
        }
    }
}

#[cfg(feature = "metrics")]
#[allow(clippy::needless_pass_by_value)]
fn update_upstream_stats(event: PoolEvent, tag: &dyn Any, keys: &[&PoolKey]) {
    use tracing::debug;
    let cluster_name = *(tag.downcast_ref::<&str>().unwrap_or(&""));
    let shard_id = get_shard_id!();

    for key in keys {
        debug!("HttpClient: {:?} for cluster {:?} (pool_key: {:?})", event, cluster_name, key);
    }

    let num_events = keys.len() as u64;
    match event {
        PoolEvent::NewConnection => {
            with_metric!(
                clusters::UPSTREAM_CX_TOTAL,
                add,
                num_events,
                shard_id,
                &[KeyValue::new("cluster", cluster_name)]
            );
            with_metric!(
                clusters::UPSTREAM_CX_ACTIVE,
                add,
                num_events,
                shard_id,
                &[KeyValue::new("cluster", cluster_name)]
            );
        },
        PoolEvent::IdleConnectionClosed => {
            with_metric!(
                clusters::UPSTREAM_CX_DESTROY,
                add,
                num_events,
                shard_id,
                &[KeyValue::new("cluster", cluster_name)]
            );
            with_metric!(
                clusters::UPSTREAM_CX_IDLE_TIMEOUT,
                add,
                num_events,
                shard_id,
                &[KeyValue::new("cluster", cluster_name)]
            );
            with_metric!(
                clusters::UPSTREAM_CX_ACTIVE,
                sub,
                num_events,
                shard_id,
                &[KeyValue::new("cluster", cluster_name)]
            );
        },
        PoolEvent::ConnectionError => {
            with_metric!(
                clusters::UPSTREAM_CX_CONNECT_FAIL,
                add,
                num_events,
                shard_id,
                &[KeyValue::new("cluster", cluster_name)]
            );
        },
        PoolEvent::ConnectionTimeout => {
            with_metric!(
                clusters::UPSTREAM_CX_CONNECT_TIMEOUT,
                add,
                num_events,
                shard_id,
                &[KeyValue::new("cluster", cluster_name)]
            );
        },
        PoolEvent::ConnectionClosed => {
            with_metric!(
                clusters::UPSTREAM_CX_DESTROY,
                add,
                num_events,
                shard_id,
                &[KeyValue::new("cluster", cluster_name)]
            );
            with_metric!(
                clusters::UPSTREAM_CX_ACTIVE,
                sub,
                num_events,
                shard_id,
                &[KeyValue::new("cluster", cluster_name)]
            );
        },
    }
}

impl<'a> RequestHandler<Request<OrionRequestBody>, RequestContext<'a>> for &HttpChannels {
    async fn to_response(
        self,
        trans_handler: &TransactionHandler,
        request: Request<OrionRequestBody>,
        ctx: RequestContext<'a>,
    ) -> Result<Response<OrionResponseBody>> {
        match self {
            HttpChannels::Single(channel) => channel.to_response(trans_handler, request, ctx).await,
            HttpChannels::MultiWithFailover { channel, failover_channels } => {
                let RequestContext { route_timeout, .. } = ctx;
                let (parts, body) = request.into_parts();
                let InstrumentedBody { inner, guard, state } = body;

                let body_timeout = inner.timeout;
                let collected = inner.collect().await.map_err(Error::from)?;
                let replay_body = http_body_util::Full::new(collected.to_bytes());

                let total_attempts = 1 + failover_channels.len();
                let mut last_error: Option<Error> = None;
                let mut last_response: Option<Response<OrionResponseBody>> = None;

                for (attempt, channel) in std::iter::once(channel).chain(failover_channels.iter()).enumerate() {
                    let has_more = attempt + 1 < total_attempts;

                    let cloned_body = InstrumentedBody {
                        inner: TimeoutBody::new(body_timeout, replay_body.clone().into()),
                        guard: guard.clone(),
                        state: state.clone(),
                    };
                    let rebuilt_req = Request::from_parts(parts.clone(), cloned_body);
                    let attempt_ctx = RequestContext { route_timeout, retry_policy: None };

                    match channel.to_response(trans_handler, rebuilt_req, attempt_ctx).await {
                        Ok(response) => {
                            if has_more {
                                debug!(
                                    attempt,
                                    cluster = channel.cluster_name,
                                    upstream = %channel.upstream_authority,
                                    "Retrying with alternative upstream endpoint"
                                );
                                last_response = Some(response);
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
                                "Failed to forward request upstream"
                            );
                            last_error = Some(err);
                        },
                    }
                }

                if let Some(response) = last_response {
                    return Ok(response);
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

impl<'a> RequestHandler<Request<OrionRequestBody>, RequestContext<'a>> for &HttpChannel {
    async fn to_response(
        self,
        _trans_handler: &TransactionHandler,
        request: Request<OrionRequestBody>,
        ctx: RequestContext<'a>,
    ) -> Result<Response<OrionResponseBody>> {
        instrument_function!(_trans_handler.clock, |nanos| {
            crate::instrumentation::REQUEST_TO_RESPONSE_TIME.observe(nanos as usize)
        });

        let version = request.version();
        let _shard_id = get_shard_id!();

        with_metric!(clusters::UPSTREAM_RQ_ACTIVE, add, 1, _shard_id, &[KeyValue::new("cluster", self.cluster_name)]);
        defer! {
            with_metric!(clusters::UPSTREAM_RQ_ACTIVE, sub, 1, _shard_id, &[KeyValue::new("cluster", self.cluster_name)]);
        }

        let RequestContext { route_timeout, retry_policy } = ctx;

        let mut retries = Retries::default();
        let start_time = std::time::Instant::now();
        let result = instrument_block!(
            _trans_handler.clock,
            |nanos| {
                crate::instrumentation::SEND_REQUEST_WAIT_RESPONSE.observe(nanos as usize);
            },
            {
                self.send_request(request, route_timeout, retry_policy, Some(&mut retries), &_trans_handler.clock).await
            }
        );

        if result.is_ok() {
            with_metric!(
                clusters::UPSTREAM_RQ_TOTAL,
                add,
                1,
                _shard_id,
                &[KeyValue::new("cluster", self.cluster_name)]
            );
        }

        with_metric!(
            clusters::UPSTREAM_RQ_RETRY,
            add,
            retries.requests as u64,
            _shard_id,
            &[KeyValue::new("cluster", self.cluster_name)]
        );

        with_metric!(
            clusters::UPSTREAM_RQ_PER_TRY_TIMEOUT,
            add,
            retries.timeouts as u64,
            _shard_id,
            &[KeyValue::new("cluster", self.cluster_name)]
        );

        HttpChannel::map_upstream_result(result, start_time.elapsed(), route_timeout, version)
    }
}

impl HttpChannel {
    pub async fn send_request(
        &self,
        mut request: Request<OrionRequestBody>,
        timeout: Option<Duration>,
        retry_policy: Option<&RetryPolicy>,
        output: Option<&mut Retries>,
        #[cfg(feature = "instrumentation")] clock: &quanta::Clock,
    ) -> Result<Response<Incoming>> {
        match &self.channel_client {
            HttpChannelClient::Plain(sender) => {
                let client = sender.get_or_build();
                let req = maybe_normalize_uri(request, false)?;
                if let Some(t) = timeout {
                    fast_timeout(
                        t,
                        self.send_with_policy(
                            req,
                            retry_policy,
                            client,
                            output,
                            #[cfg(feature = "instrumentation")]
                            clock,
                        ),
                    )
                    .await?
                } else {
                    self.send_with_policy(
                        req,
                        retry_policy,
                        client,
                        output,
                        #[cfg(feature = "instrumentation")]
                        clock,
                    )
                    .await
                }
            },
            HttpChannelClient::Tls(context) => {
                let ClientContext { configured_upstream_http_version, client: sender } = context;
                let configured_version = *configured_upstream_http_version;
                let client = sender.get_or_build();
                let req = maybe_normalize_uri(request, true)?;
                //FIXME(hayley): apply http protocol translation for plaintext too
                let req = maybe_change_http_protocol_version(req, configured_version)?;

                if let Some(t) = timeout {
                    fast_timeout(
                        t,
                        self.send_with_policy(
                            req,
                            retry_policy,
                            client,
                            output,
                            #[cfg(feature = "instrumentation")]
                            clock,
                        ),
                    )
                    .await?
                } else {
                    self.send_with_policy(
                        req,
                        retry_policy,
                        client,
                        output,
                        #[cfg(feature = "instrumentation")]
                        clock,
                    )
                    .await
                }
            },
            HttpChannelClient::Unix(uri, sender) => {
                let client = sender;
                *request.uri_mut() = uri.clone();

                if let Some(t) = timeout {
                    fast_timeout(
                        t,
                        self.send_with_policy(
                            request,
                            retry_policy,
                            client,
                            output,
                            #[cfg(feature = "instrumentation")]
                            clock,
                        ),
                    )
                    .await?
                } else {
                    self.send_with_policy(
                        request,
                        retry_policy,
                        client,
                        output,
                        #[cfg(feature = "instrumentation")]
                        clock,
                    )
                    .await
                }
            },
        }
    }

    /// Send the request and return the Result, either the Response or an Error,
    /// along with the time spent for possible retransmissions. Note: the returned
    /// duration does not include the time spent receiving the Body of the Response.
    async fn send_with_policy<C>(
        &self,
        mut req: Request<OrionRequestBody>,
        retry_policy: Option<&RetryPolicy>,
        sender: &Client<C, OrionRequestBody>,
        output: Option<&mut Retries>,
        #[cfg(feature = "instrumentation")] clock: &quanta::Clock,
    ) -> Result<Response<Incoming>>
    where
        C: Connect + Clone + Send + Sync + 'static,
    {
        if !self.enable_trailers {
            strip_trailers_headers(self.http_version, req.headers_mut());
        }

        match retry_policy {
            Some(policy) if policy.is_retriable(&req) => {
                self.send_with_retry(
                    req,
                    policy,
                    sender,
                    output,
                    #[cfg(feature = "instrumentation")]
                    clock,
                )
                .await
            },
            _ => {
                instrument_block!(
                    clock,
                    |nanos| {
                        crate::instrumentation::SEND_REQUEST.observe(nanos as usize);
                    },
                    { sender.request(req).await.map_err(Error::from) }
                )
            },
        }
    }

    async fn send_with_retry<C>(
        &self,
        req: Request<OrionRequestBody>,
        retry_policy: &RetryPolicy,
        sender: &Client<C, OrionRequestBody>,
        mut output: Option<&mut Retries>,
        #[cfg(feature = "instrumentation")] clock: &quanta::Clock,
    ) -> Result<Response<Incoming>>
    where
        C: Connect + Clone + Send + Sync + 'static,
    {
        instrument_function!(clock, |nanos| {
            crate::instrumentation::SEND_REQUEST_WITH_RETRY.observe(nanos as usize)
        });

        let (parts, body) = req.into_parts();
        let InstrumentedBody { inner, guard, state } = body;

        let body = inner.collect().await?;
        let body = http_body_util::Full::new(body.to_bytes());
        let mut last_error: Option<Error> = None;

        for (index, back_off) in retry_policy.exponential_back_off().iter().enumerate() {
            let back_off = back_off.unwrap_or(Duration::from_secs(1));

            let cloned_body = InstrumentedBody {
                inner: TimeoutBody::new(None, body.clone().into()),
                guard: guard.clone(),
                state: state.clone(),
            };

            let cloned_req: Request<OrionRequestBody> = Request::from_parts(parts.clone(), cloned_body);

            // actually send the request and wait for the response...
            let result: StdResult<Response<Incoming>, Error> = if let Some(t) = retry_policy.per_try_timeout() {
                match fast_timeout(t, sender.request(cloned_req)).await.map_err(|_| EventError::PerTryTimeout) {
                    Ok(result) => result.map_err(Into::into),
                    Err(err) => Err(err.into()),
                }
            } else {
                sender.request(cloned_req).await.map_err(Into::into)
            };

            // generate a possible retry condition...
            let Some(condition) = RetryCondition::try_infer_from(&result) else {
                return result;
            };

            if condition.is_per_try_timeout() {
                output.as_mut().map(|output| output.timeouts += 1);
            }

            // check for a possible retry...
            if !condition.should_retry(retry_policy) {
                return result;
            }

            output.as_mut().map(|output| output.requests += 1);

            // take an exponential back off break and retry...
            if index < retry_policy.num_retries() as usize {
                debug!(
                    "retry_policy: retrying request #{}/{} in {}...",
                    index + 1,
                    retry_policy.num_retries(),
                    pretty_duration(&back_off, None)
                );

                pingora_timeout::sleep(back_off).await;
            }

            last_error = Some(result.err().unwrap());
        }

        match last_error {
            Some(err) => Err(err),
            None => Err(std::io::Error::new(ErrorKind::InvalidData, "invalid retry_policy configuration").into()),
        }
    }

    fn map_upstream_result(
        result: std::result::Result<Response<Incoming>, Error>,
        elapsed: Duration,
        route_timeout: Option<Duration>,
        version: http::Version,
    ) -> StdResult<hyper::Response<OrionResponseBody>, Error> {
        match (result, elapsed) {
            (Ok(response), elapsed) => {
                // calculate the remaining timeout (relative to the route timeout) for receiving
                // the body of the incoming response...
                if let Some(residual_timeout) = route_timeout.map(|dur| dur.checked_sub(elapsed).unwrap_or_default()) {
                    // set the residual_timeout on the body of the Response
                    let (parts, body) = response.into_parts();
                    Ok(Response::from_parts(parts, TimeoutBody::new(Some(residual_timeout), body.into())))
                } else {
                    let (parts, body) = response.into_parts();
                    Ok(Response::from_parts(parts, TimeoutBody::new(None, body.into())))
                }
            },
            (Err(err), dur) => {
                if let Some(event_error) = EventError::try_infer_from(err.as_ref()) {
                    let response_flags: ResponseFlags = event_error.clone().into();
                    debug!(
                        "Event ({event_error}) occurred after {:?}: {} ({})",
                        pretty_duration(&dur, None),
                        ResponseFlagsLong(&response_flags.0).to_smolstr(),
                        ResponseFlagsShort(&response_flags.0).to_smolstr()
                    );

                    match event_error {
                        EventError::RefusedStream | EventError::IoError(_) | EventError::ConnectTimeout(_) => Ok(
                            SyntheticHttpResponse::service_unavailable(EventKind::Error(event_error), response_flags)
                                .into_response(version),
                        ),
                        EventError::PerTryTimeout | EventError::RouteTimeout => {
                            Ok(SyntheticHttpResponse::gateway_timeout(EventKind::Error(event_error), response_flags)
                                .into_response(version))
                        },
                        EventError::Reset | EventError::Http3PostConnectFailure => {
                            Ok(SyntheticHttpResponse::bad_gateway(EventKind::Error(event_error), response_flags)
                                .into_response(version))
                        },
                    }
                } else {
                    debug!("Route: error occurred after {:?}: {err}", pretty_duration(&dur, None));
                    Err(err)
                }
            },
        }
    }

    pub fn is_https(&self) -> bool {
        match &self.channel_client {
            HttpChannelClient::Plain(_) => false,
            HttpChannelClient::Tls(_) => true,
            HttpChannelClient::Unix(_, _) => false,
        }
    }

    pub fn http_version(&self) -> Codec {
        self.http_version
    }

    pub fn load(&self) -> u32 {
        let load = match &self.channel_client {
            HttpChannelClient::Plain(sender) => Arc::strong_count(sender.get_or_build()),
            HttpChannelClient::Tls(sender) => Arc::strong_count(sender.client.get_or_build()),
            HttpChannelClient::Unix(_, sender) => Arc::strong_count(sender),
        };
        u32::try_from(load).unwrap_or(u32::MAX)
    }
}

#[inline]
fn is_absolute(uri: &Uri) -> bool {
    uri.authority().is_some() && uri.scheme().is_some()
}

fn maybe_change_http_protocol_version(
    request: Request<OrionRequestBody>,
    version: Codec,
) -> Result<Request<OrionRequestBody>> {
    let request = maybe_update_host(request, version)?;
    Ok(maybe_rewrite_version(request, version))
}

fn maybe_rewrite_version(mut request: Request<OrionRequestBody>, version: Codec) -> Request<OrionRequestBody> {
    *request.version_mut() = version.into();
    request
}

fn maybe_update_host(mut request: Request<OrionRequestBody>, version: Codec) -> Result<Request<OrionRequestBody>> {
    let request_version = request.version();
    match (request_version, version) {
        (Version::HTTP_11, Codec::Http2) => {
            let headers = request.headers_mut();
            headers.remove(http::header::HOST);
        },
        (Version::HTTP_2, Codec::Http1) => {
            if let Some(authority) = request.uri().authority().cloned() {
                debug!("Swapping authority/host (http2 -> http1)");
                request.headers_mut().append(http::header::HOST, HeaderValue::from_str(authority.as_str())?);
            }
        },
        (Version::HTTP_11, Codec::Http1) | (Version::HTTP_2, Codec::Http2) => {},
        (v, _) => {
            return Err(format!("Unsupported http version {v:?}").into());
        },
    }
    Ok(request)
}

fn maybe_normalize_uri(
    mut request: Request<OrionRequestBody>,
    is_tls: bool,
) -> crate::Result<Request<OrionRequestBody>> {
    let uri = request.uri();
    if !is_absolute(uri) {
        if let Some(host_header) = request.headers().get("host") {
            let authority = host_header.to_str().map_err(|e| format!("Can't parse Host header {e:?}"))?;
            let authority = authority.parse::<Authority>().map_err(|e| format!("Can't parse uri {e:?}"))?;

            let uri = request.uri_mut();
            let mut parts = Parts::from(mem::take(uri));
            if parts.scheme.is_none() {
                parts.scheme = if is_tls { Some(http::uri::Scheme::HTTPS) } else { Some(http::uri::Scheme::HTTP) };
            }
            parts.authority = Some(authority);
            let new = Uri::from_parts(parts).map_err(|_| format!("Can't normalize uri: {uri}"))?;
            *uri = new;
        }
    }
    Ok(request)
}
