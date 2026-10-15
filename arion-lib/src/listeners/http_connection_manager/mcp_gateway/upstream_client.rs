// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct task-local MCP Streamable HTTP client and `rmcp` `StreamableHttpClient` adapter
//! backed by the gateway's clusters.
//!
//! Every request goes through the `HttpChannel` of the configured cluster, so endpoints, TLS
//! context, HTTP/1 or HTTP/2 codec, connection pools and circuit breaker all come from the cluster
//! configuration. The MCP `url` only provides the request path (and query).
//!
//! [`McpUpstreamClient`] executes all MCP client operations (`initialize`, `tools/call`,
//! `tools/list`, `prompts/*`, `resources/*`, `ping`, notifications, etc.) directly in the caller's
//! Tokio task without spawning background worker tasks or hopping across `mpsc`/`oneshot` channels.

use crate::{
    ArionRequestBody, ArionResponseBody,
    body::{
        instrumented_body::InstrumentedBody, poly_body::PolyBody, response_flags::BodyKind, timeout_body::TimeoutBody,
    },
    clusters::{RoutingPriority, clusters_manager, clusters_manager::RoutingContext},
};
use arc_swap::ArcSwap;
use arion_http_header::{LAST_EVENT_ID, MCP_PROTOCOL_VERSION, MCP_SESSION_ID};
use arion_interner::{InternedStr, StringInterner};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{Stream, StreamExt, stream::BoxStream};
use http::{
    HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HOST, WWW_AUTHENTICATE},
    uri::Scheme,
};
use http_body_util::{BodyExt, Empty, Full};
use pin_project::pin_project;
#[allow(deprecated)]
use rmcp::model::SetLevelRequestParams;
use rmcp::{
    RoleClient, ServiceError,
    model::{
        CallToolRequestMethod, CallToolRequestParams, CallToolResult, CancelTaskParams, ClientJsonRpcMessage,
        ClientNotification, ClientRequest, CompleteRequestMethod, CompleteRequestParams, CompleteResult, ConstString,
        DiscoverRequestMethod, DiscoverResult, EmptyResult, ErrorData, GetPromptRequestMethod, GetPromptRequestParams,
        GetPromptResult, GetTaskParams, GetTaskResult, InitializeRequestParams, InitializeResult,
        InitializeResultMethod, InitializedNotificationMethod, JsonObject, JsonRpcMessage, JsonRpcVersion2_0,
        ListPromptsRequestMethod, ListPromptsResult, ListResourceTemplatesRequestMethod, ListResourceTemplatesResult,
        ListResourcesRequestMethod, ListResourcesResult, ListToolsRequestMethod, ListToolsResult,
        PaginatedRequestParams, PingRequestMethod, Prompt, ProtocolVersion, ReadResourceRequestMethod,
        ReadResourceRequestParams, ReadResourceResult, RequestId, Resource, ResourceTemplate, ServerJsonRpcMessage,
        ServerResult, SetLevelRequestMethod, SubscribeRequestMethod, SubscribeRequestParams,
        SubscriptionsListenRequestMethod, SubscriptionsListenResult, TaskAckResult, Tool, UnsubscribeRequestMethod,
        UnsubscribeRequestParams, UpdateTaskParams,
    },
    service::ClientInitializeError,
    transport::{
        DynamicTransportError,
        streamable_http_client::{
            AuthRequiredError, InsufficientScopeError, StreamableHttpClient, StreamableHttpError,
            StreamableHttpPostResponse,
        },
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use smol_str::SmolStr;
use sse_stream::{Error as SseError, Sse, SseStream};
use std::{
    borrow::Cow,
    collections::HashMap,
    pin::Pin,
    str::FromStr,
    sync::{
        Arc as StdArc,
        atomic::{AtomicI64, Ordering},
    },
    task::{Poll, ready},
};

const EVENT_STREAM_MIME_TYPE: &str = "text/event-stream";
const JSON_MIME_TYPE: &str = "application/json";
const ACCEPT_EVENT_STREAM_AND_JSON: HeaderValue = HeaderValue::from_static("text/event-stream, application/json");
const CONTENT_TYPE_JSON: HeaderValue = HeaderValue::from_static(JSON_MIME_TYPE);
const DEFAULT_PROTOCOL_VERSION_HEADER: HeaderValue = HeaderValue::from_static("2025-03-26");

/// Maximum raw size of one SSE event accepted from an upstream server (same as rmcp's default).
const DEFAULT_MAX_SSE_EVENT_SIZE: usize = 16 * 1024 * 1024;
const JSON_RPC_BODY_PREVIEW_LEN: usize = 256;

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpstreamClientError {
    #[error("invalid MCP upstream url '{url}': {reason}")]
    InvalidUrl { url: String, reason: String },
    #[error("cluster '{cluster}' unavailable: {source}")]
    Cluster {
        cluster: InternedStr,
        #[source]
        source: Box<crate::Error>,
    },
    #[error("upstream request failed: {0}")]
    Upstream(#[source] Box<crate::Error>),
    #[error("failed reading upstream response body: {0}")]
    Body(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("invalid request: {0}")]
    Request(#[from] http::Error),
    #[error("json encode error: {0}")]
    Json(#[from] serde_json::Error),
}

type ClientError = StreamableHttpError<UpstreamClientError>;

/// `StreamableHttpClient` sending MCP traffic through the `HttpChannel` of a cluster.
///
/// The client is bound to a single endpoint: the `uri` argument of the trait methods is
/// ignored in favor of the target parsed once at construction. The `Host` header is taken from
/// the MCP url; `HttpChannel::send_request` rewrites it into `:authority` for HTTP/2.
#[derive(Clone, Debug)]
pub(crate) struct McpHttpClient {
    cluster_id: InternedStr,
    host_header: HeaderValue,
    target: Uri,
    #[cfg(feature = "instrumentation")]
    clock: quanta::Clock,
}

impl McpHttpClient {
    pub(crate) fn try_new(cluster: &str, url: &str) -> Result<Self, UpstreamClientError> {
        let invalid = |reason: String| UpstreamClientError::InvalidUrl { url: url.to_owned(), reason };

        let uri = Uri::from_str(url).map_err(|e| invalid(e.to_string()))?;
        let parts = uri.into_parts();
        match parts.scheme {
            Some(ref s) if *s == Scheme::HTTPS || *s == Scheme::HTTP => {},
            _ => return Err(invalid("scheme must be http or https".to_owned())),
        }
        let authority = parts.authority.ok_or_else(|| invalid("missing host".to_owned()))?;
        let host_header = HeaderValue::from_str(authority.as_str()).map_err(|e| invalid(e.to_string()))?;
        let target = parts.path_and_query.map_or(Uri::from_static("/"), Uri::from);

        Ok(Self {
            cluster_id: cluster.to_interned_str(),
            host_header,
            target,
            #[cfg(feature = "instrumentation")]
            clock: quanta::Clock::new(),
        })
    }

    /// Fast-path POST request builder for [`McpUpstreamClient`]: uses pre-validated
    /// [`HeaderValue`]s for `Mcp-Session-Id` and `MCP-Protocol-Version` with zero heap
    /// allocations and no `HashMap` iteration.
    #[inline]
    fn build_direct_post_request(
        &self,
        session_id: Option<&HeaderValue>,
        protocol_version: Option<&HeaderValue>,
        payload: Bytes,
    ) -> Request<ArionRequestBody> {
        let mut request = Request::new(request_body(Some(payload)));
        *request.method_mut() = Method::POST;
        *request.uri_mut() = self.target.clone();

        let headers = request.headers_mut();
        headers.reserve(5);
        headers.insert(HOST, self.host_header.clone());
        headers.insert(ACCEPT, ACCEPT_EVENT_STREAM_AND_JSON);
        headers.insert(CONTENT_TYPE, CONTENT_TYPE_JSON);
        if let Some(pv) = protocol_version {
            headers.insert(MCP_PROTOCOL_VERSION, pv.clone());
        }
        if let Some(sid) = session_id {
            headers.insert(MCP_SESSION_ID, sid.clone());
        }
        request
    }

    #[allow(clippy::too_many_arguments)]
    fn build_request(
        &self,
        method: Method,
        accept: bool,
        content_type: Option<HeaderValue>,
        session_id: Option<&str>,
        last_event_id: Option<&str>,
        auth_token: Option<&str>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        payload: Option<Bytes>,
    ) -> Result<Request<ArionRequestBody>, ClientError> {
        let mut request = Request::new(request_body(payload));
        *request.method_mut() = method;
        *request.uri_mut() = self.target.clone();

        let headers = request.headers_mut();
        headers.reserve(custom_headers.len() + 5);
        headers.insert(HOST, self.host_header.clone());
        if accept {
            headers.insert(ACCEPT, ACCEPT_EVENT_STREAM_AND_JSON);
        }
        if let Some(ct) = content_type {
            headers.insert(CONTENT_TYPE, ct);
        }
        if let Some(token) = auth_token {
            let mut buf = BytesMut::with_capacity("Bearer ".len() + token.len());
            buf.put_slice(b"Bearer ");
            buf.put_slice(token.as_bytes());
            let value = HeaderValue::from_maybe_shared(buf.freeze())
                .map_err(|e| StreamableHttpError::Client(UpstreamClientError::Request(e.into())))?;
            headers.insert(AUTHORIZATION, value);
        }
        for (name, value) in custom_headers {
            validate_custom_header(&name).map_err(StreamableHttpError::ReservedHeaderConflict)?;
            headers.insert(name, value);
        }
        if let Some(session_id) = session_id {
            let value = HeaderValue::from_str(session_id)
                .map_err(|e| StreamableHttpError::Client(UpstreamClientError::Request(e.into())))?;
            headers.insert(MCP_SESSION_ID, value);
        }
        if let Some(last_event_id) = last_event_id {
            let value = HeaderValue::from_str(last_event_id)
                .map_err(|e| StreamableHttpError::Client(UpstreamClientError::Request(e.into())))?;
            headers.insert(LAST_EVENT_ID, value);
        }
        Ok(request)
    }

    /// Resolve the cluster channel and send. The cluster lookup happens on every call: the
    /// cluster map is thread-local and may be replaced by xDS updates while the client lives.
    async fn send(&self, request: Request<ArionRequestBody>) -> Result<Response<ArionResponseBody>, ClientError> {
        let channels = clusters_manager::get_http_connection(&self.cluster_id, RoutingContext::None).map_err(|e| {
            StreamableHttpError::Client(UpstreamClientError::Cluster { cluster: self.cluster_id, source: Box::new(e) })
        })?;
        channels
            .channel()
            .send_request(
                request,
                None,
                None,
                RoutingPriority::Default,
                None,
                #[cfg(feature = "instrumentation")]
                &self.clock,
            )
            .await
            .map_err(|e| StreamableHttpError::Client(UpstreamClientError::Upstream(Box::new(e))))
    }
}

/// Pre-validated headers negotiated during the MCP `initialize` handshake.
#[derive(Debug)]
struct UpstreamSessionState {
    session_id: Option<HeaderValue>,
    protocol_version: HeaderValue,
}

/// Direct, zero-spawn MCP Streamable HTTP client.
///
/// Unlike `rmcp`'s `RunningService<RoleClient, ...>` + `StreamableHttpClientWorker` (which spawns
/// multiple background tasks per session and per request and hops across `mpsc`/`oneshot`
/// channels), `McpUpstreamClient` executes every request and SSE stream directly inside the
/// current Tokio task on the current worker thread.
pub struct McpUpstreamClient {
    http: McpHttpClient,
    client_info: Box<InitializeRequestParams>,
    peer_info: Box<InitializeResult>,
    state: ArcSwap<UpstreamSessionState>,
    next_id: AtomicI64,
    reinit_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for McpUpstreamClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.load();
        f.debug_struct("McpUpstreamClient")
            .field("http", &self.http)
            .field("session_id", &state.session_id)
            .field("protocol_version", &state.protocol_version)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct OutboundJsonRpcRequest<'a, P: Serialize> {
    jsonrpc: JsonRpcVersion2_0,
    id: i64,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<P>,
}

#[derive(Serialize)]
struct OutboundJsonRpcNotification<'a, P: Serialize> {
    jsonrpc: JsonRpcVersion2_0,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<P>,
}

#[derive(Serialize)]
struct BorrowedCallToolParams<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    arguments: Option<&'a JsonObject>,
}

/// Lightweight response ID that deserializes directly from JSON integers or strings without
/// allocating an intermediate `serde_json::Value`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DirectResponseId {
    Number(i64),
    String(SmolStr),
}

impl DirectResponseId {
    #[inline]
    fn matches_i64(&self, expected: i64) -> bool {
        match self {
            Self::Number(n) => *n == expected,
            Self::String(s) => s.parse::<i64>() == Ok(expected),
        }
    }
}

impl<'de> Deserialize<'de> for DirectResponseId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdVisitor;

        impl serde::de::Visitor<'_> for IdVisitor {
            type Value = DirectResponseId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON-RPC request id (integer or string)")
            }

            #[inline]
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(DirectResponseId::Number(v))
            }

            #[inline]
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                i64::try_from(v)
                    .map(DirectResponseId::Number)
                    .map_err(|_overflow| E::custom("JSON-RPC id out of i64 range"))
            }

            #[inline]
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(DirectResponseId::String(SmolStr::new(v)))
            }

            #[inline]
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(DirectResponseId::String(SmolStr::from(v)))
            }
        }

        deserializer.deserialize_any(IdVisitor)
    }
}

/// Single-pass JSON-RPC response/error envelope for a known result type `R`.
///
/// Unlike `rmcp::model::ServerJsonRpcMessage` (which uses nested `#[serde(untagged)]` enums and
/// buffers the entire JSON into `serde::Content` twice while trying 18 `ServerResult` variants),
/// this struct deserializes `R` directly from the input slice in one pass.
#[derive(Deserialize)]
#[serde(bound(deserialize = "R: Deserialize<'de>"))]
struct DirectJsonRpcEnvelope<R> {
    #[serde(default)]
    id: Option<DirectResponseId>,
    #[serde(default)]
    result: Option<R>,
    #[serde(default)]
    error: Option<ErrorData>,
}

enum DirectRpcOutcome<R> {
    Result(R),
    RpcError(ErrorData),
}

impl McpUpstreamClient {
    /// Connect to the upstream MCP server at `(cluster, url)` and perform the `initialize` +
    /// `notifications/initialized` handshake directly in the current task.
    pub(crate) async fn connect(
        cluster: &str,
        url: &str,
        client_info: InitializeRequestParams,
    ) -> Result<Self, Box<ClientInitializeError>> {
        let http = McpHttpClient::try_new(cluster, url).map_err(|err| {
            Box::new(ClientInitializeError::TransportError {
                error: DynamicTransportError::from_parts(
                    "arion-cluster",
                    std::any::TypeId::of::<McpHttpClient>(),
                    Box::new(err),
                ),
                context: "failed to build upstream MCP http client".into(),
            })
        })?;

        let (peer_info, state) = Self::perform_handshake(&http, &client_info, 1).await?;

        Ok(Self {
            http,
            client_info: Box::new(client_info),
            peer_info: Box::new(peer_info),
            state: ArcSwap::from_pointee(state),
            next_id: AtomicI64::new(2),
            reinit_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Returns the server's [`InitializeResult`] received during the initial handshake.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn peer_info(&self) -> &InitializeResult {
        &self.peer_info
    }

    #[inline]
    fn next_request_id(&self) -> i64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn into_init_transport_error(error: ClientError, context: &'static str) -> Box<ClientInitializeError> {
        Box::new(ClientInitializeError::TransportError {
            error: DynamicTransportError::from_parts(
                "arion-streamable-http",
                std::any::TypeId::of::<McpHttpClient>(),
                Box::new(error),
            ),
            context: Cow::Borrowed(context),
        })
    }

    fn into_service_error(error: ClientError) -> ServiceError {
        ServiceError::TransportSend(DynamicTransportError::from_parts(
            "arion-streamable-http",
            std::any::TypeId::of::<McpHttpClient>(),
            Box::new(error),
        ))
    }

    fn protocol_version_to_header(version: &ProtocolVersion) -> HeaderValue {
        HeaderValue::from_str(version.as_str()).unwrap_or(DEFAULT_PROTOCOL_VERSION_HEADER)
    }

    fn encode_request<P: Serialize>(id: i64, method: &str, params: Option<P>) -> Result<Bytes, ClientError> {
        let mut buf = Vec::with_capacity(192);
        serde_json::to_writer(&mut buf, &OutboundJsonRpcRequest { jsonrpc: JsonRpcVersion2_0, id, method, params })
            .map_err(|e| StreamableHttpError::Client(UpstreamClientError::Json(e)))?;
        Ok(Bytes::from(buf))
    }

    fn encode_notification<P: Serialize>(method: &str, params: Option<P>) -> Result<Bytes, ClientError> {
        let mut buf = Vec::with_capacity(128);
        serde_json::to_writer(&mut buf, &OutboundJsonRpcNotification { jsonrpc: JsonRpcVersion2_0, method, params })
            .map_err(|e| StreamableHttpError::Client(UpstreamClientError::Json(e)))?;
        Ok(Bytes::from(buf))
    }

    #[inline(never)]
    async fn expect_handshake_boxed(
        http: &McpHttpClient,
        client_info: &InitializeRequestParams,
        init_id: i64,
    ) -> Result<(InitializeResult, UpstreamSessionState), Box<ClientInitializeError>> {
        Box::pin(Self::perform_handshake(http, client_info, init_id)).await
    }

    async fn perform_handshake(
        http: &McpHttpClient,
        client_info: &InitializeRequestParams,
        init_id: i64,
    ) -> Result<(InitializeResult, UpstreamSessionState), Box<ClientInitializeError>> {
        let init_payload = Self::encode_request(init_id, InitializeResultMethod::VALUE, Some(client_info))
            .map_err(|e| Self::into_init_transport_error(e, "encode initialize request"))?;

        let init_req = http.build_direct_post_request(None, None, init_payload);
        let response = http
            .send(init_req)
            .await
            .map_err(|e| Self::into_init_transport_error(e, "send initialize request"))?;

        let status = response.status();
        check_auth(status, response.headers())
            .map_err(|e| Self::into_init_transport_error(e, "send initialize request"))?;

        if !status.is_success() {
            let err = Self::read_error_response(response, status).await;
            return match err {
                Ok(error_data) => Err(Box::new(ClientInitializeError::JsonRpcError(error_data))),
                Err(client_err) => Err(Self::into_init_transport_error(client_err, "initialize error response")),
            };
        }

        let session_id = response.headers().get(MCP_SESSION_ID).cloned();
        let outcome = Self::decode_response_body::<InitializeResult>(response, init_id)
            .await
            .map_err(|e| Self::into_init_transport_error(e, "process initialize response"))?;

        let init_result = match outcome {
            DirectRpcOutcome::Result(res) => res,
            DirectRpcOutcome::RpcError(error_data) => {
                return Err(Box::new(ClientInitializeError::JsonRpcError(error_data)));
            },
        };

        let protocol_version = Self::protocol_version_to_header(&init_result.protocol_version);
        let state = UpstreamSessionState { session_id, protocol_version };

        let notif_payload = Self::encode_notification::<()>(InitializedNotificationMethod::VALUE, None)
            .map_err(|e| Self::into_init_transport_error(e, "encode initialized notification"))?;
        let notif_req =
            http.build_direct_post_request(state.session_id.as_ref(), Some(&state.protocol_version), notif_payload);
        let notif_resp = http
            .send(notif_req)
            .await
            .map_err(|e| Self::into_init_transport_error(e, "send initialized notification"))?;

        Self::expect_notification_response(notif_resp)
            .await
            .map_err(|e| Self::into_init_transport_error(e, "process initialized notification response"))?;

        Ok((init_result, state))
    }

    async fn reinitialize(
        &self,
        expired_state: &StdArc<UpstreamSessionState>,
    ) -> Result<StdArc<UpstreamSessionState>, ServiceError> {
        let _guard = self.reinit_lock.lock().await;
        let current = self.state.load_full();
        if !StdArc::ptr_eq(&current, expired_state) {
            return Ok(current);
        }

        let id = self.next_request_id();
        let (_, new_state) = Self::expect_handshake_boxed(&self.http, &self.client_info, id)
            .await
            .map_err(|err| {
                ServiceError::TransportSend(DynamicTransportError::from_parts(
                    "arion-streamable-http",
                    std::any::TypeId::of::<RoleClient>(),
                    err,
                ))
            })?;
        let new_state = StdArc::new(new_state);
        self.state.store(StdArc::clone(&new_state));
        Ok(new_state)
    }

    async fn expect_notification_response(response: Response<ArionResponseBody>) -> Result<(), ClientError> {
        let status = response.status();
        check_auth(status, response.headers())?;
        if matches!(status, StatusCode::ACCEPTED | StatusCode::NO_CONTENT) {
            return Ok(());
        }
        if status == StatusCode::NOT_FOUND {
            return Err(StreamableHttpError::SessionExpired);
        }
        if status.is_success() {
            return Ok(());
        }
        let body = collect_body(response.into_body()).await.map_or_else(
            |_| "<failed to read response body>".to_owned(),
            |b| String::from_utf8_lossy(&b).into_owned(),
        );
        Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!("HTTP {status}: {body}"))))
    }

    #[cold]
    async fn read_error_response(
        response: Response<ArionResponseBody>,
        status: StatusCode,
    ) -> Result<ErrorData, ClientError> {
        let is_json = header_str(response.headers(), &CONTENT_TYPE).is_some_and(|ct| ct.starts_with(JSON_MIME_TYPE));
        let body = collect_body(response.into_body())
            .await
            .unwrap_or_else(|_| Bytes::from_static(b"<failed to read response body>"));
        if is_json
            && let Ok(envelope) = serde_json::from_slice::<DirectJsonRpcEnvelope<serde::de::IgnoredAny>>(&body)
            && let Some(error) = envelope.error
        {
            return Ok(error);
        }
        let text = match String::from_utf8(body.into()) {
            Ok(s) => s,
            Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
        };
        Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!("HTTP {status}: {text}"))))
    }

    async fn decode_response_body<R: DeserializeOwned>(
        response: Response<ArionResponseBody>,
        expected_id: i64,
    ) -> Result<DirectRpcOutcome<R>, ClientError> {
        let content_type = header_str(response.headers(), &CONTENT_TYPE);
        if let Some(ct) = content_type {
            if ct.starts_with(JSON_MIME_TYPE) {
                let body = collect_body(response.into_body()).await?;
                let envelope = serde_json::from_slice::<DirectJsonRpcEnvelope<R>>(&body).map_err(|e| {
                    StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!(
                        "could not parse JSON response as JSON-RPC response: {e}: {}",
                        body_preview(&body)
                    )))
                })?;
                return Self::extract_envelope_outcome(envelope, expected_id);
            }
            if ct.starts_with(EVENT_STREAM_MIME_TYPE) {
                return Self::decode_sse_response_body::<R>(response.into_body(), expected_id).await;
            }
        }
        Err(StreamableHttpError::UnexpectedContentType(content_type.map(str::to_owned)))
    }

    /// Reads an SSE response stream directly on the current task's stack (without `BoxStream`
    /// allocation or `tokio::spawn`) until the response or error matching `expected_id` arrives.
    async fn decode_sse_response_body<R: DeserializeOwned>(
        body: ArionResponseBody,
        expected_id: i64,
    ) -> Result<DirectRpcOutcome<R>, ClientError> {
        let bounded = BoundedSseByteStream {
            inner: body.into_data_stream(),
            limiter: SseEventSizeLimiter::new(DEFAULT_MAX_SSE_EVENT_SIZE),
            failed: false,
        };
        let mut sse_stream = std::pin::pin!(SseStream::from_bytes_stream(bounded));

        while let Some(event) = sse_stream.next().await {
            let sse = event?;
            let is_message_event = matches!(sse.event.as_deref(), None | Some("" | "message"));
            if !is_message_event {
                continue;
            }
            let Some(data) = sse.data else {
                continue;
            };
            if data.trim().is_empty() {
                continue;
            }
            let envelope = match serde_json::from_str::<DirectJsonRpcEnvelope<R>>(&data) {
                Ok(env) => env,
                Err(e) => {
                    tracing::debug!(target: "mcp_gateway", "ignoring non-matching or malformed SSE JSON frame: {e}");
                    continue;
                },
            };
            if envelope.result.is_none() && envelope.error.is_none() {
                // Intermediate server notification or server-initiated request on the SSE stream.
                continue;
            }
            if let Some(ref got_id) = envelope.id
                && !got_id.matches_i64(expected_id)
            {
                continue;
            }
            return Self::extract_envelope_outcome(envelope, expected_id);
        }

        Err(StreamableHttpError::UnexpectedEndOfStream)
    }

    #[inline]
    fn extract_envelope_outcome<R>(
        envelope: DirectJsonRpcEnvelope<R>,
        expected_id: i64,
    ) -> Result<DirectRpcOutcome<R>, ClientError> {
        if let Some(error) = envelope.error {
            if let Some(ref got_id) = envelope.id
                && !got_id.matches_i64(expected_id)
            {
                return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                    "uncorrelated JSON-RPC error response id",
                )));
            }
            return Ok(DirectRpcOutcome::RpcError(error));
        }
        if let Some(result) = envelope.result {
            if !envelope.id.as_ref().is_some_and(|got| got.matches_i64(expected_id)) {
                return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                    "mismatched or missing JSON-RPC response id",
                )));
            }
            return Ok(DirectRpcOutcome::Result(result));
        }
        Err(StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
            "JSON-RPC response contained neither result nor error",
        )))
    }

    async fn post_request_once<R: DeserializeOwned>(
        &self,
        id: i64,
        payload: Bytes,
        state: &UpstreamSessionState,
    ) -> Result<DirectRpcOutcome<R>, ClientError> {
        let request =
            self.http.build_direct_post_request(state.session_id.as_ref(), Some(&state.protocol_version), payload);
        let response = self.http.send(request).await?;
        let status = response.status();

        check_auth(status, response.headers())?;
        if status == StatusCode::NOT_FOUND && state.session_id.is_some() {
            return Err(StreamableHttpError::SessionExpired);
        }
        if !status.is_success() {
            return Self::read_error_response(response, status).await.map(DirectRpcOutcome::RpcError);
        }
        Self::decode_response_body::<R>(response, id).await
    }

    async fn send_raw_request<R: DeserializeOwned>(&self, id: i64, payload: Bytes) -> Result<R, ServiceError> {
        let state = self.state.load_full();
        let outcome = match self.post_request_once::<R>(id, payload.clone(), &state).await {
            Err(StreamableHttpError::SessionExpired) => {
                let new_state = self.reinitialize(&state).await?;
                self.post_request_once::<R>(id, payload, &new_state)
                    .await
                    .map_err(Self::into_service_error)?
            },
            other => other.map_err(Self::into_service_error)?,
        };
        match outcome {
            DirectRpcOutcome::Result(result) => Ok(result),
            DirectRpcOutcome::RpcError(error) => Err(ServiceError::McpError(error)),
        }
    }

    async fn send_typed_request<P: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: Option<P>,
    ) -> Result<R, ServiceError> {
        let id = self.next_request_id();
        let payload = Self::encode_request(id, method, params).map_err(Self::into_service_error)?;
        self.send_raw_request::<R>(id, payload).await
    }

    /// Fast-path `tools/call` invocation that borrows `name` and `arguments` directly,
    /// avoiding `CallToolRequestParams` construction, `arguments` map cloning, and `rmcp`'s
    /// `WithMeta` intermediate `serde_json::Value` conversion.
    pub(crate) async fn call_tool_borrowed(
        &self,
        name: &str,
        arguments: Option<&JsonObject>,
    ) -> Result<CallToolResult, ServiceError> {
        self.send_typed_request(
            CallToolRequestMethod::VALUE,
            Some(BorrowedCallToolParams { name, arguments }),
        )
        .await
    }

    /// Invoke `tools/call` with owned [`CallToolRequestParams`].
    #[allow(dead_code)]
    pub(crate) async fn call_tool(&self, params: CallToolRequestParams) -> Result<CallToolResult, ServiceError> {
        if params.meta.is_none() && params.input_responses.is_none() && params.request_state.is_none() {
            return self.call_tool_borrowed(&params.name, params.arguments.as_ref()).await;
        }
        self.send_typed_request(CallToolRequestMethod::VALUE, Some(&params)).await
    }

    /// Invoke `tools/list`.
    pub(crate) async fn list_tools(
        &self,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListToolsResult, ServiceError> {
        self.send_typed_request(ListToolsRequestMethod::VALUE, params.as_ref()).await
    }

    /// Invoke `tools/list` across all pages until `next_cursor` is `None`.
    #[allow(dead_code)]
    pub(crate) async fn list_all_tools(&self) -> Result<Vec<Tool>, ServiceError> {
        let mut tools = Vec::new();
        let mut cursor = None;
        loop {
            let result = self.list_tools(Some(PaginatedRequestParams::default().with_cursor(cursor))).await?;
            tools.extend(result.tools);
            cursor = result.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(tools)
    }

    /// Invoke `prompts/list`.
    #[allow(dead_code)]
    pub(crate) async fn list_prompts(
        &self,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListPromptsResult, ServiceError> {
        self.send_typed_request(ListPromptsRequestMethod::VALUE, params.as_ref()).await
    }

    /// Invoke `prompts/list` across all pages until `next_cursor` is `None`.
    #[allow(dead_code)]
    pub(crate) async fn list_all_prompts(&self) -> Result<Vec<Prompt>, ServiceError> {
        let mut prompts = Vec::new();
        let mut cursor = None;
        loop {
            let result = self.list_prompts(Some(PaginatedRequestParams::default().with_cursor(cursor))).await?;
            prompts.extend(result.prompts);
            cursor = result.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(prompts)
    }

    /// Invoke `prompts/get`.
    #[allow(dead_code)]
    pub(crate) async fn get_prompt(&self, params: GetPromptRequestParams) -> Result<GetPromptResult, ServiceError> {
        self.send_typed_request(GetPromptRequestMethod::VALUE, Some(&params)).await
    }

    /// Invoke `resources/list`.
    #[allow(dead_code)]
    pub(crate) async fn list_resources(
        &self,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListResourcesResult, ServiceError> {
        self.send_typed_request(ListResourcesRequestMethod::VALUE, params.as_ref()).await
    }

    /// Invoke `resources/list` across all pages until `next_cursor` is `None`.
    #[allow(dead_code)]
    pub(crate) async fn list_all_resources(&self) -> Result<Vec<Resource>, ServiceError> {
        let mut resources = Vec::new();
        let mut cursor = None;
        loop {
            let result = self.list_resources(Some(PaginatedRequestParams::default().with_cursor(cursor))).await?;
            resources.extend(result.resources);
            cursor = result.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(resources)
    }

    /// Invoke `resources/templates/list`.
    #[allow(dead_code)]
    pub(crate) async fn list_resource_templates(
        &self,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListResourceTemplatesResult, ServiceError> {
        self.send_typed_request(ListResourceTemplatesRequestMethod::VALUE, params.as_ref()).await
    }

    /// Invoke `resources/templates/list` across all pages until `next_cursor` is `None`.
    #[allow(dead_code)]
    pub(crate) async fn list_all_resource_templates(&self) -> Result<Vec<ResourceTemplate>, ServiceError> {
        let mut templates = Vec::new();
        let mut cursor = None;
        loop {
            let result =
                self.list_resource_templates(Some(PaginatedRequestParams::default().with_cursor(cursor))).await?;
            templates.extend(result.resource_templates);
            cursor = result.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(templates)
    }

    /// Invoke `resources/read`.
    #[allow(dead_code)]
    pub(crate) async fn read_resource(
        &self,
        params: ReadResourceRequestParams,
    ) -> Result<ReadResourceResult, ServiceError> {
        self.send_typed_request(ReadResourceRequestMethod::VALUE, Some(&params)).await
    }

    /// Invoke `resources/subscribe`.
    #[allow(dead_code)]
    pub(crate) async fn subscribe(&self, params: SubscribeRequestParams) -> Result<(), ServiceError> {
        let _: EmptyResult = self.send_typed_request(SubscribeRequestMethod::VALUE, Some(&params)).await?;
        Ok(())
    }

    /// Invoke `resources/unsubscribe`.
    #[allow(dead_code)]
    pub(crate) async fn unsubscribe(&self, params: UnsubscribeRequestParams) -> Result<(), ServiceError> {
        let _: EmptyResult = self.send_typed_request(UnsubscribeRequestMethod::VALUE, Some(&params)).await?;
        Ok(())
    }

    /// Invoke `completion/complete`.
    #[allow(dead_code)]
    pub(crate) async fn complete(&self, params: CompleteRequestParams) -> Result<CompleteResult, ServiceError> {
        self.send_typed_request(CompleteRequestMethod::VALUE, Some(&params)).await
    }

    /// Invoke `logging/setLevel`.
    #[allow(dead_code, deprecated)]
    pub(crate) async fn set_level(&self, params: SetLevelRequestParams) -> Result<(), ServiceError> {
        let _: EmptyResult = self.send_typed_request(SetLevelRequestMethod::VALUE, Some(&params)).await?;
        Ok(())
    }

    /// Invoke `ping`.
    #[allow(dead_code)]
    pub(crate) async fn ping(&self) -> Result<(), ServiceError> {
        let _: EmptyResult = self.send_typed_request::<(), EmptyResult>(PingRequestMethod::VALUE, None).await?;
        Ok(())
    }

    /// Invoke `tasks/get`.
    #[allow(dead_code)]
    pub(crate) async fn get_task(&self, params: GetTaskParams) -> Result<GetTaskResult, ServiceError> {
        self.send_typed_request("tasks/get", Some(&params)).await
    }

    /// Invoke `tasks/update`.
    #[allow(dead_code)]
    pub(crate) async fn update_task(&self, params: UpdateTaskParams) -> Result<(), ServiceError> {
        let _: TaskAckResult = self.send_typed_request("tasks/update", Some(&params)).await?;
        Ok(())
    }

    /// Invoke `tasks/cancel`.
    #[allow(dead_code)]
    pub(crate) async fn cancel_task(&self, params: CancelTaskParams) -> Result<(), ServiceError> {
        let _: TaskAckResult = self.send_typed_request("tasks/cancel", Some(&params)).await?;
        Ok(())
    }

    /// Send any [`ClientRequest`] and decode its matching [`ServerResult`] variant directly
    /// without going through `#[serde(untagged)] ServerResult`.
    #[allow(dead_code)]
    pub(crate) async fn send_request(&self, request: ClientRequest) -> Result<ServerResult, ServiceError> {
        match request {
            ClientRequest::PingRequest(_) => {
                self.ping().await?;
                Ok(ServerResult::EmptyResult(EmptyResult {}))
            },
            ClientRequest::InitializeRequest(req) => {
                let res: InitializeResult =
                    self.send_typed_request(InitializeResultMethod::VALUE, Some(&req.params)).await?;
                Ok(ServerResult::InitializeResult(res))
            },
            ClientRequest::DiscoverRequest(req) => {
                let res: DiscoverResult =
                    self.send_typed_request(DiscoverRequestMethod::VALUE, Some(&req.params)).await?;
                Ok(ServerResult::DiscoverResult(res))
            },
            ClientRequest::CompleteRequest(req) => {
                self.complete(req.params).await.map(ServerResult::CompleteResult)
            },
            ClientRequest::SetLevelRequest(req) => {
                self.set_level(req.params).await?;
                Ok(ServerResult::EmptyResult(EmptyResult {}))
            },
            ClientRequest::GetPromptRequest(req) => {
                self.get_prompt(req.params).await.map(ServerResult::GetPromptResult)
            },
            ClientRequest::ListPromptsRequest(req) => {
                self.list_prompts(req.params).await.map(ServerResult::ListPromptsResult)
            },
            ClientRequest::ListResourcesRequest(req) => {
                self.list_resources(req.params).await.map(ServerResult::ListResourcesResult)
            },
            ClientRequest::ListResourceTemplatesRequest(req) => {
                self.list_resource_templates(req.params).await.map(ServerResult::ListResourceTemplatesResult)
            },
            ClientRequest::ReadResourceRequest(req) => {
                self.read_resource(req.params).await.map(ServerResult::ReadResourceResult)
            },
            ClientRequest::SubscriptionsListenRequest(req) => {
                let res: SubscriptionsListenResult =
                    self.send_typed_request(SubscriptionsListenRequestMethod::VALUE, Some(&req.params)).await?;
                Ok(ServerResult::SubscriptionsListenResult(res))
            },
            ClientRequest::SubscribeRequest(req) => {
                self.subscribe(req.params).await?;
                Ok(ServerResult::EmptyResult(EmptyResult {}))
            },
            ClientRequest::UnsubscribeRequest(req) => {
                self.unsubscribe(req.params).await?;
                Ok(ServerResult::EmptyResult(EmptyResult {}))
            },
            ClientRequest::CallToolRequest(req) => {
                self.call_tool(req.params).await.map(ServerResult::CallToolResult)
            },
            ClientRequest::ListToolsRequest(req) => {
                self.list_tools(req.params).await.map(ServerResult::ListToolsResult)
            },
            ClientRequest::GetTaskRequest(req) => {
                self.get_task(req.params).await.map(ServerResult::GetTaskResult)
            },
            ClientRequest::UpdateTaskRequest(req) => {
                self.update_task(req.params).await?;
                Ok(ServerResult::TaskAckResult(TaskAckResult::new()))
            },
            ClientRequest::CancelTaskRequest(req) => {
                self.cancel_task(req.params).await?;
                Ok(ServerResult::TaskAckResult(TaskAckResult::new()))
            },
            ClientRequest::CustomRequest(req) => {
                let res: Value = self.send_typed_request(&req.method, req.params.as_ref()).await?;
                Ok(ServerResult::CustomResult(rmcp::model::CustomResult(res)))
            },
            _ => {
                let id = self.next_request_id();
                let message = ClientJsonRpcMessage::request(request, RequestId::Number(id));
                let payload = Bytes::from(
                    serde_json::to_vec(&message)
                        .map_err(|e| Self::into_service_error(StreamableHttpError::Client(UpstreamClientError::Json(e))))?,
                );
                self.send_raw_request::<ServerResult>(id, payload).await
            },
        }
    }

    /// Send any [`ClientNotification`] directly in the current task.
    #[allow(dead_code)]
    pub(crate) async fn send_notification(&self, notification: ClientNotification) -> Result<(), ServiceError> {
        let message = ClientJsonRpcMessage::notification(notification);
        let payload = Bytes::from(
            serde_json::to_vec(&message)
                .map_err(|e| Self::into_service_error(StreamableHttpError::Client(UpstreamClientError::Json(e))))?,
        );
        let state = self.state.load_full();
        let request = self.http.build_direct_post_request(
            state.session_id.as_ref(),
            Some(&state.protocol_version),
            payload.clone(),
        );
        let response = self.http.send(request).await.map_err(Self::into_service_error)?;
        match Self::expect_notification_response(response).await {
            Err(StreamableHttpError::SessionExpired) if state.session_id.is_some() => {
                let new_state = self.reinitialize(&state).await?;
                let retry_req = self.http.build_direct_post_request(
                    new_state.session_id.as_ref(),
                    Some(&new_state.protocol_version),
                    payload,
                );
                let retry_resp = self.http.send(retry_req).await.map_err(Self::into_service_error)?;
                Self::expect_notification_response(retry_resp).await.map_err(Self::into_service_error)
            },
            other => other.map_err(Self::into_service_error),
        }
    }
}

#[inline]
fn request_body(payload: Option<Bytes>) -> ArionRequestBody {
    let body = match payload {
        Some(bytes) => PolyBody::from(Full::new(bytes)),
        None => PolyBody::from(Empty::<Bytes>::new()),
    };
    InstrumentedBody::new(BodyKind::Request, TimeoutBody::new(None, body), None, |_, _, _, _| {})
}

async fn collect_body(body: ArionResponseBody) -> Result<Bytes, ClientError> {
    body.collect()
        .await
        .map(http_body_util::Collected::to_bytes)
        .map_err(|e| StreamableHttpError::Client(UpstreamClientError::Body(Box::new(e))))
}

#[inline]
fn header_str<'a>(headers: &'a http::HeaderMap, name: &HeaderName) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

#[inline]
fn is_zero_content_length(headers: &http::HeaderMap) -> bool {
    header_str(headers, &CONTENT_LENGTH).and_then(|v| v.parse::<u64>().ok()) == Some(0)
}

#[inline]
fn is_notification_like(message: &ClientJsonRpcMessage) -> bool {
    matches!(
        message,
        ClientJsonRpcMessage::Notification(_) | ClientJsonRpcMessage::Response(_) | ClientJsonRpcMessage::Error(_)
    )
}

/// Maps 401 / 403 carrying a `WWW-Authenticate` challenge to the matching rmcp errors.
#[inline]
fn check_auth(status: StatusCode, headers: &http::HeaderMap) -> Result<(), ClientError> {
    if status != StatusCode::UNAUTHORIZED && status != StatusCode::FORBIDDEN {
        return Ok(());
    }
    check_auth_slow(status, headers)
}

#[cold]
fn check_auth_slow(status: StatusCode, headers: &http::HeaderMap) -> Result<(), ClientError> {
    let Some(header) = headers.get(WWW_AUTHENTICATE) else {
        return Ok(());
    };
    let header = header.to_str().map_err(|_non_ascii| {
        StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed("invalid www-authenticate header value"))
    })?;
    if status == StatusCode::UNAUTHORIZED {
        Err(StreamableHttpError::AuthRequired(AuthRequiredError::new(header.to_owned())))
    } else {
        Err(StreamableHttpError::InsufficientScope(InsufficientScopeError::new(
            header.to_owned(),
            extract_scope_from_header(header),
        )))
    }
}

/// Rejects headers the transport manages itself; `MCP-Protocol-Version` is let through.
#[inline]
fn validate_custom_header(name: &HeaderName) -> Result<(), String> {
    if *name == MCP_PROTOCOL_VERSION {
        return Ok(());
    }
    if *name == ACCEPT || *name == MCP_SESSION_ID || *name == LAST_EVENT_ID {
        return Err(name.as_str().to_owned());
    }
    Ok(())
}

/// Extracts the `scope=` parameter of a `WWW-Authenticate` value (quoted or bare).
fn extract_scope_from_header(header: &str) -> Option<String> {
    const SCOPE_KEY: &[u8] = b"scope=";
    let pos = header.as_bytes().windows(SCOPE_KEY.len()).position(|w| w.eq_ignore_ascii_case(SCOPE_KEY))?;
    let value = header.get(pos + SCOPE_KEY.len()..)?;
    if let Some(quoted) = value.strip_prefix('"') {
        return quoted.split_once('"').map(|(scope, _)| scope.to_owned());
    }
    let scope = value.split(|c: char| c == ',' || c == ';' || c.is_whitespace()).next().unwrap_or_default();
    (!scope.is_empty()).then(|| scope.to_owned())
}

fn parse_json_rpc_error(body: &str) -> Option<ServerJsonRpcMessage> {
    match serde_json::from_str::<ServerJsonRpcMessage>(body) {
        Ok(message @ JsonRpcMessage::Error(_)) => Some(message),
        _ => None,
    }
}

fn body_preview(body: &[u8]) -> Cow<'_, str> {
    if body.is_empty() {
        return Cow::Borrowed("<empty>");
    }
    let slice = body.get(..body.len().min(JSON_RPC_BODY_PREVIEW_LEN)).unwrap_or(body);
    match std::str::from_utf8(slice) {
        Ok(s) => Cow::Borrowed(s),
        Err(e) if e.valid_up_to() > 0 => {
            // Safe slice up to the last valid UTF-8 boundary (handles multi-byte char split at 256).
            let valid = slice.get(..e.valid_up_to()).unwrap_or_default();
            String::from_utf8_lossy(valid)
        },
        Err(_) => String::from_utf8_lossy(slice),
    }
}

/// Handles non-2xx responses (including legacy sessionless `server/discover` rejections).
#[cold]
async fn handle_error_response(
    response: Response<ArionResponseBody>,
    status: StatusCode,
    discover_request_id: Option<RequestId>,
) -> Result<StreamableHttpPostResponse, ClientError> {
    let is_json = header_str(response.headers(), &CONTENT_TYPE).is_some_and(|ct| ct.starts_with(JSON_MIME_TYPE));
    let session_id = header_str(response.headers(), &MCP_SESSION_ID).map(str::to_owned);
    let body = collect_body(response.into_body()).await.map_or_else(
        |_| "<failed to read response body>".to_owned(),
        |b| match String::from_utf8(b.into()) {
            Ok(s) => s,
            Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
        },
    );
    if let Some(id) = discover_request_id
        && status.is_client_error()
        && !matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
    {
        let error = match serde_json::from_str::<ServerJsonRpcMessage>(&body) {
            Ok(ServerJsonRpcMessage::Error(error)) => error.error,
            _ => ErrorData::invalid_request(format!("server/discover rejected with HTTP {status}: {body}"), None),
        };
        return Ok(StreamableHttpPostResponse::Json(ServerJsonRpcMessage::error(error, Some(id)), None));
    }
    if is_json {
        if let Some(message) = parse_json_rpc_error(&body) {
            return Ok(StreamableHttpPostResponse::Json(message, session_id));
        }
        tracing::warn!(target: "mcp_gateway", "HTTP {status}: could not parse JSON body as a JSON-RPC error");
    }
    Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!("HTTP {status}: {body}"))))
}

fn sse_stream_of(body: ArionResponseBody, max_event_size: usize) -> BoxStream<'static, Result<Sse, SseError>> {
    bounded_sse_stream(body.into_data_stream(), max_event_size)
}

impl StreamableHttpClient for McpHttpClient {
    type Error = UpstreamClientError;

    async fn post_message(
        &self,
        uri: StdArc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<StdArc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, ClientError> {
        self.post_message_with_max_sse_event_size(
            uri,
            message,
            session_id,
            auth_header,
            custom_headers,
            DEFAULT_MAX_SSE_EVENT_SIZE,
        )
        .await
    }

    async fn post_message_with_max_sse_event_size(
        &self,
        _uri: StdArc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<StdArc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
    ) -> Result<StreamableHttpPostResponse, ClientError> {
        let payload =
            serde_json::to_vec(&message).map_err(|e| StreamableHttpError::Client(UpstreamClientError::Json(e)))?;
        let session_was_attached = session_id.is_some();
        let is_notification = is_notification_like(&message);
        let discover_request_id = match message {
            ClientJsonRpcMessage::Request(request)
                if !session_was_attached && matches!(request.request, ClientRequest::DiscoverRequest(_)) =>
            {
                Some(request.id)
            },
            _ => None,
        };

        let request = self.build_request(
            Method::POST,
            true,
            Some(CONTENT_TYPE_JSON),
            session_id.as_deref(),
            None,
            auth_header.as_deref(),
            custom_headers,
            Some(Bytes::from(payload)),
        )?;

        let response = self.send(request).await?;
        let status = response.status();

        check_auth(status, response.headers())?;
        if matches!(status, StatusCode::ACCEPTED | StatusCode::NO_CONTENT) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status == StatusCode::NOT_FOUND && session_was_attached {
            return Err(StreamableHttpError::SessionExpired);
        }

        if !status.is_success() {
            return handle_error_response(response, status, discover_request_id).await;
        }

        // The spec requires 202 for these, but some servers answer with an empty 200.
        if is_notification && is_zero_content_length(response.headers()) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }

        // rmcp only consumes the response session_id during sessionless initialization.
        let response_session_id = if session_was_attached {
            None
        } else {
            header_str(response.headers(), &MCP_SESSION_ID).map(str::to_owned)
        };

        let content_type = header_str(response.headers(), &CONTENT_TYPE);
        if let Some(ct) = content_type {
            if ct.starts_with(JSON_MIME_TYPE) {
                let body = collect_body(response.into_body()).await?;
                return match serde_json::from_slice::<ServerJsonRpcMessage>(&body) {
                    Ok(parsed) => Ok(StreamableHttpPostResponse::Json(parsed, response_session_id)),
                    Err(e) if is_notification => {
                        tracing::warn!(
                            target: "mcp_gateway",
                            "could not parse JSON response as ServerJsonRpcMessage, treating as accepted: {e}"
                        );
                        Ok(StreamableHttpPostResponse::Accepted)
                    },
                    Err(e) => Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!(
                        "could not parse JSON response as ServerJsonRpcMessage: {e}: {}",
                        body_preview(&body)
                    )))),
                };
            }
            if ct.starts_with(EVENT_STREAM_MIME_TYPE) {
                return Ok(StreamableHttpPostResponse::Sse(
                    sse_stream_of(response.into_body(), max_sse_event_size),
                    response_session_id,
                ));
            }
        }

        Err(StreamableHttpError::UnexpectedContentType(content_type.map(str::to_owned)))
    }

    async fn delete_session(
        &self,
        _uri: StdArc<str>,
        session_id: StdArc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), ClientError> {
        let request = self.build_request(
            Method::DELETE,
            false,
            None,
            Some(&session_id),
            None,
            auth_header.as_deref(),
            custom_headers,
            None,
        )?;

        let response = self.send(request).await?;
        let status = response.status();
        if status == StatusCode::METHOD_NOT_ALLOWED {
            tracing::debug!(target: "mcp_gateway", "this server doesn't support deleting session");
            return Ok(());
        }
        if !status.is_success() {
            return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!(
                "delete_session returned {status}"
            ))));
        }
        Ok(())
    }

    async fn get_stream(
        &self,
        uri: StdArc<str>,
        session_id: Option<StdArc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, ClientError> {
        self.get_stream_with_max_sse_event_size(
            uri,
            session_id,
            last_event_id,
            auth_header,
            custom_headers,
            DEFAULT_MAX_SSE_EVENT_SIZE,
        )
        .await
    }

    async fn get_stream_with_max_sse_event_size(
        &self,
        _uri: StdArc<str>,
        session_id: Option<StdArc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, ClientError> {
        let request = self.build_request(
            Method::GET,
            true,
            None,
            session_id.as_deref(),
            last_event_id.as_deref(),
            auth_header.as_deref(),
            custom_headers,
            None,
        )?;

        let response = self.send(request).await?;
        let status = response.status();

        if status == StatusCode::METHOD_NOT_ALLOWED {
            return Err(StreamableHttpError::ServerDoesNotSupportSse);
        }
        check_auth(status, response.headers())?;
        if !status.is_success() {
            return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!(
                "get_stream returned {status}"
            ))));
        }
        let content_type = header_str(response.headers(), &CONTENT_TYPE);
        match content_type {
            Some(ct) if ct.starts_with(EVENT_STREAM_MIME_TYPE) || ct.starts_with(JSON_MIME_TYPE) => {
                Ok(sse_stream_of(response.into_body(), max_sse_event_size))
            },
            other => Err(StreamableHttpError::UnexpectedContentType(other.map(str::to_owned))),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum BoundedSseStreamError {
    #[error(transparent)]
    Source(Box<dyn std::error::Error + Send + Sync>),
    #[error("SSE event exceeded the maximum size of {max_size} bytes")]
    EventTooLarge { max_size: usize },
}

/// Tracks the raw size of the SSE event being received, so a hostile upstream cannot make the
/// gateway buffer an unbounded event.
#[derive(Debug)]
struct SseEventSizeLimiter {
    max_size: usize,
    retained_size: usize,
    line_size: usize,
    line_is_comment: bool,
    previous_was_cr: bool,
}

impl SseEventSizeLimiter {
    fn new(max_size: usize) -> Self {
        Self { max_size, retained_size: 0, line_size: 0, line_is_comment: false, previous_was_cr: false }
    }

    fn observe(&mut self, mut chunk: &[u8]) -> Result<(), ()> {
        if self.previous_was_cr {
            self.previous_was_cr = false;
            if let Some(rest) = chunk.strip_prefix(b"\n") {
                chunk = rest;
            }
        }
        while let Some(&first) = chunk.first() {
            if let Some(pos) = chunk.iter().position(|&b| b == b'\r' || b == b'\n') {
                if pos > 0 {
                    if self.line_size == 0 {
                        self.line_is_comment = first == b':';
                    }
                    self.line_size = self.line_size.saturating_add(pos);
                    self.check_limit()?;
                }
                let is_cr = chunk.get(pos) == Some(&b'\r');
                self.finish_line()?;
                chunk = chunk.get(pos + 1..).unwrap_or_default();
                if is_cr {
                    if let Some(rest) = chunk.strip_prefix(b"\n") {
                        chunk = rest;
                    } else {
                        self.previous_was_cr = true;
                    }
                }
            } else {
                if self.line_size == 0 {
                    self.line_is_comment = first == b':';
                }
                self.line_size = self.line_size.saturating_add(chunk.len());
                return self.check_limit();
            }
        }
        Ok(())
    }

    fn finish_line(&mut self) -> Result<(), ()> {
        if self.line_size == 0 {
            self.retained_size = 0;
        } else if !self.line_is_comment {
            // The SSE parser inserts a newline when joining multiple data fields.
            self.retained_size = self.retained_size.saturating_add(self.line_size).saturating_add(1);
        }
        self.line_size = 0;
        self.line_is_comment = false;
        self.check_limit()
    }

    #[inline]
    fn check_limit(&self) -> Result<(), ()> {
        if self.retained_size.saturating_add(self.line_size) > self.max_size { Err(()) } else { Ok(()) }
    }
}

#[pin_project]
struct BoundedSseByteStream<S> {
    #[pin]
    inner: S,
    limiter: SseEventSizeLimiter,
    failed: bool,
}

impl<S, E> Stream for BoundedSseByteStream<S>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<Bytes, BoundedSseStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.failed {
            return Poll::Ready(None);
        }
        match ready!(this.inner.as_mut().poll_next(cx)) {
            Some(Ok(chunk)) => {
                if this.limiter.observe(&chunk).is_err() {
                    *this.failed = true;
                    Poll::Ready(Some(Err(BoundedSseStreamError::EventTooLarge { max_size: this.limiter.max_size })))
                } else {
                    Poll::Ready(Some(Ok(chunk)))
                }
            },
            Some(Err(error)) => {
                *this.failed = true;
                Poll::Ready(Some(Err(BoundedSseStreamError::Source(Box::new(error)))))
            },
            None => Poll::Ready(None),
        }
    }
}

fn bounded_sse_stream<S, E>(stream: S, max_event_size: usize) -> BoxStream<'static, Result<Sse, SseError>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let stream =
        BoundedSseByteStream { inner: stream, limiter: SseEventSizeLimiter::new(max_event_size), failed: false };
    SseStream::from_bytes_stream(stream).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_new_accepts_plain_and_tls_urls() {
        McpHttpClient::try_new("c", "http://127.0.0.1:9000/mcp").unwrap();
        McpHttpClient::try_new("c", "https://example.com/mcp?x=1").unwrap();
    }

    #[test]
    fn try_new_rejects_unsupported_scheme_and_missing_host() {
        assert!(matches!(
            McpHttpClient::try_new("c", "ftp://example.com/mcp"),
            Err(UpstreamClientError::InvalidUrl { .. })
        ));
        assert!(matches!(McpHttpClient::try_new("c", "/mcp"), Err(UpstreamClientError::InvalidUrl { .. })));
    }

    #[test]
    fn try_new_keeps_path_and_query_and_host_header() {
        let client = McpHttpClient::try_new("c", "http://127.0.0.1:9000/mcp?x=1").unwrap();
        assert_eq!(client.target.to_string(), "/mcp?x=1");
        assert_eq!(client.host_header, "127.0.0.1:9000");
        assert_eq!(client.cluster_id.as_str(), "c");
        let client = McpHttpClient::try_new("c", "http://localhost").unwrap();
        assert_eq!(client.target.to_string(), "/");
        assert_eq!(client.host_header, "localhost");
    }

    #[test]
    fn scope_is_extracted_from_quoted_and_bare_challenges() {
        assert_eq!(
            extract_scope_from_header(r#"Bearer error="insufficient_scope", scope="files:read files:write""#),
            Some("files:read files:write".to_owned())
        );
        assert_eq!(extract_scope_from_header("Bearer scope=read:data, realm=x"), Some("read:data".to_owned()));
        assert_eq!(extract_scope_from_header("Bearer realm=x"), None);
    }

    #[test]
    fn reserved_headers_are_rejected_but_protocol_version_is_allowed() {
        assert!(validate_custom_header(&HeaderName::from_static("accept")).is_err());
        assert!(validate_custom_header(&HeaderName::from_static("mcp-session-id")).is_err());
        assert!(validate_custom_header(&HeaderName::from_static("last-event-id")).is_err());
        validate_custom_header(&HeaderName::from_static("mcp-protocol-version")).unwrap();
        validate_custom_header(&HeaderName::from_static("x-custom")).unwrap();
    }

    #[test]
    fn sse_limiter_rejects_oversized_event_and_resets_on_blank_line() {
        // One `data: 0123456789\n` line retains 16 + 1 (join newline) = 17 bytes.
        let mut limiter = SseEventSizeLimiter::new(20);
        assert!(limiter.observe(b"data: 0123456789\n\n").is_ok());
        assert!(limiter.observe(b"data: 0123456789\n").is_ok());
        assert!(limiter.observe(b"data: 0123456789\n").is_err());
    }

    #[allow(clippy::indexing_slicing)]
    mod e2e {
        use super::*;
        use crate::{
            clusters::{
                cluster::PartialClusterType,
                clusters_manager::{add_cluster, remove_cluster},
            },
            secrets::SecretManager,
        };
        use arion_configuration::config::cluster::Cluster as ClusterConfig;
        use arion_data_plane_api::{
            decode::from_yaml, envoy_data_plane_api::envoy::config::cluster::v3::Cluster as EnvoyCluster,
        };
        use hyper::service::service_fn;
        use hyper_util::{
            rt::{TokioExecutor, TokioIo},
            server::conn::auto,
        };
        use rmcp::model::{ClientCapabilities, ClientConfig, Implementation};
        use serde_json::json;
        use std::sync::atomic::AtomicUsize;

        #[derive(Default)]
        struct Stats {
            connections: AtomicUsize,
            requests: AtomicUsize,
        }

        fn json_response(status: u16, body: &Value, session: bool) -> Response<Full<Bytes>> {
            let mut builder = Response::builder().status(status).header(CONTENT_TYPE, CONTENT_TYPE_JSON);
            if session {
                builder = builder.header(MCP_SESSION_ID, "test-session");
            }
            builder.body(Full::new(Bytes::from(body.to_string()))).unwrap()
        }

        async fn handle(req: Request<hyper::body::Incoming>, stats: StdArc<Stats>) -> Response<Full<Bytes>> {
            stats.requests.fetch_add(1, Ordering::Relaxed);
            if req.method() != Method::POST {
                let code = if req.method() == Method::DELETE { 200 } else { 405 };
                return Response::builder().status(code).body(Full::new(Bytes::new())).unwrap();
            }
            let session_attached = req.headers().contains_key(MCP_SESSION_ID);
            let body = req.into_body().collect().await.unwrap().to_bytes();
            let msg: Value = serde_json::from_slice(&body).unwrap();
            let id = msg["id"].clone();
            match msg["method"].as_str().unwrap_or_default() {
                "initialize" => json_response(
                    200,
                    &json!({"jsonrpc": "2.0", "id": id, "result": {
                        "protocolVersion": msg["params"]["protocolVersion"],
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "fake", "version": "0"},
                    }}),
                    true,
                ),
                "tools/list" => {
                    assert!(session_attached, "session id must be forwarded after initialize");
                    let reply = json!({"jsonrpc": "2.0", "id": id, "result": {"tools": [{
                        "name": "echo",
                        "description": "echoes",
                        "inputSchema": {"type": "object"},
                    }]}});
                    Response::builder()
                        .status(200)
                        .header(CONTENT_TYPE, EVENT_STREAM_MIME_TYPE)
                        .body(Full::new(Bytes::from(format!("data: {reply}\n\n"))))
                        .unwrap()
                },
                "tools/call" => {
                    assert!(session_attached, "session id must be forwarded after initialize");
                    let name = msg["params"]["name"].as_str().unwrap_or_default();
                    let reply = json!({"jsonrpc": "2.0", "id": id, "result": {
                        "content": [{"type": "text", "text": format!("called:{name}")}],
                        "isError": false,
                    }});
                    json_response(200, &reply, false)
                },
                m if m.starts_with("notifications/") => {
                    Response::builder().status(202).body(Full::new(Bytes::new())).unwrap()
                },
                // Legacy servers reject `server/discover` with a plain 4xx.
                _ => Response::builder().status(400).body(Full::new(Bytes::from("unsupported"))).unwrap(),
            }
        }

        async fn spawn_fake_mcp_server() -> (std::net::SocketAddr, StdArc<Stats>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let stats = StdArc::new(Stats::default());
            let server_stats = StdArc::clone(&stats);
            tokio::spawn(async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    server_stats.connections.fetch_add(1, Ordering::Relaxed);
                    let stats = StdArc::clone(&server_stats);
                    tokio::spawn(async move {
                        let service = service_fn(move |req| {
                            let stats = StdArc::clone(&stats);
                            async move { Ok::<_, std::convert::Infallible>(handle(req, stats).await) }
                        });
                        // Serves HTTP/1.1 and HTTP/2 (prior knowledge) on the same port.
                        if let Err(err) = auto::Builder::new(TokioExecutor::new())
                            .serve_connection(TokioIo::new(stream), service)
                            .await
                        {
                            tracing::debug!("fake mcp server connection closed: {err}");
                        }
                    });
                }
            });
            (addr, stats)
        }

        fn register_static_cluster(name: &str, addr: std::net::SocketAddr, http2: bool) {
            let protocol = if http2 { "http2_protocol_options: {}" } else { "http_protocol_options: {}" };
            let yaml = format!(
                r#"
name: {name}
type: STATIC
connect_timeout: 3s
typed_extension_protocol_options:
  envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
    "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
    explicit_http_config:
      {protocol}
load_assignment:
  cluster_name: {name}
  endpoints:
    - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: {ip}
                port_value: {port}
"#,
                ip = addr.ip(),
                port = addr.port(),
            );
            let cluster = ClusterConfig::try_from(from_yaml::<EnvoyCluster>(&yaml).unwrap()).unwrap();
            add_cluster(PartialClusterType::try_from((Box::new(cluster), &SecretManager::new())).unwrap()).unwrap();
        }

        async fn run_list_tools_scenario(cluster: &str, http2: bool) {
            let (addr, stats) = spawn_fake_mcp_server().await;
            register_static_cluster(cluster, addr, http2);
            let url = format!("http://{addr}/mcp");

            let info = ClientConfig::new(ClientCapabilities::default(), Implementation::new("test", "0.1.0"));
            let client =
                McpUpstreamClient::connect(cluster, &url, info).await.expect("initialize must succeed through cluster");

            for _ in 0..3 {
                let tools = client.list_tools(Option::default()).await.expect("tools/list must succeed");
                assert_eq!(tools.tools.len(), 1);
                assert_eq!(tools.tools[0].name.as_ref(), "echo");
            }
            let call_res = client.call_tool_borrowed("echo", None).await.expect("tools/call must succeed");
            assert_eq!(call_res.is_error, Some(false));

            drop(client);
            remove_cluster(cluster).unwrap();

            let requests = stats.requests.load(Ordering::Relaxed);
            let connections = stats.connections.load(Ordering::Relaxed);
            assert_eq!(requests, 6, "expected initialize, initialized, 3 tools/list and 1 tools/call, got {requests}");
            assert!(
                connections < requests,
                "connections must be reused: {connections} connections for {requests} requests"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn lists_tools_through_an_http1_cluster() {
            run_list_tools_scenario("mcp-upstream-client-h1", false).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn lists_tools_through_an_http2_cluster() {
            run_list_tools_scenario("mcp-upstream-client-h2", true).await;
        }
    }
}
