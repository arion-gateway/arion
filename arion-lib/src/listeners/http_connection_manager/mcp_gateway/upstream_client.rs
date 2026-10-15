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

//! `rmcp` Streamable HTTP client backed by the gateway's clusters.
//!
//! It replaces `reqwest::Client` as the `StreamableHttpClient` of upstream MCP transports. Every
//! request goes through the `HttpChannel` of the configured cluster, so endpoints, TLS context,
//! HTTP/1 or HTTP/2 codec, connection pools and circuit breaker all come from the cluster
//! configuration. The MCP `url` only provides the request path (and query).
//!
//! Protocol handling (auth challenges, session expiry, JSON / SSE bodies, legacy `server/discover`
//! rejections) mirrors rmcp's own `reqwest` and Unix socket clients, which keep part of that
//! logic `pub(crate)`.

use crate::{
    ArionRequestBody, ArionResponseBody,
    body::{
        instrumented_body::InstrumentedBody, poly_body::PolyBody, response_flags::BodyKind, timeout_body::TimeoutBody,
    },
    clusters::{RoutingPriority, clusters_manager, clusters_manager::RoutingContext},
};
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
use rmcp::{
    model::{ClientJsonRpcMessage, ClientRequest, ErrorData, JsonRpcMessage, RequestId, ServerJsonRpcMessage},
    transport::streamable_http_client::{
        AuthRequiredError, InsufficientScopeError, StreamableHttpClient, StreamableHttpError,
        StreamableHttpPostResponse,
    },
};
use sse_stream::{Error as SseError, Sse, SseStream};
use std::{
    borrow::Cow,
    collections::HashMap,
    pin::Pin,
    str::FromStr,
    sync::Arc as StdArc,
    task::{Poll, ready},
};

const EVENT_STREAM_MIME_TYPE: &str = "text/event-stream";
const JSON_MIME_TYPE: &str = "application/json";
const ACCEPT_EVENT_STREAM_AND_JSON: HeaderValue = HeaderValue::from_static("text/event-stream, application/json");
const CONTENT_TYPE_JSON: HeaderValue = HeaderValue::from_static(JSON_MIME_TYPE);

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

        Ok(Self { cluster_id: cluster.to_interned_str(), host_header, target })
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
        #[cfg(feature = "instrumentation")]
        let clock = quanta::Clock::new();

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
                &clock,
            )
            .await
            .map_err(|e| StreamableHttpError::Client(UpstreamClientError::Upstream(Box::new(e))))
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
        use rmcp::{
            ServiceExt,
            model::{ClientCapabilities, ClientConfig, Implementation},
            transport::{StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig},
        };
        use serde_json::{Value, json};
        use std::sync::atomic::{AtomicUsize, Ordering};

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

            let transport = StreamableHttpClientTransport::with_client(
                McpHttpClient::try_new(cluster, &url).unwrap(),
                StreamableHttpClientTransportConfig::with_uri(url.as_str()),
            );
            let info = ClientConfig::new(ClientCapabilities::default(), Implementation::new("test", "0.1.0"));
            let client = info.serve(transport).await.expect("initialize must succeed through the cluster");

            for _ in 0..3 {
                let tools = client.list_tools(Option::default()).await.expect("tools/list must succeed");
                assert_eq!(tools.tools.len(), 1);
                assert_eq!(tools.tools[0].name.as_ref(), "echo");
            }
            drop(client);
            remove_cluster(cluster).unwrap();

            let requests = stats.requests.load(Ordering::Relaxed);
            let connections = stats.connections.load(Ordering::Relaxed);
            assert!(requests >= 5, "expected initialize, initialized and 3 tools/list, got {requests}");
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

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn unknown_cluster_is_reported_as_client_error() {
            let client = McpHttpClient::try_new("mcp-upstream-client-missing", "http://127.0.0.1:9/mcp").unwrap();
            let err = client
                .get_stream(StdArc::from(""), None, None, None, HashMap::new())
                .await
                .err()
                .expect("missing cluster must fail");
            assert!(
                matches!(err, StreamableHttpError::Client(UpstreamClientError::Cluster { .. })),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn sse_limiter_ignores_comments_for_retained_size() {
        let mut limiter = SseEventSizeLimiter::new(16);
        for _ in 0..8 {
            assert!(limiter.observe(b": keep-alive\n").is_ok());
        }
    }
}
