use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::SinkExt;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
use scopeguard::defer;
use smol_str::{SmolStr, ToSmolStr};
use std::{
    panic,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::sync::Mutex;
use tracing::debug;
use uuid::Uuid;

use crate::{
    body::{
        instrumented_body::InstrumentedBody,
        response_flags::BodyKind,
        sse_body::{SseBody, SseSender},
        timeout_body::TimeoutBody,
    },
    listeners::{
        http_connection_manager::mcp_gateway::model::{self},
        http_filters::{FactoryFilter, FilterDecision},
        listener::FilterListenerContext,
        metadata::DownstreamMetadata,
    },
    OrionRequestBody, OrionResponseBody, PolyBody,
};

const SESSION_ID_PREFIX: &str = "session_id=";
const MAX_CONCURRENT_ASYNC_REQUESTS: usize = 256;

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct SessionId(pub SmolStr);

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Default)]
pub struct Session {
    listener_name: &'static str, // to handle session eviction from listener.sse_map
    session_id: SessionId,
    sender: Mutex<SseSender>,
}

#[derive(Debug, Default)]
pub struct McpGatewayListenerContext {
    sse_map: DashMap<SessionId, Arc<Session>>,
    active_async_requests: AtomicUsize,
}

#[derive(Debug)]
pub struct McpGatewayInner {
    config: McpGatewayConfig,
}

#[derive(Debug, Default, Clone)]
struct SessionContext(Option<Arc<Session>>);

impl SessionContext {
    pub fn get(&self) -> &Session {
        unsafe { self.0.as_ref().unwrap_unchecked() }
    }
}

/// McpGateway filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: Arc<McpGatewayInner>,
    session_ctx: SessionContext,
    request_id: model::RequestId,
}

impl From<McpGatewayConfig> for McpGateway {
    fn from(config: McpGatewayConfig) -> Self {
        Self {
            inner: Arc::new(McpGatewayInner { config }),
            session_ctx: SessionContext(None),
            request_id: model::RequestId::Number(0),
        }
    }
}

impl FactoryFilter for McpGateway {
    fn new_from(&self) -> Self {
        Self { inner: self.inner.clone(), session_ctx: SessionContext(None), request_id: model::RequestId::Number(0) }
    }
}

impl McpGateway {
    pub async fn apply_request(&mut self, request: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "mcp_gateway", "apply_request: processing request: {:?}", request);

        let Some(metadata) = request.extensions().get::<DownstreamMetadata>() else {
            debug!(target: "mcp_gateway", "apply_request: failed to retrieve metadata");
            return FilterDecision::internal_server_error("Failed to retrieve metadata", request.version());
        };

        // get global context for this listener
        let mcp_ctx = McpGatewayListenerContext::get_filter_context(metadata.listener_name);
        debug!(target: "mcp_gateway", "apply_request: listener name: {}", metadata.listener_name);

        match (request.method(), request.uri().path()) {
            (&Method::GET, "/sse") => self.handle_sse_handshake(&mcp_ctx, request, metadata.listener_name).await,
            (&Method::GET, "/mcp/messages") => {
                self.handle_message_endpoint(&mcp_ctx, request, metadata.listener_name).await
            },
            _ => FilterDecision::no_route_found(request.version()),
        }

        // Implement request routing/filtering logic here
        // let headers = request.headers_mut();
        // headers.append(&self.inner.config.cluster_header, HeaderValue::from_static("cluster_http"));
        // FilterDecision::Continue
    }

    pub async fn apply_response(&mut self, response: &mut Response<OrionResponseBody>) -> FilterDecision {
        // get global context for this listener
        let mcp_ctx = McpGatewayListenerContext::get_filter_context(self.session_ctx.get().listener_name);
        defer! {
            mcp_ctx.active_async_requests.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        };

        // get the SSE connection via body bridge
        let mut sender = self.session_ctx.get().sender.lock().await;

        // collect the body...
        let Ok(body) = response.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_response: failed to collect response body");
            let msg = prepare_rpc_error(
                self.request_id.clone(),
                model::ErrorData::internal_error("failed to collect response body", None),
            );
            _ = sender.send(msg).await;
            return FilterDecision::Continue;
        };

        if let Err(e) = sender.send(body.to_bytes()).await {
            let mcp_ctx = McpGatewayListenerContext::get_filter_context(self.session_ctx.get().listener_name);
            mcp_ctx.sse_map.remove(&self.session_ctx.get().session_id);
            debug!(target: "mcp_gateway", "apply_response: failed to send SSE payload for session {}, error {e}", self.session_ctx.get().session_id);
        };

        FilterDecision::Continue
    }

    pub async fn handle_message_endpoint(
        &mut self,
        state: &McpGatewayListenerContext,
        request: &mut Request<OrionRequestBody>,
        _listener_name: &'static str,
    ) -> FilterDecision {
        // extract session ID from request
        let Some(session_id) = Self::get_session_id(request) else {
            debug!(target: "mcp_gateway", "handle_message_endpoint: missing session ID!");
            return FilterDecision::bad_request(request.version());
        };

        // search for session in map
        let Some(session) = state.sse_map.get(&session_id) else {
            debug!(target: "mcp_gateway", "handle_message_endpoint: session ID {session_id} not found!");
            return FilterDecision::not_found(request.version());
        };

        // (fixme) rate limit the request. we are limiting any kind of request, but we should be
        // limiting only those requests that triggers upstream requests.
        if state.active_async_requests.load(std::sync::atomic::Ordering::Relaxed) >= MAX_CONCURRENT_ASYNC_REQUESTS {
            debug!(target: "mcp_gateway", "apply_request: rate limited!");
            return FilterDecision::rate_limited(request.version());
        }

        // increment active async requests
        state.active_async_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // save the current session ID and session
        self.session_ctx = SessionContext(Some(session.value().clone()));

        // shallow parsing the body of the request:

        let Ok(body) = request.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_request: failed to collect request body");
            return FilterDecision::internal_server_error("Failed to collect request body", request.version());
        };

        let body = body.to_bytes();

        // let message: JsonRpcMessage = serde_json::from_str(body)?;

        let mut okay = Response::new(TimeoutBody::new(None, PolyBody::from(Empty::new())));
        *okay.status_mut() = http::StatusCode::OK;
        *okay.version_mut() = request.version();

        // FIXME: prepare the upstream request...
        let mut req = Request::new(InstrumentedBody::new(
            BodyKind::Request,
            TimeoutBody::new(None, PolyBody::from(Empty::new())),
            |_, _, _| {},
        ));
        std::mem::swap(request, &mut req);

        FilterDecision::AsyncRequest(okay, Some(req))
    }

    pub async fn handle_sse_handshake(
        &mut self,
        state: &McpGatewayListenerContext,
        req: &mut Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> FilterDecision {
        debug!(target: "mcp_gateway", "handle_sse_handshake: starting SSE handshake...");
        // generate a unique session ID
        let session_id = SessionId(Uuid::new_v4().to_smolstr());

        // build the SSE response...
        let builder = Response::builder()
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .header("Connection", "keep-alive")
            .status(StatusCode::OK);

        let payload = format!("event: endpoint\r\ndata: /mcp/messages?{SESSION_ID_PREFIX}{}\r\n", session_id.0);
        let (body, mut bridge) = SseBody::new();

        let body = TimeoutBody::new(None, PolyBody::from(body));

        let Ok(_) = bridge.send(Bytes::from(payload)).await else {
            debug!(target: "mcp_gateway", "handle_sse_handshake: failed to send SSE payload");
            return FilterDecision::internal_server_error("Failed to send SSE payload", req.version());
        };

        let Ok(response) = builder.body(body) else {
            debug!(target: "mcp_gateway", "handle_sse_handshake: failed to build body for response");
            return FilterDecision::internal_server_error("Failed to build body for response", req.version());
        };

        // create a new session
        let session = Arc::new(Session {
            sender: Mutex::new(bridge),
            listener_name: listener_name,
            session_id: session_id.clone(),
        });

        // add the session to the map
        state.sse_map.insert(session_id.clone(), session.clone());
        self.session_ctx = SessionContext(Some(session));

        FilterDecision::DirectResponse(response)
    }

    // parse session id from request URI (query part)
    pub fn get_session_id(req: &Request<OrionRequestBody>) -> Option<SessionId> {
        req.uri().query().and_then(|query| {
            query.split('&').find_map(|part| {
                debug!(target: "mcp_gateway", "get_session_id: parsing session ID from query part: {}", part);
                part.split(SESSION_ID_PREFIX).nth(1).and_then(|value| {
                    debug!(target: "mcp_gateway", "get_session_id: session ID value: {}", value);
                    Some(SessionId(value.to_smolstr()))
                })
            })
        })
    }
}

fn prepare_rpc_error(request_id: model::RequestId, data: model::ErrorData) -> Bytes {
    let error = model::JsonRpcError { jsonrpc: model::JsonRpcVersion2_0, id: request_id, error: data };
    let Ok(error_msg) = serde_json::to_string(&error) else {
        panic!("Failed to serialize error message");
    };
    let mut err_msg = BytesMut::from(error_msg.as_str());
    err_msg.put_slice(b"\r\n\r\n");
    err_msg.freeze()
}

#[inline]
fn okay_response(ver: http::Version) -> Response<OrionResponseBody> {
    let mut okay = Response::new(TimeoutBody::new(None, PolyBody::from(Empty::new())));
    *okay.status_mut() = http::StatusCode::OK;
    *okay.version_mut() = ver;
    okay
}
