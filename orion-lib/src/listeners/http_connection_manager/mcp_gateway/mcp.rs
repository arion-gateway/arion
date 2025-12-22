use bytes::Bytes;
use dashmap::DashMap;
use futures::SinkExt;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
use orion_format::types::ResponseFlags as FmtResponseFlags;
use smol_str::{SmolStr, ToSmolStr};
use std::{
    num::NonZeroUsize,
    sync::{atomic::AtomicUsize, Arc},
};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    body::{
        channel_body::{ChannelBody, FrameBridge},
        instrumented_body::InstrumentedBody,
        response_flags::{BodyKind, ResponseFlags},
        timeout_body::TimeoutBody,
    },
    event_error::EventFailure,
    listeners::{
        filter_state::DownstreamMetadata,
        http_connection_manager::{FactoryFilter, FilterDecision},
        listener::get_listener_context,
        synthetic_http_response::SyntheticHttpResponse,
    },
    OrionRequestBody, OrionResponseBody, PolyBody,
};

const SESSION_ID_PREFIX: &str = "session_id=";
const MAX_CONCURRENT_ASYNC_REQUESTS: usize = 8192;

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct SessionId(pub SmolStr);

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Default)]
pub struct Session {
    bridge: tokio::sync::Mutex<FrameBridge>,
    listener_name: &'static str,
}

#[derive(Debug, Default)]
pub struct McpGatewayContext {
    sse_map: DashMap<SessionId, Arc<Session>>,
    active_async_requests: AtomicUsize,
}

#[derive(Debug, Clone, Default)]
pub struct SessionCtx {
    session_id: SessionId,
    session: Arc<Session>,
}

#[derive(Debug)]
pub struct McpGatewayInner {
    config: McpGatewayConfig,
}

/// McpGateway filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: Arc<McpGatewayInner>,
    session_ctx: SessionCtx,
}

impl From<McpGatewayConfig> for McpGateway {
    fn from(config: McpGatewayConfig) -> Self {
        Self { inner: Arc::new(McpGatewayInner { config }), session_ctx: SessionCtx::default() }
    }
}

impl FactoryFilter for McpGateway {
    fn new_from(&self) -> Self {
        Self { inner: self.inner.clone(), session_ctx: SessionCtx::default() }
    }
}

impl McpGateway {
    pub async fn apply_request(&mut self, request: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "mcp_gateway", "processing request: {:?}", request);

        let Some(metadata) = request.extensions().get::<DownstreamMetadata>() else {
            return Self::internal_server_error("Failed to retrieve metadata", request.version());
        };

        // get global context for this listener
        let listener_ctx = get_listener_context(metadata.listener_name);
        if listener_ctx.mcp.active_async_requests.load(std::sync::atomic::Ordering::Relaxed)
            >= MAX_CONCURRENT_ASYNC_REQUESTS
        {
            return Self::rate_limited(request.version());
        }

        listener_ctx.mcp.active_async_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        match (request.method(), request.uri().path()) {
            (&Method::GET, "/sse") => self.handle_sse_handshake(&listener_ctx.mcp, request, metadata.listener_name).await,
            (&Method::GET, "/dummy") => self.handle_dummy_endpoint(&listener_ctx.mcp, request, metadata.listener_name).await,
            _ => FilterDecision::DirectResponse(
                SyntheticHttpResponse::not_found(
                    EventFailure::RouteNotFound.into(),
                    ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
                )
                .into_response(request.version()),
            ),
        }
        // Implement request routing/filtering logic here
        // let headers = request.headers_mut();
        // headers.append(&self.inner.config.cluster_header, HeaderValue::from_static("cluster_http"));
        // FilterDecision::Continue
    }

    // parse session id from request URI (query part)
    pub fn get_session_id(req: &Request<OrionRequestBody>) -> Option<SessionId> {
        req.uri().query().and_then(|query| {
            query.split('&').find_map(|part| {
                debug!(target: "mcp_gateway", "Parsing session ID from query part: {}", part);
                part.split(SESSION_ID_PREFIX).nth(1).and_then(|value| {
                    debug!(target: "mcp_gateway", "Session ID value: {}", value);
                    Some(SessionId(value.to_smolstr()))
                })
            })
        })
    }

    pub async fn apply_response(&mut self, response: &mut Response<OrionResponseBody>) -> FilterDecision {
        // get global context for this listener
        let listener_ctx = get_listener_context(self.session_ctx.session.listener_name);
        listener_ctx.mcp.active_async_requests.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

        let Ok(body) = response.body_mut().collect().await else {
            return Self::internal_server_error("Failed to collect response body", response.version());
        };

        // find the session context using the current session ID
        let mut bridge = self.session_ctx.session.bridge.lock().await;

        let Ok(_) = bridge.send(body.to_bytes()).await else {
            return Self::internal_server_error("Failed to send SSE payload", response.version());
        };

        FilterDecision::Continue
    }

    pub async fn handle_dummy_endpoint(
        &mut self,
        state: &McpGatewayContext,
        downstream_req: &mut Request<OrionRequestBody>,
        _listener_name: &'static str,
    ) -> FilterDecision {
        let Some(session_id) = Self::get_session_id(downstream_req) else {
            info!(target: "mcp_gateway", "Missing session ID!");
            return Self::bad_request(downstream_req.version());
        };

        let Some(session) = state.sse_map.get(&session_id) else {
            info!(target: "mcp_gateway", "Session ID {session_id} not found!");
            return Self::not_found(downstream_req.version());
        };

        // save the current session ID
        self.session_ctx.session_id = session_id;
        self.session_ctx.session = session.value().clone();

        let mut okay = Response::new(TimeoutBody::new(None, PolyBody::from(Empty::new())));
        *okay.status_mut() = http::StatusCode::OK;
        *okay.version_mut() = downstream_req.version();

        let mut req = Request::new(InstrumentedBody::new(
            BodyKind::Request,
            TimeoutBody::new(None, PolyBody::from(Empty::new())),
            |_, _, _| {},
        ));
        std::mem::swap(downstream_req, &mut req);

        FilterDecision::AsyncRequest(okay, Some(req))
    }

    pub async fn handle_sse_handshake(
        &mut self,
        state: &McpGatewayContext,
        req: &mut Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> FilterDecision {
        // generate a unique session ID
        let session_id = SessionId(Uuid::new_v4().to_smolstr());
        self.session_ctx.session_id = session_id.clone();

        // create a new session
        let mut session = Session::default();

        // build the SSE response...
        let builder = Response::builder()
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .header("Connection", "keep-alive")
            .status(StatusCode::OK);

        let payload = format!("event: endpoint\r\ndata: /mcp/messages?{SESSION_ID_PREFIX}{}\r\n", session_id.0);
        let (body, mut bridge) = ChannelBody::new(Empty::new(), unsafe { NonZeroUsize::new_unchecked(1) });

        let body = TimeoutBody::new(None, PolyBody::from(body));

        let Ok(_) = bridge.send(Bytes::from(payload)).await else {
            return Self::internal_server_error("Failed to send SSE payload", req.version());
        };

        let Ok(response) = builder.body(body) else {
            return Self::internal_server_error("Failed to build body for response", req.version());
        };

        session.bridge = tokio::sync::Mutex::new(bridge);
        session.listener_name = listener_name;

        let session = Arc::new(session);
        self.session_ctx.session = Arc::clone(&session);

        // add the session to the map
        state.sse_map.insert(session_id.clone(), session);
        FilterDecision::DirectResponse(response)
    }

    #[inline]
    pub fn internal_server_error(msg: &str, ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::internal_server_error(
                EventFailure::DirectResponse.into(),
                ResponseFlags::default(),
                msg,
            )
            .into_response(ver),
        )
    }

    #[inline]
    pub fn bad_request(ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into()).into_response(ver),
        )
    }

    #[inline]
    pub fn not_found(ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::not_found(EventFailure::DirectResponse.into(), ResponseFlags::default())
                .into_response(ver),
        )
    }

    #[inline]
    pub fn rate_limited(ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::custom_error(
                http::StatusCode::TOO_MANY_REQUESTS,
                None,
                EventFailure::RateLimited.into(),
                ResponseFlags(FmtResponseFlags::RATE_LIMITED),
            )
            .into_response(ver),
        )
    }
}
