use std::{num::NonZeroUsize, sync::Arc};

use bytes::Bytes;
use dashmap::DashMap;
use futures::SinkExt;
use http::{Method, Request, Response, StatusCode};
use http_body_util::Empty;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
use orion_format::types::ResponseFlags as FmtResponseFlags;
use parking_lot::Mutex;
use smol_str::{SmolStr, ToSmolStr};
use tracing::debug;
use uuid::Uuid;

use crate::{
    body::{
        channel_body::{ChannelBody, FrameBridge},
        instrumented_body::InstrumentedBody,
        response_flags::ResponseFlags,
        timeout_body::TimeoutBody,
    },
    event_error::EventFailure,
    listeners::{
        filter_state::DownstreamMetadata, http_connection_manager::FilterDecision, listener::get_listener_context,
        synthetic_http_response::SyntheticHttpResponse,
    },
    PolyBody,
};

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct SessionId(pub SmolStr);

#[derive(Debug, Default)]
pub struct Session {
    bridge: Mutex<FrameBridge>,
}

#[derive(Debug, Default)]
pub struct McpGatewayContext {
    sse_map: DashMap<SessionId, Session>,
}

#[derive(Debug, Clone)]
pub struct McpGatewayInner {
    config: McpGatewayConfig,
}

/// McpGateway filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: Arc<McpGatewayInner>,
}

impl From<McpGatewayConfig> for McpGateway {
    fn from(config: McpGatewayConfig) -> Self {
        Self { inner: Arc::new(McpGatewayInner { config }) }
    }
}

impl McpGateway {
    pub async fn apply_request(
        &mut self,
        request: &mut Request<InstrumentedBody<TimeoutBody<PolyBody>>>,
    ) -> FilterDecision {
        debug!(target: "mcp", "processing request: {:?}", request);

        let Some(downstream_ctx) = request.extensions().get::<DownstreamMetadata>() else {
            return self.internal_server_error("Failed to retrieve metadata", request.version());
        };

        let listener_ctx = get_listener_context(downstream_ctx.listener_name);

        match (request.method(), request.uri().path()) {
            (&Method::GET, "/sse") => self.handle_sse_handshake(&listener_ctx.mcp, request.version()).await,
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

    pub async fn handle_sse_handshake(&mut self, state: &McpGatewayContext, ver: http::Version) -> FilterDecision {
        // generate a unique session ID
        let session_id = SessionId(Uuid::new_v4().to_smolstr());

        // create a new session
        let mut session = Session::default();

        // build the SSE response...
        let builder = Response::builder()
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .header("Connection", "keep-alive")
            .status(StatusCode::OK);

        let payload = format!("event: endpoint\r\ndata: /mcp/messages?sessionId={}\r\n", session_id.0);
        let (body, mut bridge) = ChannelBody::new(Empty::new(), unsafe { NonZeroUsize::new_unchecked(1) });
        // let body =  Full::new(Bytes::from(payload));

        let body = TimeoutBody::new(None, PolyBody::from(body));

        let Ok(_) = bridge.send(Bytes::from(payload)).await else {
            return self.internal_server_error("Failed to send SSE payload", ver);
        };

        let Ok(response) = builder.body(body) else {
            return self.internal_server_error("Failed to build body for response", ver);
        };

        session.bridge = Mutex::new(bridge);

        // add the session to the map
        state.sse_map.insert(session_id.clone(), session);
        FilterDecision::DirectResponse(response)
    }

    pub fn internal_server_error(&mut self, msg: &str, ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::internal_server_error(
                EventFailure::ExtProcError.into(),
                ResponseFlags(FmtResponseFlags::UNAUTHORIZED_EXTERNAL_SERVICE),
                msg,
            )
            .into_response(ver),
        )
    }
}
