use bytes::Bytes;
use dashmap::DashMap;
use futures::SinkExt;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
use scopeguard::defer;
use serde::Serialize;
use serde_json::json;
use smol_str::{SmolStr, ToSmolStr};
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::Mutex;
use tracing::debug;
use url::form_urlencoded;
use uuid::Uuid;

use crate::{
    body::{
        sse_body::{SseBody, SseSender},
        timeout_body::TimeoutBody,
    },
    listeners::{
        http_connection_manager::mcp_gateway::{
            model::{
                self, Annotated, CallToolResult, Implementation, InitializeResult, JsonRpcResponse, ProtocolVersion,
                RawContent, RawTextContent, ServerCapabilities, ServerResult,
            },
            sse,
            tools::ToolsRegistry,
        },
        http_filters::{FactoryFilter, FilterDecision},
        listener::FilterListenerContext,
        metadata::DownstreamMetadata,
    },
    OrionRequestBody, OrionResponseBody, PolyBody,
};

const SESSION_ID_PREFIX: &str = "sessionId=";
const MCP_MESSAGE_ENDPOINT: &str = "/mcp/message";
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
    tools: ToolsRegistry,
}

#[derive(Debug, Default, Clone)]
struct SessionContext(Option<Arc<Session>>);

impl SessionContext {
    pub fn get(&self) -> &Session {
        unsafe { self.0.as_ref().unwrap_unchecked() }
    }
}

enum MessageResponse {
    Nothing,
    Error(model::JsonRpcError),
    RpcResponse(model::JsonRpcResponse<serde_json::Value>),
    Upstream(Request<OrionRequestBody>),
}

/// McpGateway filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: Arc<McpGatewayInner>,
    session_ctx: SessionContext,
    request_id: model::RequestId,
    version: http::Version,
    initialize_request_params: Option<model::InitializeRequestParam>,
}

impl From<McpGatewayConfig> for McpGateway {
    fn from(config: McpGatewayConfig) -> Self {
        Self {
            inner: Arc::new(McpGatewayInner { config, tools: ToolsRegistry::with_dummy_tools() }),
            session_ctx: SessionContext(None),
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
        }
    }
}

impl FactoryFilter for McpGateway {
    fn new_from(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            session_ctx: SessionContext(None),
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
        }
    }
}

impl McpGateway {
    pub async fn apply_request(&mut self, request: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "mcp_gateway", "apply_request: processing request: {:?}", request);

        self.version = request.version();

        let Some(metadata) = request.extensions().get::<DownstreamMetadata>() else {
            debug!(target: "mcp_gateway", "apply_request: failed to retrieve metadata");
            return FilterDecision::internal_server_error("Failed to retrieve metadata", self.version);
        };

        // get global context for this listener
        let mcp_ctx = McpGatewayListenerContext::get_filter_context(metadata.listener_name);
        debug!(target: "mcp_gateway", "apply_request: listener name: {}", metadata.listener_name);

        match (request.method(), request.uri().path()) {
            (&Method::GET, "/sse") => self.handle_sse_handshake(&mcp_ctx, request, metadata.listener_name).await,
            (&Method::OPTIONS, "/sse") | (&Method::OPTIONS, MCP_MESSAGE_ENDPOINT) => {
                self.handle_preflight_checks(request).await
            },
            (&Method::POST, MCP_MESSAGE_ENDPOINT) => {
                self.handle_mcp_message_endpoint(&mcp_ctx, request, metadata.listener_name).await
            },
            _ => {
                debug!(target: "mcp_gateway", "apply_request: no route found");
                FilterDecision::no_route_found(self.version)
            },
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

        // collect the body...
        let Ok(body) = response.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_response: failed to collect response body");
            let error = self.build_rpc_error(model::ErrorData::internal_error("failed to collect response body", None));
            let event = sse::transport::Event::Message(&error);

            if let Err(e) = self.send_message(event.to_bytes()).await {
                return e;
            }
            return FilterDecision::Continue;
        };

        let body_string = {
            let b = std::str::from_utf8(&body.to_bytes()).map(|s| s.to_string()).unwrap_or_default();
            match response.status() {
                StatusCode::OK => {
                    if b.is_empty() {
                        "OK".into()
                    } else {
                        b
                    }
                },
                _ => {
                    if b.is_empty() {
                        format!("Upstream Error: {}", response.status().canonical_reason().unwrap_or("Unknown"))
                    } else {
                        b
                    }
                },
            }
        };

        let content = RawContent::Text(RawTextContent { text: body_string, meta: None });

        let tool_result = CallToolResult {
            content: vec![Annotated::new(content, None)],
            is_error: Some(!response.status().is_success()),
            meta: None,
            structured_content: None,
        };

        let server_result = ServerResult::CallToolResult(tool_result);
        debug!(target: "mcp_gateway", "{:#?}", server_result);

        let rpc_response = self.to_json_rpc_response(server_result);
        let event = sse::transport::Event::Message(&rpc_response);

        debug!(target: "mcp_gateway", "SENDING EVENT: {:#?}", event);

        if let Err(e) = self.send_message(event.to_bytes()).await {
            return e;
        }
        FilterDecision::Continue
    }

    #[inline]
    fn to_json_rpc_response<T: Serialize>(&self, value: T) -> JsonRpcResponse {
        let json_value = serde_json::to_value(value).unwrap();
        let result = match json_value {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        JsonRpcResponse { jsonrpc: model::JsonRpcVersion2_0, id: self.request_id.clone(), result }
    }

    pub async fn handle_mcp_message_endpoint(
        &mut self,
        state: &McpGatewayListenerContext,
        request: &mut Request<OrionRequestBody>,
        _listener_name: &'static str,
    ) -> FilterDecision {
        // extract session ID from request
        let Some(session_id) = Self::get_session_id(request) else {
            debug!(target: "mcp_gateway", "handle_message_endpoint: missing session ID!");
            return FilterDecision::bad_request(self.version);
        };

        // search for session in map
        let Some(session) = state.sse_map.get(&session_id) else {
            debug!(target: "mcp_gateway", "handle_message_endpoint: session ID {session_id} not found!");
            return FilterDecision::not_found(self.version);
        };

        // save the current session ID and session
        self.session_ctx = SessionContext(Some(session.value().clone()));

        // collect the body of the request...
        let Ok(body) = request.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_request: failed to collect request body");
            return FilterDecision::internal_server_error("Failed to collect request body", self.version);
        };

        let Ok(accepted) = self.build_accepted_response() else {
            return FilterDecision::internal_server_error("Failed to build accepted response", self.version);
        };

        let res = self.handle_rpc_message(request, body.to_bytes());
        match res {
            MessageResponse::Error(json_rpc_error) => {
                let event = sse::transport::Event::Message(&json_rpc_error);
                if let Err(e) = self.send_message(event.to_bytes()).await {
                    return e;
                }
                return FilterDecision::DirectResponse(accepted);
            },
            MessageResponse::RpcResponse(json_rpc_response) => {
                let event = sse::transport::Event::Message(&json_rpc_response);
                if let Err(e) = self.send_message(event.to_bytes()).await {
                    return e;
                }
                return FilterDecision::DirectResponse(accepted);
            },
            MessageResponse::Nothing => {
                return FilterDecision::DirectResponse(accepted);
            },
            MessageResponse::Upstream(req) => {
                if state.active_async_requests.load(std::sync::atomic::Ordering::Relaxed)
                    >= MAX_CONCURRENT_ASYNC_REQUESTS
                {
                    debug!(target: "mcp_gateway", "apply_request: rate limited!");
                    return FilterDecision::rate_limited(request.version());
                }
                state.active_async_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return FilterDecision::AsyncRequest(accepted, Some(req));
            },
        }
    }

    pub async fn handle_preflight_checks(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        let builder = Response::builder()
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Methods", "GET, POST")
            .header("Access-Control-Allow-Headers", "content-type, x-mcp-protocol-version")
            .header("Access-Control-Max-Age", "86400")
            .version(self.version)
            .status(StatusCode::NO_CONTENT);

        let Ok(response) = builder.body(TimeoutBody::new(None, PolyBody::from(Empty::new()))) else {
            debug!(target: "mcp_gateway", "handle_preflight_checks: failed to build body for response");
            return FilterDecision::internal_server_error("Failed to build body for response", req.version());
        };

        debug!(target: "mcp_gateway", "handle_preflight_checks: sending {response:?} back to client");
        FilterDecision::DirectResponse(response)
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
            .header("Access-Control-Allow-Origin", "*")
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache, no-transform")
            .header("Connection", "keep-alive")
            .version(self.version)
            .status(StatusCode::OK);

        let event: sse::transport::Event =
            sse::transport::Event::Endpoint(&format!("{MCP_MESSAGE_ENDPOINT}?{SESSION_ID_PREFIX}{}", session_id.0));

        let (body, mut sender) = SseBody::new();
        let body = TimeoutBody::new(None, PolyBody::from(body));

        let Ok(_) = sender.send(event.to_bytes()).await else {
            debug!(target: "mcp_gateway", "handle_sse_handshake: failed to send SSE payload");
            return FilterDecision::internal_server_error("Failed to send SSE payload", req.version());
        };

        let Ok(response) = builder.body(body) else {
            debug!(target: "mcp_gateway", "handle_sse_handshake: failed to build body for response");
            return FilterDecision::internal_server_error("Failed to build body for response", req.version());
        };

        // create a new session
        let session = Arc::new(Session {
            sender: Mutex::new(sender),
            listener_name: listener_name,
            session_id: session_id.clone(),
        });

        // add the session to the map
        state.sse_map.insert(session_id, session.clone());
        self.session_ctx = SessionContext(Some(session));

        debug!(target: "mcp_gateway", "handle_sse_handshake: sending {response:?} back to client");
        FilterDecision::DirectResponse(response)
    }

    pub fn get_session_id(req: &Request<OrionRequestBody>) -> Option<SessionId> {
        req.uri().query().and_then(|query| {
            form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "sessionId" || key == "session_id")
                .map(|(_, value)| SessionId(value.to_smolstr()))
        })
    }

    fn handle_rpc_message(&mut self, request: &Request<OrionRequestBody>, body: Bytes) -> MessageResponse {
        let Ok(message): Result<model::JsonRpcMessage, _> = serde_json::from_slice(&body) else {
            debug!("get_session_id: failed to parse JSON message: {:?}", body);
            return MessageResponse::Error(self.build_rpc_error(model::ErrorData::parse_error("invalid JSON", None)));
        };

        match message {
            model::JsonRpcMessage::Request(json_rpc_request) => {
                self.request_id = json_rpc_request.id.clone();
                self.handle_rpc_request(request, json_rpc_request)
            },
            model::JsonRpcMessage::Response(json_rpc_response) => {
                debug!(target: "mcp_gateway", "UNSUPPORTED RESPONSE MSG: {:#?}", json_rpc_response);
                MessageResponse::Nothing
            },
            model::JsonRpcMessage::Notification(json_rpc_notification) => {
                debug!(target: "mcp_gateway", "UNSUPPORTED NOTIFICATION MSG: {:#?}", json_rpc_notification);
                MessageResponse::Nothing
            },
            model::JsonRpcMessage::Error(json_rpc_error) => {
                debug!(target: "mcp_gateway", "UNSUPPORTED ERROR MSG: {:#?}", json_rpc_error);
                MessageResponse::Nothing
            },
        }
    }

    fn handle_rpc_request(
        &mut self,
        request: &Request<OrionRequestBody>,
        rpc: model::JsonRpcRequest,
    ) -> MessageResponse {
        match rpc.request.method.as_str() {
            "initialize" => {
                debug!(target: "mcp_gateway", "rpc initialize received");
                let Ok(init_params): Result<model::InitializeRequestParam, _> =
                    serde_json::from_value(serde_json::Value::Object(rpc.request.params))
                else {
                    debug!(target: "mcp_gateway", "handle_rpc_request: invalid params");
                    return MessageResponse::Error(
                        self.build_rpc_error(model::ErrorData::invalid_params("invalid params", None)),
                    );
                };

                self.initialize_request_params = Some(init_params);

                let capabilities = ServerCapabilities::builder()
                    .enable_tools()
                    //.enable_tool_list_changed()
                    //.enable_resources()
                    //.enable_logging()
                    .build();

                let result = InitializeResult {
                    protocol_version: ProtocolVersion::default(),
                    server_info: Implementation::from_build_env(),
                    instructions: None,
                    capabilities,
                };

                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: serde_json::to_value(result).unwrap(),
                };

                return MessageResponse::RpcResponse(response);
            },
            "ping" => {
                debug!(target: "mcp_gateway", "rpc ping received");
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: json!({}),
                };

                return MessageResponse::RpcResponse(response);
            },
            "tools/list" => {
                debug!(target: "mcp_gateway", "rpc tools/list received");
                let tools = self.inner.tools.build_list_tools();
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: serde_json::to_value(tools).unwrap(),
                };

                return MessageResponse::RpcResponse(response);
            },
            "tools/call" => {
                debug!(target: "mcp_gateway", "tools/call {:#?}", rpc);
                let Some(request) = self.inner.tools.build_request(request, &rpc.request) else {
                    return MessageResponse::Error(
                        self.build_rpc_error(model::ErrorData::invalid_params("invalid params", None)),
                    );
                };

                debug!(target: "mcp_gateway", "UPSTREAM {:#?}", request);
                return MessageResponse::Upstream(request);
            },
            _ => {
                return MessageResponse::Error(self.build_rpc_error(model::ErrorData::new(
                    model::ErrorCode::METHOD_NOT_FOUND,
                    "Method not found",
                    None,
                )));
            },
        }
    }

    async fn send_message(&self, message: Bytes) -> Result<(), FilterDecision> {
        let mut sender = self.session_ctx.get().sender.lock().await;
        if let Err(e) = sender.send(message).await {
            let mcp_ctx = McpGatewayListenerContext::get_filter_context(self.session_ctx.get().listener_name);
            mcp_ctx.sse_map.remove(&self.session_ctx.get().session_id);
            debug!(target: "mcp_gateway", "send_message: failed to send SSE message for session {}, error {e}", self.session_ctx.get().session_id);
            return Err(FilterDecision::internal_server_error(
                format!("Failed to send SSE message for session {}", self.session_ctx.get().session_id).as_str(),
                self.version,
            ));
        }
        Ok(())
    }

    #[inline]
    fn build_accepted_response(&self) -> Result<Response<OrionResponseBody>, FilterDecision> {
        let builder = Response::builder()
            .header("Access-Control-Allow-Origin", "*")
            .header("Connection", "keep-alive")
            .status(StatusCode::ACCEPTED)
            .version(self.version);
        let body = TimeoutBody::new(None, PolyBody::from(Full::from("Accepted")));
        let Ok(resp) = builder.body(body) else {
            return Err(FilterDecision::internal_server_error(
                format!("Failed to build accepted response for session {}", self.session_ctx.get().session_id).as_str(),
                self.version,
            ));
        };

        Ok(resp)
    }

    #[inline]
    fn build_rpc_error(&self, error: model::ErrorData) -> model::JsonRpcError {
        model::JsonRpcError { jsonrpc: model::JsonRpcVersion2_0, id: self.request_id.clone(), error }
    }
}
