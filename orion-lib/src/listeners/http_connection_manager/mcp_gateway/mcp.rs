use bytes::Bytes;
use dashmap::DashMap;
use futures::SinkExt;
use http::{HeaderName, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    McpGateway as McpGatewayConfig, McpServerInfo,
};
use orion_http_header::MCP_SESSION_ID;
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
            tools::ToolsRegistry,
            transport,
            transport::Transport,
        },
        http_filters::{FactoryFilter, FilterDecision},
        listener::FilterListenerContext,
        metadata::DownstreamMetadata,
    },
    OrionRequestBody, OrionResponseBody, PolyBody,
};

const EVENT_STREAM_MIME_BYTES: &[u8] = b"text/event-stream";
const MIME_TEXT_EVENT_STREAM: &str = "text/event-stream";
const MIME_APPLICATION_JSON: &str = "application/json";
const SESSION_ID_QUERY_KEY: &str = "sessionId";
const SESSION_ID_QUERY_KEY_ALT: &str = "session_id";
const MCP_MESSAGE_ENDPOINT: &str = "/mcp/message";
const MAX_CONCURRENT_ASYNC_REQUESTS: usize = 8192;

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct SessionId(pub SmolStr);

impl SessionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Default)]
pub struct Session {
    listener_name: &'static str, // to handle session eviction from listener.sse_map
    session_id: SessionId,
    transport: Transport,
    sender: Option<Mutex<SseSender>>,
}

#[derive(Debug, Default)]
pub struct McpGatewayListenerContext {
    session_map: DashMap<SessionId, Arc<Session>>,
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
    pub fn get(&self) -> Option<&Session> {
        self.0.as_ref().map(|s| s.as_ref())
    }
}

enum MessageResponse {
    Nothing,
    Error(model::JsonRpcError),
    Response(model::JsonRpcResponse<serde_json::Value>),
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
            inner: Arc::new(McpGatewayInner { config: config.clone(), tools: ToolsRegistry::with_tools(config.tools) }),
            session_ctx: SessionContext(None),
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
        }
    }
}

impl From<McpServerInfo> for Implementation {
    fn from(info: McpServerInfo) -> Self {
        Implementation { name: info.name, version: info.version, ..Default::default() }
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
        let ctx = McpGatewayListenerContext::get_filter_context(metadata.listener_name);

        match (request.method(), request.uri().path()) {
            (&Method::GET, "/sse") => self.handle_sse_handshake(&ctx, request, metadata.listener_name).await,
            (&Method::OPTIONS, "/sse") | (&Method::OPTIONS, "/mcp") | (&Method::OPTIONS, MCP_MESSAGE_ENDPOINT) => {
                self.handle_cors_options(request).await
            },
            (&Method::POST, "/mcp") | (&Method::POST, MCP_MESSAGE_ENDPOINT) => {
                self.handle_mcp_message_endpoint(&ctx, request, metadata.listener_name).await
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
        let Some(session) = self.session_ctx.get() else {
            debug!(target: "mcp_gateway", "apply_response: no session found!");
            return FilterDecision::Continue;
        };

        let ctx = McpGatewayListenerContext::get_filter_context(session.listener_name);
        defer! {
            if matches!(session.transport, Transport::Sse) {
                ctx.active_async_requests.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
        };

        // collect the body...
        let Ok(body) = response.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_response: failed to collect response body");
            let error = self.build_rpc_error(model::ErrorData::internal_error("failed to collect response body", None));
            let event = transport::sse::Event::Message(&error);
            if let Err(e) = self.send_sse_message(event.to_bytes()).await {
                return e;
            }
            return FilterDecision::Continue;
        };

        let body_string = {
            let b = std::str::from_utf8(&body.to_bytes()).map(|s| s.to_string()).unwrap_or_default();
            if !b.is_empty() {
                b
            } else if response.status() == StatusCode::OK {
                "OK".into()
            } else {
                format!("Upstream Error: {}", response.status().canonical_reason().unwrap_or("Unknown"))
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

        let json_rpc_response = self.to_json_rpc_response(server_result);

        match session.transport {
            Transport::Sse => {
                let event = transport::sse::Event::Message(&json_rpc_response);
                debug!(target: "mcp_gateway", "SENDING SSE EVENT: {:?}", event);
                if let Err(e) = self.send_sse_message(event.to_bytes()).await {
                    return e;
                }
                FilterDecision::Continue
            },
            Transport::StreamableHttp => {
                let body = serde_json::to_string(&json_rpc_response).unwrap_or_default();
                match self.build_mcp_response(
                    StatusCode::OK,
                    &[
                        (http::header::CONTENT_TYPE, MIME_APPLICATION_JSON),
                        (MCP_SESSION_ID, self.session_ctx.get().map(|ctx| ctx.session_id.as_str()).unwrap_or_default()),
                    ],
                    Some(body.into()),
                ) {
                    Ok(resp) => {
                        *response = resp;
                        FilterDecision::Continue
                    },
                    Err(_) => {
                        debug!(target: "mcp_gateway", "apply_response: Failed to build MCP response");
                        FilterDecision::Continue
                    },
                }
            },
        }
    }

    fn get_or_create_session_id(
        &mut self,
        ctx: &McpGatewayListenerContext,
        transport: Transport,
        request: &Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> Option<SessionId> {
        match transport {
            Transport::Sse => {
                let Some(session_id) = Self::get_session_id(request) else {
                    return None;
                };

                // search for session in map
                let Some(session) = ctx.session_map.get(&session_id) else {
                    return None;
                };

                // save the current session ID and session
                self.session_ctx = SessionContext(Some(session.value().clone()));
                Some(session_id)
            },
            Transport::StreamableHttp => {
                match Self::get_session_id(request) {
                    Some(session_id) => {
                        let Some(session) = ctx.session_map.get(&session_id) else {
                            return None;
                        };
                        self.session_ctx = SessionContext(Some(session.value().clone()));
                        Some(session_id)
                    },
                    None => {
                        // generate a unique session ID
                        //
                        let new_session_id = SessionId(Uuid::new_v4().to_smolstr());

                        // create a new session
                        let session = Arc::new(Session {
                            sender: None,
                            listener_name: listener_name,
                            transport: Transport::StreamableHttp,
                            session_id: new_session_id.clone(),
                        });

                        // add the session to the map
                        ctx.session_map.insert(new_session_id.clone(), session.clone());
                        self.session_ctx = SessionContext(Some(session));
                        Some(new_session_id)
                    },
                }
            },
        }
    }

    async fn handle_mcp_message_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> FilterDecision {
        // get transport type for the request
        let Some(transport) = Self::get_transport(request) else {
            debug!(target: "mcp_gateway", "handle_message_endpoint: could not get transport type from request");
            return FilterDecision::bad_request(self.version);
        };

        // get session ID from the request or generate a new one
        let Some(session_id) = self.get_or_create_session_id(ctx, transport, request, listener_name) else {
            debug!(target: "mcp_gateway", "handle_message_endpoint: could not get session ID from request");
            return FilterDecision::bad_request(self.version);
        };

        // collect the body of the request...
        let Ok(body) = request.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_request: failed to collect request body");
            return FilterDecision::internal_server_error("Failed to collect request body", self.version);
        };

        let res = self.handle_rpc_message(request, transport, body.to_bytes());
        match res {
            MessageResponse::Error(json_rpc_error) => match transport {
                Transport::Sse => {
                    let event = transport::sse::Event::Message(&json_rpc_error);
                    if let Err(e) = self.send_sse_message(event.to_bytes()).await {
                        return e;
                    }
                    match self.build_mcp_response_accepted(&[]) {
                        Ok(accepted) => return FilterDecision::DirectResponse(accepted),
                        Err(e) => return e,
                    };
                },
                Transport::StreamableHttp => {
                    let body = serde_json::to_string(&json_rpc_error).unwrap_or_default();
                    match self.build_mcp_response(
                        StatusCode::BAD_REQUEST,
                        &[(http::header::CONTENT_TYPE, MIME_APPLICATION_JSON), (MCP_SESSION_ID, session_id.as_str())],
                        Some(body.into()),
                    ) {
                        Ok(resp) => return FilterDecision::DirectResponse(resp),
                        Err(e) => return e,
                    }
                },
            },
            MessageResponse::Response(json_rpc_response) => match transport {
                Transport::Sse => {
                    let event = transport::sse::Event::Message(&json_rpc_response);
                    if let Err(e) = self.send_sse_message(event.to_bytes()).await {
                        return e;
                    }

                    match self.build_mcp_response_accepted(&[]) {
                        Ok(accepted) => return FilterDecision::DirectResponse(accepted),
                        Err(e) => return e,
                    };
                },
                Transport::StreamableHttp => {
                    let body = serde_json::to_string(&json_rpc_response).unwrap_or_default();
                    match self.build_mcp_response(
                        StatusCode::OK,
                        &[(http::header::CONTENT_TYPE, MIME_APPLICATION_JSON), (MCP_SESSION_ID, session_id.as_str())],
                        Some(body.into()),
                    ) {
                        Ok(resp) => return FilterDecision::DirectResponse(resp),
                        Err(e) => return e,
                    }
                },
            },
            MessageResponse::Nothing => match transport {
                Transport::Sse => {
                    let Ok(accepted) = self.build_mcp_response_accepted(&[]) else {
                        return FilterDecision::internal_server_error("Failed to build response", self.version);
                    };
                    return FilterDecision::DirectResponse(accepted);
                },
                Transport::StreamableHttp => {
                    let Ok(accepted) = self.build_mcp_response_accepted(&[(MCP_SESSION_ID, session_id.as_str())])
                    else {
                        return FilterDecision::internal_server_error("Failed to build response", self.version);
                    };
                    return FilterDecision::DirectResponse(accepted);
                },
            },
            MessageResponse::Upstream(req) => match transport {
                Transport::Sse => {
                    if ctx.active_async_requests.load(std::sync::atomic::Ordering::Relaxed)
                        >= MAX_CONCURRENT_ASYNC_REQUESTS
                    {
                        debug!(target: "mcp_gateway", "apply_request: rate limited!");
                        return FilterDecision::rate_limited(request.version());
                    }
                    ctx.active_async_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    let Ok(accepted) = self.build_mcp_response_accepted(&[]) else {
                        return FilterDecision::internal_server_error("Failed to build response", self.version);
                    };
                    return FilterDecision::AsyncRequest(accepted, Some(req));
                },
                Transport::StreamableHttp => {
                    *request = req;
                    return FilterDecision::Continue;
                },
            },
        }
    }

    async fn handle_sse_handshake(
        &mut self,
        ctx: &McpGatewayListenerContext,
        req: &mut Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> FilterDecision {
        debug!(target: "mcp_gateway", "handle_sse_handshake: starting SSE handshake...");

        if !matches!(Self::get_transport(req), Some(Transport::Sse)) {
            debug!(target: "mcp_gateway", "handle_sse_handshake: client does not accept SSE");
            return FilterDecision::bad_request(req.version());
        }

        // generate a unique session ID
        let session_id = SessionId(Uuid::new_v4().to_smolstr());

        // build the SSE response...
        let builder = Response::builder()
            .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(http::header::CONTENT_TYPE, MIME_TEXT_EVENT_STREAM)
            .header(http::header::CACHE_CONTROL, "no-cache, no-transform")
            .header(http::header::CONNECTION, "keep-alive")
            .version(self.version)
            .status(StatusCode::OK);

        let event: transport::sse::Event = transport::sse::Event::Endpoint(&format!(
            "{MCP_MESSAGE_ENDPOINT}?{SESSION_ID_QUERY_KEY}={}",
            session_id.as_str()
        ));

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
            sender: Some(Mutex::new(sender)),
            listener_name: listener_name,
            transport: Transport::Sse,
            session_id: session_id.clone(),
        });

        // add the session to the map
        ctx.session_map.insert(session_id, session.clone());
        self.session_ctx = SessionContext(Some(session));

        debug!(target: "mcp_gateway", "handle_sse_handshake: sending response back to client: {response:?}");
        FilterDecision::DirectResponse(response)
    }

    fn handle_rpc_message(
        &mut self,
        request: &Request<OrionRequestBody>,
        transport: Transport,
        body: Bytes,
    ) -> MessageResponse {
        let Ok(message): Result<model::JsonRpcMessage, _> = serde_json::from_slice(&body) else {
            debug!(target: "mcp_gateway", "handle_rpc_message: failed to parse JSON message: {:?}", body);
            return MessageResponse::Error(self.build_rpc_error(model::ErrorData::parse_error("invalid JSON", None)));
        };

        match message {
            model::JsonRpcMessage::Request(json_rpc_request) => {
                self.request_id = json_rpc_request.id.clone();
                self.handle_rpc_request(request, transport, json_rpc_request)
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
        transport: Transport,
        rpc: model::JsonRpcRequest,
    ) -> MessageResponse {
        match rpc.request.method.as_str() {
            "initialize" => {
                debug!(target: "mcp_gateway", "rpc initialize received (transport {transport})");

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
                    server_info: self.inner.config.server_info.clone().into(),
                    instructions: None,
                    capabilities,
                };

                match transport {
                    Transport::Sse => {
                        // session is already created
                        let response = model::JsonRpcResponse {
                            jsonrpc: model::JsonRpcVersion2_0,
                            id: self.request_id.clone(),
                            result: serde_json::to_value(result).unwrap(),
                        };

                        return MessageResponse::Response(response);
                    },
                    Transport::StreamableHttp => {
                        let response = model::JsonRpcResponse {
                            jsonrpc: model::JsonRpcVersion2_0,
                            id: self.request_id.clone(),
                            result: serde_json::to_value(result).unwrap(),
                        };

                        return MessageResponse::Response(response);
                    },
                }
            },
            "notification/initialized" => {
                debug!(target: "mcp_gateway", "rpc notification/initialized (transport {transport})");
                return MessageResponse::Nothing;
            },
            "ping" => {
                debug!(target: "mcp_gateway", "rpc ping received");
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: json!({}),
                };

                return MessageResponse::Response(response);
            },
            "tools/list" => {
                debug!(target: "mcp_gateway", "rpc tools/list received");
                let tools = self.inner.tools.build_list_tools();
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: serde_json::to_value(tools).unwrap(),
                };

                return MessageResponse::Response(response);
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

    async fn handle_cors_options(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        let builder = Response::builder()
            .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(http::header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST")
            .header(http::header::ACCESS_CONTROL_ALLOW_HEADERS, "content-type, x-mcp-protocol-version")
            .header(http::header::ACCESS_CONTROL_MAX_AGE, "86400")
            .version(self.version)
            .status(StatusCode::NO_CONTENT);

        let Ok(response) = builder.body(TimeoutBody::new(None, PolyBody::from(Empty::new()))) else {
            debug!(target: "mcp_gateway", "handle_cors_options: failed to build body for response");
            return FilterDecision::internal_server_error("Failed to build body for response", req.version());
        };

        debug!(target: "mcp_gateway", "handle_cors_options: sending {response:?} back to client");
        FilterDecision::DirectResponse(response)
    }

    async fn send_sse_message(&self, message: Bytes) -> Result<(), FilterDecision> {
        match self.session_ctx.get() {
            Some(session) => match session.sender.as_ref() {
                Some(sender) => {
                    let mut sender = sender.lock().await;
                    if let Err(e) = sender.send(message).await {
                        let ctx = McpGatewayListenerContext::get_filter_context(session.listener_name);
                        ctx.session_map.remove(&session.session_id);
                        debug!(target: "mcp_gateway", "send_sse_message: failed to send message for session {}, error {e}", session.session_id);
                        return Err(FilterDecision::internal_server_error(
                            format!("Failed to send SSE message for session {}", session.session_id).as_str(),
                            self.version,
                        ));
                    }
                },
                None => {
                    return Err(FilterDecision::internal_server_error("SSE sender not available", self.version));
                },
            },
            None => {
                return Err(FilterDecision::internal_server_error("Session context not available", self.version));
            },
        }
        Ok(())
    }

    #[inline]
    fn build_mcp_response_accepted(
        &self,
        headers: &[(HeaderName, &str)],
    ) -> Result<Response<OrionResponseBody>, FilterDecision> {
        let mut builder = Response::builder()
            .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(http::header::CONNECTION, "keep-alive")
            .status(StatusCode::ACCEPTED)
            .version(self.version);

        for (name, value) in headers {
            builder = builder.header(name.clone(), *value);
        }

        // let body = match transport {
        //     Transport::Sse => TimeoutBody::new(None, PolyBody::from(Full::from("Accepted"))),
        //     Transport::StreamableHttp => TimeoutBody::new(None, PolyBody::from(Empty::new())),
        // };

        let body = TimeoutBody::new(None, PolyBody::from(Empty::new()));
        let Ok(resp) = builder.body(body) else {
            return Err(FilterDecision::internal_server_error(
                format!(
                    "Failed to build accepted response for session {}",
                    self.session_ctx.get().map(|s| s.session_id.clone()).unwrap_or_default()
                )
                .as_str(),
                self.version,
            ));
        };

        Ok(resp)
    }

    #[inline]
    fn build_mcp_response(
        &self,
        status: StatusCode,
        headers: &[(HeaderName, &str)],
        body: Option<Bytes>,
    ) -> Result<Response<OrionResponseBody>, FilterDecision> {
        let mut builder = Response::builder()
            .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(http::header::CONNECTION, "keep-alive")
            .version(self.version)
            .status(status);

        for (name, value) in headers {
            builder = builder.header(name.clone(), *value);
        }

        let body = match body {
            Some(b) => TimeoutBody::new(None, PolyBody::from(Full::from(b))),
            None => TimeoutBody::new(None, PolyBody::from(Empty::new())),
        };

        let Ok(resp) = builder.body(body) else {
            return Err(FilterDecision::internal_server_error(
                format!(
                    "Failed to build accepted response for session {}",
                    self.session_ctx.get().map(|s| s.session_id.clone()).unwrap_or_default()
                )
                .as_str(),
                self.version,
            ));
        };

        Ok(resp)
    }

    fn get_transport(req: &Request<OrionRequestBody>) -> Option<Transport> {
        match *req.method() {
            Method::POST => {
                if let Some(query) = req.uri().query() {
                    if query.contains(SESSION_ID_QUERY_KEY) || query.contains(SESSION_ID_QUERY_KEY_ALT) {
                        return Some(Transport::Sse);
                    }
                }
                return Some(Transport::StreamableHttp);
            },
            Method::GET => {
                if let Some(accept_value) = req.headers().get(http::header::ACCEPT) {
                    let bytes = accept_value.as_bytes();
                    if bytes.windows(EVENT_STREAM_MIME_BYTES.len()).any(|w| w == EVENT_STREAM_MIME_BYTES) {
                        return Some(Transport::Sse);
                    }
                }
                None
            },

            _ => None,
        }
    }

    fn get_session_id(req: &Request<OrionRequestBody>) -> Option<SessionId> {
        if let Some(id) = req.headers().get(MCP_SESSION_ID) {
            return id.to_str().ok().map(|s| SessionId(s.to_smolstr()));
        }
        req.uri().query().and_then(|query| {
            debug!(target: "mcp_gateway", "get_session_id: query: {query}..");
            form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == SESSION_ID_QUERY_KEY || key == SESSION_ID_QUERY_KEY_ALT)
                .map(|(_, value)| SessionId(value.to_smolstr()))
        })
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

    #[inline]
    fn build_rpc_error(&self, error: model::ErrorData) -> model::JsonRpcError {
        model::JsonRpcError { jsonrpc: model::JsonRpcVersion2_0, id: self.request_id.clone(), error }
    }
}
