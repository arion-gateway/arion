use bytes::Bytes;
use dashmap::DashMap;
use futures::SinkExt;
use http::{HeaderName, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    McpGateway as McpGatewayConfig, McpServerInfo,
};
use orion_http_header::MCP_SESSION_ID;
use scopeguard::defer;
use serde::Serialize;
use serde_json::{json, Value};
use smol_str::ToSmolStr;
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::Mutex;
use tracing::debug;
use uuid::Uuid;

use rmcp::model::{
    self, Annotated, CallToolResult, Implementation, InitializeResult, JsonRpcResponse, ProtocolVersion, RawContent,
    RawTextContent, ServerCapabilities, ServerResult,
};

use crate::{
    body::{
        sse_body::{SseBody, SseSender},
        timeout_body::TimeoutBody,
    },
    listeners::{
        http_connection_manager::mcp_gateway::{
            tools::ToolsRegistry,
            transport::{self, RequestExt, SessionId, Transport},
        },
        http_filters::{FactoryFilter, FilterDecision},
        listener::FilterListenerContext,
        metadata::DownstreamMetadata,
    },
    OrionRequestBody, OrionResponseBody, PolyBody,
};

const MIME_TEXT_EVENT_STREAM: &str = "text/event-stream";
const MIME_APPLICATION_JSON: &str = "application/json";
const MCP_MESSAGE_ENDPOINT: &str = "/mcp/message";

const RPC_METHOD_INITIALIZE: &str = "initialize";
const RPC_METHOD_PING: &str = "ping";
const RPC_METHOD_TOOLS_LIST: &str = "tools/list";
const RPC_METHOD_TOOLS_CALL: &str = "tools/call";
const RPC_METHOD_NOTIFICATION_INITIALIZED: &str = "notification/initialized";

const MAX_CONCURRENT_ASYNC_REQUESTS: usize = 8192;

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
    client_origin: Option<HeaderValue>,
}

impl From<McpGatewayConfig> for McpGateway {
    fn from(config: McpGatewayConfig) -> Self {
        Self {
            inner: Arc::new(McpGatewayInner { config: config.clone(), tools: ToolsRegistry::with_tools(config.tools) }),
            session_ctx: SessionContext(None),
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
            client_origin: None,
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
            client_origin: None,
        }
    }
}

impl McpGateway {
    pub async fn apply_request(&mut self, request: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "mcp_gateway", "apply_request: processing request: {:?}", request);

        self.version = request.version();
        self.client_origin = request.headers().get(http::header::ORIGIN).cloned();

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
                    Some(body.into()),
                    &[
                        (http::header::CONTENT_TYPE, MIME_APPLICATION_JSON),
                        (MCP_SESSION_ID, self.session_ctx.get().map(|ctx| ctx.session_id.as_str()).unwrap_or_default()),
                    ],
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
        debug!(target: "mcp_gateway", "get_or_create_session_id: transport={:?}, request={:?}", transport, request);
        match transport {
            Transport::Sse => {
                let Some(session_id) = request.get_session_id() else {
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
                match request.get_session_id() {
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
        let Some(transport) = request.get_mcp_transport() else {
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
                    match self.build_mcp_response(StatusCode::ACCEPTED, None, &[]) {
                        Ok(accepted) => return FilterDecision::DirectResponse(accepted),
                        Err(e) => return e,
                    };
                },
                Transport::StreamableHttp => {
                    let body = serde_json::to_string(&json_rpc_error).unwrap_or_default();
                    match self.build_mcp_response(
                        StatusCode::BAD_REQUEST,
                        Some(body.into()),
                        &[(http::header::CONTENT_TYPE, MIME_APPLICATION_JSON), (MCP_SESSION_ID, session_id.as_str())],
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

                    match self.build_mcp_response(StatusCode::ACCEPTED, None, &[]) {
                        Ok(accepted) => return FilterDecision::DirectResponse(accepted),
                        Err(e) => return e,
                    };
                },
                Transport::StreamableHttp => {
                    let body = serde_json::to_string(&json_rpc_response).unwrap_or_default();
                    match self.build_mcp_response(
                        StatusCode::OK,
                        Some(body.into()),
                        &[(http::header::CONTENT_TYPE, MIME_APPLICATION_JSON), (MCP_SESSION_ID, session_id.as_str())],
                    ) {
                        Ok(resp) => return FilterDecision::DirectResponse(resp),
                        Err(e) => return e,
                    }
                },
            },
            MessageResponse::Nothing => match transport {
                Transport::Sse => {
                    let Ok(accepted) = self.build_mcp_response(StatusCode::ACCEPTED, None, &[]) else {
                        return FilterDecision::internal_server_error("Failed to build response", self.version);
                    };
                    return FilterDecision::DirectResponse(accepted);
                },
                Transport::StreamableHttp => {
                    let Ok(accepted) =
                        self.build_mcp_response(StatusCode::ACCEPTED, None, &[(MCP_SESSION_ID, session_id.as_str())])
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

                    let Ok(accepted) = self.build_mcp_response(StatusCode::ACCEPTED, None, &[]) else {
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

        if !matches!(req.get_mcp_transport(), Some(Transport::Sse)) {
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
            "{MCP_MESSAGE_ENDPOINT}?{}={}",
            transport::SESSION_ID_QUERY_KEY,
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
            transport: Transport::Sse,
            session_id: session_id.clone(),
            listener_name,
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
        // WORKAROUND: the current rmcp implementation fails to parse
        // {"jsonrpc":"2.0", "method":"notifications/initialized"},
        // which is a valid JSON-RPC notification according to the spec.
        // rmcp incorrectly requires the "params" field, even though it is optional.
        // This workaround injects an empty "params" object to satisfy rmcp.

        let Ok(mut value): Result<Value, _> = serde_json::from_slice(&body) else {
            debug!(target: "mcp_gateway", "handle_rpc_message: failed to parse JSON message: {:?}", body);
            return MessageResponse::Error(self.build_rpc_error(model::ErrorData::parse_error("invalid JSON", None)));
        };

        if value.get("id").is_none() && value.get("params").is_none() {
            if let Some(obj) = value.as_object_mut() {
                obj.insert("params".to_string(), serde_json::json!({}));
            }
        }

        let Ok(message): Result<model::JsonRpcMessage, _> = serde_json::from_value(value) else {
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
            RPC_METHOD_INITIALIZE => {
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
                            result: serde_json::to_value(result).unwrap_or(serde_json::Value::Null),
                        };

                        MessageResponse::Response(response)
                    },
                    Transport::StreamableHttp => {
                        let response = model::JsonRpcResponse {
                            jsonrpc: model::JsonRpcVersion2_0,
                            id: self.request_id.clone(),
                            result: serde_json::to_value(result).unwrap_or(serde_json::Value::Null),
                        };

                        MessageResponse::Response(response)
                    },
                }
            },
            RPC_METHOD_NOTIFICATION_INITIALIZED => {
                debug!(target: "mcp_gateway", "rpc notification/initialized (transport {transport})");
                MessageResponse::Nothing
            },
            RPC_METHOD_PING => {
                debug!(target: "mcp_gateway", "rpc ping received");
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: json!({}),
                };

                MessageResponse::Response(response)
            },
            RPC_METHOD_TOOLS_LIST => {
                debug!(target: "mcp_gateway", "rpc tools/list received");
                let tools = self.inner.tools.build_list_tools();
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: serde_json::to_value(tools).unwrap_or(serde_json::Value::Null),
                };

                MessageResponse::Response(response)
            },
            RPC_METHOD_TOOLS_CALL => {
                debug!(target: "mcp_gateway", "tools/call {:#?}", rpc);
                let Some(request) = self.inner.tools.build_request(request, &rpc.request) else {
                    return MessageResponse::Error(
                        self.build_rpc_error(model::ErrorData::invalid_params("invalid params", None)),
                    );
                };

                debug!(target: "mcp_gateway", "UPSTREAM {:#?}", request);
                MessageResponse::Upstream(request)
            },
            _ => MessageResponse::Error(self.build_rpc_error(model::ErrorData::new(
                model::ErrorCode::METHOD_NOT_FOUND,
                "Method not found",
                None,
            ))),
        }
    }

    async fn handle_cors_options(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        let allow_origin = self.client_origin.clone().unwrap_or_else(|| HeaderValue::from_static("*"));

        let request_headers = req
            .headers()
            .get(http::header::ACCESS_CONTROL_REQUEST_HEADERS)
            .cloned()
            .unwrap_or_else(|| HeaderValue::from_static("content-type, mcp-session-id, mcp-protocol-version"));

        let builder = Response::builder()
            .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin)
            .header(http::header::ACCESS_CONTROL_ALLOW_CREDENTIALS, "true")
            .header(http::header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS")
            .header(http::header::ACCESS_CONTROL_ALLOW_HEADERS, request_headers)
            .header(http::header::ACCESS_CONTROL_EXPOSE_HEADERS, "mcp-session-id, last-event-id, mcp-protocol-version")
            .header(http::header::VARY, "Origin, Access-Control-Request-Headers")
            .header(http::header::CONNECTION, "keep-alive")
            .version(self.version)
            .status(StatusCode::NO_CONTENT);

        let Ok(response) = builder.body(TimeoutBody::new(None, PolyBody::from(Empty::new()))) else {
            return FilterDecision::internal_server_error("Failed to build CORS body", req.version());
        };

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

    fn build_mcp_response(
        &self,
        status: StatusCode,
        body: Option<Bytes>,
        headers: &[(HeaderName, &str)],
    ) -> Result<Response<OrionResponseBody>, FilterDecision> {
        let allow_origin = self.client_origin.clone().unwrap_or_else(|| HeaderValue::from_static("*"));

        let mut builder = Response::builder()
            .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin)
            .header(http::header::ACCESS_CONTROL_ALLOW_CREDENTIALS, "true")
            .header(http::header::ACCESS_CONTROL_EXPOSE_HEADERS, "mcp-session-id,last-event-id,mcp-protocol-version")
            .header(http::header::CONNECTION, "keep-alive")
            .version(self.version)
            .status(status);

        for (name, value) in headers {
            builder = builder.header(name, *value);
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

    #[inline]
    fn to_json_rpc_response<T: Serialize>(&self, value: T) -> JsonRpcResponse {
        let json_value = serde_json::to_value(value).unwrap_or(serde_json::Value::Null);
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
