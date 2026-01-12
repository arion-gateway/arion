use bytes::Bytes;
use dashmap::DashMap;
use futures::SinkExt;
use http::{HeaderName, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
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
    self, Annotated, CallToolRequestMethod, CallToolResult, ConstString, Implementation, InitializeResult,
    InitializeResultMethod, InitializedNotificationMethod, JsonRpcError, JsonRpcResponse, ListToolsRequestMethod,
    PingRequestMethod, ProtocolVersion, RawContent, RawTextContent, ServerCapabilities, ServerResult,
};

use crate::{
    body::{
        sse_body::{SseBody, SseSender},
        timeout_body::TimeoutBody,
    },
    listeners::{
        http_connection_manager::mcp_gateway::{
            tools::ToolsRegistry,
            transport::{
                self, AcceptedMime, RequestExt, SessionId, Transport, MIME_APPLICATION_JSON, MIME_TEXT_EVENT_STREAM,
            },
        },
        http_filters::{FactoryFilter, FilterDecision},
        listener::FilterListenerContext,
        metadata::DownstreamMetadata,
    },
    OrionRequestBody, OrionResponseBody, PolyBody,
};

const MCP_MESSAGE_ENDPOINT: &str = "/mcp/message";

#[derive(Debug, Default)]
pub struct Session {
    listener_name: &'static str, // to handle session eviction from listener.sse_map
    session_id: SessionId,
    transport: Transport,
    session_sse_sender: Option<Mutex<SseSender>>,
}

#[derive(Debug, Default)]
pub struct McpGatewayListenerContext {
    session_map: DashMap<SessionId, Arc<Session>>,
    active_async_requests: AtomicUsize,
}

#[derive(Debug, thiserror::Error)]
pub enum CreateSessionError {
    #[error("Session limit reached")]
    LimitReached,
}

impl McpGatewayListenerContext {
    const MAX_CONCURRENT_ASYNC_REQUESTS: usize = 4096;
    const MAX_SESSIONS_LIMIT: usize = 65536;

    #[inline]
    pub fn try_start_async_request(&self) -> bool {
        if self.active_async_requests.load(std::sync::atomic::Ordering::Relaxed) >= Self::MAX_CONCURRENT_ASYNC_REQUESTS
        {
            return false;
        }
        self.active_async_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    }

    #[inline]
    pub fn end_async_request(&self) {
        self.active_async_requests.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn create_session(&self, listener_name: &'static str) -> Result<Arc<Session>, CreateSessionError> {
        if self.session_map.len() >= Self::MAX_SESSIONS_LIMIT {
            debug!(target: "mcp_gateway", "create_new_session: session limit reached");
            return Err(CreateSessionError::LimitReached);
        }

        let session_id = SessionId(Uuid::new_v4().to_smolstr());

        let session = Arc::new(Session {
            listener_name,
            session_id: session_id.clone(),
            transport: Transport::default(),
            session_sse_sender: None,
        });
        self.session_map.insert(session_id, session.clone());
        Ok(session)
    }

    #[inline]
    pub fn delete_session(&self, session_id: &SessionId) -> bool {
        self.session_map.remove(session_id).is_some()
    }
}

#[derive(Debug)]
pub struct McpGatewayInner {
    config: McpGatewayConfig,
    tools: ToolsRegistry,
}

enum MessageResponse {
    Nothing,
    Error(model::JsonRpcError),
    Response(model::JsonRpcResponse<serde_json::Value>),
    Upstream((Request<OrionRequestBody>, bool)),
}

/// McpGateway filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: Arc<McpGatewayInner>,
    session: Option<Arc<Session>>,
    request_id: model::RequestId,
    version: http::Version,
    initialize_request_params: Option<model::InitializeRequestParam>,
    client_origin: Option<HeaderValue>,
    sse_sender: Option<Arc<Mutex<SseSender>>>,
}

impl From<McpGatewayConfig> for McpGateway {
    fn from(config: McpGatewayConfig) -> Self {
        Self {
            inner: Arc::new(McpGatewayInner { config: config.clone(), tools: ToolsRegistry::with_tools(config.tools) }),
            session: None,
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
            client_origin: None,
            sse_sender: None,
        }
    }
}

impl FactoryFilter for McpGateway {
    fn new_from(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            session: None,
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
            client_origin: None,
            sse_sender: None,
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
            (&Method::GET, "/mcp") => FilterDecision::method_not_allowed(request.version()),
            (&Method::OPTIONS, "/sse") | (&Method::OPTIONS, "/mcp") | (&Method::OPTIONS, MCP_MESSAGE_ENDPOINT) => {
                self.handle_cors_options(request).await
            },
            (&Method::POST, "/mcp") | (&Method::POST, MCP_MESSAGE_ENDPOINT) => {
                self.handle_mcp_post_endpoint(&ctx, request, metadata.listener_name).await
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
        debug!(target: "mcp_gateway", "apply_response: processing response...");

        // get global context for this listener
        let Some(session) = self.session.as_deref() else {
            debug!(target: "mcp_gateway", "apply_response: no session found!");
            return FilterDecision::Continue;
        };

        let ctx = McpGatewayListenerContext::get_filter_context(session.listener_name);
        let is_async = { matches!(session.transport, Transport::Sse) || self.sse_sender.as_ref().is_some() };

        defer! {
            if is_async {
                ctx.end_async_request();
            }
        };

        // collect the body...
        let Ok(body) = response.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_response: failed to collect response body");
            let error = self.build_rpc_error(model::ErrorData::internal_error("failed to collect response body", None));
            match session.transport {
                Transport::Sse => {
                    let Some(sender) = session.session_sse_sender.as_ref() else {
                        debug!(target: "mcp_gateway", "apply_response: SSE sender not available for session {}", session.session_id);
                        return FilterDecision::internal_server_error("SSE sender not available", self.version);
                    };

                    let event = transport::sse::Event::Message(&error);
                    let mut sender = sender.lock().await;
                    if let Err(e) = Self::send_sse_message(&mut sender, event.to_bytes(), self.version).await {
                        return e;
                    }
                },
                Transport::StreamableHttp => {
                    let event = transport::streamable_http::Event::Message(&error);
                    if let Some(sender) = &mut self.sse_sender {
                        let mut sender_guard = sender.lock().await;
                        let sender = &mut *sender_guard;
                        if let Err(e) = Self::send_sse_message(sender, event.to_bytes(), self.version).await {
                            return e;
                        }
                        sender.close();
                    }
                },
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
                let Some(sender) = session.session_sse_sender.as_ref() else {
                    debug!(target: "mcp_gateway", "apply_response: SSE sender not available for session {}", session.session_id);
                    return FilterDecision::internal_server_error("SSE sender not available", self.version);
                };
                debug!(target: "mcp_gateway", "apply_response: SSE response...");
                let event = transport::sse::Event::Message(&json_rpc_response);
                debug!(target: "mcp_gateway", "SENDING SSE EVENT: {:?}", event);
                let mut sender = sender.lock().await;
                if let Err(e) = Self::send_sse_message(&mut sender, event.to_bytes(), self.version).await {
                    return e;
                }
                FilterDecision::Continue
            },
            Transport::StreamableHttp => match self.sse_sender.as_mut() {
                Some(sender) => {
                    debug!(target: "mcp_gateway", "apply_response: streamable HTTP (async SSE)...");
                    let mut sender_guard = sender.lock().await;
                    let sender = &mut *sender_guard;
                    let event = transport::streamable_http::Event::Message(&json_rpc_response);
                    if let Err(e) = sender.send(event.to_bytes()).await {
                        debug!(target: "mcp_gateway", "send_streamable_http_message: failed to send message for session {}, error {e}", session.session_id);
                    }
                    sender.close();
                    FilterDecision::Continue
                },
                None => {
                    debug!(target: "mcp_gateway", "apply_response: streamable HTTP (application/json)...");
                    let body = serde_json::to_vec(&json_rpc_response).unwrap_or_default();
                    match self.build_mcp_response(
                        StatusCode::OK,
                        Self::build_mcp_body(Some(body.into())),
                        &[
                            (http::header::CONTENT_TYPE, MIME_APPLICATION_JSON),
                            (MCP_SESSION_ID, self.session.as_deref().map(|s| s.session_id.as_str()).unwrap_or_default()),
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
            },
        }
    }

    fn get_valid_session(
        &mut self,
        ctx: &McpGatewayListenerContext,
        transport: Transport,
        request: &Request<OrionRequestBody>,
    ) -> Option<Arc<Session>> {
        debug!(target: "mcp_gateway", "get_or_create_session_id: transport={:?}, request={:?}", transport, request);
        match transport {
            Transport::Sse => {
                let Some(session_id) = request.get_mcp_session_id() else {
                    debug!(target: "mcp_gateway", "get_or_create_session_id: SSE transport requires session ID but none found in request");
                    return None;
                };

                // search for session in map
                let Some(session) = ctx.session_map.get(&session_id) else {
                    debug!(target: "mcp_gateway", "get_or_create_session_id: session {} not found in session map", session_id);
                    return None;
                };

                // self.session_ctx = SessionContext(Some(session.value().clone()));
                Some(session.clone())
            },
            Transport::StreamableHttp => match request.get_mcp_session_id() {
                Some(session_id) => {
                    let Some(session) = ctx.session_map.get(&session_id) else {
                        debug!(target: "mcp_gateway", "get_or_create_session_id: StreamableHttp session {} not found in session map", session_id);
                        return None;
                    };

                    Some(session.clone())
                },
                None => None,
            },
        }
    }

    async fn handle_mcp_post_endpoint(
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

        if matches!(transport, Transport::StreamableHttp) {
            let accept = request.get_mcp_accepted_mime();
            if !matches!(accept, Some(AcceptedMime::EventStreamAndJson)) {
                debug!(target: "mcp_gateway", "handle_message_endpoint: unsupported MIME type for StreamableHttp transport");
                return FilterDecision::bad_request(self.version);
            }
        }

        // get session ID from the request. It may get None, in which case we create a new session+id in the "initialize" method
        let mut session = self.get_valid_session(ctx, transport, request);

        // collect the body of the request...
        let Ok(body) = request.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_request: failed to collect request body");
            return FilterDecision::internal_server_error("Failed to collect request body", self.version);
        };

        //
        // handle the JSON RPC message
        //
        let response =
            self.handle_rpc_json_message(ctx, request, transport, body.to_bytes(), listener_name, &mut session);

        if let Some(session) = session {
            self.session = Some(session);
        }

        match response {
            MessageResponse::Error(json_rpc_error) => match transport {
                Transport::Sse => {
                    let Some(sender) = self.session.as_deref().and_then(|s| s.session_sse_sender.as_ref()) else {
                        debug!(target: "mcp_gateway", "handle_mcp_message_endpoint: SSE sender not available");
                        return FilterDecision::internal_server_error("SSE sender not available", self.version);
                    };
                    let event = transport::sse::Event::Message(&json_rpc_error);
                    let mut sender = sender.lock().await;
                    if let Err(e) = Self::send_sse_message(&mut sender, event.to_bytes(), self.version).await {
                        return e;
                    }
                    match self.build_mcp_response(StatusCode::ACCEPTED, Self::build_mcp_body(None), &[]) {
                        Ok(accepted) => return FilterDecision::DirectResponse(accepted),
                        Err(e) => return e,
                    };
                },
                Transport::StreamableHttp => {
                    let body = serde_json::to_string(&json_rpc_error).unwrap_or_default();
                    let session_id = self.get_current_session_id();

                    let headers: &[(http::header::HeaderName, &str)] = match session_id {
                        Some(session_id) => &[
                            (http::header::CONTENT_TYPE, MIME_APPLICATION_JSON),
                            (MCP_SESSION_ID, session_id.as_str()),
                        ],
                        None => &[(http::header::CONTENT_TYPE, MIME_APPLICATION_JSON)],
                    };

                    match self.build_mcp_response(
                        StatusCode::BAD_REQUEST,
                        Self::build_mcp_body(Some(body.into())),
                        headers,
                    ) {
                        Ok(resp) => return FilterDecision::DirectResponse(resp),
                        Err(e) => return e,
                    }
                },
            },
            MessageResponse::Response(json_rpc_response) => match transport {
                Transport::Sse => {
                    let Some(sender) = self.session.as_deref().and_then(|s| s.session_sse_sender.as_ref()) else {
                        debug!(target: "mcp_gateway", "handle_mcp_message_endpoint: SSE sender not available");
                        return FilterDecision::internal_server_error("SSE sender not available", self.version);
                    };
                    let event = transport::sse::Event::Message(&json_rpc_response);
                    let mut sender = sender.lock().await;
                    if let Err(e) = Self::send_sse_message(&mut sender, event.to_bytes(), self.version).await {
                        return e;
                    }

                    match self.build_mcp_response(StatusCode::ACCEPTED, Self::build_mcp_body(None), &[]) {
                        Ok(accepted) => return FilterDecision::DirectResponse(accepted),
                        Err(e) => return e,
                    };
                },
                Transport::StreamableHttp => {
                    let body = serde_json::to_vec(&json_rpc_response).unwrap_or_default();
                    let session_id = self.get_current_session_id();
                    let headers: &[(http::header::HeaderName, &str)] = match session_id {
                        Some(session_id) => &[
                            (http::header::CONTENT_TYPE, MIME_APPLICATION_JSON),
                            (MCP_SESSION_ID, session_id.as_str()),
                        ],
                        None => &[(http::header::CONTENT_TYPE, MIME_APPLICATION_JSON)],
                    };

                    match self.build_mcp_response(StatusCode::OK, Self::build_mcp_body(Some(body.into())), headers) {
                        Ok(resp) => return FilterDecision::DirectResponse(resp),
                        Err(e) => return e,
                    }
                },
            },
            MessageResponse::Nothing => match transport {
                Transport::Sse => {
                    let Ok(accepted) = self.build_mcp_response(StatusCode::ACCEPTED, Self::build_mcp_body(None), &[])
                    else {
                        return FilterDecision::internal_server_error("Failed to build response", self.version);
                    };
                    return FilterDecision::DirectResponse(accepted);
                },
                Transport::StreamableHttp => {
                    let session_id = self.get_current_session_id();
                    let headers: &[(HeaderName, &str)] =
                        if let Some(session_id) = session_id { &[(MCP_SESSION_ID, session_id.as_str())] } else { &[] };
                    let Ok(accepted) =
                        self.build_mcp_response(StatusCode::ACCEPTED, Self::build_mcp_body(None), headers)
                    else {
                        return FilterDecision::internal_server_error("Failed to build response", self.version);
                    };
                    return FilterDecision::DirectResponse(accepted);
                },
            },
            MessageResponse::Upstream((upstream_request, async_call)) => match transport {
                Transport::Sse => {
                    debug!(target: "mcp_gateway", "apply_request: legacy SSE...");
                    if !ctx.try_start_async_request() {
                        debug!(target: "mcp_gateway", "apply_request: rate limited!");
                        return FilterDecision::rate_limited(request.version());
                    }

                    let Ok(accepted) = self.build_mcp_response(StatusCode::ACCEPTED, Self::build_mcp_body(None), &[])
                    else {
                        return FilterDecision::internal_server_error("Failed to build response", self.version);
                    };
                    return FilterDecision::AsyncRequest(accepted, Some(upstream_request));
                },
                Transport::StreamableHttp if async_call => {
                    debug!(target: "mcp_gateway", "apply_request: streamable http...");
                    if !ctx.try_start_async_request() {
                        debug!(target: "mcp_gateway", "apply_request: rate limited!");
                        return FilterDecision::rate_limited(request.version());
                    }

                    let (body, mut sender) = SseBody::new();

                    // priming event...
                    let event: transport::streamable_http::Event = transport::streamable_http::Event::Priming;
                    if let Err(e) = sender.send(event.to_bytes()).await {
                        debug!(target: "mcp_gateway", "handle_mcp_message_endpoint: failed to send priming event: {e}");
                        return FilterDecision::internal_server_error("Failed to send priming event", self.version);
                    };

                    // save the sender for use on response
                    self.sse_sender = Some(Arc::new(Mutex::new(sender)));

                    let body = TimeoutBody::new(None, PolyBody::from(body));
                    let Ok(mut okay) = self.build_mcp_response(
                        StatusCode::OK,
                        body,
                        &[
                            (http::header::CONTENT_TYPE, MIME_TEXT_EVENT_STREAM),
                            (http::header::CACHE_CONTROL, "no-cache"),
                        ],
                    ) else {
                        return FilterDecision::internal_server_error("Failed to build response", self.version);
                    };

                    if let Some(session_id) = request.headers().get(MCP_SESSION_ID) {
                        okay.headers_mut().insert(MCP_SESSION_ID, session_id.clone());
                    }

                    debug!(target: "mcp_gateway", "apply_request: returning async request...");
                    return FilterDecision::AsyncRequest(okay, Some(upstream_request));
                },
                Transport::StreamableHttp => {
                    *request = upstream_request;
                    return FilterDecision::Continue;
                },
            },
        }
    }

    async fn handle_sse_handshake(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> FilterDecision {
        debug!(target: "mcp_gateway", "handle_sse_handshake: starting SSE handshake...");

        if !matches!(request.get_mcp_transport(), Some(Transport::Sse)) {
            debug!(target: "mcp_gateway", "handle_sse_handshake: client does not accept SSE");
            return FilterDecision::bad_request(request.version());
        }

        // build the SSE response...
        let builder = Response::builder()
            .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(http::header::CONTENT_TYPE, MIME_TEXT_EVENT_STREAM)
            .header(http::header::CACHE_CONTROL, "no-cache, no-transform")
            .header(http::header::CONNECTION, "keep-alive")
            .version(self.version)
            .status(StatusCode::OK);

        // create a new session
        let session = match ctx.create_session(listener_name) {
            Ok(session) => session,
            Err(err) => {
                debug!(target: "mcp_gateway", "handle_sse_handshake: failed to create new session: {err}");
                return FilterDecision::internal_server_error("Failed to create new session", request.version());
            },
        };

        let event: transport::sse::Event = transport::sse::Event::Endpoint(&format!(
            "{MCP_MESSAGE_ENDPOINT}?{}={}",
            transport::SESSION_ID_QUERY_KEY,
            session.session_id.as_str()
        ));

        let (body, mut sender) = SseBody::new();
        let body = TimeoutBody::new(None, PolyBody::from(body));

        let Ok(_) = sender.send(event.to_bytes()).await else {
            debug!(target: "mcp_gateway", "handle_sse_handshake: failed to send SSE payload");
            ctx.delete_session(&session.session_id);
            return FilterDecision::internal_server_error("Failed to send SSE payload", request.version());
        };

        let Ok(response) = builder.body(body) else {
            debug!(target: "mcp_gateway", "handle_sse_handshake: failed to build body for response");
            ctx.delete_session(&session.session_id);
            return FilterDecision::internal_server_error("Failed to build body for response", request.version());
        };

        self.session = Some(session);
        FilterDecision::DirectResponse(response)
    }

    fn handle_rpc_json_message(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &Request<OrionRequestBody>,
        transport: Transport,
        body: Bytes,
        listener_name: &'static str,
        session: &mut Option<Arc<Session>>,
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
                self.handle_rpc_json_request(ctx, request, transport, json_rpc_request, listener_name, session)
            },
            model::JsonRpcMessage::Response(json_rpc_response) => {
                debug!(target: "mcp_gateway", "Unsupported json rpc response: {:#?}", json_rpc_response);
                MessageResponse::Nothing
            },
            model::JsonRpcMessage::Notification(json_rpc_notification) => {
                debug!(target: "mcp_gateway", "Unsupported json rpc notification: {:#?}", json_rpc_notification);
                MessageResponse::Nothing
            },
            model::JsonRpcMessage::Error(json_rpc_error) => {
                debug!(target: "mcp_gateway", "Unsupported json rpc error: {:#?}", json_rpc_error);
                MessageResponse::Nothing
            },
        }
    }

    fn handle_rpc_json_request(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &Request<OrionRequestBody>,
        transport: Transport,
        rpc: model::JsonRpcRequest,
        listener_name: &'static str,
        session: &mut Option<Arc<Session>>,
    ) -> MessageResponse {
        match rpc.request.method.as_str() {
            InitializeResultMethod::VALUE => {
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

                let server_info = {
                    let info = &self.inner.config.server_info;
                    Implementation { name: info.name.clone(), version: info.version.clone(), ..Default::default() }
                };

                let result = InitializeResult {
                    protocol_version: ProtocolVersion::default(),
                    instructions: None,
                    capabilities,
                    server_info,
                };

                //
                // generate a new unique session ID,
                //

                let Ok(new_session) = ctx.create_session(listener_name) else {
                    debug!(target: "mcp_gateway", "handle_sse_handshake: failed to create new session!");
                    return MessageResponse::Error(
                        self.build_rpc_error(model::ErrorData::parse_error("Failed to create new session", None)),
                    );
                };

                *session = Some(new_session);

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
            InitializedNotificationMethod::VALUE => {
                debug!(target: "mcp_gateway", "rpc notification/initialized (transport {transport})");
                MessageResponse::Nothing
            },
            PingRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "rpc ping received");
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: json!({}),
                };

                MessageResponse::Response(response)
            },
            ListToolsRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "rpc tools/list received");
                let tools = self.inner.tools.build_list_tools();
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: serde_json::to_value(tools).unwrap_or(serde_json::Value::Null),
                };

                MessageResponse::Response(response)
            },
            CallToolRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "tools/call {:#?}", rpc);
                let (request, r#async) = match self.inner.tools.build_request(
                    request,
                    &rpc.request,
                    &self.inner.config.cluster_header,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        debug!(target: "mcp_gateway", "handle_rpc_request: tools/call failed to build request: {e:#}");
                        return MessageResponse::Error(
                            self.build_rpc_error(model::ErrorData::invalid_params("invalid params", None)),
                        );
                    },
                };

                debug!(target: "mcp_gateway", "UPSTREAM {:#?}", request);
                MessageResponse::Upstream((request, r#async))
            },
            _ => {
                debug!(target: "mcp_gateway", "handle_rpc_request: unknown rpc method '{}'", rpc.request.method);
                MessageResponse::Error(self.build_rpc_error(model::ErrorData::new(
                    model::ErrorCode::METHOD_NOT_FOUND,
                    "Method not found",
                    None,
                )))
            },
        }
    }

    async fn handle_cors_options(&mut self, request: &mut Request<OrionRequestBody>) -> FilterDecision {
        let allow_origin = self.client_origin.clone().unwrap_or_else(|| HeaderValue::from_static("*"));

        let request_headers = request
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
            debug!(target: "mcp_gateway", "handle_cors_options: failed to build CORS response body");
            return FilterDecision::internal_server_error("Failed to build CORS body", request.version());
        };

        FilterDecision::DirectResponse(response)
    }

    async fn send_sse_message(
        sender: &mut SseSender,
        message: Bytes,
        version: http::Version,
    ) -> Result<(), FilterDecision> {
        if let Err(e) = sender.send(message).await {
            debug!(target: "mcp_gateway", "send_sse_message: failed to send message: {e}");
            return Err(FilterDecision::internal_server_error("Failed to send SSE message", version));
        }
        Ok(())
    }

    #[inline]
    fn build_mcp_body(bytes: Option<Bytes>) -> TimeoutBody<PolyBody> {
        match bytes {
            Some(b) => TimeoutBody::new(None, PolyBody::from(Full::from(b))),
            None => TimeoutBody::new(None, PolyBody::from(Empty::new())),
        }
    }

    fn build_mcp_response(
        &self,
        status: StatusCode,
        body: TimeoutBody<PolyBody>,
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

        let Ok(resp) = builder.body(body) else {
            debug!(target: "mcp_gateway", "build_mcp_response: failed to build response body for session {}",
                self.session.as_deref().map(|s| s.session_id.clone()).unwrap_or_default());
            return Err(FilterDecision::internal_server_error(
                format!(
                    "Failed to build accepted response for session {}",
                    self.session.as_deref().map(|s| s.session_id.clone()).unwrap_or_default()
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

    #[inline]
    pub fn get_current_session_id(&self) -> Option<&SessionId> {
        self.session.as_deref().map(|s| &s.session_id)
    }
}
