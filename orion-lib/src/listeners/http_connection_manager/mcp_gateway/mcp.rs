use bytes::Bytes;
use dashmap::DashMap;
use futures::SinkExt;
use http::{HeaderName, Method, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
use orion_http_header::MCP_SESSION_ID;
use parking_lot::Mutex;
use scopeguard::defer;
use serde::Serialize;
use serde_json::{json, Value};
use smol_str::ToSmolStr;
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::Mutex as TokioMutex;
use tracing::debug;
use uuid::Uuid;

use rmcp::{
    model::{
        self, Annotated, CallToolRequestMethod, CallToolResult, ConstString, Implementation, InitializeRequestParams,
        InitializeResult, InitializeResultMethod, InitializedNotificationMethod, JsonRpcResponse,
        ListToolsRequestMethod, PingRequestMethod, ProtocolVersion, RawContent, RawTextContent, ServerCapabilities,
        ServerResult,
    },
    service::RunningService,
    RoleClient,
};

use crate::{
    body::{
        sse_body::{SseBody, SseSender},
        timeout_body::TimeoutBody,
    },
    listeners::{
        http_connection_manager::mcp_gateway::{
            tools::{CallToolError::RbacDenied, ToolsRegistry},
            transport::{
                self, AcceptedMime, RequestExt, SessionId, Transport, MIME_APPLICATION_JSON, MIME_TEXT_EVENT_STREAM,
            },
        },
        http_filters::{FilterDecision, FilterFactory},
        listener::FilterListenerContext,
        metadata::DownstreamMetadata,
    },
    OrionRequestBody, OrionResponseBody, PolyBody,
};

const MCP_MESSAGE_ENDPOINT: &str = "/mcp";
const SSE_MESSAGE_ENDPOINT: &str = "/sse";
const SESSION_IDLE_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(60);

#[derive(Debug)]
pub struct Session {
    pub listener_name: &'static str, // to handle session eviction from listener.sse_map
    pub session_id: SessionId,
    pub transport: Transport,
    pub session_sse_sender: Option<TokioMutex<SseSender>>,
    pub last_activity: Mutex<tokio::time::Instant>,
    pub mcp_upstreams: DashMap<String, RunningService<RoleClient, InitializeRequestParams>, ahash::RandomState>,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            listener_name: "",
            session_id: SessionId::default(),
            transport: Transport::default(),
            session_sse_sender: None,
            last_activity: Mutex::new(tokio::time::Instant::now()),
            mcp_upstreams: DashMap::with_hasher(ahash::RandomState::default()),
        }
    }
}

#[derive(Debug)]
pub struct McpGatewayListenerContext {
    session_map: Arc<DashMap<SessionId, Arc<Session>, ahash::RandomState>>,
    active_async_requests: AtomicUsize,
    cleanup_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl McpGatewayListenerContext {
    fn cleanup(session_map: &DashMap<SessionId, Arc<Session>, ahash::RandomState>) {
        let now = tokio::time::Instant::now();
        session_map.retain(|_, session| {
            let last_activity = session.last_activity.lock();
            let idle = now.duration_since(*last_activity);
            let retain = idle < SESSION_IDLE_TIMEOUT;
            if !retain {
                debug!(target: "mcp_gateway", "Session {} has been idle for {} seconds (dropped)", session.session_id, idle.as_secs());
            }
            retain
        });
    }

    pub fn start_cleanup_task(&self) {
        let mut clean_task = self.cleanup_task.lock();
        if clean_task.is_none() {
            let session_map = Arc::clone(&self.session_map);
            let task = tokio::spawn(async move {
                loop {
                    pingora_timeout::sleep(SESSION_IDLE_TIMEOUT / 2).await;
                    Self::cleanup(&session_map);
                }
            });
            *clean_task = Some(task);
        }
    }
}

impl Default for McpGatewayListenerContext {
    fn default() -> Self {
        Self {
            session_map: Arc::new(DashMap::default()),
            active_async_requests: Default::default(),
            cleanup_task: Mutex::new(None),
        }
    }
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

    pub fn create_session(
        &self,
        listener_name: &'static str,
        sse_sender: Option<SseSender>,
        transport: Transport,
    ) -> Result<Arc<Session>, CreateSessionError> {
        if self.session_map.len() >= Self::MAX_SESSIONS_LIMIT {
            debug!(target: "mcp_gateway", "create_session: session limit reached");
            return Err(CreateSessionError::LimitReached);
        }

        let session_id = SessionId(Uuid::new_v4().to_smolstr());

        let session = Arc::new(Session {
            listener_name,
            session_id: session_id.clone(),
            transport,
            session_sse_sender: sse_sender.map(TokioMutex::new),
            last_activity: Mutex::new(tokio::time::Instant::now()),
            mcp_upstreams: DashMap::with_hasher(ahash::RandomState::default()),
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

pub enum MessageResult {
    Nothing,
    JsonRpcError(model::JsonRpcError),
    JsonRcpResponse(model::JsonRpcResponse<serde_json::Value>),
    UpstreamRequest((http::Request<OrionRequestBody>, bool)),
}

/// McpGateway filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: Arc<McpGatewayInner>,
    session: Option<Arc<Session>>,
    request_id: model::RequestId,
    version: http::Version,
    initialize_request_params: Option<model::InitializeRequestParams>,
    sse_sender: Option<Arc<TokioMutex<SseSender>>>,
}

impl From<McpGatewayConfig> for McpGateway {
    fn from(config: McpGatewayConfig) -> Self {
        Self {
            inner: Arc::new(McpGatewayInner { config: config.clone(), tools: ToolsRegistry::with_tools(config.tools) }),
            session: None,
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
            sse_sender: None,
        }
    }
}

impl FilterFactory for McpGateway {
    fn new_from(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            session: None,
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
            sse_sender: None,
        }
    }
}

impl McpGateway {
    pub async fn apply_request(&mut self, request: &mut http::Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "mcp_gateway", "apply_request: processing request: {:?}", request);

        self.version = request.version();

        let Some(metadata) = request.extensions().get::<DownstreamMetadata>() else {
            debug!(target: "mcp_gateway", "apply_request: failed to retrieve metadata");
            return FilterDecision::internal_server_error("Failed to retrieve metadata", self.version);
        };

        // get global context for this listener
        let ctx = McpGatewayListenerContext::get_filter_context(metadata.listener_name);

        // ensure cleanup task is running
        ctx.start_cleanup_task();

        match (request.method(), request.uri().path()) {
            (&Method::GET, SSE_MESSAGE_ENDPOINT) => {
                self.handle_sse_handshake(&ctx, request, metadata.listener_name).await
            },
            (&Method::GET, MCP_MESSAGE_ENDPOINT) => FilterDecision::method_not_allowed(request.version()),
            // (&Method::OPTIONS, SSE_MESSAGE_ENDPOINT) | (&Method::OPTIONS, MCP_MESSAGE_ENDPOINT) => {
            //     self.handle_cors_options(request).await
            // },
            (&Method::POST, MCP_MESSAGE_ENDPOINT) => {
                self.handle_mcp_post_endpoint(&ctx, request, metadata.listener_name).await
            },
            (&Method::DELETE, MCP_MESSAGE_ENDPOINT) => self.handle_mcp_delete_endpoint(&ctx, request).await,
            _ => {
                debug!(target: "mcp_gateway", "apply_request: no route found");
                FilterDecision::no_route_found(self.version)
            },
        }
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

        let body_string = Self::extract_body_string(&body.to_bytes(), response.status());
        let content = RawContent::Text(RawTextContent { text: body_string, meta: None });

        let tool_result = CallToolResult {
            content: vec![Annotated::new(content, None)],
            is_error: Some(!response.status().is_success()),
            meta: None,
            structured_content: None,
        };

        let server_result = ServerResult::CallToolResult(tool_result);
        debug!(target: "mcp_gateway", "apply_response: {:#?}", server_result);

        let json_rpc_response = self.to_json_rpc_response(server_result);

        match session.transport {
            Transport::Sse => {
                let Some(sender) = session.session_sse_sender.as_ref() else {
                    debug!(target: "mcp_gateway", "apply_response: SSE sender not available for session {}", session.session_id);
                    return FilterDecision::internal_server_error("SSE sender not available", self.version);
                };
                debug!(target: "mcp_gateway", "apply_response: SSE response...");
                let event = transport::sse::Event::Message(&json_rpc_response);
                debug!(target: "mcp_gateway", "apply_response: SENDING SSE EVENT: {:?}", event);
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
                        debug!(target: "mcp_gateway", "apply_response: failed to send message for session {}, error {e}", session.session_id);
                    }
                    sender.close();
                    FilterDecision::Continue
                },
                None => {
                    debug!(target: "mcp_gateway", "apply_response: streamable HTTP (application/json)...");
                    let body = serde_json::to_vec(&json_rpc_response).unwrap_or_default();
                    let headers = self.build_json_headers_with_session();
                    match self.build_mcp_response(StatusCode::OK, Self::build_mcp_body(Some(body.into())), &headers) {
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

    async fn handle_mcp_delete_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<OrionRequestBody>,
    ) -> FilterDecision {
        let Some(session_id) = request.get_mcp_session_id() else {
            debug!(target: "mcp_gateway", "handle_mcp_delete_endpoint: session ID but none found in request");
            return FilterDecision::bad_request(request.version());
        };

        if !ctx.delete_session(&session_id) {
            debug!(target: "mcp_gateway", "handle_mcp_delete_endpoint: StreamableHttp session {} not found in session map", session_id);
            return FilterDecision::not_found(request.version());
        };

        let builder = Response::builder()
            .header(http::header::CONNECTION, "keep-alive")
            .version(self.version)
            .status(StatusCode::ACCEPTED);

        let Ok(response) = builder.body(TimeoutBody::new(None, PolyBody::from(Empty::new()))) else {
            unreachable!("handle_mcp_delete_endpoint: Failed to build response body");
        };

        FilterDecision::DirectResponse(response)
    }

    async fn handle_mcp_post_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> FilterDecision {
        // get transport type for the request
        let Some(transport) = request.get_mcp_transport() else {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: could not get transport type from request");
            return FilterDecision::bad_request(self.version);
        };

        if matches!(transport, Transport::StreamableHttp) {
            let accept = request.get_mcp_accepted_mime();
            if !matches!(accept, Some(AcceptedMime::EventStreamAndJson)) {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: unsupported MIME type for StreamableHttp transport");
                return FilterDecision::bad_request(self.version);
            }
        }

        let req_ext = request.extensions().clone();
        let req_headers = request.headers().clone();

        // collect the body of the request...
        let Ok(body) = request.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to collect request body");
            return FilterDecision::internal_server_error("Failed to collect request body", self.version);
        };

        //
        // get session ID from the request. It may return None, in which case we create a new session id in the "initialize" method
        //
        let mut session = self.get_valid_session(ctx, transport, request);
        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: >>> initial session: {:?} <<<", session);

        //
        // handle the JSON RPC message
        //
        let response = self
            .handle_rpc_json_message(ctx, req_ext, req_headers, transport, body.to_bytes(), listener_name, &mut session)
            .await;

        //
        // save session...
        //
        if let Some(session) = session {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: saving current session {}", session.session_id);
            self.session = Some(session);
        } else {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: *** no session available ***");
        }

        match response {
            MessageResult::JsonRpcError(json_rpc_error) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: handling Error response...");
                match transport {
                    Transport::Sse => {
                        let Some(sender) = self.session.as_deref().and_then(|s| s.session_sse_sender.as_ref()) else {
                            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: SSE sender not available");
                            return FilterDecision::internal_server_error("SSE sender not available", self.version);
                        };
                        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: sending SSE message...");
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
                        let headers = self.build_json_headers_with_session();
                        match self.build_mcp_response(
                            StatusCode::BAD_REQUEST,
                            Self::build_mcp_body(Some(body.into())),
                            &headers,
                        ) {
                            Ok(resp) => return FilterDecision::DirectResponse(resp),
                            Err(e) => return e,
                        }
                    },
                }
            },
            MessageResult::JsonRcpResponse(json_rpc_response) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: handling Response...");
                match transport {
                    Transport::Sse => {
                        let Some(sender) = self.session.as_deref().and_then(|s| s.session_sse_sender.as_ref()) else {
                            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: SSE sender not available");
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
                        let headers = self.build_json_headers_with_session();
                        match self.build_mcp_response(StatusCode::OK, Self::build_mcp_body(Some(body.into())), &headers)
                        {
                            Ok(resp) => return FilterDecision::DirectResponse(resp),
                            Err(e) => return e,
                        }
                    },
                }
            },
            MessageResult::Nothing => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: handling Nothing...");
                let headers = self.build_session_id_headers();
                let Ok(accepted) = self.build_mcp_response(StatusCode::ACCEPTED, Self::build_mcp_body(None), &headers)
                else {
                    return FilterDecision::internal_server_error("Failed to build response", self.version);
                };
                return FilterDecision::DirectResponse(accepted);
            },
            MessageResult::UpstreamRequest((upstream_request, async_call)) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: handling Upstream...");
                match transport {
                    Transport::Sse => {
                        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: legacy SSE...");
                        if !ctx.try_start_async_request() {
                            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: rate limited!");
                            return FilterDecision::rate_limited(request.version());
                        }

                        let Ok(accepted) =
                            self.build_mcp_response(StatusCode::ACCEPTED, Self::build_mcp_body(None), &[])
                        else {
                            return FilterDecision::internal_server_error("Failed to build response", self.version);
                        };
                        return FilterDecision::AsyncRequest(accepted, Some(upstream_request));
                    },
                    Transport::StreamableHttp if async_call => {
                        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: streamable http...");
                        if !ctx.try_start_async_request() {
                            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: rate limited!");
                            return FilterDecision::rate_limited(request.version());
                        }

                        let (body, mut sender) = SseBody::new();

                        // priming event...
                        let event: transport::streamable_http::Event = transport::streamable_http::Event::Priming;
                        if let Err(e) = sender.send(event.to_bytes()).await {
                            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to send priming event: {e}");
                            return FilterDecision::internal_server_error("Failed to send priming event", self.version);
                        };

                        // save the sender for use on response
                        self.sse_sender = Some(Arc::new(TokioMutex::new(sender)));

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

                        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: returning async request...");
                        return FilterDecision::AsyncRequest(okay, Some(upstream_request));
                    },
                    Transport::StreamableHttp => {
                        *request = upstream_request;
                        return FilterDecision::Continue;
                    },
                }
            },
        }
    }

    async fn handle_sse_handshake(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> FilterDecision {
        debug!(target: "mcp_gateway", "handle_sse_handshake: starting SSE handshake...");

        if !matches!(request.get_mcp_transport(), Some(Transport::Sse)) {
            debug!(target: "mcp_gateway", "handle_sse_handshake: client does not accept SSE");
            return FilterDecision::bad_request(request.version());
        }

        // build the SSE response...
        let builder = Response::builder()
            //.header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(http::header::CONTENT_TYPE, MIME_TEXT_EVENT_STREAM)
            .header(http::header::CACHE_CONTROL, "no-cache, no-transform")
            .header(http::header::CONNECTION, "keep-alive")
            .version(self.version)
            .status(StatusCode::OK);

        let (body, mut sender) = SseBody::new();

        // create a new session
        let session = match ctx.create_session(listener_name, Some(sender.clone()), Transport::Sse) {
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

        let body = TimeoutBody::new(None, PolyBody::from(body));

        let Ok(_) = sender.send(event.to_bytes()).await else {
            debug!(target: "mcp_gateway", "handle_sse_handshake: failed to send SSE payload");
            ctx.delete_session(&session.session_id);
            return FilterDecision::internal_server_error("Failed to send SSE payload", request.version());
        };

        let Ok(response) = builder.body(body) else {
            unreachable!("handle_sse_handshake: failed to build body for response");
        };

        self.session = Some(session);
        FilterDecision::DirectResponse(response)
    }

    async fn handle_rpc_json_message(
        &mut self,
        ctx: &McpGatewayListenerContext,
        req_ext: http::Extensions,
        req_headers: http::HeaderMap,
        transport: Transport,
        body: Bytes,
        listener_name: &'static str,
        session: &mut Option<Arc<Session>>,
    ) -> MessageResult {
        debug!(target: "mcp_gateway", "handle_rpc_json_message: transport: {transport}, session: {session:?}, listener: {listener_name}");

        // WORKAROUND: the current rmcp implementation fails to parse
        // {"jsonrpc":"2.0", "method":"notifications/initialized"},
        // which is a valid JSON-RPC notification according to the spec.
        // rmcp incorrectly requires the "params" field, even though it is optional.
        // This workaround injects an empty "params" object to satisfy rmcp.

        let Ok(mut value): Result<Value, _> = serde_json::from_slice(&body) else {
            debug!(target: "mcp_gateway", "handle_rpc_json_message: failed to parse JSON message: {:?}", body);
            return MessageResult::JsonRpcError(
                self.build_rpc_error(model::ErrorData::parse_error("invalid JSON", None)),
            );
        };

        if value.get("id").is_none() && value.get("params").is_none() {
            if let Some(obj) = value.as_object_mut() {
                obj.insert("params".to_string(), serde_json::json!({}));
            }
        }

        let Ok(message): Result<model::JsonRpcMessage, _> = serde_json::from_value(value) else {
            debug!(target: "mcp_gateway", "handle_rpc_json_message: failed to parse JSON message: {:?}", body);
            return MessageResult::JsonRpcError(
                self.build_rpc_error(model::ErrorData::parse_error("invalid JSON", None)),
            );
        };

        match message {
            model::JsonRpcMessage::Request(json_rpc_request) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: json rpc Request on {transport}, session_id: {}...", session.as_ref().map(|s| s.session_id.clone()).unwrap_or_default());
                self.request_id = json_rpc_request.id.clone();
                self.handle_rpc_json_request(
                    ctx,
                    req_ext,
                    req_headers,
                    transport,
                    json_rpc_request,
                    listener_name,
                    session,
                )
                .await
            },
            model::JsonRpcMessage::Response(json_rpc_response) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: Unsupported json rpc Response: {:#?}", json_rpc_response);
                MessageResult::Nothing
            },
            model::JsonRpcMessage::Notification(json_rpc_notification) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: Unsupported json rpc Notification: {:#?}", json_rpc_notification);
                MessageResult::Nothing
            },
            model::JsonRpcMessage::Error(json_rpc_error) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: Unsupported json rpc Error: {:#?}", json_rpc_error);
                MessageResult::Nothing
            },
        }
    }

    async fn handle_rpc_json_request(
        &mut self,
        ctx: &McpGatewayListenerContext,
        req_ext: http::Extensions,
        req_headers: http::HeaderMap,
        transport: Transport,
        rpc: model::JsonRpcRequest,
        listener_name: &'static str,
        session: &mut Option<Arc<Session>>,
    ) -> MessageResult {
        match rpc.request.method.as_str() {
            InitializeResultMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: initialize received (transport {transport})");

                let Ok(init_params): Result<model::InitializeRequestParams, _> =
                    serde_json::from_value(serde_json::Value::Object(rpc.request.params))
                else {
                    debug!(target: "mcp_gateway", "handle_rpc_json_request: invalid params");
                    return MessageResult::JsonRpcError(
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
                    Implementation {
                        name: info.name.to_string(),
                        version: info.version.to_string(),
                        ..Default::default()
                    }
                };

                let result = InitializeResult {
                    protocol_version: ProtocolVersion::default(),
                    instructions: None,
                    capabilities,
                    server_info,
                };

                //
                // with StreamableHttp transport, generate a new unique session ID
                //

                if matches!(transport, Transport::StreamableHttp) {
                    debug!(target: "mcp_gateway", "handle_rpc_json_request: creating new session ========================= ");
                    let Ok(new_session) = ctx.create_session(listener_name, None, Transport::StreamableHttp) else {
                        debug!(target: "mcp_gateway", "handle_rpc_json_request: failed to create new session!");
                        return MessageResult::JsonRpcError(
                            self.build_rpc_error(model::ErrorData::parse_error("Failed to create new session", None)),
                        );
                    };

                    *session = Some(new_session);
                }

                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: serde_json::to_value(result).unwrap_or(serde_json::Value::Null),
                };

                MessageResult::JsonRcpResponse(response)
            },
            InitializedNotificationMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: notification/initialized (transport: {transport})");
                MessageResult::Nothing
            },
            PingRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: ping received");
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: json!({}),
                };

                MessageResult::JsonRcpResponse(response)
            },
            ListToolsRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: tools/list received");
                let tools = self.inner.tools.build_list_tools(req_ext).await;
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: serde_json::to_value(tools).unwrap_or(serde_json::Value::Null),
                };

                MessageResult::JsonRcpResponse(response)
            },
            CallToolRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: tools/call");

                let Some(session) = session.as_ref() else {
                    return MessageResult::JsonRpcError(self.build_rpc_error(model::ErrorData::new(
                        model::ErrorCode::INTERNAL_ERROR,
                        "Session not available",
                        None,
                    )));
                };

                let resp = match self
                    .inner
                    .tools
                    .call(&req_ext, &req_headers, &rpc, &self.inner.config.cluster_header, &session)
                    .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        debug!(target: "mcp_gateway", "handle_rpc_json_request: tools/call failed: {e:#}");
                        let error_data = if matches!(e, RbacDenied(_)) {
                            model::ErrorData::new(
                                model::ErrorCode::INVALID_REQUEST,
                                "Access denied by RBAC policy",
                                None,
                            )
                        } else {
                            model::ErrorData::invalid_params("Invalid params", None)
                        };

                        return MessageResult::JsonRpcError(self.build_rpc_error(error_data));
                    },
                };

                resp
            },
            _ => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: unknown rpc method '{}'", rpc.request.method);
                MessageResult::JsonRpcError(self.build_rpc_error(model::ErrorData::new(
                    model::ErrorCode::METHOD_NOT_FOUND,
                    "Method not found",
                    None,
                )))
            },
        }
    }

    fn get_valid_session(
        &mut self,
        ctx: &McpGatewayListenerContext,
        transport: Transport,
        request: &http::Request<OrionRequestBody>,
    ) -> Option<Arc<Session>> {
        match transport {
            Transport::Sse => {
                let Some(session_id) = request.get_mcp_session_id() else {
                    debug!(target: "mcp_gateway", "get_valid_session: SSE transport requires session ID but none found in request");
                    return None;
                };

                // search for session in map
                let Some(session) = ctx.session_map.get(&session_id) else {
                    debug!(target: "mcp_gateway", "get_valid_session: session {} not found in session map", session_id);
                    return None;
                };

                debug!(target: "mcp_gateway", "get_valid_session: found session {}", session_id);
                Some(session.clone())
            },
            Transport::StreamableHttp => match request.get_mcp_session_id() {
                Some(session_id) => {
                    let Some(session) = ctx.session_map.get(&session_id) else {
                        debug!(target: "mcp_gateway", "get_valid_session: StreamableHttp session {} not found in session map", session_id);
                        return None;
                    };

                    Some(session.clone())
                },
                None => None,
            },
        }
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

    /// Extract body string from response bytes, with fallback messages based on status.
    fn extract_body_string(body: &Bytes, status: StatusCode) -> String {
        std::str::from_utf8(body).ok().filter(|s| !s.is_empty()).map(String::from).unwrap_or_else(|| {
            if status == StatusCode::OK {
                "OK".into()
            } else {
                format!("Upstream Error: {}", status.canonical_reason().unwrap_or("Unknown"))
            }
        })
    }

    /// Build headers with Content-Type and optional session ID for JSON responses.
    fn build_json_headers_with_session(&self) -> Vec<(HeaderName, &str)> {
        let mut headers = vec![(http::header::CONTENT_TYPE, MIME_APPLICATION_JSON)];
        if let Some(session_id) = self.get_current_session_id() {
            headers.push((MCP_SESSION_ID, session_id.as_str()));
        }
        headers
    }

    /// Build headers with only session ID (no Content-Type), for accepted responses.
    fn build_session_id_headers(&self) -> Vec<(HeaderName, &str)> {
        if let Some(session_id) = self.get_current_session_id() {
            vec![(MCP_SESSION_ID, session_id.as_str())]
        } else {
            vec![]
        }
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
        let mut builder =
            Response::builder().header(http::header::CONNECTION, "keep-alive").version(self.version).status(status);

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
