use atomic_time::AtomicInstant;
use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, DashSet};
use futures::SinkExt;
use http::{HeaderName, Method, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
use orion_http_header::MCP_SESSION_ID;
use parking_lot::Mutex;
use scopeguard::defer;
use serde::Serialize;
use serde_json::{json, Value};
use smallvec::{smallvec, SmallVec};
use smol_str::{SmolStr, ToSmolStr};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, error, info};
use uuid::Uuid;

use rmcp::{
    model::{
        self, Annotated, CallToolRequestMethod, CallToolResult, ConstString, Implementation, InitializeRequestParams,
        InitializeResult, InitializeResultMethod, InitializedNotificationMethod, JsonRpcResponse,
        ListToolsRequestMethod, NumberOrString, PingRequestMethod, RawContent, RawTextContent, ServerCapabilities,
        ServerNotification, ServerResult,
    },
    service::{ClientInitializeError, RunningService},
    RoleClient, ServiceError,
};

use crate::{
    body::{
        sink_body::{SinkBody, SinkSender},
        timeout_body::TimeoutBody,
    },
    extensions_context::MetadataContext,
    listeners::{
        http_connection_manager::mcp_gateway::{
            embeddings,
            tools::{CallToolError, ToolBuilderError, ToolEntry, ToolsRegistry},
            transcoder::{Transcoder, TranscoderType},
            transport::{self, AcceptedMime, RequestExt, SessionId, MIME_APPLICATION_JSON, MIME_TEXT_EVENT_STREAM},
        },
        http_filters::{FilterDecision, FilterFactory},
        listener::FilterListenerContext,
    },
    OrionRequestBody, OrionResponseBody, PolyBody,
};

const MCP_MESSAGE_ENDPOINT: &str = "/mcp";
const SESSION_IDLE_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(300);

#[allow(clippy::struct_field_names)]
pub struct Session {
    pub listener_name: &'static str,
    pub session_id: SessionId,
    pub last_activity: AtomicInstant,
    pub mcp_upstreams: DashMap<String, RunningService<RoleClient, InitializeRequestParams>, ahash::RandomState>,
    pub prompt: Mutex<Option<String>>,
    pub active_tools: DashSet<SmolStr, ahash::RandomState>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("listener_name", &self.listener_name)
            .field("session_id", &self.session_id)
            .field("last_activity", &self.last_activity.load(std::sync::atomic::Ordering::Relaxed))
            .field("mcp_upstreams", &self.mcp_upstreams)
            .field("prompt", &self.prompt)
            .field("active_tools", &self.active_tools)
            .finish()
    }
}

impl Default for Session {
    fn default() -> Self {
        Self {
            listener_name: "",
            session_id: SessionId::default(),
            last_activity: AtomicInstant::now(),
            mcp_upstreams: DashMap::with_hasher(ahash::RandomState::default()),
            prompt: Mutex::new(None),
            active_tools: DashSet::with_hasher(ahash::RandomState::default()),
        }
    }
}

#[derive(Debug)]
pub struct McpGatewayListenerContext {
    session_map: Arc<DashMap<SessionId, Arc<Session>, ahash::RandomState>>,
    active_async_requests: AtomicUsize,
    cleanup_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cleanup_task_started: AtomicBool,
}

impl McpGatewayListenerContext {
    fn cleanup(session_map: &DashMap<SessionId, Arc<Session>, ahash::RandomState>) {
        let now = std::time::Instant::now();
        session_map.retain(|_, session| {
            let last_activity = session.last_activity.load(std::sync::atomic::Ordering::Relaxed);
            let idle = now.saturating_duration_since(last_activity);
            let retain = idle < SESSION_IDLE_TIMEOUT;
            if !retain {
                debug!(target: "mcp_gateway", "Session {} has been idle for {} seconds (dropped)", session.session_id, idle.as_secs());
            }
            retain
        });
    }

    pub fn start_cleanup_task(&self) {
        if !self.cleanup_task_started.load(Ordering::Acquire) {
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
                self.cleanup_task_started.store(true, Ordering::Release);
            }
        }
    }
}

impl Default for McpGatewayListenerContext {
    fn default() -> Self {
        Self {
            session_map: Arc::new(DashMap::default()),
            active_async_requests: AtomicUsize::default(),
            cleanup_task: Mutex::new(None),
            cleanup_task_started: AtomicBool::new(false),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("Create session limit reached")]
    CreateLimitReached,
    #[error("Session not found")]
    NotFound,
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

    pub fn create_session(&self, listener_name: &'static str) -> Result<Arc<Session>, SessionError> {
        if self.session_map.len() >= Self::MAX_SESSIONS_LIMIT {
            debug!(target: "mcp_gateway", "create_session: session limit reached");
            return Err(SessionError::CreateLimitReached);
        }

        let session_id = SessionId(Uuid::new_v4().to_smolstr());

        let session = Arc::new(Session {
            listener_name,
            session_id: session_id.clone(),
            last_activity: AtomicInstant::now(),
            mcp_upstreams: DashMap::with_hasher(ahash::RandomState::default()),
            prompt: Mutex::new(None),
            active_tools: DashSet::with_hasher(ahash::RandomState::default()),
        });
        self.session_map.insert(session_id, Arc::clone(&session));
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
    tools: Arc<ToolsRegistry>,
    tds_registration: Option<TdsRegistration>,
}

// Drop-tied so listener removal/replacement releases the uniqueness claim and
// unsubscribes from the xDS handler, avoiding zombie registrations.
#[derive(Debug)]
struct TdsRegistration {
    runtime_id: usize,
    server_name: SmolStr,
    scope: SmolStr,
}

impl Drop for McpGatewayInner {
    fn drop(&mut self) {
        if let Some(reg) = self.tds_registration.take() {
            super::xds_handler::unsubscribe_from_updates(&reg.scope, &self.tools);
            super::uniqueness::release(reg.runtime_id, &reg.server_name);
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub enum MessageResult {
    Nothing,
    JsonRpcError(model::JsonRpcError),
    JsonRpcResponse(model::JsonRpcResponse<Value>),
    JsonRpcResponseNewSession(model::JsonRpcResponse<Value>, Arc<Session>),
    JsonRpcNotificationResponse(model::JsonRpcNotification<ServerNotification>, model::JsonRpcResponse<Value>),
    UpstreamRequest((http::Request<OrionRequestBody>, bool, Arc<ToolEntry>)),
}

/// `McpGateway` filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: Arc<McpGatewayInner>,
    current_session: Option<Arc<Session>>,
    request_id: model::RequestId,
    version: http::Version,
    initialize_request_params: Option<model::InitializeRequestParams>,
    streamable_async_sender: Option<Arc<TokioMutex<SinkSender>>>,
    current_tool: Option<Arc<ToolEntry>>,
}

impl TryFrom<McpGatewayConfig> for McpGateway {
    type Error = ToolBuilderError;

    fn try_from(config: McpGatewayConfig) -> Result<Self, Self::Error> {
        let embeddings_client = match config.semantic_search_tool.as_ref().and_then(|s| s.embeddings.clone()) {
            Some(cfg) => {
                let cfg = cfg.normalized().map_err(ToolBuilderError::InvalidEmbeddingsConfig)?;
                Some(Arc::new(embeddings::EmbeddingsClient::from_config(cfg)))
            },
            None => None,
        };

        let tools = Arc::new(ToolsRegistry::with_config(
            config.tools.clone(),
            config.dynamic_mcp_servers.clone(),
            config.semantic_search_tool.clone(),
            embeddings_client,
        )?);

        let tds_registration = if let Some(tds) = &config.tds {
            let runtime_id = crate::runtime_context::get_runtime_id();
            let server_name: SmolStr = config.server_info.name.as_str().into();
            super::uniqueness::claim(runtime_id, server_name.clone()).map_err(ToolBuilderError::DuplicateServerName)?;
            let scope: SmolStr = format!("{server_name}/{config_name}", config_name = tds.config_name).into();
            super::xds_handler::subscribe_for_updates(scope.clone(), &tools);
            Some(TdsRegistration { runtime_id, server_name, scope })
        } else {
            None
        };

        Ok(Self {
            inner: Arc::new(McpGatewayInner { config, tools, tds_registration }),
            current_session: None,
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
            streamable_async_sender: None,
            current_tool: None,
        })
    }
}

impl FilterFactory for McpGateway {
    fn new_from(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            current_session: None,
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
            streamable_async_sender: None,
            current_tool: None,
        }
    }
}

impl McpGateway {
    pub async fn apply_request(&mut self, request: &mut http::Request<OrionRequestBody>) -> FilterDecision {
        //debug!(target: "mcp_gateway", "apply_request: {:?}", request);

        self.version = request.version();

        let Some(metadata) = request.extensions().get::<MetadataContext>() else {
            debug!(target: "mcp_gateway", "apply_request: failed to retrieve metadata");
            return FilterDecision::internal_server_error("Failed to retrieve metadata", self.version);
        };

        // get global context for this listener
        let ctx = McpGatewayListenerContext::get_filter_context(metadata.downstream.listener_name);

        // ensure cleanup task is running
        ctx.start_cleanup_task();

        match (request.method(), request.uri().path()) {
            (&Method::GET, MCP_MESSAGE_ENDPOINT) => FilterDecision::method_not_allowed(request.version()),
            // (&Method::OPTIONS, SSE_MESSAGE_ENDPOINT) | (&Method::OPTIONS, MCP_MESSAGE_ENDPOINT) => {
            //     self.handle_cors_options(request).await
            // },
            (&Method::POST, MCP_MESSAGE_ENDPOINT) => {
                self.handle_mcp_post_endpoint(&ctx, request, metadata.downstream.listener_name).await
            },
            (&Method::DELETE, MCP_MESSAGE_ENDPOINT) => self.handle_mcp_delete_endpoint(&ctx, request),
            _ => {
                debug!(target: "mcp_gateway", "apply_request: no route found");
                FilterDecision::no_route_found(self.version)
            },
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn apply_response(&mut self, response: &mut Response<OrionResponseBody>) -> FilterDecision {
        // debug!(target: "mcp_gateway", "apply_response: {response:?}");

        // get global context for this listener
        let Some(session) = self.current_session.as_deref() else {
            debug!(target: "mcp_gateway", "apply_response: no session found!");
            return FilterDecision::Continue;
        };

        let ctx = McpGatewayListenerContext::get_filter_context(session.listener_name);
        let is_async = self.streamable_async_sender.as_ref().is_some();

        defer! {
            if is_async {
                ctx.end_async_request();
            }
        };

        // collect the body...
        let Ok(body) = response.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_response: failed to collect response body");
            let error =
                self.build_json_rpc_error(model::ErrorData::internal_error("failed to collect response body", None));
            let event = transport::streamable_http::Event::Message(&error);
            if let Some(sender) = &mut self.streamable_async_sender {
                let mut sender_guard = sender.lock().await;
                let sender = &mut *sender_guard;
                let mut buf = BytesMut::with_capacity(1024);
                if let Err(e) = event.write_to(&mut buf) {
                    debug!(target: "mcp_gateway", "apply_response: failed to serialize SSE event: {e}");
                    return FilterDecision::internal_server_error("Failed to serialize SSE event", self.version);
                }
                if let Err(e) = Self::send_sse_message(sender, buf.freeze(), self.version).await {
                    return e;
                }
                sender.close();
            }

            return FilterDecision::Continue;
        };

        let body_bytes = body.to_bytes();
        let upstream_status = response.status();
        let body_string = McpGateway::extract_body_string(&body_bytes, upstream_status);

        let structured_content = if let Some(tool) = self.current_tool.take().as_deref() {
            let value = match &tool.transcoder {
                TranscoderType::Rest(rest_transcoder) => {
                    // For REST transcoder, decode upstream message only if
                    // we have to validate it against an output_schema
                    if tool.output_schema_validator.is_some() {
                        match rest_transcoder.decode(body_bytes, upstream_status) {
                            Ok(value) => Some(value),
                            Err(e) => {
                                debug!(target: "mcp_gateway", "apply_response: transcoder decode error: {e}");
                                Some(json!({
                                    "error": format!("Failed to decode upstream response: {e}"),
                                        "status": upstream_status.as_u16()
                                }))
                            },
                        }
                    } else {
                        None
                    }
                },
                TranscoderType::FunctionGraph(_) => unimplemented!(),
                TranscoderType::NoTranscoder => None,
            };
            value.map(|value| match tool.validate_against_output_schema(&value) {
                Ok(()) => value,
                Err(e) => {
                    debug!(target: "mcp_gateway", "apply_response: output schema validation error: {e}");
                    json!({
                        "error": format!("Failed to validate upstream response against output schema: {e}"),
                            "status": upstream_status.as_u16()
                    })
                },
            })
        } else {
            error!(target: "mcp_gateway", "apply_response: no current tool recorded to process upstream response");
            None
        };

        // Build the CallToolResult with the raw response as content. If we are
        // here the response has been validated against the output schema.
        // The result_value will be injected as structured_content.

        let tool_result = if let Some(value) = structured_content {
            if upstream_status.is_success() {
                CallToolResult::structured(value)
            } else {
                CallToolResult::structured_error(value)
            }
        } else {
            let content = RawContent::Text(RawTextContent { text: body_string, meta: None });
            let content = vec![Annotated::new(content, None)];
            if upstream_status.is_success() {
                CallToolResult::success(content)
            } else {
                CallToolResult::error(content)
            }
        };

        let server_result = ServerResult::CallToolResult(tool_result);
        debug!(target: "mcp_gateway", "apply_response: {:?}", server_result);

        let json_rpc_response = self.to_json_rpc_response(server_result);

        if let Some(sender) = self.streamable_async_sender.as_mut() {
            debug!(target: "mcp_gateway", "apply_response: streamable HTTP (async)...");
            let mut sender_guard = sender.lock().await;
            let sender = &mut *sender_guard;
            let event = transport::streamable_http::Event::Message(&json_rpc_response);
            let mut buf = BytesMut::with_capacity(1024);
            if let Err(e) = event.write_to(&mut buf) {
                debug!(target: "mcp_gateway", "apply_response: failed to serialize SSE event: {e}");
                return FilterDecision::internal_server_error("Failed to serialize SSE event", self.version);
            }
            if let Err(e) = sender.send(buf.freeze()).await {
                debug!(target: "mcp_gateway", "apply_response: failed to send message for session {}, error {e}", session.session_id);
            }
            sender.close();
        } else {
            debug!(target: "mcp_gateway", "apply_response: streamable HTTP (sync)...");
            let body = serde_json::to_vec(&json_rpc_response).unwrap_or_default();
            let headers = self.build_http_headers_with_content_type(Some(MIME_APPLICATION_JSON));
            if let Ok(resp) =
                self.build_mcp_http_response(StatusCode::OK, Self::build_mcp_response_body(Some(body.into())), &headers)
            {
                *response = resp;
            } else {
                debug!(target: "mcp_gateway", "apply_response: Failed to build MCP response");
            }
        }
        FilterDecision::Continue
    }

    fn handle_mcp_delete_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<OrionRequestBody>,
    ) -> FilterDecision {
        let Some(session_id) = request.get_mcp_session_id() else {
            debug!(target: "mcp_gateway", "handle_mcp_delete_endpoint: session ID but none found in request");
            return FilterDecision::bad_request("", request.version());
        };

        if !ctx.delete_session(&session_id) {
            debug!(target: "mcp_gateway", "handle_mcp_delete_endpoint: StreamableHttp session {} not found in session map", session_id);
            return FilterDecision::not_found(request.version());
        }

        let builder = Response::builder()
            .header(http::header::CONNECTION, "keep-alive")
            .version(self.version)
            .status(StatusCode::ACCEPTED);

        let Ok(response) = builder.body(TimeoutBody::new(None, PolyBody::from(Empty::new()))) else {
            unreachable!("handle_mcp_delete_endpoint: Failed to build response body");
        };

        FilterDecision::DirectResponse(Box::new(response))
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_mcp_post_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> FilterDecision {
        let accept = request.get_mcp_accepted_mime();
        if !matches!(accept, Some(AcceptedMime::EventStreamAndJson)) {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: unsupported MIME type");
            let err = self.build_json_rpc_error(model::ErrorData::invalid_request("Unsupported MIME type", None));
            let body = serde_json::to_string(&err).unwrap_or_default();
            return FilterDecision::bad_request(&body, self.version);
        }

        // collect the body of the request...
        //

        let Ok(body) = request.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to collect request body");
            return FilterDecision::internal_server_error("Failed to collect request body", self.version);
        };

        let body = body.to_bytes();

        let json_rpc_message = match serde_json::from_slice::<model::JsonRpcMessage>(&body) {
            Ok(message) => message,
            Err(err) => {
                info!(target: "mcp_gateway", "handle_rpc_json_message: failed to parse json message: {body:?} ({err:?})");

                let headers = self.build_http_headers_with_content_type(None);
                match self.build_mcp_http_response(
                    StatusCode::BAD_REQUEST,
                    Self::build_mcp_response_body(None),
                    &headers,
                ) {
                    Ok(http_response) => return FilterDecision::DirectResponse(Box::new(http_response)),
                    Err(e) => return e,
                };
            },
        };

        self.request_id = match &json_rpc_message {
            model::JsonRpcMessage::Request(json_rpc_request) => json_rpc_request.id.clone(),
            model::JsonRpcMessage::Response(json_rpc_response) => json_rpc_response.id.clone(),
            model::JsonRpcMessage::Error(json_rcp_error) => {
                json_rcp_error.id.clone().unwrap_or(NumberOrString::Number(0))
            },
            model::JsonRpcMessage::Notification(_) => NumberOrString::Number(0),
        };

        // get session ID from the request. It may return None, in which case we create a new session id in the "initialize" method
        //
        let mut session_id = request.get_mcp_session_id();

        let session: Option<Arc<Session>> = match &session_id {
            Some(session_id) => {
                let Ok(session) = Self::get_valid_session(ctx, session_id) else {
                    return FilterDecision::not_found(self.version);
                };

                Some(session)
            },
            None => None,
        };

        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: previous session: {session_id:?} -> {session:?}");

        // handle the JSON RPC message
        //

        let msg_result = match self
            .handle_rpc_json_message(
                ctx,
                request.extensions(),
                request.headers(),
                request.version(),
                json_rpc_message,
                listener_name,
                session.as_ref(),
            )
            .await
        {
            Ok(msg_result) => msg_result,
            Err(decision) => {
                return decision;
            },
        };

        // annotate the current_session...
        //

        if let MessageResult::JsonRpcResponseNewSession(_, new_session) = &msg_result {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: new session created");
            session_id = Some(new_session.session_id.clone());
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: current session: {session_id:?} -> {:?}", new_session);
            self.current_session = Some(Arc::clone(&new_session));
        } else if let Some(session) = session {
            session_id = Some(session.session_id.clone());
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: current session: {session_id:?} -> {:?}", session);
            self.current_session = Some(Arc::clone(&session));
        } else {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: current session: None (internal bug)!");
        }

        match msg_result {
            MessageResult::JsonRpcError(json_rpc_error) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return ({json_rpc_error:?})");
                let body = serde_json::to_string(&json_rpc_error).unwrap_or_default();
                let headers = self.build_http_headers_with_content_type(Some(MIME_APPLICATION_JSON));
                match self.build_mcp_http_response(
                    StatusCode::OK,
                    Self::build_mcp_response_body(Some(body.into())),
                    &headers,
                ) {
                    Ok(resp) => FilterDecision::DirectResponse(Box::new(resp)),
                    Err(e) => e,
                }
            },
            MessageResult::JsonRpcResponse(json_rpc_response)
            | MessageResult::JsonRpcResponseNewSession(json_rpc_response, _) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return ({json_rpc_response:?})");
                let body = serde_json::to_vec(&json_rpc_response).unwrap_or_default();
                let headers = self.build_http_headers_with_content_type(Some(MIME_APPLICATION_JSON));
                match self.build_mcp_http_response(
                    StatusCode::OK,
                    Self::build_mcp_response_body(Some(body.into())),
                    &headers,
                ) {
                    Ok(resp) => FilterDecision::DirectResponse(Box::new(resp)),
                    Err(e) => e,
                }
            },
            MessageResult::JsonRpcNotificationResponse(json_rpc_notif, json_rpc_response) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return ({json_rpc_notif:?} + {json_rpc_response:?})");
                let notif = transport::streamable_http::Event::Message(&json_rpc_notif);
                let resp = transport::streamable_http::Event::Message(&json_rpc_response);
                let mut buf = BytesMut::with_capacity(1024);

                if let Err(e) = notif.write_to(&mut buf) {
                    debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to serialize notification SSE event: {e}");
                    return FilterDecision::internal_server_error("Failed to serialize SSE event", self.version);
                }
                if let Err(e) = resp.write_to(&mut buf) {
                    debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to serialize response SSE event: {e}");
                    return FilterDecision::internal_server_error("Failed to serialize SSE event", self.version);
                }
                let body = buf.freeze();

                let headers = self.build_http_headers_with_content_type(Some(MIME_TEXT_EVENT_STREAM));
                match self.build_mcp_http_response(StatusCode::OK, Self::build_mcp_response_body(Some(body)), &headers)
                {
                    Ok(resp) => FilterDecision::DirectResponse(Box::new(resp)),
                    Err(e) => e,
                }
            },
            MessageResult::UpstreamRequest((upstream_request, async_call, _)) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: send upstream ({upstream_request:?}");
                if async_call {
                    debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: streamable http...");
                    if !ctx.try_start_async_request() {
                        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: rate limited!");
                        return FilterDecision::rate_limited(None, request.version());
                    }

                    let (body, mut sender) = SinkBody::new();

                    // priming event...
                    let event: transport::streamable_http::Event = transport::streamable_http::Event::Priming;
                    let mut buf = BytesMut::with_capacity(1024);
                    if let Err(e) = event.write_to(&mut buf) {
                        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to serialize priming event: {e}");
                        return FilterDecision::internal_server_error(
                            "Failed to serialize priming event",
                            self.version,
                        );
                    }
                    if let Err(e) = sender.send(buf.freeze()).await {
                        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to send priming event: {e}");
                        return FilterDecision::internal_server_error("Failed to send priming event", self.version);
                    }

                    // save the sender for use on response
                    self.streamable_async_sender = Some(Arc::new(TokioMutex::new(sender)));

                    let body = TimeoutBody::new(None, PolyBody::from(body));
                    let Ok(mut okay) = self.build_mcp_http_response(
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
                    FilterDecision::AsyncRequest(Box::new(okay), Some(Box::new(upstream_request)))
                } else {
                    *request = upstream_request;
                    FilterDecision::Continue
                }
            },
            MessageResult::Nothing => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return Nothing response.");
                let headers = self.build_http_headers_with_content_type(None);
                let Ok(accepted) =
                    self.build_mcp_http_response(StatusCode::ACCEPTED, Self::build_mcp_response_body(None), &headers)
                else {
                    return FilterDecision::internal_server_error("Failed to build response", self.version);
                };
                FilterDecision::DirectResponse(Box::new(accepted))
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_rpc_json_message(
        &mut self,
        ctx: &McpGatewayListenerContext,
        req_ext: &http::Extensions,
        req_headers: &http::HeaderMap,
        req_version: http::Version,
        json_rpc_message: model::JsonRpcMessage,
        listener_name: &'static str,
        session: Option<&Arc<Session>>,
    ) -> Result<MessageResult, FilterDecision> {
        debug!(target: "mcp_gateway", "handle_rpc_json_message: session: {session:?}, listener: {listener_name}");

        match json_rpc_message {
            model::JsonRpcMessage::Request(json_rpc_request) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: json rpc Request, session_id: {}...", session.as_ref().map(|s| s.session_id.clone()).unwrap_or_default());
                self.handle_rpc_json_request(
                    ctx,
                    req_ext,
                    req_headers,
                    req_version,
                    json_rpc_request,
                    listener_name,
                    session,
                )
                .await
            },
            model::JsonRpcMessage::Notification(json_rpc_notification) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: rpc Notification: {:#?}", json_rpc_notification);
                if session.is_none() {
                    let err = self.build_json_rpc_error(model::ErrorData::invalid_request(
                        "Missing or invalid mcp-session-id",
                        None,
                    ));
                    let body = serde_json::to_string(&err).unwrap_or_default();
                    return Err(FilterDecision::bad_request(&body, req_version));
                }
                Ok(MessageResult::Nothing)
            },
            model::JsonRpcMessage::Response(json_rpc_response) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: rpc Response: {:#?}", json_rpc_response);
                if session.is_none() {
                    let err = self.build_json_rpc_error(model::ErrorData::invalid_request(
                        "Missing or invalid mcp-session-id",
                        None,
                    ));
                    let body = serde_json::to_string(&err).unwrap_or_default();
                    return Err(FilterDecision::bad_request(&body, req_version));
                }
                Ok(MessageResult::Nothing)
            },
            model::JsonRpcMessage::Error(json_rpc_error) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: rpc Error: {:#?}", json_rpc_error);
                if session.is_none() {
                    let err = self.build_json_rpc_error(model::ErrorData::invalid_request(
                        "Missing or invalid mcp-session-id",
                        None,
                    ));
                    let body = serde_json::to_string(&err).unwrap_or_default();
                    return Err(FilterDecision::bad_request(&body, req_version));
                }
                Ok(MessageResult::Nothing)
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_lines)]
    async fn handle_rpc_json_request(
        &mut self,
        ctx: &McpGatewayListenerContext,
        req_ext: &http::Extensions,
        req_headers: &http::HeaderMap,
        req_version: http::Version,
        rpc: model::JsonRpcRequest,
        listener_name: &'static str,
        session: Option<&Arc<Session>>,
    ) -> Result<MessageResult, FilterDecision> {
        if matches!(rpc.request.method.as_str(), InitializeResultMethod::VALUE) {
            debug!(target: "mcp_gateway", "handle_rpc_json_request: 'initialize' method received");
            if session.is_some() {
                info!(target: "mcp_gateway", "handle_rpc_json_request: session already initialized!");
                return Ok(MessageResult::JsonRpcError(self.build_json_rpc_error(model::ErrorData::invalid_request(
                    "Initialize already called for this session",
                    None,
                ))));
            };

            let Ok(init_params): Result<model::InitializeRequestParams, _> =
                serde_json::from_value(serde_json::Value::Object(rpc.request.params))
            else {
                info!(target: "mcp_gateway", "handle_rpc_json_request: invalid params!");
                return Ok(MessageResult::JsonRpcError(
                    self.build_json_rpc_error(model::ErrorData::invalid_params("invalid params", None)),
                ));
            };

            self.initialize_request_params = Some(init_params);

            let capabilities = ServerCapabilities::builder().enable_tools().enable_tool_list_changed().build();

            let server_info = {
                let info = &self.inner.config.server_info;
                Implementation::new(info.name.clone(), info.version.clone())
            };

            let result = InitializeResult::new(capabilities).with_server_info(server_info);

            let Ok(new_session) = ctx.create_session(listener_name) else {
                info!(target: "mcp_gateway", "handle_rpc_json_request: failed to create new session!");
                return Ok(MessageResult::JsonRpcError(
                    self.build_json_rpc_error(model::ErrorData::parse_error("Failed to create new session", None)),
                ));
            };

            debug!(target: "mcp_gateway", "handle_rpc_json_request: new session: {new_session:?}");

            let response = model::JsonRpcResponse {
                jsonrpc: model::JsonRpcVersion2_0,
                id: self.request_id.clone(),
                result: serde_json::to_value(result).unwrap_or(serde_json::Value::Null),
            };

            return Ok(MessageResult::JsonRpcResponseNewSession(response, new_session));
        }

        // check integrety session...

        let Some(session) = session else {
            let err =
                self.build_json_rpc_error(model::ErrorData::invalid_request("Missing or invalid mcp-session-id", None));
            let body = serde_json::to_string(&err).unwrap_or_default();
            return Err(FilterDecision::bad_request(&body, req_version));
        };

        match rpc.request.method.as_str() {
            InitializedNotificationMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: 'notification/initialized'");
                Ok(MessageResult::Nothing)
            },
            PingRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: 'ping' received");
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: json!({}),
                };

                Ok(MessageResult::JsonRpcResponse(response))
            },
            ListToolsRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: 'tools/list'");
                let tools = match self.inner.tools.build_list_tools(req_ext, &session).await {
                    Ok(tools) => tools,
                    Err(err) => {
                        debug!(target: "mcp_gateway", "handle_rpc_json_request: 'tools/list' failed: {err:#}");
                        return Ok(MessageResult::JsonRpcError(
                            self.build_json_rpc_error(model::ErrorData::internal_error(err.to_string(), None)),
                        ));
                    },
                };
                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: serde_json::to_value(tools).unwrap_or(serde_json::Value::Null),
                };

                Ok(MessageResult::JsonRpcResponse(response))
            },
            CallToolRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: 'tools/call'");

                let msg_result = match self
                    .inner
                    .tools
                    .call(req_ext, req_headers, &rpc, self.inner.config.cluster_header.as_ref(), &session)
                    .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        debug!(target: "mcp_gateway", "handle_rpc_json_request: 'tools/call' failed: {err:#}");
                        let error_data = match err {
                            CallToolError::NameNotString => model::ErrorData::invalid_params(
                                "'name' parameter is missing or not a valid string",
                                None,
                            ),
                            CallToolError::ToolNotFound(ref name) => {
                                model::ErrorData::resource_not_found(format!("Tool '{name}' not found"), None)
                            },
                            CallToolError::RbacDenied(ref tool) => model::ErrorData::invalid_request(
                                format!("Access denied by RBAC policy for tool '{tool}'"),
                                None,
                            ),
                            CallToolError::FunctionGraphNotImplemented => model::ErrorData::internal_error(
                                "FunctionGraph transcoding is not yet implemented",
                                None,
                            ),
                            CallToolError::InvalidHeaderValue(ref e) => {
                                model::ErrorData::internal_error(format!("Invalid header value: {e}"), None)
                            },
                            CallToolError::TranscoderError { ref tool, ref reason } => {
                                model::ErrorData::internal_error(
                                    format!("Transcoder error for tool '{tool}': {reason}"),
                                    None,
                                )
                            },
                            CallToolError::ClientInitializeError(ref init_err) => match init_err {
                                ClientInitializeError::JsonRpcError(error_data) => error_data.clone(),
                                ClientInitializeError::ConnectionClosed(ref msg) => {
                                    model::ErrorData::internal_error(format!("Upstream connection closed: {msg}"), None)
                                },
                                ClientInitializeError::TransportError { ref error, ref context } => {
                                    model::ErrorData::internal_error(
                                        format!("Upstream transport error ({context}): {error}"),
                                        None,
                                    )
                                },
                                ClientInitializeError::Cancelled => model::ErrorData::internal_error(
                                    "Upstream client initialization was cancelled",
                                    None,
                                ),
                                _ => model::ErrorData::internal_error(
                                    format!("Upstream client initialization error: {init_err}"),
                                    None,
                                ),
                            },
                            CallToolError::SerdeError(ref e) => {
                                model::ErrorData::parse_error(format!("Serialization error: {e}"), None)
                            },
                            CallToolError::ServiceError(ref svc_err) => match svc_err {
                                ServiceError::McpError(error_data) => error_data.clone(),
                                ServiceError::TransportSend(ref e) => model::ErrorData::internal_error(
                                    format!("Upstream transport send error: {e}"),
                                    None,
                                ),
                                ServiceError::TransportClosed => {
                                    model::ErrorData::internal_error("Upstream transport closed", None)
                                },
                                ServiceError::UnexpectedResponse => {
                                    model::ErrorData::internal_error("Unexpected response from upstream", None)
                                },
                                ServiceError::Cancelled { ref reason } => model::ErrorData::internal_error(
                                    format!("Upstream request cancelled: {}", reason.as_deref().unwrap_or("<unknown>")),
                                    None,
                                ),
                                ServiceError::Timeout { timeout } => model::ErrorData::internal_error(
                                    format!("Upstream request timed out after {timeout:?}"),
                                    None,
                                ),
                                _ => {
                                    model::ErrorData::internal_error(format!("Upstream service error: {svc_err}"), None)
                                },
                            },
                            CallToolError::ValidationError(e) => {
                                model::ErrorData::internal_error(format!("Json schema validation error: {e}"), None)
                            },
                        };

                        return Ok(MessageResult::JsonRpcError(self.build_json_rpc_error(error_data)));
                    },
                };

                if let MessageResult::UpstreamRequest((_, _, tool)) = &msg_result {
                    self.current_tool = Some(Arc::clone(tool));
                }

                Ok(msg_result)
            },
            _ => {
                info!(target: "mcp_gateway", "handle_rpc_json_request: '{}' method unsupported.", rpc.request.method);
                Ok(MessageResult::JsonRpcError(self.build_json_rpc_error(model::ErrorData::new(
                    model::ErrorCode::METHOD_NOT_FOUND,
                    "Method not found",
                    None,
                ))))
            },
        }
    }

    fn get_valid_session(
        ctx: &McpGatewayListenerContext,
        session_id: &SessionId,
    ) -> Result<Arc<Session>, SessionError> {
        let Some(session) = ctx.session_map.get(session_id) else {
            debug!(target: "mcp_gateway", "get_valid_session: valid session {} not found in session map", session_id);
            return Err(SessionError::NotFound);
        };

        session.last_activity.store(std::time::Instant::now(), std::sync::atomic::Ordering::Relaxed);
        let session: Arc<Session> = Arc::clone(&session);
        debug!(target: "mcp_gateway", "get_valid_session: {session_id} -> {session:?}");
        Ok(session)
    }

    async fn send_sse_message(
        sender: &mut SinkSender,
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
        Some(String::from_utf8_lossy(body)).filter(|s| !s.is_empty()).map(String::from).unwrap_or_else(|| {
            if status == StatusCode::OK {
                "OK".into()
            } else {
                format!("Upstream Error: {}", status.canonical_reason().unwrap_or("Unknown"))
            }
        })
    }

    /// Build headers with optional Content-Type and optional session ID for JSON responses.
    fn build_http_headers_with_content_type(
        &self,
        content_type: Option<&'static str>,
    ) -> SmallVec<[(HeaderName, &str); 2]> {
        let mut headers = smallvec![];
        if let Some(session_id) = self.get_current_session_id() {
            headers.push((MCP_SESSION_ID, session_id.as_str()));
        }
        if let Some(content_type) = content_type {
            headers.push((http::header::CONTENT_TYPE, content_type));
        }
        headers
    }

    #[inline]
    fn build_mcp_response_body(body: Option<Bytes>) -> TimeoutBody<PolyBody> {
        match body {
            Some(b) => TimeoutBody::new(None, PolyBody::from(Full::from(b))),
            None => TimeoutBody::new(None, PolyBody::from(Empty::new())),
        }
    }

    #[allow(clippy::result_large_err)]
    fn build_mcp_http_response(
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
                self.current_session.as_deref().map(|s| s.session_id.clone()).unwrap_or_default());
            return Err(FilterDecision::internal_server_error(
                format!(
                    "Failed to build accepted response for session {}",
                    self.current_session.as_deref().map(|s| s.session_id.clone()).unwrap_or_default()
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
    fn build_json_rpc_error(&self, error: model::ErrorData) -> model::JsonRpcError {
        model::JsonRpcError { jsonrpc: model::JsonRpcVersion2_0, id: Some(self.request_id.clone()), error }
    }

    #[inline]
    pub fn get_current_session_id(&self) -> Option<&SessionId> {
        self.current_session.as_deref().map(|s| &s.session_id)
    }
}
