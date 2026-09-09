use atomic_time::AtomicInstant;
use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, DashSet};
use http::{HeaderName, Method, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
use orion_http_header::MCP_SESSION_ID;
use parking_lot::Mutex;
use serde_json::{json, Value};
use smallvec::{smallvec, SmallVec};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc as StdArc,
};
use tracing::{debug, info};
use uuid::Uuid;

use rmcp::{
    model::{
        self, CallToolRequestMethod, ConstString, Implementation, InitializeRequestParams, InitializeResult,
        InitializeResultMethod, InitializedNotificationMethod, ListToolsRequestMethod, NumberOrString,
        PingRequestMethod, ServerCapabilities, ServerNotification,
    },
    service::{ClientInitializeError, RunningService},
    RoleClient, ServiceError,
};

use crate::{
    body::timeout_body::TimeoutBody,
    listeners::{
        http_connection_manager::{
            mcp_gateway::{
                embeddings,
                tools::{CallToolError, ToolBuilderError, ToolsRegistry},
                transport::{self, AcceptedMime, RequestExt, SessionId, MIME_APPLICATION_JSON, MIME_TEXT_EVENT_STREAM},
            },
            RequestCtx,
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
    pub mcp_upstreams: DashMap<String, StdArc<RunningService<RoleClient, InitializeRequestParams>>, ahash::RandomState>,
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
    session_map: StdArc<DashMap<SessionId, StdArc<Session>, ahash::RandomState>>,
    cleanup_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cleanup_task_started: AtomicBool,
}

impl McpGatewayListenerContext {
    fn cleanup(session_map: &DashMap<SessionId, StdArc<Session>, ahash::RandomState>) {
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
                let session_map = StdArc::clone(&self.session_map);
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
            session_map: StdArc::new(DashMap::default()),
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
    const MAX_SESSIONS_LIMIT: usize = 65536;

    pub fn create_session(&self, listener_name: &'static str) -> Result<StdArc<Session>, SessionError> {
        if self.session_map.len() >= Self::MAX_SESSIONS_LIMIT {
            debug!(target: "mcp_gateway", "create_session: session limit reached");
            return Err(SessionError::CreateLimitReached);
        }

        let session_id = SessionId(Uuid::new_v4().to_smolstr());

        let session = StdArc::new(Session {
            listener_name,
            session_id: session_id.clone(),
            last_activity: AtomicInstant::now(),
            mcp_upstreams: DashMap::with_hasher(ahash::RandomState::default()),
            prompt: Mutex::new(None),
            active_tools: DashSet::with_hasher(ahash::RandomState::default()),
        });
        self.session_map.insert(session_id, StdArc::clone(&session));
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
    tools: StdArc<ToolsRegistry>,
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
    JsonRpcResponseNewSession(model::JsonRpcResponse<Value>, StdArc<Session>),
    JsonRpcNotificationResponse(model::JsonRpcNotification<ServerNotification>, model::JsonRpcResponse<Value>),
}

/// `McpGateway` filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: StdArc<McpGatewayInner>,
    current_session: Option<StdArc<Session>>,
    request_id: model::RequestId,
    version: http::Version,
    initialize_request_params: Option<model::InitializeRequestParams>,
}

impl TryFrom<McpGatewayConfig> for McpGateway {
    type Error = ToolBuilderError;

    fn try_from(config: McpGatewayConfig) -> Result<Self, Self::Error> {
        let embeddings_client = match config.semantic_search_tool.as_ref().and_then(|s| s.embeddings.clone()) {
            Some(cfg) => {
                let cfg = cfg.normalized().map_err(ToolBuilderError::InvalidEmbeddingsConfig)?;
                Some(StdArc::new(embeddings::EmbeddingsClient::from_config(cfg)))
            },
            None => None,
        };

        let tools = StdArc::new(ToolsRegistry::with_config(
            config.tools.clone(),
            config.dynamic_mcp_servers.clone(),
            config.semantic_search_tool.clone(),
            embeddings_client,
        )?);

        let tds_registration = if let Some(tds) = &config.tds {
            let runtime_id = crate::runtime_context::get_runtime_id();
            let server_name = config.server_info.name.clone();
            super::uniqueness::claim(runtime_id, server_name.clone()).map_err(ToolBuilderError::DuplicateServerName)?;
            let scope = format_smolstr!("{server_name}/{config_name}", config_name = tds.config_name);
            super::xds_handler::subscribe_for_updates(scope.clone(), &tools);
            Some(TdsRegistration { runtime_id, server_name, scope })
        } else {
            None
        };

        Ok(Self {
            inner: StdArc::new(McpGatewayInner { config, tools, tds_registration }),
            current_session: None,
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
        })
    }
}

impl FilterFactory for McpGateway {
    fn new_from(&self) -> Self {
        Self {
            inner: StdArc::clone(&self.inner),
            current_session: None,
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
        }
    }
}

impl McpGateway {
    pub async fn apply_request(
        &mut self,
        request: &mut http::Request<OrionRequestBody>,
        req_ctx: &RequestCtx,
    ) -> FilterDecision {
        //debug!(target: "mcp_gateway", "apply_request: {:?}", request);

        self.version = request.version();

        let listener_name = req_ctx.conn.listener_name();

        // get global context for this listener
        let ctx = McpGatewayListenerContext::get_filter_context(listener_name);

        // ensure cleanup task is running
        ctx.start_cleanup_task();

        match (request.method(), request.uri().path()) {
            (&Method::GET, MCP_MESSAGE_ENDPOINT) => {
                FilterDecision::method_not_allowed("Method not allowed", request.version())
            },
            // (&Method::OPTIONS, SSE_MESSAGE_ENDPOINT) | (&Method::OPTIONS, MCP_MESSAGE_ENDPOINT) => {
            //     self.handle_cors_options(request).await
            // },
            (&Method::POST, MCP_MESSAGE_ENDPOINT) => {
                self.handle_mcp_post_endpoint(&ctx, request, req_ctx, listener_name).await
            },
            (&Method::DELETE, MCP_MESSAGE_ENDPOINT) => self.handle_mcp_delete_endpoint(&ctx, request),
            _ => {
                debug!(target: "mcp_gateway", "apply_request: no route found");
                FilterDecision::no_route_found("Route not found", self.version)
            },
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn apply_response(&mut self, _response: &mut Response<OrionResponseBody>) -> FilterDecision {
        FilterDecision::Continue
    }

    fn handle_mcp_delete_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<OrionRequestBody>,
    ) -> FilterDecision {
        let Some(session_id) = request.get_mcp_session_id() else {
            debug!(target: "mcp_gateway", "handle_mcp_delete_endpoint: session ID but none found in request");
            return FilterDecision::bad_request("Missing session ID", request.version());
        };

        if !ctx.delete_session(&session_id) {
            debug!(target: "mcp_gateway", "handle_mcp_delete_endpoint: StreamableHttp session {} not found in session map", session_id);
            return FilterDecision::not_found("Session not found", request.version());
        }

        let builder = Response::builder()
            .header(http::header::CONNECTION, "keep-alive")
            .header(MCP_SESSION_ID, session_id.as_str())
            .version(self.version)
            .status(StatusCode::ACCEPTED);

        let Ok(response) = builder.body(TimeoutBody::new(None, PolyBody::from(Empty::new())).into()) else {
            unreachable!("handle_mcp_delete_endpoint: Failed to build response body");
        };

        FilterDecision::DirectResponse(Box::new(response))
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_mcp_post_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<OrionRequestBody>,
        req_ctx: &RequestCtx,
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
            let err =
                self.build_json_rpc_error(model::ErrorData::internal_error("Failed to collect request body", None));
            let body_str = serde_json::to_string(&err).unwrap_or_default();
            return FilterDecision::internal_server_error(&body_str, self.version);
        };

        let body = body.to_bytes();

        let json_rpc_message = match serde_json::from_slice::<model::JsonRpcMessage>(&body) {
            Ok(message) => message,
            Err(err) => {
                info!(target: "mcp_gateway", "handle_rpc_json_message: failed to parse json message: {body:?} ({err:?})");
                let err_data = self.build_json_rpc_error(model::ErrorData::parse_error("Parse error", None));
                let body_str = serde_json::to_string(&err_data).unwrap_or_default();
                return FilterDecision::bad_request(&body_str, self.version);
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

        let session: Option<StdArc<Session>> = match &session_id {
            Some(session_id) => {
                let Ok(session) = Self::get_valid_session(ctx, session_id) else {
                    let err = self.build_json_rpc_error(model::ErrorData::invalid_request("Session not found", None));
                    let body_str = serde_json::to_string(&err).unwrap_or_default();
                    return FilterDecision::not_found(&body_str, self.version);
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
                req_ctx,
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
            self.current_session = Some(StdArc::clone(new_session));
        } else if let Some(session) = session {
            session_id = Some(session.session_id.clone());
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: current session: {session_id:?} -> {:?}", session);
            self.current_session = Some(StdArc::clone(&session));
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
                    let err = self
                        .build_json_rpc_error(model::ErrorData::internal_error("Failed to serialize SSE event", None));
                    let body_str = serde_json::to_string(&err).unwrap_or_default();
                    return FilterDecision::internal_server_error(&body_str, self.version);
                }
                if let Err(e) = resp.write_to(&mut buf) {
                    debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to serialize response SSE event: {e}");
                    let err = self
                        .build_json_rpc_error(model::ErrorData::internal_error("Failed to serialize SSE event", None));
                    let body_str = serde_json::to_string(&err).unwrap_or_default();
                    return FilterDecision::internal_server_error(&body_str, self.version);
                }
                let body = buf.freeze();

                let headers = self.build_http_headers_with_content_type(Some(MIME_TEXT_EVENT_STREAM));
                match self.build_mcp_http_response(StatusCode::OK, Self::build_mcp_response_body(Some(body)), &headers)
                {
                    Ok(resp) => FilterDecision::DirectResponse(Box::new(resp)),
                    Err(e) => e,
                }
            },
            MessageResult::Nothing => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return Nothing response.");
                let headers = self.build_http_headers_with_content_type(None);
                let Ok(accepted) =
                    self.build_mcp_http_response(StatusCode::ACCEPTED, Self::build_mcp_response_body(None), &headers)
                else {
                    let err =
                        self.build_json_rpc_error(model::ErrorData::internal_error("Failed to build response", None));
                    let body_str = serde_json::to_string(&err).unwrap_or_default();
                    return FilterDecision::internal_server_error(&body_str, self.version);
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
        req_ctx: &RequestCtx,
        req_version: http::Version,
        json_rpc_message: model::JsonRpcMessage,
        listener_name: &'static str,
        session: Option<&StdArc<Session>>,
    ) -> Result<MessageResult, FilterDecision> {
        debug!(target: "mcp_gateway", "handle_rpc_json_message: session: {session:?}, listener: {listener_name}");

        match json_rpc_message {
            model::JsonRpcMessage::Request(json_rpc_request) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: json rpc Request, session_id: {}...", session.as_ref().map(|s| s.session_id.clone()).unwrap_or_default());
                self.handle_rpc_json_request(
                    ctx,
                    req_ext,
                    req_headers,
                    req_ctx,
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
        req_ctx: &RequestCtx,
        req_version: http::Version,
        rpc: model::JsonRpcRequest,
        listener_name: &'static str,
        session: Option<&StdArc<Session>>,
    ) -> Result<MessageResult, FilterDecision> {
        if matches!(rpc.request.method.as_str(), InitializeResultMethod::VALUE) {
            debug!(target: "mcp_gateway", "handle_rpc_json_request: 'initialize' method received");
            if session.is_some() {
                info!(target: "mcp_gateway", "handle_rpc_json_request: session already initialized!");
                let err = self.build_json_rpc_error(model::ErrorData::invalid_request(
                    "Initialize already called for this session",
                    None,
                ));
                let body = serde_json::to_string(&err).unwrap_or_default();
                return Err(FilterDecision::bad_request(&body, req_version));
            }

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
                Implementation::new(info.name.to_string(), info.version.to_string())
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
                let tools = match self.inner.tools.build_list_tools(req_ext, req_ctx, session).await {
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
                    .call(req_ext, req_headers, req_ctx, &rpc, &self.inner.config.upstream_limits, session)
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
    ) -> Result<StdArc<Session>, SessionError> {
        let Some(session) = ctx.session_map.get(session_id) else {
            debug!(target: "mcp_gateway", "get_valid_session: valid session {} not found in session map", session_id);
            return Err(SessionError::NotFound);
        };

        session.last_activity.store(std::time::Instant::now(), std::sync::atomic::Ordering::Relaxed);
        let session: StdArc<Session> = StdArc::clone(&session);
        debug!(target: "mcp_gateway", "get_valid_session: {session_id} -> {session:?}");
        Ok(session)
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

        let Ok(resp) = builder.body(body.into()) else {
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
    fn build_json_rpc_error(&self, error: model::ErrorData) -> model::JsonRpcError {
        model::JsonRpcError { jsonrpc: model::JsonRpcVersion2_0, id: Some(self.request_id.clone()), error }
    }

    #[inline]
    pub fn get_current_session_id(&self) -> Option<&SessionId> {
        self.current_session.as_deref().map(|s| &s.session_id)
    }
}
