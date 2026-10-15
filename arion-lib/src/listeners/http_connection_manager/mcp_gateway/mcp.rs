// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
use arion_http_header::MCP_SESSION_ID;
use arion_interner::InternedStr;
use atomic_time::AtomicInstant;
use bytes::{Bytes, BytesMut};
use http::{HeaderValue, Method, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use papaya::{HashMap as PapayaMap, HashSet as PapayaSet};
use parking_lot::Mutex;
use smol_str::{SmolStr, ToSmolStr, format_smolstr};
use std::sync::{
    Arc as StdArc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{debug, info};
use uuid::Uuid;

use rmcp::{
    RoleClient, ServiceError,
    model::{
        self, CallToolRequestMethod, ConstString, Implementation, InitializeRequestParams, InitializeResult,
        InitializeResultMethod, InitializedNotificationMethod, ListToolsRequestMethod, NumberOrString,
        PingRequestMethod, ServerCapabilities, ServerNotification,
    },
    service::{ClientInitializeError, RunningService},
};

use crate::{
    ArionRequestBody, ArionResponseBody, PolyBody,
    body::timeout_body::TimeoutBody,
    listeners::{
        http_connection_manager::{
            RequestCtx,
            mcp_gateway::{
                embeddings,
                tools::{CallToolError, ToolBuilderError, ToolsRegistry},
                transport::{self, AcceptedMime, MIME_APPLICATION_JSON, MIME_TEXT_EVENT_STREAM, RequestExt, SessionId},
                upstream,
            },
        },
        http_filters::{FilterDecision, FilterFactory},
        listener::FilterListenerContext,
    },
};

const MCP_MESSAGE_ENDPOINT: &str = "/mcp";
const SESSION_IDLE_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(300);
const CONNECTION_KEEP_ALIVE: HeaderValue = HeaderValue::from_static("keep-alive");
const CONTENT_TYPE_JSON: HeaderValue = HeaderValue::from_static(MIME_APPLICATION_JSON);
const CONTENT_TYPE_EVENT_STREAM: HeaderValue = HeaderValue::from_static(MIME_TEXT_EVENT_STREAM);

#[allow(clippy::struct_field_names)]
pub struct Session {
    pub listener_name: InternedStr,
    pub session_id: SessionId,
    pub session_id_header: HeaderValue,
    pub last_activity: AtomicInstant,
    pub mcp_upstreams:
        PapayaMap<InternedStr, StdArc<RunningService<RoleClient, InitializeRequestParams>>, ahash::RandomState>,
    pub prompt: Mutex<Option<String>>,
    pub active_tools: PapayaSet<SmolStr, ahash::RandomState>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("listener_name", &self.listener_name)
            .field("session_id", &self.session_id)
            .field("session_id_header", &self.session_id_header)
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
            listener_name: "".into(),
            session_id: SessionId::default(),
            session_id_header: HeaderValue::from_static(""),
            last_activity: AtomicInstant::now(),
            mcp_upstreams: PapayaMap::with_hasher(ahash::RandomState::default()),
            prompt: Mutex::new(None),
            active_tools: PapayaSet::with_hasher(ahash::RandomState::default()),
        }
    }
}

#[derive(Debug)]
pub struct McpGatewayListenerContext {
    session_map: StdArc<PapayaMap<SessionId, StdArc<Session>, ahash::RandomState>>,
    cleanup_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cleanup_task_started: AtomicBool,
}

impl McpGatewayListenerContext {
    fn cleanup(session_map: &PapayaMap<SessionId, StdArc<Session>, ahash::RandomState>) {
        let now = std::time::Instant::now();
        session_map.pin().retain(|_, session| {
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
            session_map: StdArc::new(PapayaMap::with_hasher(ahash::RandomState::new())),
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

    pub fn create_session(&self, listener_name: InternedStr) -> Result<StdArc<Session>, SessionError> {
        if self.session_map.len() >= Self::MAX_SESSIONS_LIMIT {
            debug!(target: "mcp_gateway", "create_session: session limit reached");
            return Err(SessionError::CreateLimitReached);
        }

        let session_id = SessionId(Uuid::new_v4().to_smolstr());
        let session_id_header =
            HeaderValue::from_str(session_id.as_str()).unwrap_or(HeaderValue::from_static(""));

        let session = StdArc::new(Session {
            listener_name,
            session_id: session_id.clone(),
            session_id_header,
            last_activity: AtomicInstant::now(),
            mcp_upstreams: PapayaMap::with_hasher(ahash::RandomState::default()),
            prompt: Mutex::new(None),
            active_tools: PapayaSet::with_hasher(ahash::RandomState::default()),
        });
        self.session_map.pin().insert(session_id, StdArc::clone(&session));
        Ok(session)
    }

    #[inline]
    pub fn delete_session(&self, session_id: &str) -> bool {
        self.session_map.pin().remove(session_id).is_some()
    }
}

#[derive(Debug)]
pub struct McpGatewayInner {
    config: McpGatewayConfig,
    tools: StdArc<ToolsRegistry>,
    initialize_result: InitializeResult,
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
    JsonRpcResponse(Bytes),
    JsonRpcResponseNewSession(Bytes, StdArc<Session>),
    JsonRpcNotificationResponse(
        model::JsonRpcNotification<ServerNotification>,
        model::JsonRpcResponse<model::CallToolResult>,
    ),
}

/// `McpGateway` filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: StdArc<McpGatewayInner>,
    request_id: model::RequestId,
    version: http::Version,
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

        let capabilities = ServerCapabilities::builder().enable_tools().enable_tool_list_changed().build();
        let server_info =
            Implementation::new(config.server_info.name.to_string(), config.server_info.version.to_string());
        let initialize_result = InitializeResult::new(capabilities).with_server_info(server_info);

        Ok(Self {
            inner: StdArc::new(McpGatewayInner { config, tools, initialize_result, tds_registration }),
            request_id: model::RequestId::Number(0),
            version: http::Version::default(),
        })
    }
}

impl FilterFactory for McpGateway {
    fn new_from(&self) -> Self {
        Self {
            inner: StdArc::clone(&self.inner),
            request_id: model::RequestId::Number(0),
            version: http::Version::default(),
        }
    }
}

impl McpGateway {
    pub async fn apply_request(
        &mut self,
        request: &mut http::Request<ArionRequestBody>,
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

    #[allow(clippy::unused_self)]
    pub fn apply_response(
        &mut self,
        _response: &mut Response<ArionResponseBody>,
    ) -> impl Future<Output = FilterDecision> {
        std::future::ready(FilterDecision::Continue)
    }

    fn handle_mcp_delete_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<ArionRequestBody>,
    ) -> FilterDecision {
        let Some(session_id_header) = request.headers().get(MCP_SESSION_ID).cloned() else {
            debug!(target: "mcp_gateway", "handle_mcp_delete_endpoint: session ID but none found in request");
            return FilterDecision::bad_request("Missing session ID", request.version());
        };
        let Ok(session_id) = session_id_header.to_str() else {
            debug!(target: "mcp_gateway", "handle_mcp_delete_endpoint: invalid session ID header");
            return FilterDecision::bad_request("Missing session ID", request.version());
        };

        if !ctx.delete_session(session_id) {
            debug!(target: "mcp_gateway", "handle_mcp_delete_endpoint: StreamableHttp session {} not found in session map", session_id);
            return FilterDecision::not_found("Session not found", request.version());
        }

        let mut response = Response::new(TimeoutBody::new(None, PolyBody::from(Empty::new())).into());
        *response.status_mut() = StatusCode::ACCEPTED;
        *response.version_mut() = self.version;
        let headers = response.headers_mut();
        headers.insert(http::header::CONNECTION, CONNECTION_KEEP_ALIVE);
        headers.insert(MCP_SESSION_ID, session_id_header);

        FilterDecision::DirectResponse(Box::new(response))
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_mcp_post_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<ArionRequestBody>,
        req_ctx: &RequestCtx,
        listener_name: InternedStr,
    ) -> FilterDecision {
        let accept = request.get_mcp_accepted_mime();
        if !matches!(accept, Some(AcceptedMime::EventStreamAndJson)) {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: unsupported MIME type");
            let err = self.build_json_rpc_error(model::ErrorData::invalid_request("Unsupported MIME type", None));
            let body = serde_json::to_string(&err).unwrap_or_default();
            return FilterDecision::bad_request(body, self.version);
        }

        // collect the body of the request...
        //

        let Ok(body) = request.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to collect request body");
            let err =
                self.build_json_rpc_error(model::ErrorData::internal_error("Failed to collect request body", None));
            let body_str = serde_json::to_string(&err).unwrap_or_default();
            return FilterDecision::internal_server_error(body_str, self.version);
        };

        let body = body.to_bytes();

        let json_rpc_message = match serde_json::from_slice::<model::JsonRpcMessage>(&body) {
            Ok(message) => message,
            Err(err) => {
                info!(target: "mcp_gateway", "handle_rpc_json_message: failed to parse json message: {body:?} ({err:?})");
                let err_data = self.build_json_rpc_error(model::ErrorData::parse_error("Parse error", None));
                let body_str = serde_json::to_string(&err_data).unwrap_or_default();
                return FilterDecision::bad_request(body_str, self.version);
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
        let session: Option<StdArc<Session>> = match request.get_mcp_session_id() {
            Some(session_id) => {
                let Ok(session) = Self::get_valid_session(ctx, session_id) else {
                    let err = self.build_json_rpc_error(model::ErrorData::invalid_request("Session not found", None));
                    let body_str = serde_json::to_string(&err).unwrap_or_default();
                    return FilterDecision::not_found(body_str, self.version);
                };

                Some(session)
            },
            None => None,
        };

        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: previous session: {session:?}");

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

        let session_header = session.as_deref().map(|s| &s.session_id_header);

        match msg_result {
            MessageResult::JsonRpcError(json_rpc_error) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return ({json_rpc_error:?})");
                let body = serde_json::to_vec(&json_rpc_error).unwrap_or_default();
                let resp = self.build_mcp_http_response(
                    StatusCode::OK,
                    Self::build_mcp_response_body(Some(body.into())),
                    session_header,
                    Some(CONTENT_TYPE_JSON),
                );
                FilterDecision::DirectResponse(Box::new(resp))
            },
            MessageResult::JsonRpcResponse(body) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return ({} bytes)", body.len());
                let resp = self.build_mcp_http_response(
                    StatusCode::OK,
                    Self::build_mcp_response_body(Some(body)),
                    session_header,
                    Some(CONTENT_TYPE_JSON),
                );
                FilterDecision::DirectResponse(Box::new(resp))
            },
            MessageResult::JsonRpcResponseNewSession(body, new_session) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: new session created: {new_session:?}");
                let resp = self.build_mcp_http_response(
                    StatusCode::OK,
                    Self::build_mcp_response_body(Some(body)),
                    Some(&new_session.session_id_header),
                    Some(CONTENT_TYPE_JSON),
                );
                FilterDecision::DirectResponse(Box::new(resp))
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
                    return FilterDecision::internal_server_error(body_str, self.version);
                }
                if let Err(e) = resp.write_to(&mut buf) {
                    debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to serialize response SSE event: {e}");
                    let err = self
                        .build_json_rpc_error(model::ErrorData::internal_error("Failed to serialize SSE event", None));
                    let body_str = serde_json::to_string(&err).unwrap_or_default();
                    return FilterDecision::internal_server_error(body_str, self.version);
                }
                let body = buf.freeze();

                let resp = self.build_mcp_http_response(
                    StatusCode::OK,
                    Self::build_mcp_response_body(Some(body)),
                    session_header,
                    Some(CONTENT_TYPE_EVENT_STREAM),
                );
                FilterDecision::DirectResponse(Box::new(resp))
            },
            MessageResult::Nothing => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return Nothing response.");
                let accepted = self.build_mcp_http_response(
                    StatusCode::ACCEPTED,
                    Self::build_mcp_response_body(None),
                    session_header,
                    None,
                );
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
        listener_name: InternedStr,
        session: Option<&StdArc<Session>>,
    ) -> Result<MessageResult, FilterDecision> {
        debug!(target: "mcp_gateway", "handle_rpc_json_message: session: {session:?}, listener: {listener_name}");

        match json_rpc_message {
            model::JsonRpcMessage::Request(json_rpc_request) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: json rpc Request, session_id: {}...", session.map_or("", |s| s.session_id.as_str()));
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
                    return Err(FilterDecision::bad_request(body, req_version));
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
                    return Err(FilterDecision::bad_request(body, req_version));
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
                    return Err(FilterDecision::bad_request(body, req_version));
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
        listener_name: InternedStr,
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
                return Err(FilterDecision::bad_request(body, req_version));
            }

            if serde_json::from_value::<model::InitializeRequestParams>(serde_json::Value::Object(rpc.request.params))
                .is_err()
            {
                info!(target: "mcp_gateway", "handle_rpc_json_request: invalid params!");
                return Ok(MessageResult::JsonRpcError(
                    self.build_json_rpc_error(model::ErrorData::invalid_params("invalid params", None)),
                ));
            }

            let Ok(new_session) = ctx.create_session(listener_name) else {
                info!(target: "mcp_gateway", "handle_rpc_json_request: failed to create new session!");
                return Ok(MessageResult::JsonRpcError(
                    self.build_json_rpc_error(model::ErrorData::parse_error("Failed to create new session", None)),
                ));
            };

            debug!(target: "mcp_gateway", "handle_rpc_json_request: new session: {new_session:?}");

            let (response, _) = upstream::serialize_json_rpc_response(&rpc.id, &self.inner.initialize_result)
                .unwrap_or_else(|_| (Bytes::new(), 0));

            return Ok(MessageResult::JsonRpcResponseNewSession(response, new_session));
        }

        // check integrity session...

        let Some(session) = session else {
            let err =
                self.build_json_rpc_error(model::ErrorData::invalid_request("Missing or invalid mcp-session-id", None));
            let body = serde_json::to_string(&err).unwrap_or_default();
            return Err(FilterDecision::bad_request(body, req_version));
        };

        match rpc.request.method.as_str() {
            InitializedNotificationMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: 'notification/initialized'");
                Ok(MessageResult::Nothing)
            },
            PingRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: 'ping' received");
                let (response, _) =
                    upstream::serialize_json_rpc_response(&rpc.id, &serde_json::Map::new())
                        .unwrap_or_else(|_| (Bytes::new(), 0));

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
                let (response, _) = upstream::serialize_json_rpc_response(&rpc.id, &tools)
                    .unwrap_or_else(|_| (Bytes::new(), 0));

                Ok(MessageResult::JsonRpcResponse(response))
            },
            CallToolRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: 'tools/call'");

                let msg_result = match self
                    .inner
                    .tools
                    .call(req_ext, req_headers, req_ctx, rpc, &self.inner.config.upstream_limits, session)
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
                            CallToolError::ToolNotFound(name) => {
                                model::ErrorData::resource_not_found(format!("Tool '{name}' not found"), None)
                            },
                            CallToolError::RbacDenied(tool) => model::ErrorData::invalid_request(
                                format!("Access denied by RBAC policy for tool '{tool}'"),
                                None,
                            ),
                            CallToolError::FunctionGraphNotImplemented => model::ErrorData::internal_error(
                                "FunctionGraph transcoding is not yet implemented",
                                None,
                            ),
                            CallToolError::TranscoderError { tool, reason } => {
                                model::ErrorData::internal_error(
                                    format!("Transcoder error for tool '{tool}': {reason}"),
                                    None,
                                )
                            },
                            CallToolError::ClientInitializeError(init_err) => match *init_err {
                                ClientInitializeError::JsonRpcError(error_data) => error_data,
                                ClientInitializeError::ConnectionClosed(msg) => {
                                    model::ErrorData::internal_error(format!("Upstream connection closed: {msg}"), None)
                                },
                                ClientInitializeError::TransportError { error, context } => {
                                    model::ErrorData::internal_error(
                                        format!("Upstream transport error ({context}): {error}"),
                                        None,
                                    )
                                },
                                ClientInitializeError::Cancelled => model::ErrorData::internal_error(
                                    "Upstream client initialization was cancelled",
                                    None,
                                ),
                                other => model::ErrorData::internal_error(
                                    format!("Upstream client initialization error: {other}"),
                                    None,
                                ),
                            },
                            CallToolError::SerdeError(e) => {
                                model::ErrorData::parse_error(format!("Serialization error: {e}"), None)
                            },
                            CallToolError::ServiceError(svc_err) => match *svc_err {
                                ServiceError::McpError(error_data) => error_data,
                                ServiceError::TransportSend(e) => model::ErrorData::internal_error(
                                    format!("Upstream transport send error: {e}"),
                                    None,
                                ),
                                ServiceError::TransportClosed => {
                                    model::ErrorData::internal_error("Upstream transport closed", None)
                                },
                                ServiceError::UnexpectedResponse => {
                                    model::ErrorData::internal_error("Unexpected response from upstream", None)
                                },
                                ServiceError::Cancelled { reason } => model::ErrorData::internal_error(
                                    format!("Upstream request cancelled: {}", reason.as_deref().unwrap_or("<unknown>")),
                                    None,
                                ),
                                ServiceError::Timeout { timeout } => model::ErrorData::internal_error(
                                    format!("Upstream request timed out after {timeout:?}"),
                                    None,
                                ),
                                other => {
                                    model::ErrorData::internal_error(format!("Upstream service error: {other}"), None)
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
        session_id: &str,
    ) -> Result<StdArc<Session>, SessionError> {
        let Some(session) = ctx.session_map.pin().get(session_id).map(StdArc::clone) else {
            debug!(target: "mcp_gateway", "get_valid_session: valid session {} not found in session map", session_id);
            return Err(SessionError::NotFound);
        };

        session.last_activity.store(std::time::Instant::now(), std::sync::atomic::Ordering::Relaxed);
        debug!(target: "mcp_gateway", "get_valid_session: {session_id} -> {session:?}");
        Ok(session)
    }

    #[inline]
    fn build_mcp_response_body(body: Option<Bytes>) -> TimeoutBody<PolyBody> {
        match body {
            Some(b) => TimeoutBody::new(None, PolyBody::from(Full::from(b))),
            None => TimeoutBody::new(None, PolyBody::from(Empty::new())),
        }
    }

    #[inline]
    fn build_mcp_http_response(
        &self,
        status: StatusCode,
        body: TimeoutBody<PolyBody>,
        session_header: Option<&HeaderValue>,
        content_type: Option<HeaderValue>,
    ) -> Response<ArionResponseBody> {
        let mut resp = Response::new(body.into());
        *resp.status_mut() = status;
        *resp.version_mut() = self.version;

        let headers = resp.headers_mut();
        headers.insert(http::header::CONNECTION, CONNECTION_KEEP_ALIVE);
        if let Some(session_header) = session_header {
            headers.insert(MCP_SESSION_ID, session_header.clone());
        }
        if let Some(ct) = content_type {
            headers.insert(http::header::CONTENT_TYPE, ct);
        }

        resp
    }

    #[inline]
    fn build_json_rpc_error(&self, error: model::ErrorData) -> model::JsonRpcError {
        model::JsonRpcError { jsonrpc: model::JsonRpcVersion2_0, id: Some(self.request_id.clone()), error }
    }
}
