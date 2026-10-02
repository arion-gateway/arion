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

use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    DisclosureMode, McpGateway as McpGatewayConfig,
};
use arion_http_header::MCP_SESSION_ID;
use atomic_time::AtomicInstant;
use bytes::Bytes;
use http::{HeaderName, Method, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use papaya::HashMap as PapayaMap;
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
        PingRequestMethod, ServerCapabilities,
    },
    service::RunningService,
    RoleClient,
};

use crate::{
    body::timeout_body::TimeoutBody,
    listeners::{
        http_connection_manager::{
            mcp_gateway::{
                embeddings,
                tools::{CallToolError, ToolBuilderError, ToolSearchRequest, ToolsRegistry},
                transport::{AcceptedMime, RequestExt, SessionId, MIME_APPLICATION_JSON},
            },
            RequestCtx,
        },
        http_filters::{FilterDecision, FilterFactory},
        listener::FilterListenerContext,
    },
    ArionRequestBody, ArionResponseBody, PolyBody,
};

const MCP_MESSAGE_ENDPOINT: &str = "/mcp";
const MCP_SEARCH_ENDPOINT: &str = "/mcp/search";
const SESSION_IDLE_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(300);

#[allow(clippy::struct_field_names)]
pub struct Session {
    pub listener_name: &'static str,
    pub session_id: SessionId,
    pub last_activity: AtomicInstant,
    pub mcp_upstreams:
        PapayaMap<String, StdArc<RunningService<RoleClient, InitializeRequestParams>>, ahash::RandomState>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("listener_name", &self.listener_name)
            .field("session_id", &self.session_id)
            .field("last_activity", &self.last_activity.load(std::sync::atomic::Ordering::Relaxed))
            .field("mcp_upstreams", &self.mcp_upstreams)
            .finish()
    }
}

#[cfg(test)]
impl Default for Session {
    fn default() -> Self {
        Self {
            listener_name: "",
            session_id: SessionId::default(),
            last_activity: AtomicInstant::now(),
            mcp_upstreams: PapayaMap::with_hasher(ahash::RandomState::default()),
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
    const MAX_SESSIONS_LIMIT: usize = 65536;

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

    pub fn create_session(&self, listener_name: &'static str) -> Option<StdArc<Session>> {
        if self.session_map.len() >= Self::MAX_SESSIONS_LIMIT {
            debug!(target: "mcp_gateway", "create_session: session limit reached");
            return None;
        }

        let session_id = SessionId(Uuid::new_v4().to_smolstr());

        let session = StdArc::new(Session {
            listener_name,
            session_id: session_id.clone(),
            last_activity: AtomicInstant::now(),
            mcp_upstreams: PapayaMap::with_hasher(ahash::RandomState::default()),
        });
        self.session_map.pin().insert(session_id, StdArc::clone(&session));
        Some(session)
    }

    #[inline]
    pub fn delete_session(&self, session_id: &SessionId) -> bool {
        self.session_map.pin().remove(session_id).is_some()
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
}

/// `McpGateway` filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: StdArc<McpGatewayInner>,
    current_session: Option<StdArc<Session>>,
    request_id: model::RequestId,
    version: http::Version,
}

impl McpGateway {
    fn with_inner(inner: StdArc<McpGatewayInner>) -> Self {
        Self {
            inner,
            current_session: None,
            request_id: model::RequestId::Number(0),
            version: http::Version::default(),
        }
    }
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

        Ok(Self::with_inner(StdArc::new(McpGatewayInner { config, tools, tds_registration })))
    }
}

impl FilterFactory for McpGateway {
    fn new_from(&self) -> Self {
        Self::with_inner(StdArc::clone(&self.inner))
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
            (&Method::POST, MCP_SEARCH_ENDPOINT) => self.handle_search_post_endpoint(request, req_ctx).await,
            (_, MCP_SEARCH_ENDPOINT) => FilterDecision::method_not_allowed("Method not allowed", request.version()),
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

    async fn handle_search_post_endpoint(
        &self,
        request: &mut http::Request<ArionRequestBody>,
        req_ctx: &RequestCtx,
    ) -> FilterDecision {
        let body = match request.body_mut().collect().await {
            Ok(body) => body.to_bytes(),
            Err(error) => {
                debug!(target: "mcp_gateway", "failed to collect /mcp/search body: {error}");
                return FilterDecision::bad_request("Invalid request body", request.version());
            },
        };
        let search_request = match serde_json::from_slice::<ToolSearchRequest>(&body) {
            Ok(search_request) => search_request,
            Err(error) => {
                debug!(target: "mcp_gateway", "failed to parse /mcp/search body: {error}");
                return FilterDecision::bad_request("Malformed or invalid search request", request.version());
            },
        };

        let tools = match self
            .inner
            .tools
            .search_tools(request.extensions(), req_ctx, search_request, DisclosureMode::Eager)
            .await
        {
            Ok(tools) => tools,
            Err(CallToolError::ValidationError(error)) => {
                return FilterDecision::bad_request(&error, request.version());
            },
            Err(error) => {
                debug!(target: "mcp_gateway", "/mcp/search failed: {error:#}");
                return FilterDecision::internal_server_error("Tool search failed", request.version());
            },
        };

        let body = match serde_json::to_vec(&json!({ "tools": tools })) {
            Ok(body) => Bytes::from(body),
            Err(error) => {
                debug!(target: "mcp_gateway", "failed to serialize /mcp/search response: {error}");
                return FilterDecision::internal_server_error("Tool search failed", request.version());
            },
        };
        let headers: SmallVec<[(HeaderName, &str); 2]> = smallvec![(http::header::CONTENT_TYPE, MIME_APPLICATION_JSON)];
        match self.build_mcp_http_response(StatusCode::OK, Self::build_mcp_response_body(Some(body)), &headers) {
            Ok(response) => FilterDecision::DirectResponse(Box::new(response)),
            Err(decision) => decision,
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn apply_response(&mut self, _response: &mut Response<ArionResponseBody>) -> FilterDecision {
        FilterDecision::Continue
    }

    fn handle_mcp_delete_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<ArionRequestBody>,
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

    async fn handle_mcp_post_endpoint(
        &mut self,
        ctx: &McpGatewayListenerContext,
        request: &mut http::Request<ArionRequestBody>,
        req_ctx: &RequestCtx,
        listener_name: &'static str,
    ) -> FilterDecision {
        let accept = request.get_mcp_accepted_mime();
        if !matches!(accept, Some(AcceptedMime::EventStreamAndJson)) {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: unsupported MIME type");
            return self.rpc_error_decision(
                model::ErrorData::invalid_request("Unsupported MIME type", None),
                FilterDecision::bad_request,
            );
        }

        // collect the body of the request...
        //

        let Ok(body) = request.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: failed to collect request body");
            return self.rpc_error_decision(
                model::ErrorData::internal_error("Failed to collect request body", None),
                FilterDecision::internal_server_error,
            );
        };

        let body = body.to_bytes();

        let json_rpc_message = match serde_json::from_slice::<model::JsonRpcMessage>(&body) {
            Ok(message) => message,
            Err(err) => {
                info!(target: "mcp_gateway", "handle_rpc_json_message: failed to parse json message: {body:?} ({err:?})");
                return self.rpc_error_decision(
                    model::ErrorData::parse_error("Parse error", None),
                    FilterDecision::bad_request,
                );
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
        let session = if let Some(session_id) = request.get_mcp_session_id() {
            let Some(session) = Self::get_valid_session(ctx, &session_id) else {
                return self.rpc_error_decision(
                    model::ErrorData::invalid_request("Session not found", None),
                    FilterDecision::not_found,
                );
            };
            Some(session)
        } else {
            None
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

        self.current_session = match &msg_result {
            MessageResult::JsonRpcResponseNewSession(_, new_session) => Some(StdArc::clone(new_session)),
            _ => session,
        };
        debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: current session: {:?}", self.current_session);

        let (status, body) = match msg_result {
            MessageResult::JsonRpcError(json_rpc_error) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return ({json_rpc_error:?})");
                (StatusCode::OK, Some(serde_json::to_vec(&json_rpc_error).unwrap_or_default()))
            },
            MessageResult::JsonRpcResponse(json_rpc_response)
            | MessageResult::JsonRpcResponseNewSession(json_rpc_response, _) => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return ({json_rpc_response:?})");
                (StatusCode::OK, Some(serde_json::to_vec(&json_rpc_response).unwrap_or_default()))
            },
            MessageResult::Nothing => {
                debug!(target: "mcp_gateway", "handle_mcp_post_endpoint: return Nothing response.");
                (StatusCode::ACCEPTED, None)
            },
        };

        let headers = self.build_http_headers_with_content_type(body.is_some().then_some(MIME_APPLICATION_JSON));
        match self.build_mcp_http_response(status, Self::build_mcp_response_body(body.map(Bytes::from)), &headers) {
            Ok(resp) => FilterDecision::DirectResponse(Box::new(resp)),
            Err(decision) => decision,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_rpc_json_message(
        &mut self,
        ctx: &McpGatewayListenerContext,
        req_ext: &http::Extensions,
        req_headers: &http::HeaderMap,
        req_ctx: &RequestCtx,
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
                    json_rpc_request,
                    listener_name,
                    session,
                )
                .await
            },
            message @ (model::JsonRpcMessage::Notification(_)
            | model::JsonRpcMessage::Response(_)
            | model::JsonRpcMessage::Error(_)) => {
                debug!(target: "mcp_gateway", "handle_rpc_json_message: {message:#?}");
                if session.is_none() {
                    return Err(self.rpc_error_decision(
                        model::ErrorData::invalid_request("Missing or invalid mcp-session-id", None),
                        FilterDecision::bad_request,
                    ));
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
        rpc: model::JsonRpcRequest,
        listener_name: &'static str,
        session: Option<&StdArc<Session>>,
    ) -> Result<MessageResult, FilterDecision> {
        if matches!(rpc.request.method.as_str(), InitializeResultMethod::VALUE) {
            debug!(target: "mcp_gateway", "handle_rpc_json_request: 'initialize' method received");
            if session.is_some() {
                info!(target: "mcp_gateway", "handle_rpc_json_request: session already initialized!");
                return Err(self.rpc_error_decision(
                    model::ErrorData::invalid_request("Initialize already called for this session", None),
                    FilterDecision::bad_request,
                ));
            }

            // validate the params (their content is not used yet)
            if serde_json::from_value::<InitializeRequestParams>(Value::Object(rpc.request.params)).is_err() {
                info!(target: "mcp_gateway", "handle_rpc_json_request: invalid params!");
                return Ok(MessageResult::JsonRpcError(
                    self.build_json_rpc_error(model::ErrorData::invalid_params("invalid params", None)),
                ));
            }

            let capabilities = ServerCapabilities::builder().enable_tools().enable_tool_list_changed().build();

            let server_info = {
                let info = &self.inner.config.server_info;
                Implementation::new(info.name.to_string(), info.version.to_string())
            };

            let result = InitializeResult::new(capabilities).with_server_info(server_info);

            let Some(new_session) = ctx.create_session(listener_name) else {
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

        // check integrity session...

        let Some(session) = session else {
            return Err(self.rpc_error_decision(
                model::ErrorData::invalid_request("Missing or invalid mcp-session-id", None),
                FilterDecision::bad_request,
            ));
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
                let tools = match self.inner.tools.build_list_tools(req_ext, req_ctx).await {
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

                match self
                    .inner
                    .tools
                    .call(req_ext, req_headers, req_ctx, &rpc, &self.inner.config.upstream_limits, session)
                    .await
                {
                    Ok(result) => Ok(result),
                    Err(err) => {
                        debug!(target: "mcp_gateway", "handle_rpc_json_request: 'tools/call' failed: {err:#}");
                        Ok(MessageResult::JsonRpcError(self.build_json_rpc_error(err.into())))
                    },
                }
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

    fn get_valid_session(ctx: &McpGatewayListenerContext, session_id: &SessionId) -> Option<StdArc<Session>> {
        let Some(session) = ctx.session_map.pin().get(session_id).map(StdArc::clone) else {
            debug!(target: "mcp_gateway", "get_valid_session: valid session {} not found in session map", session_id);
            return None;
        };

        session.last_activity.store(std::time::Instant::now(), std::sync::atomic::Ordering::Relaxed);
        debug!(target: "mcp_gateway", "get_valid_session: {session_id} -> {session:?}");
        Some(session)
    }

    /// Serialize a JSON-RPC error into the body of a `FilterDecision` built by `f`.
    fn rpc_error_decision(
        &self,
        error: model::ErrorData,
        f: fn(&str, http::Version) -> FilterDecision,
    ) -> FilterDecision {
        let body = serde_json::to_string(&self.build_json_rpc_error(error)).unwrap_or_default();
        f(&body, self.version)
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
    ) -> Result<Response<ArionResponseBody>, FilterDecision> {
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
