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
    McpGateway as McpGatewayConfig, McpGatewayMode as ConfigGatewayMode, McpServerInfo,
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
    Arc as StdArc, LazyLock,
};
use tracing::{debug, info};
use uuid::Uuid;

use rmcp::{
    model::{
        self, CallToolRequestMethod, ConstString, Implementation, InitializeRequestParams, InitializeResult,
        InitializeResultMethod, InitializedNotificationMethod, ListToolsRequestMethod, NumberOrString,
        PingRequestMethod, ServerCapabilities, Tool,
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
                tools::{
                    call_tool_result_into_value, CallToolError, ToolBuilderError, ToolCallContext, ToolId,
                    ToolSearchRequest, ToolSearchScope, ToolsRegistry, TOOL_SEARCH_TOOL_NAME,
                },
                transport::{AcceptedMime, RequestExt, SessionId, MIME_APPLICATION_JSON},
            },
            RequestCtx,
        },
        http_filters::{FilterDecision, FilterFactory},
        listener::FilterListenerContext,
    },
    ArionRequestBody, ArionResponseBody, PolyBody,
};

#[cfg(feature = "codemode")]
use crate::listeners::http_connection_manager::mcp_gateway::codemode::CodeMode;

const MCP_MESSAGE_ENDPOINT: &str = "/mcp";
const MCP_SEARCH_ENDPOINT: &str = "/mcp/search";
const SESSION_IDLE_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(300);
const TOOL_SEARCH_DESCRIPTION_NAME_LIMIT: usize = 256;

static TOOL_SEARCH_INPUT_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "minLength": 1,
                "description": "Natural-language description of the capability to find."
            },
            "namespace": {
                "type": "string",
                "minLength": 1,
                "description": "Optional namespace to search within."
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "description": "Optional requested result count; values above the configured maximum are clamped."
            }
        },
        "required": ["query"],
        "additionalProperties": false
    })
});

fn parse_json_rpc_message(body: &[u8]) -> serde_json::Result<model::JsonRpcMessage> {
    let mut value = serde_json::from_slice::<Value>(body)?;

    if let Value::Object(message) = &mut value {
        if message.contains_key("method") && message.contains_key("id") && !message.contains_key("params") {
            message.insert("params".to_owned(), Value::Object(serde_json::Map::new()));
        }
    }

    serde_json::from_value(value)
}

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
    server_info: McpServerInfo,
    tools: StdArc<ToolsRegistry>,
    mode: GatewayMode,
    tds_registration: Option<TdsRegistration>,
}

#[derive(Debug)]
struct ToolsMode;

#[derive(Debug)]
enum GatewayMode {
    Tools(ToolsMode),
    #[cfg(feature = "codemode")]
    Code(CodeMode),
}

impl GatewayMode {
    fn exposes_rest_search(&self) -> bool {
        matches!(self, Self::Tools(_))
    }

    async fn list_tools(
        &self,
        tools: &StdArc<ToolsRegistry>,
        req_ext: &http::Extensions,
        req_ctx: &RequestCtx,
    ) -> Result<model::ListToolsResult, CallToolError> {
        match self {
            Self::Tools(mode) => mode.list_tools(tools, req_ext, req_ctx).await,
            #[cfg(feature = "codemode")]
            Self::Code(mode) => mode.list_tools(tools, req_ext, req_ctx).await,
        }
    }

    async fn call_tool(
        &self,
        tools: &StdArc<ToolsRegistry>,
        cx: ToolCallContext<'_>,
        name: &str,
        arguments: Value,
    ) -> Result<model::CallToolResult, CallToolError> {
        match self {
            Self::Tools(mode) => mode.call_tool(tools, cx, name, arguments).await,
            #[cfg(feature = "codemode")]
            Self::Code(mode) => mode.call_tool(tools, cx, name, arguments).await,
        }
    }
}

impl ToolsMode {
    async fn list_tools(
        &self,
        registry: &ToolsRegistry,
        req_ext: &http::Extensions,
        req_ctx: &RequestCtx,
    ) -> Result<model::ListToolsResult, CallToolError> {
        registry.ensure_tools_current(req_ctx).await?;
        let (mut tools, progressive_names) = registry.list_disclosed_tools(req_ext);
        if !progressive_names.is_empty() {
            let Value::Object(input_schema) = &*TOOL_SEARCH_INPUT_SCHEMA else { unreachable!() };
            tools.push(Tool::new(
                TOOL_SEARCH_TOOL_NAME,
                Self::tool_search_description(progressive_names),
                StdArc::new(input_schema.clone()),
            ));
            tools.sort_by(|left, right| left.name.cmp(&right.name));
        }
        Ok(model::ListToolsResult {
            tools,
            next_cursor: None,
            meta: None,
            ttl_ms: None,
            result_type: None,
            cache_scope: None,
        })
    }

    fn tool_search_description(mut names: Vec<SmolStr>) -> String {
        let total = names.len();
        names.truncate(TOOL_SEARCH_DESCRIPTION_NAME_LIMIT);
        let names_json = serde_json::to_string(&names).unwrap_or_else(|_| "[]".to_owned());
        let availability = if total > names.len() {
            format!(
                "Available tool names (truncated; showing {} of {total}; more are available, try searching by describing what you need): {names_json}",
                names.len()
            )
        } else {
            format!("Available tool names: {names_json}")
        };
        format!(
            "Search for additional tools using `query`, optionally filter by `namespace`, and limit the returned results with `limit`. Returns full MCP tool definitions. When you find a relevant tool, use the exact returned `name` and follow its `inputSchema`; never invent or normalize names. {availability}"
        )
    }

    async fn call_tool(
        &self,
        registry: &ToolsRegistry,
        cx: ToolCallContext<'_>,
        name: &str,
        arguments: Value,
    ) -> Result<model::CallToolResult, CallToolError> {
        if name == TOOL_SEARCH_TOOL_NAME {
            registry.ensure_tools_current(cx.request).await?;
            if !registry.has_progressive_tools(cx.extensions) {
                return Err(CallToolError::ToolNotFound(TOOL_SEARCH_TOOL_NAME.to_owned()));
            }
            let request: ToolSearchRequest = serde_json::from_value(arguments)
                .map_err(|error| CallToolError::ValidationError(format!("invalid tool_search arguments: {error}")))?;
            let tools = registry.search_tools(cx.extensions, cx.request, request, ToolSearchScope::Progressive).await?;
            let text = serde_json::to_string_pretty(&tools).unwrap_or_else(|_| "[]".to_owned());
            let content = model::ContentBlock::text(text);
            return Ok(model::CallToolResult::success(vec![content]));
        }

        let id = ToolId::parse_qualified(name).ok_or_else(|| CallToolError::ToolNotFound(name.to_owned()))?;
        registry.invoke_tool(cx, &id, arguments).await
    }
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
        #[cfg(not(feature = "codemode"))]
        if matches!(&config.mode, ConfigGatewayMode::Code(_)) {
            return Err(ToolBuilderError::CodeModeFeatureDisabled);
        }

        let mode = match &config.mode {
            ConfigGatewayMode::Tools(_) => GatewayMode::Tools(ToolsMode),
            ConfigGatewayMode::Code(code_mode) => {
                #[cfg(feature = "codemode")]
                {
                    GatewayMode::Code(CodeMode::try_new(code_mode)?)
                }
                #[cfg(not(feature = "codemode"))]
                {
                    let _ = code_mode;
                    unreachable!("codemode configuration was rejected above")
                }
            },
        };
        let embeddings_client = match config.search.embeddings.clone() {
            Some(cfg) => {
                let cfg = cfg.normalized().map_err(ToolBuilderError::InvalidEmbeddingsConfig)?;
                Some(StdArc::new(embeddings::EmbeddingsClient::from_config(cfg)))
            },
            None => None,
        };

        let tools = ToolsRegistry::with_config(
            config.tools.clone(),
            config.dynamic_mcp_servers.clone(),
            config.search.clone(),
            config.upstream_limits.clone(),
            embeddings_client,
        )?
        .with_open_api_sources(config.open_api_tool_sources.clone())?;
        let tools = StdArc::new(tools);

        let tds_registration = if let Some(tds) = &config.tds {
            let runtime_id = crate::runtime_context::get_runtime_id();
            let server_name = config.server_info.name.clone();
            super::uniqueness::claim(runtime_id, server_name.clone()).map_err(ToolBuilderError::DuplicateServerName)?;
            let scope = format_smolstr!("{server_name}/{config_name}", config_name = tds.config_name);
            #[cfg(feature = "codemode")]
            let toolkits = match &mode {
                GatewayMode::Code(mode) => Some(mode.toolkits()),
                GatewayMode::Tools(_) => None,
            };
            super::xds_handler::subscribe_for_updates(
                scope.clone(),
                &tools,
                #[cfg(feature = "codemode")]
                toolkits,
            );
            Some(TdsRegistration { runtime_id, server_name, scope })
        } else {
            None
        };

        Ok(Self::with_inner(StdArc::new(McpGatewayInner {
            server_info: config.server_info,
            tools,
            mode,
            tds_registration,
        })))
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
            (_, MCP_SEARCH_ENDPOINT) if !self.inner.mode.exposes_rest_search() => {
                FilterDecision::no_route_found("Route not found", request.version())
            },
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

        if let Err(error) = self.inner.tools.ensure_tools_current(req_ctx).await {
            debug!(target: "mcp_gateway", "/mcp/search registry refresh failed: {error:#}");
            return FilterDecision::internal_server_error("Tool search failed", request.version());
        }
        let tools = match self
            .inner
            .tools
            .search_tools(request.extensions(), req_ctx, search_request, ToolSearchScope::All)
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

        let json_rpc_message = match parse_json_rpc_message(&body) {
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
                let info = &self.inner.server_info;
                Implementation::new(info.name.to_string(), info.version.to_string())
            };

            let result = InitializeResult::new(capabilities).with_server_info(server_info);
            #[cfg(feature = "codemode")]
            let result = match &self.inner.mode {
                GatewayMode::Tools(_) => result,
                GatewayMode::Code(_) => result.with_instructions(CodeMode::instructions()),
            };

            let Some(new_session) = ctx.create_session(listener_name) else {
                info!(target: "mcp_gateway", "handle_rpc_json_request: failed to create new session!");
                return Ok(MessageResult::JsonRpcError(
                    self.build_json_rpc_error(model::ErrorData::parse_error("Failed to create new session", None)),
                ));
            };

            debug!(target: "mcp_gateway", "handle_rpc_json_request: new session: {new_session:?}");

            let response = self.build_json_rpc_response(serde_json::to_value(result).unwrap_or(Value::Null));

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
                Ok(MessageResult::JsonRpcResponse(self.build_json_rpc_response(json!({}))))
            },
            ListToolsRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: 'tools/list'");
                let tools = match self.inner.mode.list_tools(&self.inner.tools, req_ext, req_ctx).await {
                    Ok(tools) => tools,
                    Err(err) => {
                        debug!(target: "mcp_gateway", "handle_rpc_json_request: 'tools/list' failed: {err:#}");
                        return Ok(MessageResult::JsonRpcError(
                            self.build_json_rpc_error(model::ErrorData::internal_error(err.to_string(), None)),
                        ));
                    },
                };
                Ok(MessageResult::JsonRpcResponse(
                    self.build_json_rpc_response(serde_json::to_value(tools).unwrap_or(Value::Null)),
                ))
            },
            CallToolRequestMethod::VALUE => {
                debug!(target: "mcp_gateway", "handle_rpc_json_request: 'tools/call'");

                let mut params = rpc.request.params;
                let Some(name) = params.get("name").and_then(Value::as_str).map(str::to_owned) else {
                    return Ok(MessageResult::JsonRpcError(
                        self.build_json_rpc_error(CallToolError::NameNotString.into()),
                    ));
                };
                let arguments = params.remove("arguments").unwrap_or(Value::Null);

                let cx = ToolCallContext { extensions: req_ext, headers: req_headers, request: req_ctx, session };
                let result = self.inner.mode.call_tool(&self.inner.tools, cx, &name, arguments).await;

                match result.and_then(|result| call_tool_result_into_value(result).map_err(CallToolError::from)) {
                    Ok(result) => Ok(MessageResult::JsonRpcResponse(self.build_json_rpc_response(result))),
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
    fn build_json_rpc_response(&self, result: Value) -> model::JsonRpcResponse<Value> {
        model::JsonRpcResponse { jsonrpc: model::JsonRpcVersion2_0, id: self.request_id.clone(), result }
    }

    #[inline]
    pub fn get_current_session_id(&self) -> Option<&SessionId> {
        self.current_session.as_deref().map(|s| &s.session_id)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tools::{TestCallContext, CODEMODE_EXECUTE_TOOL_NAME};
    use super::*;
    use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
        DisclosureMode, EmbeddingVector, HttpUpstreamPolicy, McpCodeMode, McpGatewayMode, McpTool, McpToolSearch,
        McpToolsMode, UpstreamBackend, UpstreamLimits,
    };

    #[test]
    fn params_less_request_is_normalized_before_rmcp_deserialization() {
        let message =
            parse_json_rpc_message(br#"{"method":"tools/list","jsonrpc":"2.0","id":1}"#).expect("params-less request");

        let model::JsonRpcMessage::Request(request) = message else {
            panic!("expected JSON-RPC request");
        };
        assert_eq!(request.id, NumberOrString::Number(1));
        assert_eq!(request.request.method, "tools/list");
        assert!(request.request.params.is_empty());
    }

    #[test]
    fn existing_request_params_are_preserved() {
        let message =
            parse_json_rpc_message(br#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"cursor":"next"}}"#)
                .expect("request with params");

        let model::JsonRpcMessage::Request(request) = message else {
            panic!("expected JSON-RPC request");
        };
        assert_eq!(request.request.params, serde_json::Map::from_iter([("cursor".to_owned(), json!("next"))]));
    }

    #[test]
    fn params_less_notification_remains_a_notification() {
        let message = parse_json_rpc_message(br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
            .expect("params-less notification");

        let model::JsonRpcMessage::Notification(notification) = message else {
            panic!("expected JSON-RPC notification");
        };
        assert_eq!(notification.notification.method, "notifications/initialized");
        assert!(notification.notification.params.is_empty());
    }

    #[test]
    fn response_with_an_id_is_not_normalized_as_a_request() {
        let message =
            parse_json_rpc_message(br#"{"jsonrpc":"2.0","id":1,"result":{"status":"ok"}}"#).expect("JSON-RPC response");

        let model::JsonRpcMessage::Response(response) = message else {
            panic!("expected JSON-RPC response");
        };
        assert_eq!(response.id, NumberOrString::Number(1));
        assert_eq!(response.result.get("status"), Some(&json!("ok")));
    }

    #[test]
    fn explicit_null_params_are_not_normalized() {
        let message = parse_json_rpc_message(br#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":null}"#)
            .expect("explicit null params retain rmcp behavior");

        assert!(matches!(message, model::JsonRpcMessage::Notification(_)));
    }

    #[test]
    fn malformed_json_and_invalid_params_remain_parse_errors() {
        parse_json_rpc_message(br#"{"jsonrpc":"2.0""#).unwrap_err();
        parse_json_rpc_message(br#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":[]}"#).unwrap_err();
    }

    fn config_with_codemode() -> McpGatewayConfig {
        McpGatewayConfig {
            server_info: McpServerInfo { name: "test".into(), version: "1".into() },
            tools: Vec::new(),
            open_api_tool_sources: Vec::new(),
            dynamic_mcp_servers: Vec::new(),
            tds: None,
            search: McpToolSearch::default(),
            mode: McpGatewayMode::Code(McpCodeMode::default()),
            upstream_limits: UpstreamLimits::default(),
        }
    }

    #[cfg(feature = "codemode")]
    #[tokio::test]
    async fn codemode_invocation_runs_as_a_normal_tool_call_result() {
        let gateway = McpGateway::try_from(config_with_codemode()).expect("codemode-enabled gateway");
        let GatewayMode::Code(mode) = &gateway.inner.mode else { panic!("codemode") };
        let cx = TestCallContext::default();
        let result = mode
            .call_tool(
                &gateway.inner.tools,
                cx.context(),
                CODEMODE_EXECUTE_TOOL_NAME,
                json!({ "code": "return { answer: 42 as number }" }),
            )
            .await
            .expect("execute result");

        let structured = result.structured_content.expect("structured execute result");
        assert_eq!(structured["status"], "completed");
        assert_eq!(structured["result"], json!({ "answer": 42 }));
        assert_eq!(structured["toolCalls"], json!([]));
        assert_eq!(result.is_error, Some(false));
    }

    #[cfg(not(feature = "codemode"))]
    #[test]
    fn enabled_codemode_requires_the_build_feature() {
        assert!(matches!(McpGateway::try_from(config_with_codemode()), Err(ToolBuilderError::CodeModeFeatureDisabled)));
    }

    fn config_with_tools_mode() -> McpGatewayConfig {
        McpGatewayConfig {
            server_info: McpServerInfo { name: "test".into(), version: "1".into() },
            tools: Vec::new(),
            open_api_tool_sources: Vec::new(),
            dynamic_mcp_servers: Vec::new(),
            tds: None,
            search: McpToolSearch::default(),
            mode: McpGatewayMode::Tools(McpToolsMode),
            upstream_limits: UpstreamLimits::default(),
        }
    }

    fn actual_tool(name: &str, disclosure_mode: DisclosureMode) -> McpTool {
        McpTool {
            namespace: "test".into(),
            name: name.into(),
            description: format!("{name} description").into(),
            input_schema: serde_json::Map::new(),
            output_schema: serde_json::Map::new(),
            backend: UpstreamBackend::Rest {
                method: Method::GET,
                path: format!("/{name}").into(),
                query_params: Vec::new(),
                cluster: "test".into(),
                upstream_policy: Box::new(HttpUpstreamPolicy::default()),
                body_template: None,
                body_json: None,
            },
            rbac: None,
            embedding: EmbeddingVector::default(),
            disclosure_mode,
        }
    }

    #[tokio::test]
    async fn tools_mode_listing_is_controlled_only_by_disclosure() {
        let cases = [
            (vec![actual_tool("progressive_a", DisclosureMode::Progressive)], vec![TOOL_SEARCH_TOOL_NAME]),
            (
                vec![
                    actual_tool("progressive", DisclosureMode::Progressive),
                    actual_tool("eager", DisclosureMode::Eager),
                ],
                vec!["test.eager", TOOL_SEARCH_TOOL_NAME],
            ),
            (
                vec![actual_tool("eager_a", DisclosureMode::Eager), actual_tool("eager_b", DisclosureMode::Eager)],
                vec!["test.eager_a", "test.eager_b"],
            ),
        ];

        for (tools, expected) in cases {
            let mut config = config_with_tools_mode();
            config.tools = tools;
            let gateway = McpGateway::try_from(config).expect("tools-mode gateway");
            let mode = match &gateway.inner.mode {
                GatewayMode::Tools(mode) => mode,
                #[cfg(feature = "codemode")]
                GatewayMode::Code(_) => panic!("tools mode"),
            };
            let listed = mode
                .list_tools(&gateway.inner.tools, &http::Extensions::new(), &RequestCtx::default())
                .await
                .expect("tools/list");
            let names = listed.tools.iter().map(|tool| tool.name.as_ref()).collect::<Vec<_>>();
            assert_eq!(names, expected);
        }
    }

    #[tokio::test]
    async fn tools_mode_does_not_accept_execute() {
        let gateway = McpGateway::try_from(config_with_tools_mode()).expect("tools-mode gateway");
        let mode = match &gateway.inner.mode {
            GatewayMode::Tools(mode) => mode,
            #[cfg(feature = "codemode")]
            GatewayMode::Code(_) => panic!("tools mode"),
        };
        let cx = TestCallContext::default();
        let result = mode
            .call_tool(&gateway.inner.tools, cx.context(), CODEMODE_EXECUTE_TOOL_NAME, json!({ "code": "42" }))
            .await;
        assert!(matches!(result, Err(CallToolError::ToolNotFound(name)) if name == CODEMODE_EXECUTE_TOOL_NAME));
    }
}
