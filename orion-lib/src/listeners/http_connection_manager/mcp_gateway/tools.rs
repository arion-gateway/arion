use crate::listeners::http_connection_manager::mcp_gateway::{
    embeddings::{self, Bm25Document},
    mcp::{MessageResult, Session},
    rbac::{
        Action as RbacAction, JwtClaimField, JwtHeaderField, JwtHeaderMatcher, JwtPayloadMatcher,
        Permission as RbacPermission, ToolRbac,
    },
    transcoder::{
        rest::{BODY_TEMPLATE_NAME, DEFAULT_USER_AGENT, PATH_TEMPLATE_NAME},
        FunctionGraphTranscoder, RestTranscoder, Transcoder, TranscoderType,
    },
};
use crate::with_metric;
use atomic_time::AtomicInstant;
use dashmap::DashMap;
use http::{header::InvalidHeaderValue, HeaderValue};
use jsonschema::Validator;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    ClusterHeader, DynamicMcpServer, EmbeddingVector, McpBackendTransportUpstream, McpSemanticSearch, McpTool,
    UpstreamBackend,
};
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResult, ClientCapabilities, ClientInfo, Content, Implementation,
        InitializeRequestParams, JsonRpcNotification, RawContent, RawTextContent, ServerNotification,
        ToolListChangedNotification,
    },
    service::ClientInitializeError,
    transport::StreamableHttpClientTransport,
    ServiceError, ServiceExt,
};
#[cfg(feature = "metrics")]
use {crate::get_shard_id, opentelemetry::KeyValue};

use rmcp::model;
use rmcp::model::{ListToolsResult, Tool};
use rmcp::service::{RoleClient, RunningService};
use scopeguard::defer;
use serde_json::{json, Value};
use smol_str::SmolStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;
use tracing::{debug, info, warn};
use upon::Engine;

const DYNAMIC_TOOL_SEPARATOR: &str = "__";
const INDEFINITE_CACHE_LIFETIME: Duration = Duration::from_secs(100 * 365 * 24 * 3600);

const SEMANTIC_SEARCH_TOOL_NAME: &str = "semantic_search";
static SEMANTIC_SEARCH_TOOL_INPUT_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "type": "object",
        "properties": {
            "user_query": { "type": "string", "description": "The prompt or summary text." }
        },
        "required": ["user_query"],
    })
});

#[derive(Debug)]
pub struct ToolsRegistry {
    tools: DashMap<SmolStr, Arc<ToolEntry>, ahash::RandomState>,
    dynamic_mcp_servers: DashMap<SmolStr, Arc<DynamicMcpServerEntry>, ahash::RandomState>,
    semantic_search: Option<McpSemanticSearch>,
    bootstrapped: OnceCell<()>,
    embeddings_client: Option<Arc<embeddings::EmbeddingsClient>>,
    bm25_enabled: bool,
}

#[derive(Debug, Clone)]
pub enum ToolSource {
    Provided,
    Dynamic { server_name: SmolStr, upstream_tool_name: SmolStr },
}

#[derive(Debug)]
pub struct ToolEntry {
    pub conf: McpTool,
    pub source: ToolSource,
    pub transcoder: TranscoderType,
    pub input_schema_validator: Option<Validator>,
    pub output_schema_validator: Option<Validator>,
    pub rbac: Option<ToolRbac>,
    pub bm25_doc: Option<Bm25Document>,
    pub embedding: arc_swap::ArcSwapOption<Vec<f32>>,
}

pub struct DynamicMcpServerEntry {
    pub conf: DynamicMcpServer,
    pub rbac: Option<ToolRbac>,
    pub expires_at: AtomicInstant,
    refresh_in_progress: AtomicBool,
}

impl std::fmt::Debug for DynamicMcpServerEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicMcpServerEntry")
            .field("conf", &self.conf)
            .field("rbac", &self.rbac)
            .field("expires_at", &self.expires_at.load(Ordering::Relaxed))
            .field("refresh_in_progress", &self.refresh_in_progress.load(Ordering::Relaxed))
            .finish()
    }
}

impl DynamicMcpServerEntry {
    fn new(conf: DynamicMcpServer) -> Self {
        let rbac = conf.rbac.as_ref().map(convert_config_rbac_to_runtime);
        Self { conf, rbac, expires_at: AtomicInstant::new(Instant::now()), refresh_in_progress: AtomicBool::new(false) }
    }

    #[inline]
    fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at.load(Ordering::Relaxed)
    }

    fn bump_deadline(&self) {
        let lifetime = self.conf.cache_duration.unwrap_or(INDEFINITE_CACHE_LIFETIME);
        let deadline = Instant::now().checked_add(lifetime).unwrap_or_else(|| Instant::now() + Duration::from_secs(60));
        self.expires_at.store(deadline, Ordering::Relaxed);
    }
}

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum CallToolError {
    #[error("'name' parameter is missing or not a string")]
    NameNotString,
    #[error("tool '{0}' not found in registry")]
    ToolNotFound(String),
    #[error("access to tool '{0}' denied by RBAC policy")]
    RbacDenied(String),
    #[error("FunctionGraph transcoding is not yet implemented")]
    FunctionGraphNotImplemented,
    #[error("HeaderValue: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("Transcoder: tool: {tool} reason: {reason}")]
    TranscoderError { tool: String, reason: String },
    #[error("Client initialization error: {0}")]
    ClientInitializeError(#[from] ClientInitializeError),
    #[error("ServiceError: {0}")]
    ServiceError(#[from] ServiceError),
    #[error("SerdeError: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Validation error: {0}")]
    ValidationError(String),
    #[error("semantic search unavailable: embeddings failed and BM25 fallback is disabled")]
    SemanticSearchUnavailable,
}

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum ListToolsError {
    #[error("Unsupported transport")]
    UnsupportedTransport,
    #[error("Client: {0}")]
    ClientError(#[from] ClientInitializeError),
    #[error("ServiceError: {0}")]
    ServiceError(#[from] ServiceError),
}

#[derive(Debug, thiserror::Error)]
pub enum ToolBuilderError {
    #[error("Invalid input schema")]
    InvalidInputSchema(String),
    #[error("Invalid output schema")]
    InvalidOutputSchema(String),
    #[error("Failed compiling template: {0}")]
    FailedCompilingTemplate(#[from] upon::Error),
    #[error("Duplicate tool name: {0}")]
    DuplicateTool(SmolStr),
    #[error("Duplicate dynamic MCP server name: {0}")]
    DuplicateDynamicServer(SmolStr),
    #[error("MCP server name '{0}' already registered on this runtime")]
    DuplicateServerName(SmolStr),
    #[error(
        "Tool '{tool}' has supplied embedding of dimension {got}, but embeddings service produces dimension {expected}"
    )]
    EmbeddingDimensionMismatch { tool: SmolStr, expected: usize, got: usize },
    #[error("Tool '{0}' has a supplied embedding with non-finite components")]
    EmbeddingNotFinite(SmolStr),
    #[error("Invalid embeddings config: {0}")]
    InvalidEmbeddingsConfig(String),
}

impl ToolEntry {
    #[allow(clippy::result_large_err)]
    fn validate_json_against_schema(
        validator: Option<&jsonschema::Validator>,
        arguments: &Value,
    ) -> Result<(), CallToolError> {
        if let Some(validator) = validator {
            if let Some(err) = validator.iter_errors(arguments).next() {
                return Err(CallToolError::ValidationError(err.to_string()));
            }
        }
        Ok(())
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn validate_against_input_schema(&self, arguments: &Value) -> Result<(), CallToolError> {
        Self::validate_json_against_schema(self.input_schema_validator.as_ref(), arguments)
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn validate_against_output_schema(&self, arguments: &Value) -> Result<(), CallToolError> {
        Self::validate_json_against_schema(self.output_schema_validator.as_ref(), arguments)
    }
}

impl ToolsRegistry {
    pub fn with_config(
        tools: Vec<McpTool>,
        dynamic_mcp_servers: Vec<DynamicMcpServer>,
        semantic_search: Option<McpSemanticSearch>,
        embeddings_client: Option<Arc<embeddings::EmbeddingsClient>>,
    ) -> Result<Self, ToolBuilderError> {
        let bm25_enabled =
            semantic_search.as_ref().is_some_and(|ss| ss.embeddings.as_ref().is_none_or(|e| e.allow_bm25_fallback));
        let registry = Self {
            tools: DashMap::with_hasher(ahash::RandomState::new()),
            dynamic_mcp_servers: DashMap::with_hasher(ahash::RandomState::new()),
            semantic_search,
            bootstrapped: OnceCell::new(),
            embeddings_client,
            bm25_enabled,
        };

        for tool_conf in tools {
            registry.validate_supplied_embedding(&tool_conf)?;

            let entry = build_tool_entry(tool_conf, ToolSource::Provided, registry.bm25_enabled)?;
            let name = entry.conf.name.clone();
            if registry.tools.insert(name.clone(), Arc::new(entry)).is_some() {
                return Err(ToolBuilderError::DuplicateTool(name));
            }
        }

        for server in dynamic_mcp_servers {
            let name = server.name.clone();
            let entry = Arc::new(DynamicMcpServerEntry::new(server));
            if registry.dynamic_mcp_servers.insert(name.clone(), entry).is_some() {
                return Err(ToolBuilderError::DuplicateDynamicServer(name));
            }
        }

        Ok(registry)
    }

    fn validate_supplied_embedding(&self, tool: &McpTool) -> Result<(), ToolBuilderError> {
        let supplied = tool.embedding.as_slice();
        if supplied.is_empty() {
            return Ok(());
        }
        if !supplied.iter().all(|x| x.is_finite()) {
            return Err(ToolBuilderError::EmbeddingNotFinite(tool.name.clone()));
        }
        let Some(client) = self.embeddings_client.as_ref() else {
            return Ok(());
        };
        let Some(dimensions) = client.dimensions() else {
            return Ok(());
        };
        if supplied.len() != dimensions {
            return Err(ToolBuilderError::EmbeddingDimensionMismatch {
                tool: tool.name.clone(),
                expected: dimensions,
                got: supplied.len(),
            });
        }
        Ok(())
    }

    /// Top-K parameter from the configured similarity block, or the default
    /// (10) when no `semantic_search_tool` is configured.
    fn similarity_top_k(&self) -> usize {
        self.semantic_search.as_ref().map(|s| s.similarity.top_k).unwrap_or(10)
    }

    pub async fn bootstrap(&self) -> Result<(), CallToolError> {
        self.bootstrapped
            .get_or_init(|| async {
                for server in self.dynamic_mcp_servers.iter() {
                    self.fetch_and_materialise(server.value()).await;
                }
            })
            .await;
        self.embed_unembedded_tools(None).await;
        Ok(())
    }

    async fn ensure_tools_current(&self) -> Result<(), CallToolError> {
        self.bootstrap().await?;
        self.refresh_expired_dynamic_servers().await;
        self.embed_unembedded_tools(None).await;
        Ok(())
    }

    async fn embed_unembedded_tools(&self, restrict_to: Option<&[SmolStr]>) {
        let Some(client) = self.embeddings_client.as_ref() else {
            return;
        };

        let mut names: Vec<SmolStr> = Vec::new();
        let mut texts: Vec<String> = Vec::new();
        for shard in self.tools.iter() {
            let entry = shard.value();
            if let Some(allowed) = restrict_to {
                if !allowed.contains(&entry.conf.name) {
                    continue;
                }
            }
            if entry.embedding.load_full().is_some() {
                continue;
            }
            names.push(entry.conf.name.clone());
            texts.push(embeddings::build_search_text(
                &entry.conf.name,
                &entry.conf.description,
                &entry.conf.input_schema,
            ));
        }

        if texts.is_empty() {
            return;
        }

        match client.embed_batch(&texts).await {
            Ok(vectors) => {
                for (name, vec) in names.iter().zip(vectors) {
                    if let Some(shard) = self.tools.get(name) {
                        shard.value().embedding.store(Some(vec));
                    }
                }
                debug!(target: "mcp_gateway", "Embedded {} tools via {}", names.len(), client.description());
            },
            Err(err) => {
                if self.bm25_enabled {
                    warn!(
                        target: "mcp_gateway",
                        "embed_batch failed for {} tools: {err}; semantic_search will use BM25 fallback",
                        names.len()
                    );
                } else {
                    warn!(
                        target: "mcp_gateway",
                        "embed_batch failed for {} tools: {err}; semantic_search will fail until embeddings are available",
                        names.len()
                    );
                }
                #[cfg(feature = "metrics")]
                let shard_id = get_shard_id!();
                with_metric!(
                    orion_metrics::metrics::mcp::EMBEDDING_FAILURES_TOTAL,
                    add,
                    1,
                    shard_id,
                    &[KeyValue::new("endpoint", client.description().to_owned())]
                );
            },
        }
    }

    pub async fn embed_tool_if_unembedded(&self, name: &SmolStr) {
        let names = [name.clone()];
        self.embed_unembedded_tools(Some(&names)).await;
    }

    #[allow(clippy::unused_async)]
    pub async fn add_tool(&self, tool: McpTool) -> Result<(), ToolBuilderError> {
        let name = tool.name.clone();
        if let Some(existing) = self.tools.get(&name) {
            if !matches!(existing.source, ToolSource::Provided) {
                return Err(ToolBuilderError::DuplicateTool(name));
            }
        }
        self.validate_supplied_embedding(&tool)?;
        let entry = build_tool_entry(tool, ToolSource::Provided, self.bm25_enabled)?;
        if let Some(existing) = self.tools.get(&name) {
            if !matches!(existing.source, ToolSource::Provided) {
                return Err(ToolBuilderError::DuplicateTool(name));
            }
        }
        self.tools.insert(name, Arc::new(entry));
        Ok(())
    }

    pub fn remove_tool(&self, name: &str) -> bool {
        self.tools.remove_if(name, |_, v| matches!(v.source, ToolSource::Provided)).is_some()
    }

    pub async fn add_dynamic_server(&self, server: DynamicMcpServer) -> Result<(), ToolBuilderError> {
        let name = server.name.clone();
        let entry = Arc::new(DynamicMcpServerEntry::new(server));
        if self.dynamic_mcp_servers.insert(name.clone(), Arc::clone(&entry)).is_some() {
            self.evict_dynamic_tools_for(&name);
        }
        self.fetch_and_materialise(&entry).await;
        Ok(())
    }

    pub fn remove_dynamic_server(&self, name: &str) -> bool {
        if self.dynamic_mcp_servers.remove(name).is_none() {
            return false;
        }
        self.evict_dynamic_tools_for(name);
        true
    }

    #[inline]
    pub fn get_tool_by_name(&self, name: &str) -> Option<Arc<ToolEntry>> {
        self.tools.get(name).map(|r| Arc::clone(r.value()))
    }

    pub async fn build_list_tools(
        &self,
        req_ext: &http::Extensions,
        session: Option<&Arc<Session>>,
    ) -> Result<ListToolsResult, CallToolError> {
        self.ensure_tools_current().await?;

        let mut tools = Vec::with_capacity(self.tools.len() + 1);

        if self.semantic_search.is_some() {
            let Value::Object(semantic_search_input_schema) = &*SEMANTIC_SEARCH_TOOL_INPUT_SCHEMA else {
                unreachable!()
            };

            tools.push(Tool::new(
                SEMANTIC_SEARCH_TOOL_NAME,
                "Pass the user prompt or a summary to discover relevant tools.",
                Arc::new(semantic_search_input_schema.clone()),
            ));
        }

        // Handle different semantic search modes:
        // - No semantic search: always fill all tools
        // - Assisted discovery mode: only fill tools after prompt is set (notification flow)
        // - Direct call mode: always fill all tools (client calls semantic search tool directly)
        let should_fill_tools = self.semantic_search.as_ref().map_or(true, |ss| {
            if ss.enable_assisted_discovery {
                session.as_ref().is_some_and(|s| s.prompt.lock().is_some())
            } else {
                true
            }
        });

        if should_fill_tools {
            self.fill_list_tools(req_ext, session, &mut tools);
        }

        Ok(ListToolsResult { tools, next_cursor: None, meta: None })
    }

    fn fill_list_tools(&self, req_ext: &http::Extensions, session: Option<&Arc<Session>>, tools: &mut Vec<Tool>) {
        let session = session.as_ref();

        let restrict_to_active = self.semantic_search.is_some() && session.is_some_and(|s| !s.active_tools.is_empty());

        for entry in self.tools.iter() {
            let entry = entry.value();
            if !entry.rbac.as_ref().is_none_or(|rbac| rbac.is_permitted(req_ext)) {
                continue;
            }
            if restrict_to_active {
                let session = session.expect("restrict_to_active implies session");
                if !session.active_tools.contains(&entry.conf.name) {
                    continue;
                }
            }
            tools.push(tool_from_entry(entry));
        }
    }

    async fn refresh_expired_dynamic_servers(&self) {
        let now = Instant::now();
        let expired: Vec<Arc<DynamicMcpServerEntry>> = self
            .dynamic_mcp_servers
            .iter()
            .filter(|e| e.value().is_expired(now))
            .map(|e| Arc::clone(e.value()))
            .collect();

        for entry in expired {
            self.refresh_if_idle(entry).await;
        }
    }

    async fn refresh_if_idle(&self, server: Arc<DynamicMcpServerEntry>) {
        if server.refresh_in_progress.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            debug!(target: "mcp_gateway", "Refresh already in progress for dynamic MCP server '{}'", server.conf.name);
            return;
        }
        defer! {
            server.refresh_in_progress.store(false, Ordering::Release);
        }

        if !server.is_expired(Instant::now()) {
            return;
        }

        self.fetch_and_materialise(&server).await;
    }

    async fn fetch_and_materialise(&self, server: &DynamicMcpServerEntry) {
        let server_name = &server.conf.name;
        match Self::fetch_dynamic_server_tools(server, self.bm25_enabled).await {
            Ok(materialised) => {
                self.evict_dynamic_tools_for(server_name);
                for entry in materialised {
                    self.tools.insert(entry.conf.name.clone(), Arc::new(entry));
                }
                server.bump_deadline();
                debug!(target: "mcp_gateway", "Refreshed dynamic MCP server '{}'", server_name);
            },
            Err(err) => {
                warn!(target: "mcp_gateway", "Failed to refresh dynamic MCP server '{}': {}", server_name, err);
            },
        }
    }

    fn evict_dynamic_tools_for(&self, server_name: &str) {
        self.tools.retain(|_, entry| match &entry.source {
            ToolSource::Dynamic { server_name: owner, .. } => owner.as_str() != server_name,
            ToolSource::Provided => true,
        });
    }

    async fn fetch_dynamic_server_tools(
        server: &DynamicMcpServerEntry,
        build_bm25_doc: bool,
    ) -> Result<Vec<ToolEntry>, ListToolsError> {
        let server_name = server.conf.name.clone();
        let upstream_tools = Self::list_upstream_tools(&server.conf.transport, &server.conf.url).await?;

        let entries = upstream_tools
            .into_iter()
            .map(|tool| {
                let upstream_tool_name: SmolStr = tool.name.as_ref().into();
                let exposed_name: SmolStr = format!("{server_name}{DYNAMIC_TOOL_SEPARATOR}{upstream_tool_name}").into();
                let description = tool.description.as_deref().map(ToString::to_string).unwrap_or_default();
                let input_schema = tool.input_schema.as_ref().clone();
                let conf = McpTool {
                    name: exposed_name,
                    description,
                    input_schema,
                    output_schema: serde_json::Map::new(),
                    backend: UpstreamBackend::McpServer {
                        transport: server.conf.transport.clone(),
                        url: server.conf.url.clone(),
                    },
                    rbac: server.conf.rbac.clone(),
                    embedding: EmbeddingVector::default(),
                };
                let bm25_doc = build_bm25_doc
                    .then(|| Bm25Document::from_tool_parts(&conf.name, &conf.description, &conf.input_schema));
                ToolEntry {
                    conf,
                    source: ToolSource::Dynamic { server_name: server_name.clone(), upstream_tool_name },
                    transcoder: TranscoderType::NoTranscoder,
                    input_schema_validator: None,
                    output_schema_validator: None,
                    rbac: server.rbac.clone(),
                    bm25_doc,
                    embedding: arc_swap::ArcSwapOption::default(),
                }
            })
            .collect();

        Ok(entries)
    }

    async fn list_upstream_tools(
        transport: &McpBackendTransportUpstream,
        url: &str,
    ) -> Result<Vec<Tool>, ListToolsError> {
        debug!(target: "mcp_gateway", "Listing tools from upstream with transport {transport:?} at {url}");
        match transport {
            McpBackendTransportUpstream::StreamableHttp => {
                let client = Self::get_mcp_client(url).await?;
                let tools = client.list_tools(Option::default()).await?;
                Ok(tools.tools)
            },
            McpBackendTransportUpstream::Sse => Err(ListToolsError::UnsupportedTransport),
        }
    }

    async fn get_mcp_client(
        url: &str,
    ) -> Result<RunningService<RoleClient, InitializeRequestParams>, ClientInitializeError> {
        debug!(target: "mcp_gateway", "Creating MCP client for URL: {url}...");
        let transport = StreamableHttpClientTransport::from_uri(url);
        let client_info =
            ClientInfo::new(ClientCapabilities::default(), Implementation::new(DEFAULT_USER_AGENT, "0.1.0"));
        client_info.serve(transport).await.inspect_err(|e| {
            info!(target: "mcp_gateway", "get_mcp_client error: {}!", e);
        })
    }

    pub async fn call_semantic_search_tool(
        &self,
        req_ext: &http::Extensions,
        rpc: &model::JsonRpcRequest,
        session: &Arc<Session>,
    ) -> Result<MessageResult, CallToolError> {
        let arguments = match &rpc.request.params.get("arguments") {
            Some(serde_json::Value::Object(o)) => o.clone(),
            _ => {
                return Err(CallToolError::ValidationError("call_semantic_search_tool: missing arguments".into()));
            },
        };

        let Some(Value::String(prompt)) = arguments.get("user_query") else {
            return Err(CallToolError::ValidationError(
                "call_semantic_search_tool: invalid user_query argument".into(),
            ));
        };

        let Some(semantic_search) = &self.semantic_search else {
            return Err(CallToolError::ValidationError("Semantic search not configured".into()));
        };

        self.ensure_tools_current().await?;

        let ranked = self.rank_tools_for_query(req_ext, prompt).await?;
        session.active_tools.clear();
        for entry in &ranked {
            session.active_tools.insert(entry.conf.name.clone());
        }
        {
            let mut session_prompt = session.prompt.lock();
            *session_prompt = Some(prompt.clone());
        }

        if semantic_search.enable_assisted_discovery {
            // Assisted discovery mode: send notification to trigger client re-list. The follow-up
            // tools/list will read `active_tools` via `fill_list_tools` and emit those names.
            let json_rpc_notification = JsonRpcNotification::<ServerNotification> {
                jsonrpc: model::JsonRpcVersion2_0,
                notification: ServerNotification::ToolListChangedNotification(ToolListChangedNotification::default()),
            };

            let text_content = RawTextContent {
                text: "Context acquired. Relevant APIs loaded. The tool list has been updated, please proceed with the new tools.".to_owned(),
                meta: None
            };

            let success_message = Content { raw: RawContent::Text(text_content), annotations: None };
            let json_result = serde_json::to_value(CallToolResult::success(vec![success_message]))?;

            let json_rpc_response =
                model::JsonRpcResponse { jsonrpc: model::JsonRpcVersion2_0, id: rpc.id.clone(), result: json_result };

            Ok(MessageResult::JsonRpcNotificationResponse(json_rpc_notification, json_rpc_response))
        } else {
            // Direct call mode: serialise the ranked tools straight into the call result so the
            // client receives the discovered tools without a follow-up tools/list round-trip.
            let tools: Vec<Tool> = ranked.iter().map(|e| tool_from_entry(e)).collect();
            let tools_json = serde_json::to_value(&tools).unwrap_or_else(|_| serde_json::Value::Array(vec![]));

            let text_content = RawTextContent {
                text: serde_json::to_string_pretty(&tools_json).unwrap_or_else(|_| "[]".to_string()),
                meta: None,
            };

            let success_message = Content { raw: RawContent::Text(text_content), annotations: None };
            let json_result = serde_json::to_value(CallToolResult::success(vec![success_message]))?;

            let json_rpc_response =
                model::JsonRpcResponse { jsonrpc: model::JsonRpcVersion2_0, id: rpc.id.clone(), result: json_result };

            Ok(MessageResult::JsonRpcResponse(json_rpc_response))
        }
    }

    /// Score every RBAC-permitted tool against the user query and return the top-K
    /// entries by descending similarity. Cosine ranking is used only when the
    /// remote embeddings client can embed the query and every candidate has a vector;
    /// otherwise the whole candidate set is scored with BM25, unless fallback is disabled.
    async fn rank_tools_for_query(
        &self,
        req_ext: &http::Extensions,
        user_query: &str,
    ) -> Result<Vec<Arc<ToolEntry>>, CallToolError> {
        let mut candidates: Vec<Arc<ToolEntry>> = Vec::with_capacity(self.tools.len());
        for entry in self.tools.iter() {
            let entry = Arc::clone(entry.value());
            if !entry.rbac.as_ref().map_or(true, |r| r.is_permitted(req_ext)) {
                continue;
            }
            candidates.push(entry);
        }

        let mut scored = match self.vector_scores(user_query, &candidates).await {
            Some(scored) => scored,
            None if self.bm25_enabled => self.bm25_scores(user_query, &candidates),
            None => return Err(CallToolError::SemanticSearchUnavailable),
        };

        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.conf.name.cmp(&b.1.conf.name)));

        let top_k = self.similarity_top_k();
        if top_k > 0 && scored.len() > top_k {
            scored.truncate(top_k);
        }
        Ok(scored.into_iter().map(|(_, e)| e).collect())
    }

    async fn vector_scores(
        &self,
        user_query: &str,
        candidates: &[Arc<ToolEntry>],
    ) -> Option<Vec<(f32, Arc<ToolEntry>)>> {
        let client = self.embeddings_client.as_ref()?;
        let query_embedding = match client.embed_query(user_query).await {
            Ok(v) => v,
            Err(err) => {
                warn!(target: "mcp_gateway", "embed_query failed: {err}; cosine ranking unavailable");
                #[cfg(feature = "metrics")]
                let shard_id = get_shard_id!();
                with_metric!(
                    orion_metrics::metrics::mcp::EMBEDDING_FAILURES_TOTAL,
                    add,
                    1,
                    shard_id,
                    &[KeyValue::new("endpoint", client.description().to_owned())]
                );
                return None;
            },
        };

        let mut scored = Vec::with_capacity(candidates.len());
        for entry in candidates {
            let Some(tool_embedding) = entry.embedding.load_full() else {
                debug!(
                    target: "mcp_gateway",
                    "tool '{}' has no embedding yet; excluded from cosine ranking",
                    entry.conf.name
                );
                continue;
            };
            let score = match embeddings::cosine_similarity(query_embedding.as_slice(), tool_embedding.as_slice()) {
                Ok(score) => score,
                Err(err) => {
                    warn!(
                        target: "mcp_gateway",
                        "cosine similarity failed for tool '{}': {err}; cosine ranking unavailable",
                        entry.conf.name
                    );
                    return None;
                },
            };
            scored.push((score, Arc::clone(entry)));
        }
        Some(scored)
    }

    fn bm25_scores(&self, user_query: &str, candidates: &[Arc<ToolEntry>]) -> Vec<(f32, Arc<ToolEntry>)> {
        static EMPTY_BM25_DOC: LazyLock<Bm25Document> = LazyLock::new(|| Bm25Document::from_text(""));

        let documents: Vec<&Bm25Document> =
            candidates.iter().map(|entry| entry.bm25_doc.as_ref().unwrap_or(&EMPTY_BM25_DOC)).collect();
        embeddings::bm25_scores(user_query, &documents)
            .into_iter()
            .zip(candidates.iter())
            .map(|(score, entry)| (score, Arc::clone(entry)))
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn call(
        &self,
        req_ext: &http::Extensions,
        req_headers: &http::HeaderMap,
        rpc: &model::JsonRpcRequest,
        cluster_header: Option<&ClusterHeader>,
        session: &Arc<Session>,
    ) -> Result<MessageResult, CallToolError> {
        let name = rpc.request.params.get("name").and_then(Value::as_str).ok_or(CallToolError::NameNotString)?;
        debug!(target: "mcp_gateway", "call: method:{} tool '{name}'", rpc.request.method);

        if name == SEMANTIC_SEARCH_TOOL_NAME {
            if self.semantic_search.is_some() {
                return self.call_semantic_search_tool(req_ext, rpc, session).await;
            }
            // todo(francesco) we should send back an error if agent is invoking semantic_search tool and none is configured
        }

        let filter_by_active = self.semantic_search.as_ref().is_some_and(|ss| ss.enable_assisted_discovery);
        let entry = self
            .get_tool_by_name(name)
            .filter(|_| !filter_by_active || session.active_tools.contains(name))
            .ok_or_else(|| CallToolError::ToolNotFound(name.to_string()))?;

        if let Some(rbac) = &entry.rbac {
            if !rbac.is_permitted(req_ext) {
                return Err(CallToolError::RbacDenied(name.to_string()));
            }
        }

        if entry.input_schema_validator.is_some() {
            let arguments = rpc.request.params.get("arguments").cloned().unwrap_or(Value::Null);
            entry.validate_against_input_schema(&arguments)?;
        }

        match (&entry.conf.backend, &entry.transcoder) {
            (UpstreamBackend::Rest { cluster, r#async, .. }, TranscoderType::Rest(transcoder)) => {
                let mut upstream_request = transcoder
                    .encode(req_headers, &rpc.request)
                    .map_err(|e| CallToolError::TranscoderError { tool: name.to_owned(), reason: e.to_string() })?;
                if let Some(cluster_header) = cluster_header {
                    let headers = upstream_request.headers_mut();
                    headers.append(cluster_header.0.clone(), HeaderValue::from_str(cluster)?);
                }
                Ok(MessageResult::UpstreamRequest((upstream_request, *r#async, Arc::clone(&entry))))
            },
            (UpstreamBackend::McpServer { url, .. }, TranscoderType::NoTranscoder) => {
                // For dynamic tools the upstream expects the original tool
                // name (pre-namespacing). For static McpServer-backed tools
                // the exposed name IS the upstream name.
                let upstream_tool_name = match &entry.source {
                    ToolSource::Dynamic { upstream_tool_name, .. } => upstream_tool_name.clone(),
                    ToolSource::Provided => entry.conf.name.clone(),
                };

                let client = match session.mcp_upstreams.entry(url.to_owned()) {
                    dashmap::Entry::Occupied(entry) => entry.into_ref(),
                    dashmap::Entry::Vacant(vacant_entry) => {
                        let new_client = Self::get_mcp_client(url).await?;
                        vacant_entry.insert(new_client)
                    },
                };

                let call_params = match &rpc.request.params.get("arguments") {
                    Some(&serde_json::Value::Object(ref args)) => {
                        CallToolRequestParams::new(upstream_tool_name.to_string()).with_arguments(args.clone())
                    },
                    _ => CallToolRequestParams::new(upstream_tool_name.to_string()),
                };

                let tool_result = match client.call_tool(call_params).await {
                    Ok(res) => res,
                    Err(err) => {
                        if matches!(&err, ServiceError::TransportSend(_) | ServiceError::TransportClosed) {
                            // drop reference before removing the map entry to avoid deadlock
                            info!(target: "mcp_gateway", "removing MCP client for URL {url} due to transport error!");
                            drop(client);
                            session.mcp_upstreams.remove(url);
                        }
                        return Err(err.into());
                    },
                };

                debug!(target: "mcp_gateway", "Received result from tool {name}@{upstream_tool_name}: {:?}", tool_result);

                let json_result = serde_json::to_value(tool_result)?;

                let json_rcp_response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: rpc.id.clone(),
                    result: json_result,
                };

                Ok(MessageResult::JsonRpcResponse(json_rcp_response))
            },
            (UpstreamBackend::FunctionGraph, TranscoderType::FunctionGraph(_)) => {
                Err(CallToolError::FunctionGraphNotImplemented)
            },
            _ => unreachable!(),
        }
    }
}

fn build_tool_entry(
    tool_conf: McpTool,
    source: ToolSource,
    build_bm25_doc: bool,
) -> Result<ToolEntry, ToolBuilderError> {
    let rbac = tool_conf.rbac.as_ref().map(convert_config_rbac_to_runtime);
    let input_schema_validator = if tool_conf.input_schema.is_empty() {
        None
    } else {
        Some(
            Validator::new(&Value::Object(tool_conf.input_schema.clone()))
                .map_err(|e| ToolBuilderError::InvalidInputSchema(e.to_string()))?,
        )
    };
    let output_schema_validator = if tool_conf.output_schema.is_empty() {
        None
    } else {
        Some(
            Validator::new(&Value::Object(tool_conf.output_schema.clone()))
                .map_err(|e| ToolBuilderError::InvalidOutputSchema(e.to_string()))?,
        )
    };
    let transcoder = match &tool_conf.backend {
        UpstreamBackend::Rest { method, path, query_params, body_template, .. } => {
            let mut template_engine: Engine<'static> = upon::Engine::new();
            template_engine.add_template(PATH_TEMPLATE_NAME, path.clone())?;
            if let Some(body_template) = body_template {
                template_engine.add_template(BODY_TEMPLATE_NAME, body_template.clone())?;
            }
            TranscoderType::Rest(RestTranscoder {
                method: method.clone(),
                query_params: query_params.clone(),
                has_body_template: body_template.is_some(),
                template_engine,
            })
        },
        UpstreamBackend::FunctionGraph => TranscoderType::FunctionGraph(FunctionGraphTranscoder {}),
        UpstreamBackend::McpServer { .. } => TranscoderType::NoTranscoder,
    };
    let embedding = {
        let supplied = tool_conf.embedding.as_slice();
        if supplied.is_empty() {
            arc_swap::ArcSwapOption::default()
        } else {
            let mut v: Vec<f32> = supplied.to_vec();
            embeddings::normalise_in_place(&mut v);
            arc_swap::ArcSwapOption::from(Some(Arc::new(v)))
        }
    };
    let bm25_doc = build_bm25_doc
        .then(|| Bm25Document::from_tool_parts(&tool_conf.name, &tool_conf.description, &tool_conf.input_schema));
    Ok(ToolEntry {
        conf: tool_conf,
        source,
        transcoder,
        rbac,
        input_schema_validator,
        output_schema_validator,
        bm25_doc,
        embedding,
    })
}

fn tool_from_entry(entry: &ToolEntry) -> Tool {
    let tool = Tool::new(
        entry.conf.name.to_string(),
        entry.conf.description.clone(),
        Arc::new(entry.conf.input_schema.clone()),
    );
    if !entry.conf.output_schema.is_empty() {
        tool.with_raw_output_schema(Arc::new(entry.conf.output_schema.clone()))
    } else {
        tool
    }
}

use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpRbacPermission;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpToolRbac;
use orion_configuration::config::network_filters::network_rbac::Action;

/// Convert configuration RBAC to runtime RBAC
fn convert_config_rbac_to_runtime(config_rbac: &McpToolRbac) -> ToolRbac {
    let action = match config_rbac.action {
        Action::Allow => RbacAction::Allow,
        Action::Deny => RbacAction::Deny,
    };

    let permissions = config_rbac
        .permissions
        .iter()
        .map(|p| match p {
            McpRbacPermission::JwtHeader { field, value } => {
                #[allow(clippy::wildcard_in_or_patterns)]
                let header_field = match field.as_str() {
                    "alg" | "algorithm" => JwtHeaderField::Algorithm,
                    "typ" | "type" => JwtHeaderField::Type,
                    "cty" | "content_type" => JwtHeaderField::ContentType,
                    "jku" | "json_key_url" => JwtHeaderField::JsonKeyURL,
                    "jwk" | "json_web_key" => JwtHeaderField::JsonWebKey,
                    "x5u" | "x509_url" => JwtHeaderField::X509URL,
                    "x5c" | "x509_certificate_chain" => JwtHeaderField::X509CertificateChain,
                    "x5t" | "x509_certificate_sha1_thumbprint" => JwtHeaderField::X509CertificateSHA1Thumbprint,
                    "x5t#S256" | "x509_certificate_sha256_thumbprint" => {
                        JwtHeaderField::X509CertificateSHA256Thumbprint
                    },
                    "crit" | "critical" => JwtHeaderField::Critical,
                    "enc" | "encryption" => JwtHeaderField::Encryption,
                    "zip" => JwtHeaderField::Zip,
                    "url" => JwtHeaderField::Url,
                    "nonce" => JwtHeaderField::Nonce,
                    "kid" | "key_id" | _ => JwtHeaderField::KeyID,
                };
                RbacPermission::JwtHeader(JwtHeaderMatcher { field: header_field, value: value.clone() })
            },
            McpRbacPermission::JwtClaim { field, value } => {
                let claim_field = match field.as_str() {
                    "iss" | "issuer" => JwtClaimField::Issuer,
                    "sub" | "subject" => JwtClaimField::Subject,
                    "aud" | "audience" => JwtClaimField::Audience,
                    "exp" | "expiration" => JwtClaimField::Expiration,
                    "iat" | "issued_at" => JwtClaimField::IssuedAt,
                    "nbf" | "not_before" => JwtClaimField::NotBefore,
                    "jti" | "jwt_id" => JwtClaimField::JwtID,
                    custom => JwtClaimField::Extra(custom.into()),
                };
                RbacPermission::JwtClaim(JwtPayloadMatcher { field: claim_field, value: value.clone() })
            },
        })
        .collect();

    ToolRbac { action, permissions }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_tool_with_schemas(
        input_schema: serde_json::Map<String, Value>,
        output_schema: serde_json::Map<String, Value>,
    ) -> McpTool {
        McpTool {
            name: "test_tool".into(),
            description: "A test tool".into(),
            input_schema,
            output_schema,
            backend: UpstreamBackend::Rest {
                method: http::Method::GET,
                path: "/test".into(),
                query_params: vec![],
                cluster: "test_cluster".into(),
                r#async: false,
                body_template: None,
            },
            rbac: None,
            embedding: EmbeddingVector::default(),
        }
    }

    fn registry_with_tool(tool: McpTool) -> ToolsRegistry {
        ToolsRegistry::with_config(vec![tool], Vec::new(), None, None).unwrap()
    }

    #[test]
    fn test_input_schema_validation_passes_with_valid_arguments() {
        let input_schema = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "username": { "type": "string" },
                "age": { "type": "integer" }
            },
            "required": ["username"]
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
        let registry = registry_with_tool(tool);
        let tool_entry = registry.get_tool_by_name("test_tool").unwrap();

        let args = json!({
            "username": "john_doe",
            "age": 25
        });

        assert!(tool_entry.validate_against_input_schema(&args).is_ok());
    }

    #[test]
    fn test_input_schema_validation_fails_with_missing_required_field() {
        let input_schema = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "username": { "type": "string" }
            },
            "required": ["username"]
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
        let registry = registry_with_tool(tool);
        let tool_entry = registry.get_tool_by_name("test_tool").unwrap();

        let args = json!({});

        let result = tool_entry.validate_against_input_schema(&args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("username"), "Error should mention missing field: {err_msg}");
    }

    #[test]
    fn test_input_schema_validation_fails_with_wrong_type() {
        let input_schema = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "age": { "type": "integer" }
            }
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
        let registry = registry_with_tool(tool);
        let tool_entry = registry.get_tool_by_name("test_tool").unwrap();

        let args = json!({
            "age": "not a number"
        });

        let result = tool_entry.validate_against_input_schema(&args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("number") || err_msg.contains("integer"),
            "Error should mention type mismatch: {err_msg}"
        );
    }

    #[test]
    fn test_input_schema_validation_skips_when_empty() {
        let tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        let registry = registry_with_tool(tool);
        let tool_entry = registry.get_tool_by_name("test_tool").unwrap();

        let args = json!({
            "anything": "goes",
            "count": 123
        });

        assert!(tool_entry.validate_against_input_schema(&args).is_ok());
    }

    #[test]
    fn test_output_schema_validation_passes_with_valid_response() {
        let output_schema = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "temperature": { "type": "number" },
                "unit": { "type": "string" }
            },
            "required": ["temperature"]
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(serde_json::Map::new(), output_schema);
        let registry = registry_with_tool(tool);
        let tool_entry = registry.get_tool_by_name("test_tool").unwrap();

        let response = json!({
            "temperature": 72.5,
            "unit": "F"
        });

        assert!(tool_entry.validate_against_output_schema(&response).is_ok());
    }

    #[test]
    fn test_output_schema_validation_fails_with_invalid_response() {
        let output_schema = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "temperature": { "type": "number" }
            },
            "required": ["temperature"]
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(serde_json::Map::new(), output_schema);
        let registry = registry_with_tool(tool);
        let tool_entry = registry.get_tool_by_name("test_tool").unwrap();

        let response = json!({
            "temperature": "hot" // Should be a number
        });

        let result = tool_entry.validate_against_output_schema(&response);
        assert!(result.is_err());
    }

    #[test]
    fn test_output_schema_validation_skips_when_empty() {
        let tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        let registry = registry_with_tool(tool);
        let tool_entry = registry.get_tool_by_name("test_tool").unwrap();

        let response = json!({
            "arbitrary": "data",
            "nested": {
                "value": 123
            }
        });

        assert!(tool_entry.validate_against_output_schema(&response).is_ok());
    }

    #[test]
    fn test_with_config_fails_with_invalid_input_schema() {
        let input_schema = serde_json::from_value(json!({ "type": "invalid_type" })).unwrap();
        let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
        let result = ToolsRegistry::with_config(vec![tool], Vec::new(), None, None);
        assert!(matches!(result, Err(ToolBuilderError::InvalidInputSchema(_))));
    }

    #[test]
    fn test_with_config_fails_with_invalid_output_schema() {
        let output_schema = serde_json::from_value(json!({ "type": "invalid_type" })).unwrap();
        let tool = create_test_tool_with_schemas(serde_json::Map::new(), output_schema);
        let result = ToolsRegistry::with_config(vec![tool], Vec::new(), None, None);
        assert!(matches!(result, Err(ToolBuilderError::InvalidOutputSchema(_))));
    }

    #[test]
    fn test_nested_object_validation() {
        let input_schema = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "age": { "type": "integer" }
                    },
                    "required": ["name"]
                }
            }
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
        let registry = registry_with_tool(tool);
        let tool_entry = registry.get_tool_by_name("test_tool").unwrap();

        let valid_args = json!({
            "user": {
                "name": "Alice",
                "age": 30
            }
        });
        tool_entry.validate_against_input_schema(&valid_args).unwrap();

        let invalid_args = json!({
            "user": {
                "age": 30
            }
        });
        assert!(tool_entry.validate_against_input_schema(&invalid_args).is_err());
    }

    #[test]
    fn test_array_validation() {
        let input_schema = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            }
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
        let registry = registry_with_tool(tool);
        let tool_entry = registry.get_tool_by_name("test_tool").unwrap();

        let valid_args = json!({ "tags": ["rust", "mcp", "api"] });
        assert!(tool_entry.validate_against_input_schema(&valid_args).is_ok());

        let invalid_args = json!({ "tags": [1, 2, 3] });
        assert!(tool_entry.validate_against_input_schema(&invalid_args).is_err());
    }

    #[tokio::test]
    async fn test_add_and_remove_provided_tool() {
        let registry = ToolsRegistry::with_config(Vec::new(), Vec::new(), None, None).unwrap();
        let tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());

        registry.add_tool(tool).await.unwrap();
        assert!(registry.get_tool_by_name("test_tool").is_some());

        assert!(registry.remove_tool("test_tool"));
        assert!(registry.get_tool_by_name("test_tool").is_none());
        assert!(!registry.remove_tool("test_tool"));
    }

    #[test]
    fn test_remove_tool_skips_dynamic_entries() {
        let registry = ToolsRegistry::with_config(Vec::new(), Vec::new(), None, None).unwrap();

        let dynamic_conf = McpTool {
            name: "srv__tool".into(),
            description: String::new(),
            input_schema: serde_json::Map::new(),
            output_schema: serde_json::Map::new(),
            backend: UpstreamBackend::McpServer {
                transport: McpBackendTransportUpstream::StreamableHttp,
                url: "http://localhost/mcp".into(),
            },
            rbac: None,
            embedding: EmbeddingVector::default(),
        };
        let entry = build_tool_entry(
            dynamic_conf,
            ToolSource::Dynamic { server_name: "srv".into(), upstream_tool_name: "tool".into() },
            true,
        )
        .unwrap();
        registry.tools.insert("srv__tool".into(), Arc::new(entry));

        assert!(!registry.remove_tool("srv__tool"));
        assert!(registry.get_tool_by_name("srv__tool").is_some());
    }

    #[test]
    fn test_duplicate_provided_tool_names_rejected() {
        let tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        let tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        let result = ToolsRegistry::with_config(vec![tool_a, tool_b], Vec::new(), None, None);
        assert!(matches!(result, Err(ToolBuilderError::DuplicateTool(_))));
    }

    #[tokio::test]
    async fn test_remove_dynamic_server_evicts_its_tools() {
        let registry = ToolsRegistry::with_config(Vec::new(), Vec::new(), None, None).unwrap();

        // Pretend a server previously materialised two tools
        let server_conf = DynamicMcpServer {
            name: "srv".into(),
            description: String::new(),
            transport: McpBackendTransportUpstream::StreamableHttp,
            url: "http://localhost/mcp".into(),
            cache_duration: None,
            rbac: None,
        };
        let server = Arc::new(DynamicMcpServerEntry::new(server_conf));
        registry.dynamic_mcp_servers.insert("srv".into(), Arc::clone(&server));

        for upstream in ["alpha", "beta"] {
            let conf = McpTool {
                name: format!("srv__{upstream}").into(),
                description: String::new(),
                input_schema: serde_json::Map::new(),
                output_schema: serde_json::Map::new(),
                backend: UpstreamBackend::McpServer {
                    transport: McpBackendTransportUpstream::StreamableHttp,
                    url: "http://localhost/mcp".into(),
                },
                rbac: None,
                embedding: EmbeddingVector::default(),
            };
            let entry = build_tool_entry(
                conf,
                ToolSource::Dynamic { server_name: "srv".into(), upstream_tool_name: upstream.into() },
                true,
            )
            .unwrap();
            registry.tools.insert(entry.conf.name.clone(), Arc::new(entry));
        }

        // And a provided tool that must be left alone
        let provided = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        registry.add_tool(provided).await.unwrap();

        assert!(registry.remove_dynamic_server("srv"));
        assert!(registry.get_tool_by_name("srv__alpha").is_none());
        assert!(registry.get_tool_by_name("srv__beta").is_none());
        assert!(registry.get_tool_by_name("test_tool").is_some());
    }

    #[tokio::test]
    async fn semantic_search_ranks_with_cosine_and_populates_active_tools() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, SimilarityConfig,
        };

        let client = embeddings::EmbeddingsClient::test_one_hot();
        let semantic_search = Some(McpSemanticSearch {
            enable_assisted_discovery: false,
            embeddings: None,
            similarity: SimilarityConfig { top_k: 2 },
        });

        let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_a.name = "weather_get_forecast".into();
        tool_a.description = "Get the weather forecast".into();
        let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_b.name = "user_profile_get".into();
        tool_b.description = "Get user profile".into();
        let mut tool_c = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_c.name = "admin_delete".into();
        tool_c.description = "Admin delete operation".into();

        let registry = ToolsRegistry::with_config(
            vec![tool_a, tool_b, tool_c],
            Vec::new(),
            semantic_search,
            Some(Arc::clone(&client)),
        )
        .unwrap();

        registry.bootstrap().await.unwrap();

        for name in ["weather_get_forecast", "user_profile_get", "admin_delete"] {
            let entry = registry.get_tool_by_name(name).unwrap();
            assert!(entry.embedding.load_full().is_some(), "tool '{name}' was not embedded");
        }

        let req_ext = http::Extensions::new();
        let ranked = registry
            .rank_tools_for_query(&req_ext, "what is the weather today")
            .await
            .expect("semantic search ranking should succeed");
        assert_eq!(ranked.len(), 2, "top_k=2 should return exactly 2 tools");
        assert_eq!(ranked[0].conf.name.as_str(), "weather_get_forecast", "weather tool should rank first");
    }

    #[tokio::test]
    async fn semantic_search_ranks_with_bm25_without_remote_embeddings() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, SimilarityConfig,
        };

        let semantic_search = Some(McpSemanticSearch {
            enable_assisted_discovery: false,
            embeddings: None,
            similarity: SimilarityConfig { top_k: 1 },
        });

        let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_a.name = "weather_get_forecast".into();
        tool_a.description = "Get the weather forecast".into();
        let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_b.name = "user_profile_get".into();
        tool_b.description = "Get user profile".into();

        let registry = ToolsRegistry::with_config(vec![tool_a, tool_b], Vec::new(), semantic_search, None).unwrap();

        let req_ext = http::Extensions::new();
        let ranked =
            registry.rank_tools_for_query(&req_ext, "weather forecast").await.expect("BM25 ranking should succeed");
        assert_eq!(ranked.len(), 1, "top_k=1 should return exactly 1 tool");
        assert_eq!(ranked[0].conf.name.as_str(), "weather_get_forecast", "BM25 should rank weather first");
    }

    #[tokio::test]
    async fn add_tool_replacement_refreshes_bm25_ranking() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, SimilarityConfig,
        };

        let semantic_search = Some(McpSemanticSearch {
            enable_assisted_discovery: false,
            embeddings: None,
            similarity: SimilarityConfig { top_k: 1 },
        });

        let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_a.name = "alpha".into();
        tool_a.description = "Weather forecasts and temperature reports".into();
        let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_b.name = "beta".into();
        tool_b.description = "Billing invoices and payment records".into();

        let registry = ToolsRegistry::with_config(vec![tool_a], Vec::new(), semantic_search, None).unwrap();
        registry.add_tool(tool_b).await.unwrap();

        let req_ext = http::Extensions::new();
        let ranked =
            registry.rank_tools_for_query(&req_ext, "billing invoices").await.expect("BM25 ranking should succeed");
        assert_eq!(ranked[0].conf.name.as_str(), "beta");

        let mut replacement = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        replacement.name = "alpha".into();
        replacement.description = "Billing billing billing invoices invoices ledger reconciliation".into();
        registry.add_tool(replacement).await.unwrap();

        let ranked = registry
            .rank_tools_for_query(&req_ext, "billing invoices")
            .await
            .expect("BM25 ranking should succeed after replacement");
        assert_eq!(ranked[0].conf.name.as_str(), "alpha");
    }

    #[tokio::test]
    async fn bm25_fallback_disabled_skips_tf_and_errors() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, RemoteEmbeddings, SimilarityConfig,
        };

        let client = embeddings::EmbeddingsClient::test_failing();
        let semantic_search = Some(McpSemanticSearch {
            enable_assisted_discovery: false,
            embeddings: Some(RemoteEmbeddings {
                cluster: "embeddings".into(),
                model_id: "test-model".into(),
                path: RemoteEmbeddings::default_path(),
                timeout: None,
                dimensions: None,
                allow_bm25_fallback: false,
            }),
            similarity: SimilarityConfig { top_k: 1 },
        });

        let mut tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool.name = "weather_get_forecast".into();
        tool.description = "Get the weather forecast".into();

        let registry = ToolsRegistry::with_config(vec![tool], Vec::new(), semantic_search, Some(client)).unwrap();
        let entry = registry.get_tool_by_name("weather_get_forecast").unwrap();
        assert!(entry.bm25_doc.is_none(), "BM25 document should not be built when fallback is disabled");

        let req_ext = http::Extensions::new();
        let result = registry.rank_tools_for_query(&req_ext, "weather forecast").await;
        assert!(matches!(result, Err(CallToolError::SemanticSearchUnavailable)));
    }

    #[tokio::test]
    async fn bm25_fallback_allowed_degrades_to_bm25() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, RemoteEmbeddings, SimilarityConfig,
        };

        let client = embeddings::EmbeddingsClient::test_failing();
        let semantic_search = Some(McpSemanticSearch {
            enable_assisted_discovery: false,
            embeddings: Some(RemoteEmbeddings {
                cluster: "embeddings".into(),
                model_id: "test-model".into(),
                path: RemoteEmbeddings::default_path(),
                timeout: None,
                dimensions: None,
                allow_bm25_fallback: true,
            }),
            similarity: SimilarityConfig { top_k: 1 },
        });

        let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_a.name = "weather_get_forecast".into();
        tool_a.description = "Get the weather forecast".into();
        let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_b.name = "billing_lookup".into();
        tool_b.description = "Find billing invoices and payment records".into();

        let registry =
            ToolsRegistry::with_config(vec![tool_a, tool_b], Vec::new(), semantic_search, Some(client)).unwrap();
        assert!(registry.get_tool_by_name("billing_lookup").unwrap().bm25_doc.is_some());

        let req_ext = http::Extensions::new();
        let ranked = registry
            .rank_tools_for_query(&req_ext, "billing invoices")
            .await
            .expect("BM25 fallback should rank candidates when embeddings fail");
        assert_eq!(ranked[0].conf.name.as_str(), "billing_lookup");
    }

    #[tokio::test]
    async fn add_tool_inserts_even_when_embeddings_client_fails() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, SimilarityConfig,
        };

        let client = embeddings::EmbeddingsClient::test_failing();
        let semantic_search = Some(McpSemanticSearch {
            enable_assisted_discovery: false,
            embeddings: None,
            similarity: SimilarityConfig::default(),
        });
        let registry = ToolsRegistry::with_config(Vec::new(), Vec::new(), semantic_search, Some(client)).unwrap();
        let mut tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool.name = "tds_tool".into();

        let res = registry.add_tool(tool).await;
        assert!(res.is_ok(), "embeddings failure should not block insertion: {res:?}");
        assert!(registry.get_tool_by_name("tds_tool").is_some());
        assert!(registry.get_tool_by_name("tds_tool").unwrap().embedding.load_full().is_none());
    }

    #[tokio::test]
    async fn embed_tool_if_unembedded_swallows_embeddings_failure() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, SimilarityConfig,
        };

        let client = embeddings::EmbeddingsClient::test_failing();
        let semantic_search = Some(McpSemanticSearch {
            enable_assisted_discovery: false,
            embeddings: None,
            similarity: SimilarityConfig::default(),
        });
        let mut tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool.name = "needs_embedding".into();

        let registry =
            ToolsRegistry::with_config(vec![tool], Vec::new(), semantic_search, Some(Arc::clone(&client))).unwrap();

        registry.embed_tool_if_unembedded(&"needs_embedding".into()).await;
        assert!(
            registry.get_tool_by_name("needs_embedding").unwrap().embedding.load_full().is_none(),
            "embeddings failure should leave the tool unembedded"
        );
    }

    #[tokio::test]
    async fn unembedded_tool_is_excluded_from_cosine_ranking_not_abort() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, RemoteEmbeddings, SimilarityConfig,
        };

        let client = embeddings::EmbeddingsClient::test_one_hot();
        let semantic_search = Some(McpSemanticSearch {
            enable_assisted_discovery: false,
            embeddings: Some(RemoteEmbeddings {
                cluster: "embeddings".into(),
                model_id: "test-model".into(),
                path: RemoteEmbeddings::default_path(),
                timeout: None,
                dimensions: Some(3),
                allow_bm25_fallback: false,
            }),
            similarity: SimilarityConfig { top_k: 5 },
        });

        let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_a.name = "weather_get_forecast".into();
        tool_a.description = "Get the weather forecast".into();
        tool_a.embedding = EmbeddingVector(vec![1.0, 0.0, 0.0]);

        let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool_b.name = "billing_lookup".into();
        tool_b.description = "Find billing invoices".into();

        let registry =
            ToolsRegistry::with_config(vec![tool_a, tool_b], Vec::new(), semantic_search, Some(client)).unwrap();

        assert!(registry.get_tool_by_name("weather_get_forecast").unwrap().embedding.load_full().is_some());
        assert!(registry.get_tool_by_name("billing_lookup").unwrap().embedding.load_full().is_none());

        let req_ext = http::Extensions::new();
        let result = registry.rank_tools_for_query(&req_ext, "weather forecast").await;

        let ranked = result.expect("unembedded tool must not cause SemanticSearchUnavailable");
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].conf.name.as_str(), "weather_get_forecast");
    }

    #[tokio::test]
    async fn bootstrap_retries_after_failure_without_latching() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, SimilarityConfig,
        };

        let client = embeddings::EmbeddingsClient::test_flaky();
        let semantic_search = Some(McpSemanticSearch {
            enable_assisted_discovery: false,
            embeddings: None,
            similarity: SimilarityConfig::default(),
        });
        let mut tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        tool.name = "flaky_tool".into();

        let registry =
            ToolsRegistry::with_config(vec![tool], Vec::new(), semantic_search, Some(Arc::clone(&client))).unwrap();

        // First call: embeddings fail, bootstrap still succeeds because BM25 can
        // serve as the fallback ranking path.
        registry.bootstrap().await.expect("embeddings failure should not fail bootstrap");
        assert!(
            registry.get_tool_by_name("flaky_tool").unwrap().embedding.load_full().is_none(),
            "tool must remain unembedded after a failed bootstrap"
        );

        // Second call: remote embeddings recover, bootstrap succeeds and stores the embedding.
        registry.bootstrap().await.expect("retry must succeed once embeddings recover");
        assert!(
            registry.get_tool_by_name("flaky_tool").unwrap().embedding.load_full().is_some(),
            "embedding should be stored after a successful retry"
        );
    }
}
