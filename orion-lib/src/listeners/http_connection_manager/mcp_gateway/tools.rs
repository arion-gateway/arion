use crate::listeners::http_connection_manager::mcp_gateway::{
    mcp::{MessageResult, Session, ToolRegistryIndex},
    rbac::{
        Action as RbacAction, JwtClaimField, JwtHeaderField, JwtHeaderMatcher, JwtPayloadMatcher,
        Permission as RbacPermission, ToolRbac,
    },
    transcoder::{
        FunctionGraphTranscoder, RestTranscoder, Transcoder, TranscoderType, rest::{BODY_TEMPLATE_NAME, DEFAULT_USER_AGENT, PATH_TEMPLATE_NAME}
    },
};
use dashmap::DashMap;
use http::{header::InvalidHeaderValue, HeaderValue};
use jsonschema::Validator;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    ClusterHeader, McpBackendTransportUpstream, McpTool, UpstreamBackend,
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

use rmcp::model;
use rmcp::model::{ListToolsResult, Tool};
use rmcp::service::{RoleClient, RunningService};
use serde_json::{json, Value};
use smol_str::{SmolStr, ToSmolStr};
use std::sync::LazyLock;
use std::{borrow::Cow, sync::Arc, time::Instant};
use tracing::{debug, info};
use upon::Engine;

const DYNAMIC_TOOL_DISCOVERY: &str = "dynamic_tool_discovery";

static DYNAMIC_TOOL_DISCOVERY_INPUT_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "type": "object",
        "properties": {
            "user_query": { "type": "string", "description": "The prompt or summary text." }
        },
        "required": ["user_query"],
    })
});

#[derive(Debug, Clone)]
struct CachedEntry<T> {
    expiration: Instant,
    entry: T,
}

#[derive(Debug)]
pub struct ToolsRegistry {
    registry: Vec<ToolEntry>,
    cache: DashMap<SmolStr, CachedEntry<Vec<Tool>>, ahash::RandomState>,
    dynamic_tool_discovery: bool,
}

#[derive(Debug)]
pub struct ToolEntry {
    pub conf: McpTool,
    pub transcoder: TranscoderType,
    pub input_schema_validator: Option<Validator>,
    pub output_schema_validator: Option<Validator>,
    pub rbac: Option<ToolRbac>,
}

#[derive(Debug, thiserror::Error)]
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
}

#[derive(Debug, thiserror::Error)]
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
}

impl ToolEntry {
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
    pub fn validate_against_input_schema(&self, arguments: &Value) -> Result<(), CallToolError> {
        Self::validate_json_against_schema(self.input_schema_validator.as_ref(), arguments)
    }

    #[inline]
    pub fn validate_against_output_schema(&self, arguments: &Value) -> Result<(), CallToolError> {
        Self::validate_json_against_schema(self.output_schema_validator.as_ref(), arguments)
    }
}

impl ToolsRegistry {
    pub fn with_tools(tools: Vec<McpTool>, dynamic_tool_discovery: bool) -> Result<Self, ToolBuilderError> {
        let registry: Vec<ToolEntry> = tools
            .into_iter()
            .map(|tool_conf| -> Result<ToolEntry, ToolBuilderError> {
                let rbac = tool_conf.rbac.as_ref().map(convert_config_rbac_to_runtime);
                let input_schema_validator = if !tool_conf.input_schema.is_empty() {
                    Some(
                        Validator::new(&Value::Object(tool_conf.input_schema.clone()))
                            .map_err(|e| ToolBuilderError::InvalidInputSchema(e.to_string()))?,
                    )
                } else {
                    None
                };
                let output_schema_validator = if !tool_conf.output_schema.is_empty() {
                    Some(
                        Validator::new(&Value::Object(tool_conf.output_schema.clone()))
                            .map_err(|e| ToolBuilderError::InvalidOutputSchema(e.to_string()))?,
                    )
                } else {
                    None
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
                    UpstreamBackend::FunctionGraph { .. } => TranscoderType::FunctionGraph(FunctionGraphTranscoder {}),
                    UpstreamBackend::McpServer { .. } => TranscoderType::NoTranscoder,
                };
                Ok(ToolEntry { conf: tool_conf, transcoder, rbac, input_schema_validator, output_schema_validator })
            })
            .collect::<Result<Vec<_>, ToolBuilderError>>()?;
        Ok(ToolsRegistry { registry, cache: DashMap::with_hasher(ahash::RandomState::new()), dynamic_tool_discovery })
    }

    /// Get a tool by name as an Arc for cheap cloning
    #[inline]
    pub fn get_tool_by_index(&self, tool_index: ToolRegistryIndex) -> Option<&ToolEntry> {
        self.registry.get(tool_index.0)
    }

    pub async fn build_list_tools(
        &self,
        req_ext: &http::Extensions,
        session: &Option<Arc<Session>>,
    ) -> ListToolsResult {
        let mut tools = Vec::with_capacity(self.registry.len() + 1);
        if self.dynamic_tool_discovery {
            let Value::Object(discovery_input_schema) = &*DYNAMIC_TOOL_DISCOVERY_INPUT_SCHEMA else { unreachable!() };

            tools.push(Tool {
                name: DYNAMIC_TOOL_DISCOVERY.into(),
                description: Some("Pass the user prompt or a summary to discover relevant tools.".into()),
                input_schema: Arc::new(discovery_input_schema.clone()),
                output_schema: None,
                title: None,
                annotations: None,
                icons: None,
                meta: None,
            });
        }

        // FIXME: This is a dummy dynamic tool discovery strategy that
        // returns the complete list of tools after the dynamic_tool_discovery is invoked.
        //
        if !self.dynamic_tool_discovery || session.as_ref().is_some_and(|session| session.prompt.lock().is_some()) {
            self.fill_list_tools(req_ext, session, &mut tools).await;
        }

        ListToolsResult { tools, next_cursor: None, meta: None }
    }

    fn filter_tool_by_vector_similarity(tool: &ToolEntry, prompt: Option<&String>) -> bool {
        // TODO: This is a dummy implementation of vector similarity.
        // If any word in the prompt is present in the tool description,
        // the tool is selected.

        let Some(prompt) = prompt else {
            return true;
        };

        let description = tool.conf.description.to_lowercase();
        prompt.split_whitespace().any(|word| description.contains(&word.to_lowercase()))
    }

    async fn fill_list_tools(&self, req_ext: &http::Extensions, session: &Option<Arc<Session>>, tools: &mut Vec<Tool>) {
        let Some(session) = session.as_ref() else {
            debug!(target: "mcp_gateway", "build_list_tool_apis without session!");
            return;
        };

        // reset the list of active tools for this session...
        session.active_tools.clear();

        // populate the list of active tools as well as the list of tools to return...
        for entry in self
            .registry
            .iter()
            .filter(|entry| entry.rbac.as_ref().map_or(true, |rbac| rbac.is_permitted(req_ext)))
            .filter(|entry| Self::filter_tool_by_vector_similarity(entry, session.prompt.lock().as_ref()))
        {
            match &entry.conf.backend {
                UpstreamBackend::Rest { .. } => {
                    tools.push(Tool {
                        name: Cow::Owned(entry.conf.name.to_string()),
                        description: Some(entry.conf.description.clone().into()),
                        input_schema: Arc::new(entry.conf.input_schema.clone()),
                        output_schema: (!entry.conf.output_schema.is_empty())
                            .then(|| Arc::new(entry.conf.output_schema.clone())),
                        title: None,
                        annotations: None,
                        icons: None,
                        meta: None,
                    });
                    if self.dynamic_tool_discovery {
                        session.active_tools.insert(entry.conf.name.clone());
                    }
                },
                UpstreamBackend::McpServer { transport, url, cache_duration } => {
                    if let Some(r) = self.cache.get(&entry.conf.name) {
                        if std::time::Instant::now() < r.expiration {
                            tools.extend(r.entry.iter().cloned());
                            continue;
                        }
                    }

                    match self.get_list_tools_from_upstream(&transport, &url, &entry.conf.name).await {
                        Ok(up_tools) => {
                            if let Some(expiration) =
                                cache_duration.as_ref().and_then(|d| std::time::Instant::now().checked_add(d.clone()))
                            {
                                self.cache.insert(
                                    entry.conf.name.to_smolstr(),
                                    CachedEntry { entry: up_tools.clone(), expiration },
                                );
                            }

                            tools.extend(up_tools.iter().cloned());
                            if self.dynamic_tool_discovery {
                                up_tools.iter().for_each(|t| {
                                    session.active_tools.insert(t.name.to_smolstr());
                                });
                            }
                        },
                        Err(err) => {
                            info!(target: "mcp_gateway", "Failed to list tools: {}!", err);
                        },
                    }
                },
                UpstreamBackend::FunctionGraph {} => todo!(),
            }
        }
    }

    pub async fn get_list_tools_from_upstream(
        &self,
        transport: &McpBackendTransportUpstream,
        url: &str,
        namespace: &str,
    ) -> Result<Vec<Tool>, ListToolsError> {
        debug!(target: "mcp_gateway", "Getting list of tools from upstream with transport {transport:?}");
        match transport {
            McpBackendTransportUpstream::StreamableHttp => self.get_list_tools_streamable_http(url, namespace).await,
            McpBackendTransportUpstream::Sse => Err(ListToolsError::UnsupportedTransport),
        }
    }

    pub async fn get_list_tools_streamable_http(
        &self,
        url: &str,
        namespace: &str,
    ) -> Result<Vec<Tool>, ListToolsError> {
        let client = Self::get_mcp_client(url).await?;

        // List tools
        let mut tools = client.list_tools(Default::default()).await?;

        for tool in &mut tools.tools {
            tool.name = Cow::Owned(format!("{namespace}__{}", tool.name));
        }

        Ok(tools.tools)
    }

    async fn get_mcp_client(
        url: &str,
    ) -> Result<RunningService<RoleClient, InitializeRequestParams>, ClientInitializeError> {
        debug!(target: "mcp_gateway", "Creating MCP client for URL: {url}...");
        let transport = StreamableHttpClientTransport::from_uri(url);
        let client_info = ClientInfo {
            meta: None,
            protocol_version: Default::default(),
            capabilities: ClientCapabilities::default(),
            client_info: Implementation {
                name: DEFAULT_USER_AGENT.into(),
                title: None,
                version: "0.0.1".to_string(),
                website_url: None,
                icons: None,
            },
        };
        client_info.serve(transport).await.inspect_err(|e| {
            info!(target: "mcp_gateway", "get_mcp_client error: {}!", e);
        })
    }

    pub async fn call_dynamic_tool_discovery(
        &self,
        rpc: &model::JsonRpcRequest,
        session: &Session,
    ) -> Result<MessageResult, CallToolError> {
        let arguments = match &rpc.request.params.get("arguments") {
            Some(&serde_json::Value::Object(ref o)) => o.clone(),
            _ => {
                return Err(CallToolError::ValidationError("call_dynamic_tool_discovery: missing arguments".into()));
            },
        };

        let Some(Value::String(prompt)) = arguments.get("user_query") else {
            return Err(CallToolError::ValidationError(
                "call_dynamic_tool_discovery: invalid user_query argument".into(),
            ));
        };

        // send a notifications/tools/list_changed...
        //

        // Wrap the server notification inside the generic JSON-RPC structure.
        let json_rpc_notification = JsonRpcNotification::<ServerNotification> {
            jsonrpc: model::JsonRpcVersion2_0,
            notification: ServerNotification::ToolListChangedNotification(ToolListChangedNotification::default()),
        };

        let mut session_prompt = session.prompt.lock();
        *session_prompt = Some(prompt.clone());
        drop(session_prompt);

        let text_content = RawTextContent {
            text: "Context acquired. Relevant APIs loaded. The tool list has been updated, please proceed with the new tools.".to_string(),
            meta: None
        };

        let success_message = Content { raw: RawContent::Text(text_content), annotations: None };

        let tool_result = CallToolResult {
            content: vec![success_message],
            structured_content: None,
            is_error: Some(false),
            meta: None,
        };

        let json_result = serde_json::to_value(tool_result)?;

        let json_rpc_response =
            model::JsonRpcResponse { jsonrpc: model::JsonRpcVersion2_0, id: rpc.id.clone(), result: json_result };

        Ok(MessageResult::JsonRpcNotificationResponse(json_rpc_notification, json_rpc_response))
    }

    pub async fn call(
        &self,
        req_ext: &http::Extensions,
        req_headers: &http::HeaderMap,
        rpc: &model::JsonRpcRequest,
        cluster_header: &Option<ClusterHeader>,
        session: &Session,
    ) -> Result<MessageResult, CallToolError> {
        // get tool name, and in case of upstream MCP, sub-tool name as well...
        let (tool_name, tool_sub_name) = {
            let name = rpc.request.params.get("name").ok_or(CallToolError::NameNotString)?;
            let name = name.as_str().ok_or(CallToolError::NameNotString)?;
            match name.split_once("__") {
                Some((tool_name, sub_name)) => (tool_name, sub_name),
                None => (name, name),
            }
        };

        debug!(target: "mcp_gateway", "call: method:{} tool {tool_sub_name}@{tool_name}", rpc.request.method);

        if self.dynamic_tool_discovery && tool_name == DYNAMIC_TOOL_DISCOVERY {
            return self.call_dynamic_tool_discovery(rpc, session).await;
        }

        let (index, entry) = self
            .registry
            .iter()
            .enumerate()
            .find(|(_, e)| e.conf.name == tool_name)
            .filter(|(_, e)| session.active_tools.contains(&e.conf.name))
            .ok_or_else(|| CallToolError::ToolNotFound(tool_name.to_string()))?;

        if let Some(rbac) = &entry.rbac {
            if !rbac.is_permitted(req_ext) {
                return Err(CallToolError::RbacDenied(tool_name.to_string()));
            }
        }

        // Validate the request message arguments against the input schema
        if entry.input_schema_validator.is_some() {
            if let Some(arguments) = rpc.request.params.get("arguments") {
                entry.validate_against_input_schema(arguments)?;
            } else {
                let arguments = Value::Null;
                entry.validate_against_input_schema(&arguments)?;
            }
        }

        match (&entry.conf.backend, &entry.transcoder) {
            (
                UpstreamBackend::Rest { method: _, path: _, query_params: _, cluster, r#async, body_template: _ },
                TranscoderType::Rest(transcoder),
            ) => {
                let mut upstream_request = transcoder.encode(req_headers, &rpc.request).map_err(|e| {
                    CallToolError::TranscoderError { tool: tool_name.to_owned(), reason: e.to_string() }
                })?;
                if let Some(cluster_header) = cluster_header {
                    let headers = upstream_request.headers_mut();
                    headers.append(cluster_header.0.clone(), HeaderValue::from_str(&cluster)?);
                }
                Ok(MessageResult::UpstreamRequest((upstream_request, *r#async, ToolRegistryIndex(index))))
            },
            (UpstreamBackend::McpServer { url, .. }, TranscoderType::NoTranscoder) => {
                let client = match session.mcp_upstreams.entry(url.to_owned()) {
                    dashmap::Entry::Occupied(entry) => entry.into_ref(),
                    dashmap::Entry::Vacant(vacant_entry) => {
                        let new_client = Self::get_mcp_client(url).await?;
                        vacant_entry.insert(new_client)
                    },
                };

                let arguments = match &rpc.request.params.get("arguments") {
                    Some(&serde_json::Value::Object(ref o)) => Some(o.clone()),
                    _ => None,
                };

                let tool_result = match client
                    .call_tool(CallToolRequestParams {
                        meta: None,
                        name: tool_sub_name.to_owned().into(),
                        arguments,
                        task: None,
                    })
                    .await
                {
                    Ok(res) => res,
                    Err(err) => {
                        match &err {
                            ServiceError::TransportSend(_) | ServiceError::TransportClosed => {
                                // delete the client, will be re-created on next call:
                                // first, drop the reference to client, to avoid deadlock, then remove from session map
                                info!(target: "mcp_gateway", "removing MCP client for URL {url} due to transport error!");
                                drop(client);
                                session.mcp_upstreams.remove(url);
                            },
                            _ => (),
                        };
                        return Err(err.into());
                    },
                };

                debug!(target: "mcp_gateway", "Received result from tool{tool_name}@{tool_sub_name}: {:?}", tool_result);

                let json_result = serde_json::to_value(tool_result)?;

                let json_rcp_response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: rpc.id.clone(),
                    result: json_result,
                };

                Ok(MessageResult::JsonRpcResponse(json_rcp_response))
            },
            (UpstreamBackend::FunctionGraph {}, TranscoderType::FunctionGraph(_)) => {
                return Err(CallToolError::FunctionGraphNotImplemented);
            },
            _ => unreachable!(),
        }
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
                let header_field = match field.as_str() {
                    "alg" | "algorithm" => JwtHeaderField::Algorithm,
                    "typ" | "type" => JwtHeaderField::Type,
                    "cty" | "content_type" => JwtHeaderField::ContentType,
                    "jku" | "json_key_url" => JwtHeaderField::JsonKeyURL,
                    "jwk" | "json_web_key" => JwtHeaderField::JsonWebKey,
                    "kid" | "key_id" => JwtHeaderField::KeyID,
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
                    _ => JwtHeaderField::KeyID, // default fallback
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
                    "jti" | "jwt_id" => JwtClaimField::JWTID,
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
        }
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
        let registry = ToolsRegistry::with_tools(vec![tool], false).unwrap();
        let tool_entry = registry.get_tool_by_index(ToolRegistryIndex(0)).unwrap();

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
        let registry = ToolsRegistry::with_tools(vec![tool], false).unwrap();
        let tool_entry = registry.get_tool_by_index(ToolRegistryIndex(0)).unwrap();

        let args = json!({});

        let result = tool_entry.validate_against_input_schema(&args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("username"), "Error should mention missing field: {}", err_msg);
    }

    #[test]
    fn test_input_schema_validation_fails_with_wrong_type() {
        let input_schema = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "count": { "type": "number" }
            }
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
        let registry = ToolsRegistry::with_tools(vec![tool], false).unwrap();
        let tool_entry = registry.get_tool_by_index(ToolRegistryIndex(0)).unwrap();

        let args = json!({
            "count": "not a number"
        });

        let result = tool_entry.validate_against_input_schema(&args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("number"), "Error should mention type mismatch: {}", err_msg);
    }

    #[test]
    fn test_input_schema_validation_skips_when_empty() {
        let tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        let registry = ToolsRegistry::with_tools(vec![tool], false).unwrap();
        let tool_entry = registry.get_tool_by_index(ToolRegistryIndex(0)).unwrap();

        // Any args should pass when schema is empty
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
        let registry = ToolsRegistry::with_tools(vec![tool], false).unwrap();
        let tool_entry = registry.get_tool_by_index(ToolRegistryIndex(0)).unwrap();

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
        let registry = ToolsRegistry::with_tools(vec![tool], false).unwrap();
        let tool_entry = registry.get_tool_by_index(ToolRegistryIndex(0)).unwrap();

        let response = json!({
            "temperature": "hot" // Should be a number
        });

        let result = tool_entry.validate_against_output_schema(&response);
        assert!(result.is_err());
    }

    #[test]
    fn test_output_schema_validation_skips_when_empty() {
        let tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
        let registry = ToolsRegistry::with_tools(vec![tool], false).unwrap();
        let tool_entry = registry.get_tool_by_index(ToolRegistryIndex(0)).unwrap();

        // Any response should pass when schema is empty
        let response = json!({
            "arbitrary": "data",
            "nested": {
                "value": 123
            }
        });

        assert!(tool_entry.validate_against_output_schema(&response).is_ok());
    }

    #[test]
    fn test_with_tools_fails_with_invalid_input_schema() {
        let input_schema = serde_json::from_value(json!({
            "type": "invalid_type" // Invalid schema
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
        let result = ToolsRegistry::with_tools(vec![tool], false);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ToolBuilderError::InvalidInputSchema(_)));
    }

    #[test]
    fn test_with_tools_fails_with_invalid_output_schema() {
        let output_schema = serde_json::from_value(json!({
            "type": "invalid_type" // Invalid schema
        }))
        .unwrap();

        let tool = create_test_tool_with_schemas(serde_json::Map::new(), output_schema);
        let result = ToolsRegistry::with_tools(vec![tool], false);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ToolBuilderError::InvalidOutputSchema(_)));
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
        let registry = ToolsRegistry::with_tools(vec![tool], false).unwrap();
        let tool_entry = registry.get_tool_by_index(ToolRegistryIndex(0)).unwrap();

        // Valid nested object
        let valid_args = json!({
            "user": {
                "name": "Alice",
                "age": 30
            }
        });
        assert!(tool_entry.validate_against_input_schema(&valid_args).is_ok());

        // Invalid - missing required nested field
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
        let registry = ToolsRegistry::with_tools(vec![tool], false).unwrap();
        let tool_entry = registry.get_tool_by_index(ToolRegistryIndex(0)).unwrap();

        // Valid array
        let valid_args = json!({
            "tags": ["rust", "mcp", "api"]
        });
        assert!(tool_entry.validate_against_input_schema(&valid_args).is_ok());

        // Invalid - wrong item type
        let invalid_args = json!({
            "tags": [1, 2, 3]
        });
        let result = tool_entry.validate_against_input_schema(&invalid_args);
        assert!(result.is_err());
    }
}
