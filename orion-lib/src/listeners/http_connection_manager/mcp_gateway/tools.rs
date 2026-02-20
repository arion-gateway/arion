use crate::listeners::http_connection_manager::mcp_gateway::{
    mcp::{MessageResult, Session, ToolRegistryIndex},
    rbac::{
        Action as RbacAction, JwtClaimField, JwtHeaderField, JwtHeaderMatcher, JwtPayloadMatcher,
        Permission as RbacPermission, ToolRbac,
    },
    transcoder::{
        rest::{BODY_TEMPLATE_NAME, DEFAULT_USER_AGENT, PATH_TEMPLATE_NAME},
        FunctionGraphTranscoder, RestTranscoder, Transcoder, TranscoderType,
    },
};
use dashmap::DashMap;
use http::{header::InvalidHeaderValue, HeaderValue};
use jsonschema::Validator;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    ClusterHeader, McpBackendTransportUpstream, McpTool, UpstreamBackend,
};
use rmcp::{
    model::{CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation, InitializeRequestParams},
    service::ClientInitializeError,
    transport::StreamableHttpClientTransport,
    ServiceError, ServiceExt,
};

use rmcp::model;
use rmcp::model::{ListToolsResult, Tool};
use rmcp::service::{RoleClient, RunningService};
use serde_json::Value;
use smol_str::{SmolStr, ToSmolStr};
use std::{borrow::Cow, sync::Arc, time::Instant};
use tracing::{debug, info};
use upon::Engine;

#[derive(Debug, Clone)]
struct CachedEntry<T> {
    expiration: Instant,
    entry: T,
}

#[derive(Debug)]
pub struct ToolsRegistry {
    registry: Vec<ToolEntry>,
    cache: DashMap<SmolStr, CachedEntry<Vec<Tool>>, ahash::RandomState>,
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

    pub fn validate_against_input_schema(&self, arguments: &Value) -> Result<(), CallToolError> {
        Self::validate_json_against_schema(self.input_schema_validator.as_ref(), arguments)
    }

    pub fn validate_against_output_schema(&self, arguments: &Value) -> Result<(), CallToolError> {
        Self::validate_json_against_schema(self.output_schema_validator.as_ref(), arguments)
    }
}

impl ToolsRegistry {
    pub fn with_tools(tools: Vec<McpTool>) -> Result<Self, ToolBuilderError> {
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
        Ok(ToolsRegistry { registry, cache: DashMap::with_hasher(ahash::RandomState::new()) })
    }

    /// Get a tool by name as an Arc for cheap cloning
    #[inline]
    pub fn get_tool_by_index(&self, tool_index: ToolRegistryIndex) -> Option<&ToolEntry> {
        self.registry.get(tool_index.0)
    }

    pub async fn build_list_tools(&self, req_ext: &http::Extensions) -> ListToolsResult {
        let mut tools = Vec::with_capacity(self.registry.len());
        for entry in self.registry.iter() {
            if let Some(rbac) = &entry.rbac {
                if !rbac.is_permitted(req_ext) {
                    continue;
                }
            }

            match &entry.conf.backend {
                UpstreamBackend::Rest { .. } => {
                    tools.push(Tool {
                        name: Cow::Owned(entry.conf.name.to_string()),
                        description: Some(entry.conf.description.clone().into()),
                        input_schema: Arc::new(entry.conf.input_schema.clone()),
                        title: None,
                        output_schema: None,
                        annotations: None,
                        icons: None,
                        meta: None,
                    });
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
                            if let Some(cache_duration) = cache_duration {
                                if let Some(expiration) = std::time::Instant::now().checked_add(cache_duration.clone())
                                {
                                    self.cache.insert(
                                        entry.conf.name.to_smolstr(),
                                        CachedEntry { entry: up_tools.clone(), expiration },
                                    );
                                }
                            }

                            tools.extend(up_tools.iter().cloned());
                        },
                        Err(err) => {
                            info!(target: "mcp_gateway", "Failed to list tools: {}!", err);
                        },
                    }
                },
                UpstreamBackend::FunctionGraph {} => todo!(),
            }
        }

        ListToolsResult { tools, next_cursor: None, meta: None }
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

    pub async fn call(
        &self,
        req_ext: &http::Extensions,
        req_headers: &http::HeaderMap,
        rpc: &model::JsonRpcRequest,
        cluster_header: &Option<ClusterHeader>,
        session: &Session,
    ) -> Result<MessageResult, CallToolError> {
        // get tool name, and in case of upstream MCP, sub-tool name as well...
        let (backend_name, tool_name) = {
            let name = rpc.request.params.get("name").ok_or(CallToolError::NameNotString)?;
            let name = name.as_str().ok_or(CallToolError::NameNotString)?;
            match name.split_once("__") {
                Some((tool, sub_name)) => (tool, sub_name),
                None => (name, name),
            }
        };

        debug!(target: "mcp_gateway", "call: method:{} tool {tool_name}@{backend_name}", rpc.request.method);

        let (index, entry) = self
            .registry
            .iter()
            .enumerate()
            .find(|(_, e)| e.conf.name == backend_name)
            .ok_or_else(|| CallToolError::ToolNotFound(backend_name.to_string()))?;

        if let Some(rbac) = &entry.rbac {
            if !rbac.is_permitted(req_ext) {
                return Err(CallToolError::RbacDenied(backend_name.to_string()));
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
                    CallToolError::TranscoderError { tool: backend_name.to_owned(), reason: e.to_string() }
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
                        name: tool_name.to_owned().into(),
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

                debug!(target: "mcp_gateway", "Received result from tool{backend_name}@{tool_name}: {:?}", tool_result);

                let json_result = serde_json::to_value(tool_result)?;

                let json_rcp_response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: rpc.id.clone(),
                    result: json_result,
                };

                Ok(MessageResult::JsonRcpResponse(json_rcp_response))
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
        let registry = ToolsRegistry::with_tools(vec![tool]).unwrap();
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
        let registry = ToolsRegistry::with_tools(vec![tool]).unwrap();
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
        let registry = ToolsRegistry::with_tools(vec![tool]).unwrap();
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
        let registry = ToolsRegistry::with_tools(vec![tool]).unwrap();
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
        let registry = ToolsRegistry::with_tools(vec![tool]).unwrap();
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
        let registry = ToolsRegistry::with_tools(vec![tool]).unwrap();
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
        let registry = ToolsRegistry::with_tools(vec![tool]).unwrap();
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
        let result = ToolsRegistry::with_tools(vec![tool]);

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
        let result = ToolsRegistry::with_tools(vec![tool]);

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
        let registry = ToolsRegistry::with_tools(vec![tool]).unwrap();
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
        let registry = ToolsRegistry::with_tools(vec![tool]).unwrap();
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
