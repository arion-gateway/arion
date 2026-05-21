use std::time::Duration;

use crate::config::core::DataSource;
use crate::config::network_filters::network_rbac::Action;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol_str::SmolStr;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ClusterHeader(#[serde(with = "http_serde_ext::header_name")] pub http::HeaderName);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpGateway {
    pub cluster_header: Option<ClusterHeader>,
    pub server_info: McpServerInfo,
    pub tools: Vec<McpTool>,
    pub dynamic_mcp_servers: Vec<DynamicMcpServer>,
    pub tds: Option<TdsSpecifier>,
    pub semantic_search_tool: Option<McpSemanticSearch>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DynamicMcpServer {
    pub name: SmolStr,
    pub description: String,
    pub transport: McpBackendTransportUpstream,
    pub url: String,
    pub cache_duration: Option<Duration>,
    pub rbac: Option<McpToolRbac>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TdsSpecifier {
    pub config_name: SmolStr,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpServerInfo {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpTool {
    pub name: SmolStr,
    pub description: String,
    pub input_schema: Map<String, Value>,
    pub output_schema: Map<String, Value>,
    pub backend: UpstreamBackend,
    pub rbac: Option<McpToolRbac>,
    #[serde(default, skip_serializing_if = "EmbeddingVector::is_empty")]
    pub embedding: EmbeddingVector,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(transparent)]
pub struct EmbeddingVector(pub Vec<f32>);

impl EmbeddingVector {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn as_slice(&self) -> &[f32] {
        &self.0
    }
}

impl From<Vec<f32>> for EmbeddingVector {
    fn from(v: Vec<f32>) -> Self {
        Self(v)
    }
}

impl PartialEq for EmbeddingVector {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len() && self.0.iter().zip(&other.0).all(|(a, b)| a.to_bits() == b.to_bits())
    }
}

impl Eq for EmbeddingVector {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum UpstreamBackend {
    Rest {
        #[serde(with = "http_serde_ext::method")]
        method: http::Method,
        path: String,
        query_params: Vec<McpRestQueryParams>,
        cluster: String,
        r#async: bool,
        body_template: Option<String>,
    },
    McpServer {
        transport: McpBackendTransportUpstream,
        url: String,
    },
    FunctionGraph {},
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum McpBackendTransportUpstream {
    Sse,
    StreamableHttp,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpRestQueryParams {
    pub name: SmolStr,
    pub source: SmolStr,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpToolRbac {
    pub action: Action,
    pub permissions: Vec<McpRbacPermission>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpRbacPermission {
    JwtHeader { field: SmolStr, value: SmolStr },
    JwtClaim { field: SmolStr, value: SmolStr },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpSemanticSearch {
    pub enable_assisted_discovery: bool,
    pub embeddings_service: SmolStr,
    #[serde(default)]
    pub similarity: SimilarityConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SimilarityConfig {
    #[serde(default = "SimilarityConfig::default_top_k")]
    pub top_k: usize,
}

impl SimilarityConfig {
    const fn default_top_k() -> usize {
        10
    }
}

impl Default for SimilarityConfig {
    fn default() -> Self {
        Self { top_k: Self::default_top_k() }
    }
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use std::str::FromStr;

    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::core::RustType;
    use crate::config::{required, GenericError};

    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        mcp_server_backend::TransportUpstream as OrionTransportUpstream, permission,
        tool::UpstreamBackend as OrionUpstreamBackend, tool_rbac::Action as OrionAction,
        DynamicMcpServer as OrionDynamicMcpServer, JwtClaimMatcher, JwtHeaderMatcher, McpGateway as OrionMcpGateway,
        Permission as OrionPermission, QueryParam as OrionMcpQueryParams, SemanticSearch as OrionSemanticSearch,
        ServerInfo as OrionMcpServerInfo, SimilarityConfig as OrionSimilarityConfig, TdsSpecifier as OrionTdsSpecifier,
        Tool as OrionTool, ToolRbac as OrionToolRbac,
    };
    use tracing::warn;

    impl From<OrionTransportUpstream> for McpBackendTransportUpstream {
        fn from(trans: OrionTransportUpstream) -> Self {
            match trans {
                OrionTransportUpstream::Sse => McpBackendTransportUpstream::Sse,
                OrionTransportUpstream::StreamableHttp => McpBackendTransportUpstream::StreamableHttp,
            }
        }
    }

    impl TryFrom<OrionMcpGateway> for McpGateway {
        type Error = GenericError;
        fn try_from(orion: OrionMcpGateway) -> Result<Self, Self::Error> {
            let OrionMcpGateway { cluster_header, server_info, tools, semantic_search_tool, tds, dynamic_mcp_servers } =
                orion;
            let server_info = required!(server_info)?;
            let cluster_header: Option<http::HeaderName> = cluster_header.map(TryInto::try_into).transpose()?;
            let cluster_header = cluster_header.map(ClusterHeader);
            let tools = tools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
            let dynamic_mcp_servers =
                dynamic_mcp_servers.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;

            Ok(McpGateway {
                cluster_header,
                server_info: server_info.try_into()?,
                tools,
                dynamic_mcp_servers,
                tds: tds.map(TryInto::try_into).transpose()?,
                semantic_search_tool: semantic_search_tool.map(TryInto::try_into).transpose()?,
            })
        }
    }

    impl TryFrom<OrionDynamicMcpServer> for DynamicMcpServer {
        type Error = GenericError;
        fn try_from(orion: OrionDynamicMcpServer) -> Result<Self, Self::Error> {
            let transport = orion.transport().into();
            let OrionDynamicMcpServer { name, description, url, cache_duration, rbac, transport: _ } = orion;
            if name.is_empty() {
                return Err(GenericError::from_msg("DynamicMcpServer.name must not be empty"));
            }
            if url.is_empty() {
                return Err(GenericError::from_msg("DynamicMcpServer.url must not be empty"));
            }
            let cache_duration = cache_duration
                .map(|d| -> Result<Duration, GenericError> {
                    let dur: RustType<Duration> = d.try_into()?;
                    Ok(dur.into_inner())
                })
                .transpose()?;

            Ok(DynamicMcpServer {
                name: name.into(),
                description,
                transport,
                url,
                cache_duration,
                rbac: rbac.map(TryInto::try_into).transpose()?,
            })
        }
    }

    impl TryFrom<OrionTdsSpecifier> for TdsSpecifier {
        type Error = GenericError;
        fn try_from(orion: OrionTdsSpecifier) -> Result<Self, Self::Error> {
            if orion.config_name.is_empty() {
                return Err(GenericError::from_msg(
                    "TdsSpecifier.config_name specifying the resource name from the xDS Tools Discovery Service must not be empty",
                ));
            }
            if orion.config_name.contains('/') {
                return Err(GenericError::from_msg(
                    "TdsSpecifier.config_name must not contain '/' (used as xDS resource-id separator)",
                ));
            }
            Ok(TdsSpecifier { config_name: orion.config_name.into() })
        }
    }

    impl TryFrom<OrionTool> for McpTool {
        type Error = GenericError;

        fn try_from(orion: OrionTool) -> Result<Self, Self::Error> {
            let OrionTool { name, description, input_schema, output_schema, upstream_backend, rbac, embedding } = orion;
            let backend = required!(upstream_backend)?.try_into()?;

            match backend {
                UpstreamBackend::FunctionGraph { .. } => unimplemented!("FunctionGraph backend is not supported yet"),
                _ => (),
            }

            let validate_schema_fn = |schema, name| -> Result<Map<String, Value>, GenericError> {
                if let Some(schema) = schema {
                    let bytes = TryInto::<DataSource>::try_into(schema)?.to_bytes_blocking()?;
                    let string = String::from_utf8(bytes)?;
                    let schema: Map<String, Value> = serde_json::from_str(&string)?;
                    // Verify the schema is valid; we are still storing the raw
                    // schema and recreate the validator in orion-lib as
                    // jsonschema::Validator is not Serialize and cannot be
                    // added to the configuration type
                    jsonschema::Validator::new(&Value::Object(schema.clone()))
                        .map_err(|e| GenericError::from_msg(format!("Invalid {name}_schema: {e}")))?;
                    Ok(schema)
                } else {
                    Ok(Map::new())
                }
            };

            let input_schema = match backend {
                UpstreamBackend::Rest { .. } | UpstreamBackend::FunctionGraph { .. } => {
                    validate_schema_fn(input_schema, "input")?
                },
                UpstreamBackend::McpServer { .. } => {
                    // input_schema is ignored for MCP backends
                    if let Some(_) = input_schema {
                        warn!("input_schema is ignored for MCP backends");
                    }
                    Map::new()
                },
            };

            let output_schema = match backend {
                UpstreamBackend::Rest { .. } | UpstreamBackend::FunctionGraph { .. } => {
                    validate_schema_fn(output_schema, "output")?
                },
                UpstreamBackend::McpServer { .. } => {
                    // output_schema is ignored for MCP backends
                    if let Some(_) = output_schema {
                        warn!("output_schema is ignored for MCP backends");
                    }
                    Map::new()
                },
            };

            let rbac = rbac.map(TryInto::try_into).transpose()?;
            Ok(McpTool {
                name: name.into(),
                description,
                input_schema,
                output_schema,
                backend,
                rbac,
                embedding: EmbeddingVector(embedding),
            })
        }
    }

    impl TryFrom<OrionUpstreamBackend> for UpstreamBackend {
        type Error = GenericError;

        fn try_from(value: OrionUpstreamBackend) -> Result<Self, GenericError> {
            match value {
                OrionUpstreamBackend::RestBackend(be) => {
                    let cluster = be.cluster;
                    let cluster = required!(cluster)?;
                    let template_ds: Option<DataSource> = be.body_template.map(|ds| ds.try_into()).transpose()?;
                    let template_bytes = template_ds.map(|t| t.to_bytes_blocking()).transpose()?;
                    let body_template = template_bytes.map(String::from_utf8).transpose()?;
                    Ok(UpstreamBackend::Rest {
                        method: http::Method::from_str(&be.method)?,
                        path: be.path,
                        query_params: be.query_params.into_iter().map(Into::into).collect(),
                        cluster,
                        r#async: be.r#async,
                        body_template,
                    })
                },
                OrionUpstreamBackend::McpServerBackend(be) => {
                    Ok(UpstreamBackend::McpServer { transport: be.transport().into(), url: be.url })
                },
                OrionUpstreamBackend::FunctionGraphBackend(_) => todo!(),
            }
        }
    }

    impl From<OrionMcpQueryParams> for McpRestQueryParams {
        fn from(orion: OrionMcpQueryParams) -> Self {
            McpRestQueryParams { name: orion.name.into(), source: orion.source.into() }
        }
    }

    impl TryFrom<OrionMcpServerInfo> for McpServerInfo {
        type Error = GenericError;
        fn try_from(orion: OrionMcpServerInfo) -> Result<Self, Self::Error> {
            if orion.name.is_empty() {
                return Err(GenericError::from_msg("McpServerInfo.name must not be empty"));
            }
            if orion.name.contains('/') {
                return Err(GenericError::from_msg(
                    "McpServerInfo.name must not contain '/' (used as xDS resource-id separator)",
                ));
            }
            Ok(McpServerInfo { name: orion.name, version: orion.version })
        }
    }

    impl From<OrionAction> for Action {
        fn from(action: OrionAction) -> Self {
            match action {
                OrionAction::Allow => Action::Allow,
                OrionAction::Deny => Action::Deny,
            }
        }
    }

    impl TryFrom<OrionToolRbac> for McpToolRbac {
        type Error = GenericError;
        fn try_from(orion: OrionToolRbac) -> Result<Self, Self::Error> {
            let action = orion.action().into();
            let permissions = orion.permissions;
            if permissions.is_empty() {
                return Err(GenericError::from_msg("Tool RBAC must have at least one permission"));
            }

            let permissions = permissions.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;

            Ok(McpToolRbac { action, permissions })
        }
    }

    impl TryFrom<OrionPermission> for McpRbacPermission {
        type Error = GenericError;
        fn try_from(orion: OrionPermission) -> Result<Self, Self::Error> {
            let OrionPermission { permission_type } = orion;
            let permission_type = required!(permission_type)?;

            match permission_type {
                permission::PermissionType::JwtHeader(JwtHeaderMatcher { field, value }) => {
                    if field.is_empty() || value.is_empty() {
                        return Err(GenericError::from_msg("JWT header field and value cannot be empty"));
                    }
                    Ok(McpRbacPermission::JwtHeader { field: field.into(), value: value.into() })
                },
                permission::PermissionType::JwtClaim(JwtClaimMatcher { field, value }) => {
                    if field.is_empty() || value.is_empty() {
                        return Err(GenericError::from_msg("JWT claim field and value cannot be empty"));
                    }
                    Ok(McpRbacPermission::JwtClaim { field: field.into(), value: value.into() })
                },
            }
        }
    }

    impl TryFrom<OrionSemanticSearch> for McpSemanticSearch {
        type Error = GenericError;
        fn try_from(orion: OrionSemanticSearch) -> Result<Self, Self::Error> {
            let OrionSemanticSearch { enable_assisted_discovery, embeddings_service, similarity } = orion;

            let embeddings_service = required!(embeddings_service)?;
            let similarity = similarity.map(TryInto::try_into).transpose()?.unwrap_or_default();

            Ok(McpSemanticSearch {
                enable_assisted_discovery,
                embeddings_service: embeddings_service.into(),
                similarity,
            })
        }
    }

    impl TryFrom<OrionSimilarityConfig> for SimilarityConfig {
        type Error = GenericError;
        fn try_from(orion: OrionSimilarityConfig) -> Result<Self, Self::Error> {
            let OrionSimilarityConfig { top_k } = orion;
            Ok(SimilarityConfig { top_k: top_k as usize })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_tool_rbac_config_parsing() {
            use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
                permission, JwtClaimMatcher, Permission as OrionPermission, ToolRbac as OrionToolRbac,
            };

            // Create a sample tool RBAC configuration
            let orion_rbac = OrionToolRbac {
                action: 0, // ALLOW
                permissions: vec![
                    OrionPermission {
                        permission_type: Some(permission::PermissionType::JwtClaim(JwtClaimMatcher {
                            field: "role".to_string(),
                            value: "admin".to_string(),
                        })),
                    },
                    OrionPermission {
                        permission_type: Some(permission::PermissionType::JwtClaim(JwtClaimMatcher {
                            field: "department".to_string(),
                            value: "engineering".to_string(),
                        })),
                    },
                ],
            };

            // Convert to our internal format
            let rbac: McpToolRbac = orion_rbac.try_into().unwrap();

            // Verify the conversion
            assert_eq!(rbac.action, Action::Allow);
            assert_eq!(rbac.permissions.len(), 2);

            match &rbac.permissions[0] {
                McpRbacPermission::JwtClaim { field, value } => {
                    assert_eq!(field.as_str(), "role");
                    assert_eq!(value.as_str(), "admin");
                },
                _ => panic!("Expected JwtClaim permission"),
            }

            match &rbac.permissions[1] {
                McpRbacPermission::JwtClaim { field, value } => {
                    assert_eq!(field.as_str(), "department");
                    assert_eq!(value.as_str(), "engineering");
                },
                _ => panic!("Expected JwtClaim permission"),
            }
        }

        #[test]
        fn test_tool_rbac_deny_action() {
            use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
                permission, JwtClaimMatcher, Permission as OrionPermission, ToolRbac as OrionToolRbac,
            };

            let orion_rbac = OrionToolRbac {
                action: 1, // DENY
                permissions: vec![OrionPermission {
                    permission_type: Some(permission::PermissionType::JwtClaim(JwtClaimMatcher {
                        field: "role".to_string(),
                        value: "guest".to_string(),
                    })),
                }],
            };

            let rbac: McpToolRbac = orion_rbac.try_into().unwrap();
            assert_eq!(rbac.action, Action::Deny);
        }

        #[test]
        fn test_tool_rbac_validation_errors() {
            use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::ToolRbac as OrionToolRbac;

            // Test empty permissions
            let orion_rbac = OrionToolRbac { action: 0, permissions: vec![] };

            let result: Result<McpToolRbac, _> = orion_rbac.try_into();
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("at least one permission"));
        }

        #[test]
        fn test_semantic_search_round_trip() {
            let orion = OrionSemanticSearch {
                enable_assisted_discovery: true,
                similarity: Some(OrionSimilarityConfig { top_k: 5 }),
                embeddings_service: "mcp-default".to_string(),
            };
            let parsed: McpSemanticSearch = orion.try_into().unwrap();
            assert!(parsed.enable_assisted_discovery);
            assert_eq!(parsed.similarity.top_k, 5);
            assert_eq!(parsed.embeddings_service.as_str(), "mcp-default");
        }

        #[test]
        fn test_semantic_search_defaults_similarity() {
            let orion = OrionSemanticSearch {
                enable_assisted_discovery: false,
                similarity: None,
                embeddings_service: "mcp-default".to_string(),
            };
            let parsed: McpSemanticSearch = orion.try_into().unwrap();
            assert_eq!(parsed.similarity.top_k, 10);
        }

        #[test]
        fn test_semantic_search_empty_service_is_rejected() {
            let orion = OrionSemanticSearch {
                enable_assisted_discovery: false,
                similarity: None,
                embeddings_service: String::new(),
            };
            let result: Result<McpSemanticSearch, _> = orion.try_into();
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("embeddings_service"));
        }
    }
}
