use std::time::Duration;

use crate::config::core::DataSource;
use crate::config::network_filters::http_connection_manager::RetryPolicy;
use crate::config::network_filters::network_rbac::Action;
use http::uri::Authority;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol_str::SmolStr;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpGateway {
    pub server_info: McpServerInfo,
    pub tools: Vec<McpTool>,
    pub dynamic_mcp_servers: Vec<DynamicMcpServer>,
    pub tds: Option<TdsSpecifier>,
    pub semantic_search_tool: Option<McpSemanticSearch>,
    #[serde(default)]
    pub upstream_limits: UpstreamLimits,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct UpstreamLimits {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub max_response_bytes: usize,
}

impl UpstreamLimits {
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);
    pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
}

impl Default for UpstreamLimits {
    fn default() -> Self {
        Self { timeout: Self::DEFAULT_TIMEOUT, max_response_bytes: Self::DEFAULT_MAX_RESPONSE_BYTES }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct HttpUpstreamPolicy {
    #[serde(with = "http_serde_ext::authority::option", skip_serializing_if = "Option::is_none", default)]
    pub authority: Option<Authority>,
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none", default)]
    pub timeout: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub retry_policy: Option<RetryPolicy>,
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
        #[serde(default)]
        upstream_policy: HttpUpstreamPolicy,
        body_template: Option<String>,
    },
    McpServer {
        transport: McpBackendTransportUpstream,
        url: String,
    },
    FunctionGraph,
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
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub embeddings: Option<RemoteEmbeddings>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteEmbeddings {
    pub cluster: SmolStr,
    pub model_id: SmolStr,
    #[serde(default = "RemoteEmbeddings::default_path", skip_serializing_if = "RemoteEmbeddings::is_default_path")]
    pub path: String,
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none", default)]
    pub timeout: Option<Duration>,
    pub dimensions: usize,
}

impl RemoteEmbeddings {
    pub fn default_path() -> String {
        "/v1/embeddings".to_owned()
    }

    fn is_default_path(path: &str) -> bool {
        path == Self::default_path()
    }

    pub fn normalized(mut self) -> Result<Self, String> {
        if self.cluster.is_empty() {
            return Err("RemoteEmbeddings.cluster must not be empty".to_owned());
        }
        if self.model_id.is_empty() {
            return Err("RemoteEmbeddings.model_id must not be empty".to_owned());
        }
        if self.dimensions == 0 {
            return Err("RemoteEmbeddings.dimensions must be greater than zero".to_owned());
        }
        if self.path.is_empty() {
            self.path = Self::default_path();
        } else if !self.path.starts_with('/') {
            self.path.insert(0, '/');
        }
        Ok(self)
    }
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use std::{num::NonZeroUsize, str::FromStr};

    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::core::RustType;
    use crate::config::{required, GenericError, WithNodeOnResult};

    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        mcp_server_backend::TransportUpstream as OrionTransportUpstream, permission,
        tool::UpstreamBackend as OrionUpstreamBackend, tool_rbac::Action as OrionAction,
        DynamicMcpServer as OrionDynamicMcpServer, HttpUpstreamPolicy as OrionHttpUpstreamPolicy, JwtClaimMatcher,
        JwtHeaderMatcher, McpGateway as OrionMcpGateway, Permission as OrionPermission,
        QueryParam as OrionMcpQueryParams, RemoteEmbeddings as OrionRemoteEmbeddings,
        SemanticSearch as OrionSemanticSearch, ServerInfo as OrionMcpServerInfo,
        SimilarityConfig as OrionSimilarityConfig, TdsSpecifier as OrionTdsSpecifier, Tool as OrionTool,
        ToolRbac as OrionToolRbac,
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
            let OrionMcpGateway {
                server_info,
                tools,
                semantic_search_tool,
                tds,
                dynamic_mcp_servers,
                upstream_timeout,
                max_upstream_response_bytes,
            } = orion;
            let server_info = required!(server_info)?;
            let tools = tools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
            let dynamic_mcp_servers =
                dynamic_mcp_servers.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;

            Ok(McpGateway {
                server_info: server_info.try_into()?,
                tools,
                dynamic_mcp_servers,
                tds: tds.map(TryInto::try_into).transpose()?,
                semantic_search_tool: semantic_search_tool.map(TryInto::try_into).transpose()?,
                upstream_limits: parse_upstream_limits(upstream_timeout, max_upstream_response_bytes)?,
            })
        }
    }

    fn parse_upstream_limits(
        upstream_timeout: Option<orion_data_plane_api::envoy_data_plane_api::google::protobuf::Duration>,
        max_upstream_response_bytes: Option<u64>,
    ) -> Result<UpstreamLimits, GenericError> {
        let timeout = upstream_timeout
            .map(parse_positive_duration)
            .transpose()
            .with_node("upstream_timeout")?
            .unwrap_or(UpstreamLimits::DEFAULT_TIMEOUT);
        let max_response_bytes = max_upstream_response_bytes
            .map(|value| -> Result<usize, GenericError> {
                NonZeroUsize::try_from(usize::try_from(value)?)
                    .map(NonZeroUsize::get)
                    .map_err(|_| GenericError::from_msg("must be greater than zero"))
            })
            .transpose()
            .with_node("max_upstream_response_bytes")?
            .unwrap_or(UpstreamLimits::DEFAULT_MAX_RESPONSE_BYTES);

        Ok(UpstreamLimits { timeout, max_response_bytes })
    }

    fn parse_positive_duration(
        duration: orion_data_plane_api::envoy_data_plane_api::google::protobuf::Duration,
    ) -> Result<Duration, GenericError> {
        let duration = RustType::<Duration>::try_from(duration)?.into_inner();
        (!duration.is_zero()).then_some(duration).ok_or_else(|| GenericError::from_msg("must be greater than zero"))
    }

    impl TryFrom<OrionHttpUpstreamPolicy> for HttpUpstreamPolicy {
        type Error = GenericError;

        fn try_from(orion: OrionHttpUpstreamPolicy) -> Result<Self, Self::Error> {
            let authority = orion
                .authority
                .map(|authority| {
                    Authority::from_str(&authority)
                        .map_err(|e| GenericError::from_msg_with_cause("Invalid HttpUpstreamPolicy.authority", e))
                })
                .transpose()?;
            let timeout = orion.timeout.map(parse_positive_duration).transpose().with_node("timeout")?;
            let retry_policy = orion.retry_policy.map(TryInto::try_into).transpose().with_node("retry_policy")?;

            Ok(Self { authority, timeout, retry_policy })
        }
    }

    impl TryFrom<OrionDynamicMcpServer> for DynamicMcpServer {
        type Error = GenericError;
        fn try_from(orion: OrionDynamicMcpServer) -> Result<Self, Self::Error> {
            let transport = orion.transport().into();
            let OrionDynamicMcpServer { name, description, url, cache_duration, rbac, .. } = orion;
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

            if let UpstreamBackend::FunctionGraph = backend {
                unimplemented!("FunctionGraph backend is not supported yet")
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
                UpstreamBackend::Rest { .. } | UpstreamBackend::FunctionGraph => {
                    validate_schema_fn(input_schema, "input")?
                },
                UpstreamBackend::McpServer { .. } => {
                    // input_schema is ignored for MCP backends
                    if input_schema.is_some() {
                        warn!("input_schema is ignored for MCP backends");
                    }
                    Map::new()
                },
            };

            let output_schema = match backend {
                UpstreamBackend::Rest { .. } | UpstreamBackend::FunctionGraph => {
                    validate_schema_fn(output_schema, "output")?
                },
                UpstreamBackend::McpServer { .. } => {
                    // output_schema is ignored for MCP backends
                    if output_schema.is_some() {
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
                    let template_ds: Option<DataSource> = be.body_template.map(TryInto::try_into).transpose()?;
                    let template_bytes = template_ds.map(|t| t.to_bytes_blocking()).transpose()?;
                    let body_template = template_bytes.map(String::from_utf8).transpose()?;
                    Ok(UpstreamBackend::Rest {
                        method: http::Method::from_str(&be.method)?,
                        path: be.path,
                        query_params: be.query_params.into_iter().map(Into::into).collect(),
                        cluster,
                        upstream_policy: be.upstream_policy.map(TryInto::try_into).transpose()?.unwrap_or_default(),
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
            let OrionSemanticSearch { enable_assisted_discovery, similarity, embeddings } = orion;

            let similarity = similarity.map(TryInto::try_into).transpose()?.unwrap_or_default();
            let embeddings = embeddings.map(TryInto::try_into).transpose()?;

            Ok(McpSemanticSearch { enable_assisted_discovery, embeddings, similarity })
        }
    }

    impl TryFrom<OrionRemoteEmbeddings> for RemoteEmbeddings {
        type Error = GenericError;
        fn try_from(orion: OrionRemoteEmbeddings) -> Result<Self, Self::Error> {
            let OrionRemoteEmbeddings { cluster, model_id, path, timeout, dimensions } = orion;
            let timeout = timeout
                .map(|d| -> Result<Duration, GenericError> {
                    let dur: RustType<Duration> = d.try_into()?;
                    Ok(dur.into_inner())
                })
                .transpose()?;
            RemoteEmbeddings {
                cluster: cluster.into(),
                model_id: model_id.into(),
                path,
                timeout,
                dimensions: dimensions as usize,
            }
            .normalized()
            .map_err(GenericError::from_msg)
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
        #![allow(clippy::assertions_on_result_states)]
        use super::*;

        #[test]
        #[allow(clippy::indexing_slicing)]
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
                            field: "role".to_owned(),
                            value: "admin".to_owned(),
                        })),
                    },
                    OrionPermission {
                        permission_type: Some(permission::PermissionType::JwtClaim(JwtClaimMatcher {
                            field: "department".to_owned(),
                            value: "engineering".to_owned(),
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
                McpRbacPermission::JwtHeader { .. } => panic!("Expected JwtClaim permission"),
            }

            match &rbac.permissions[1] {
                McpRbacPermission::JwtClaim { field, value } => {
                    assert_eq!(field.as_str(), "department");
                    assert_eq!(value.as_str(), "engineering");
                },
                McpRbacPermission::JwtHeader { .. } => panic!("Expected JwtClaim permission"),
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
                        field: "role".to_owned(),
                        value: "guest".to_owned(),
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
                embeddings: Some(OrionRemoteEmbeddings {
                    cluster: "embeddings".to_owned(),
                    model_id: "test-model".to_owned(),
                    path: "/v1/embeddings".to_owned(),
                    timeout: None,
                    dimensions: 384,
                }),
            };
            let parsed: McpSemanticSearch = orion.try_into().unwrap();
            assert!(parsed.enable_assisted_discovery);
            assert_eq!(parsed.similarity.top_k, 5);
            let embeddings = parsed.embeddings.expect("remote embeddings config");
            assert_eq!(embeddings.cluster.as_str(), "embeddings");
            assert_eq!(embeddings.model_id.as_str(), "test-model");
            assert_eq!(embeddings.path, "/v1/embeddings");
            assert_eq!(embeddings.dimensions, 384);
        }

        #[test]
        fn test_semantic_search_defaults_similarity() {
            let orion = OrionSemanticSearch { enable_assisted_discovery: false, similarity: None, embeddings: None };
            let parsed: McpSemanticSearch = orion.try_into().unwrap();
            assert_eq!(parsed.similarity.top_k, 10);
            assert!(parsed.embeddings.is_none());
        }

        #[test]
        fn test_semantic_search_empty_remote_cluster_is_rejected() {
            let result: Result<RemoteEmbeddings, _> = OrionRemoteEmbeddings {
                cluster: String::new(),
                model_id: "test-model".to_owned(),
                path: String::new(),
                timeout: None,
                dimensions: 384,
            }
            .try_into();
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("cluster"));
        }

        fn remote(cluster: &str, model_id: &str, path: &str, dimensions: usize) -> RemoteEmbeddings {
            RemoteEmbeddings {
                cluster: cluster.into(),
                model_id: model_id.into(),
                path: path.to_owned(),
                timeout: None,
                dimensions,
            }
        }

        #[test]
        fn normalized_prepends_leading_slash_and_defaults_empty_path() {
            assert_eq!(remote("c", "m", "v1/embeddings", 384).normalized().unwrap().path, "/v1/embeddings");
            assert_eq!(remote("c", "m", "", 384).normalized().unwrap().path, RemoteEmbeddings::default_path());
            assert_eq!(remote("c", "m", "/custom", 384).normalized().unwrap().path, "/custom");
        }

        #[test]
        fn normalized_rejects_empty_cluster_empty_model_and_zero_dimensions() {
            assert!(remote("", "m", "/p", 384).normalized().is_err());
            assert!(remote("c", "", "/p", 384).normalized().is_err());
            assert!(remote("c", "m", "/p", 0).normalized().is_err());
            assert!(remote("c", "m", "/p", 384).normalized().is_ok());
        }
    }
}
