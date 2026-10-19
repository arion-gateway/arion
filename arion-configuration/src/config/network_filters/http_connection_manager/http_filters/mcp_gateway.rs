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

pub mod openapi;

use std::time::Duration;

use crate::config::core::DataSource;
use crate::config::network_filters::http_connection_manager::RetryPolicy;
use crate::config::network_filters::network_rbac::Action;
use arion_interner::InternedStr;
use http::uri::Authority;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol_str::{SmolStr, format_smolstr};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpGateway {
    pub server_info: McpServerInfo,
    pub mode: McpGatewayMode,
    #[serde(default)]
    pub search: McpToolSearch,
    pub tools: Vec<McpTool>,
    #[serde(default)]
    pub upstream_limits: UpstreamLimits,
    pub dynamic_mcp_servers: Vec<DynamicMcpServer>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_api_tool_sources: Vec<McpOpenApiToolSource>,
    pub tds: Option<TdsSpecifier>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpServerInfo {
    pub name: InternedStr,
    pub version: SmolStr,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum McpGatewayMode {
    Tools(McpToolsMode),
    Code(McpCodeMode),
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpToolsMode;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpCodeMode {
    #[serde(default = "McpCodeMode::default_execution_timeout", with = "humantime_serde")]
    pub execution_timeout: Duration,
    #[serde(default = "McpCodeMode::default_max_host_calls")]
    pub max_host_calls: usize,
    #[serde(default = "McpCodeMode::default_max_concurrent_executions")]
    pub max_concurrent_executions: usize,
    #[serde(default = "McpCodeMode::default_max_concurrent_tool_calls")]
    pub max_concurrent_tool_calls: usize,
    #[serde(default = "McpCodeMode::default_max_output_bytes")]
    pub max_output_bytes: usize,
    #[serde(default = "McpCodeMode::default_memory_limit_bytes")]
    pub memory_limit_bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_artifact_path: Option<SmolStr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub toolkits: Vec<McpToolkit>,
}

impl McpCodeMode {
    pub const fn default_execution_timeout() -> Duration {
        Duration::from_secs(30)
    }

    pub const fn default_max_host_calls() -> usize {
        100
    }

    pub const fn default_max_concurrent_executions() -> usize {
        32
    }

    pub const fn default_max_concurrent_tool_calls() -> usize {
        8
    }

    pub const fn default_max_output_bytes() -> usize {
        1024 * 1024
    }

    pub const fn default_memory_limit_bytes() -> usize {
        64 * 1024 * 1024
    }
}

impl Default for McpCodeMode {
    fn default() -> Self {
        Self {
            execution_timeout: Self::default_execution_timeout(),
            max_host_calls: Self::default_max_host_calls(),
            max_concurrent_executions: Self::default_max_concurrent_executions(),
            max_concurrent_tool_calls: Self::default_max_concurrent_tool_calls(),
            max_output_bytes: Self::default_max_output_bytes(),
            memory_limit_bytes: Self::default_memory_limit_bytes(),
            engine_artifact_path: None,
            toolkits: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpToolkit {
    pub name: InternedStr,
    pub description: SmolStr,
    pub instructions: SmolStr,
    pub tools: Vec<McpToolkitToolRef>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpToolkitToolRef {
    pub namespace: InternedStr,
    pub name: InternedStr,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpToolSearch {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub embeddings: Option<RemoteEmbeddings>,
    #[serde(default = "McpToolSearch::default_max_allowable_results")]
    pub max_allowable_results: usize,
}

impl McpToolSearch {
    pub const fn default_max_allowable_results() -> usize {
        5
    }
}

impl Default for McpToolSearch {
    fn default() -> Self {
        Self { embeddings: None, max_allowable_results: Self::default_max_allowable_results() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteEmbeddings {
    pub cluster: InternedStr,
    pub model_id: SmolStr,
    #[serde(default = "RemoteEmbeddings::default_path", skip_serializing_if = "RemoteEmbeddings::is_default_path")]
    pub path: SmolStr,
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none", default)]
    pub timeout: Option<Duration>,
    pub dimensions: usize,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_batch_size: Option<usize>,
}

pub const REMOTE_EMBEDDINGS_PATH: &str = "/v1/embeddings";

impl RemoteEmbeddings {
    pub fn default_path() -> SmolStr {
        SmolStr::new_inline(REMOTE_EMBEDDINGS_PATH)
    }

    fn is_default_path(path: &str) -> bool {
        path == REMOTE_EMBEDDINGS_PATH
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
        if self.max_batch_size == Some(0) {
            return Err("RemoteEmbeddings.max_batch_size must be greater than zero".to_owned());
        }
        if self.path.is_empty() {
            self.path = Self::default_path();
        } else if !self.path.starts_with('/') {
            self.path = format_smolstr!("/{}", self.path);
        }
        Ok(self)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpTool {
    pub namespace: InternedStr,
    pub name: SmolStr,
    pub description: SmolStr,
    pub input_schema: Map<String, Value>,
    pub output_schema: Map<String, Value>,
    pub backend: UpstreamBackend,
    pub rbac: Option<McpToolRbac>,
    #[serde(default, skip_serializing_if = "EmbeddingVector::is_empty")]
    pub embedding: EmbeddingVector,
    #[serde(default)]
    pub disclosure_mode: DisclosureMode,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DisclosureMode {
    #[default]
    Progressive,
    Eager,
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
        path: SmolStr,
        query_params: Vec<McpRestQueryParams>,
        cluster: InternedStr,
        #[serde(default)]
        upstream_policy: Box<HttpUpstreamPolicy>,
        body_template: Option<SmolStr>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body_json: Option<SmolStr>,
    },
    McpServer {
        transport: McpBackendTransportUpstream,
        path: SmolStr,
        /// Cluster used to reach the MCP server (endpoints, TLS, HTTP/1 or HTTP/2, Host/:authority).
        cluster: InternedStr,
    },
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DynamicMcpServer {
    pub namespace: InternedStr,
    pub description: SmolStr,
    pub transport: McpBackendTransportUpstream,
    pub path: SmolStr,
    /// Cluster used to reach the MCP server (endpoints, TLS, HTTP/1 or HTTP/2, Host/:authority).
    pub cluster: InternedStr,
    pub cache_duration: Option<Duration>,
    pub rbac: Option<McpToolRbac>,
    #[serde(default)]
    pub tool_disclosure_mode: DisclosureMode,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpOpenApiToolSource {
    pub namespace: InternedStr,
    pub tools: Vec<McpTool>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TdsSpecifier {
    pub config_name: InternedStr,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use std::{num::NonZeroUsize, str::FromStr};

    use arion_interner::{MAX_ENTRIES_ENV_VAR, StringInterner};

    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::core::RustType;
    use crate::config::{GenericError, WithNodeOnResult, required};

    use super::*;
    use arion_data_plane_api::envoy_data_plane_api::arion::extensions::filters::http::mcp::mcp_gateway::v3::{
        CodeMode as ArionCodeMode, DisclosureMode as ArionDisclosureMode, DynamicMcpServer as ArionDynamicMcpServer,
        HttpUpstreamPolicy as ArionHttpUpstreamPolicy, JwtClaimMatcher, JwtHeaderMatcher,
        McpGateway as ArionMcpGateway, OpenApiToolSource as ArionOpenApiToolSource, Permission as ArionPermission,
        QueryParam as ArionMcpQueryParams, RemoteEmbeddings as ArionRemoteEmbeddings, ServerInfo as ArionMcpServerInfo,
        TdsSpecifier as ArionTdsSpecifier, Tool as ArionTool, ToolRbac as ArionToolRbac, ToolSearch as ArionToolSearch,
        Toolkit as ArionToolkit, ToolkitToolRef as ArionToolkitToolRef, mcp_gateway::Mode as ArionMcpGatewayMode,
        mcp_server_backend::TransportUpstream as ArionTransportUpstream, permission,
        rest_backend::Body as ArionRestBody, tool::UpstreamBackend as ArionUpstreamBackend,
        tool_rbac::Action as ArionAction,
    };
    use tracing::{info, warn};

    fn intern_identifier(value: &str, field: &'static str) -> Result<InternedStr, GenericError> {
        value.try_to_interned_str().ok_or_else(|| {
            GenericError::from_msg(format!("{field}: string interner is full; raise {MAX_ENTRIES_ENV_VAR}"))
        })
    }

    impl From<ArionTransportUpstream> for McpBackendTransportUpstream {
        fn from(trans: ArionTransportUpstream) -> Self {
            match trans {
                ArionTransportUpstream::Sse => McpBackendTransportUpstream::Sse,
                ArionTransportUpstream::StreamableHttp => McpBackendTransportUpstream::StreamableHttp,
            }
        }
    }

    impl From<ArionDisclosureMode> for DisclosureMode {
        fn from(mode: ArionDisclosureMode) -> Self {
            match mode {
                ArionDisclosureMode::Progressive => Self::Progressive,
                ArionDisclosureMode::Eager => Self::Eager,
            }
        }
    }

    impl TryFrom<ArionMcpGateway> for McpGateway {
        type Error = GenericError;
        fn try_from(arion: ArionMcpGateway) -> Result<Self, Self::Error> {
            let ArionMcpGateway {
                server_info,
                mode,
                search,
                tools,
                open_api_tool_sources,
                upstream_timeout,
                max_upstream_response_bytes,
                dynamic_mcp_servers,
                tds,
            } = arion;
            let server_info = required!(server_info)?;
            let mode = match required!(mode)? {
                ArionMcpGatewayMode::ToolsMode(_) => McpGatewayMode::Tools(McpToolsMode),
                ArionMcpGatewayMode::CodeMode(code_mode) => McpGatewayMode::Code(code_mode.try_into()?),
            };
            let search = search.map(TryInto::try_into).transpose()?.unwrap_or_default();
            let tools = tools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
            let open_api_tool_sources =
                open_api_tool_sources.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
            let upstream_limits = parse_upstream_limits(upstream_timeout, max_upstream_response_bytes)?;
            let dynamic_mcp_servers =
                dynamic_mcp_servers.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
            let tds = tds.map(TryInto::try_into).transpose()?;

            Ok(McpGateway {
                server_info: server_info.try_into()?,
                mode,
                search,
                tools,
                upstream_limits,
                dynamic_mcp_servers,
                open_api_tool_sources,
                tds,
            })
        }
    }

    impl TryFrom<ArionCodeMode> for McpCodeMode {
        type Error = GenericError;

        fn try_from(arion: ArionCodeMode) -> Result<Self, Self::Error> {
            let ArionCodeMode {
                execution_timeout,
                max_host_calls,
                max_concurrent_executions,
                max_concurrent_tool_calls,
                max_output_bytes,
                memory_limit_bytes,
                engine_artifact_path,
                toolkits,
            } = arion;

            let toolkits = toolkits.into_iter().map(TryInto::try_into).collect::<Result<Vec<McpToolkit>, _>>()?;

            Ok(Self {
                execution_timeout: execution_timeout
                    .map(parse_positive_duration)
                    .transpose()
                    .with_node("execution_timeout")?
                    .unwrap_or_else(Self::default_execution_timeout),
                max_host_calls: usize_with_default(max_host_calls, Self::default_max_host_calls()),
                max_concurrent_executions: usize_with_default(
                    max_concurrent_executions,
                    Self::default_max_concurrent_executions(),
                ),
                max_concurrent_tool_calls: usize_with_default(
                    max_concurrent_tool_calls,
                    Self::default_max_concurrent_tool_calls(),
                ),
                max_output_bytes: usize_from_u64_with_default(max_output_bytes, Self::default_max_output_bytes())?,
                memory_limit_bytes: usize_from_u64_with_default(
                    memory_limit_bytes,
                    Self::default_memory_limit_bytes(),
                )?,
                engine_artifact_path: engine_artifact_path.map(Into::into),
                toolkits,
            })
        }
    }

    // Registry validation also covers resources received over TDS.
    impl TryFrom<ArionToolkit> for McpToolkit {
        type Error = GenericError;

        fn try_from(toolkit: ArionToolkit) -> Result<Self, Self::Error> {
            Ok(Self {
                name: intern_identifier(&toolkit.name, "Toolkit.name")?,
                description: toolkit.description.into(),
                instructions: toolkit.instructions.into(),
                tools: toolkit.tools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?,
            })
        }
    }

    impl TryFrom<ArionToolkitToolRef> for McpToolkitToolRef {
        type Error = GenericError;

        fn try_from(tool: ArionToolkitToolRef) -> Result<Self, Self::Error> {
            Ok(Self {
                namespace: intern_identifier(&tool.namespace, "ToolkitToolRef.namespace")?,
                name: intern_identifier(&tool.name, "ToolkitToolRef.name")?,
            })
        }
    }

    fn usize_with_default(value: u32, default: usize) -> usize {
        if value == 0 { default } else { value as usize }
    }

    fn usize_from_u64_with_default(value: u64, default: usize) -> Result<usize, GenericError> {
        if value == 0 { Ok(default) } else { Ok(usize::try_from(value)?) }
    }

    fn parse_upstream_limits(
        upstream_timeout: Option<arion_data_plane_api::envoy_data_plane_api::google::protobuf::Duration>,
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
                    .map_err(|_err| GenericError::from_msg("must be greater than zero"))
            })
            .transpose()
            .with_node("max_upstream_response_bytes")?
            .unwrap_or(UpstreamLimits::DEFAULT_MAX_RESPONSE_BYTES);

        Ok(UpstreamLimits { timeout, max_response_bytes })
    }

    fn parse_positive_duration(
        duration: arion_data_plane_api::envoy_data_plane_api::google::protobuf::Duration,
    ) -> Result<Duration, GenericError> {
        let duration = RustType::<Duration>::try_from(duration)?.into_inner();
        (!duration.is_zero()).then_some(duration).ok_or_else(|| GenericError::from_msg("must be greater than zero"))
    }

    fn validate_mcp_path(path: String, field: &'static str) -> Result<String, GenericError> {
        let path = required!(path).with_node(field)?;
        let uri = http::Uri::from_str(&path)
            .map_err(|error| GenericError::from_msg_with_cause(format!("Invalid {field}"), error))?;
        if uri.scheme().is_some() || uri.authority().is_some() {
            return Err(GenericError::from_msg(format!("{field} must be a request path, not an absolute URL")));
        }
        Ok(path)
    }

    fn validate_rest_path(path: String) -> Result<String, GenericError> {
        let path = required!(path).with_node("RestBackend.path")?;

        // Ensure any statically defined paths are valid.
        if !path.contains('{') && !path.contains('}') {
            let normalized = (!path.starts_with('/')).then(|| format!("/{path}"));
            let path_and_query = normalized.as_deref().unwrap_or(&path);
            http::uri::PathAndQuery::from_str(path_and_query)
                .map_err(|error| GenericError::from_msg_with_cause("Invalid RestBackend.path", error))?;
        }

        Ok(path)
    }

    impl TryFrom<ArionHttpUpstreamPolicy> for HttpUpstreamPolicy {
        type Error = GenericError;

        fn try_from(arion: ArionHttpUpstreamPolicy) -> Result<Self, Self::Error> {
            let authority = arion
                .authority
                .map(|authority| {
                    Authority::from_str(&authority)
                        .map_err(|e| GenericError::from_msg_with_cause("Invalid HttpUpstreamPolicy.authority", e))
                })
                .transpose()?;
            let timeout = arion.timeout.map(parse_positive_duration).transpose().with_node("timeout")?;
            let retry_policy = arion.retry_policy.map(TryInto::try_into).transpose().with_node("retry_policy")?;

            Ok(Self { authority, timeout, retry_policy })
        }
    }

    impl TryFrom<ArionDynamicMcpServer> for DynamicMcpServer {
        type Error = GenericError;
        fn try_from(arion: ArionDynamicMcpServer) -> Result<Self, Self::Error> {
            let transport = arion.transport().into();
            let tool_disclosure_mode = arion.tool_disclosure_mode().into();
            let ArionDynamicMcpServer { namespace, description, path, cluster, cache_duration, rbac, .. } = arion;
            if namespace.is_empty() {
                return Err(GenericError::from_msg("DynamicMcpServer.namespace must not be empty"));
            }
            let path = validate_mcp_path(path, "DynamicMcpServer.path")?;
            if cluster.is_empty() {
                return Err(GenericError::from_msg("DynamicMcpServer.cluster must not be empty"));
            }
            let cache_duration = cache_duration.map(parse_positive_duration).transpose().with_node("cache_duration")?;

            Ok(DynamicMcpServer {
                namespace: intern_identifier(&namespace, "DynamicMcpServer.namespace")?,
                description: description.into(),
                transport,
                path: path.into(),
                cluster: intern_identifier(&cluster, "DynamicMcpServer.cluster")?,
                cache_duration,
                rbac: rbac.map(TryInto::try_into).transpose()?,
                tool_disclosure_mode,
            })
        }
    }

    impl TryFrom<ArionOpenApiToolSource> for McpOpenApiToolSource {
        type Error = GenericError;
        fn try_from(arion: ArionOpenApiToolSource) -> Result<Self, Self::Error> {
            let tool_disclosure_mode = arion.tool_disclosure_mode().into();
            let ArionOpenApiToolSource {
                spec,
                namespace,
                cluster,
                upstream_policy,
                rbac,
                path_prefix,
                skip_unsupported_operations,
                include_operations,
                exclude_operations,
                ..
            } = arion;
            let namespace = required!(namespace).with_node("OpenApiToolSource.namespace")?;
            let cluster = required!(cluster).with_node("OpenApiToolSource.cluster")?;
            let spec = TryInto::<DataSource>::try_into(required!(spec)?)?.to_bytes_blocking()?;
            let params = openapi::OpenApiExpansionParams {
                namespace: intern_identifier(&namespace, "OpenApiToolSource.namespace")?,
                cluster: intern_identifier(&cluster, "OpenApiToolSource.cluster")?,
                upstream_policy: upstream_policy.map(TryInto::try_into).transpose()?.unwrap_or_default(),
                rbac: rbac.map(TryInto::try_into).transpose()?,
                tool_disclosure_mode,
                path_prefix,
                skip_unsupported_operations,
                include_operations,
                exclude_operations,
            };
            let tools = openapi::openapi_to_tools(&spec, &params).with_node("OpenApiToolSource")?;
            info!(
                "Derived {} MCP tools from OpenAPI source '{}': [{}]",
                tools.len(),
                params.namespace,
                tools.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>().join(", ")
            );
            Ok(McpOpenApiToolSource { namespace: params.namespace, tools })
        }
    }

    impl TryFrom<ArionTdsSpecifier> for TdsSpecifier {
        type Error = GenericError;
        fn try_from(arion: ArionTdsSpecifier) -> Result<Self, Self::Error> {
            if arion.config_name.is_empty() {
                return Err(GenericError::from_msg(
                    "TdsSpecifier.config_name specifying the resource name from the xDS Tools Discovery Service must not be empty",
                ));
            }
            if arion.config_name.contains('/') {
                return Err(GenericError::from_msg(
                    "TdsSpecifier.config_name must not contain '/' (used as xDS resource-id separator)",
                ));
            }
            Ok(TdsSpecifier { config_name: intern_identifier(&arion.config_name, "TdsSpecifier.config_name")? })
        }
    }

    impl TryFrom<ArionTool> for McpTool {
        type Error = GenericError;

        fn try_from(arion: ArionTool) -> Result<Self, Self::Error> {
            let disclosure_mode = arion.disclosure_mode().into();
            let ArionTool {
                namespace,
                name,
                description,
                input_schema,
                output_schema,
                upstream_backend,
                rbac,
                embedding,
                ..
            } = arion;
            let description = required!(description).with_node("Tool.description")?;
            let backend = required!(upstream_backend)?.try_into()?;

            let validate_schema_fn = |schema, name| -> Result<Map<String, Value>, GenericError> {
                if let Some(schema) = schema {
                    let bytes = TryInto::<DataSource>::try_into(schema)?.to_bytes_blocking()?;
                    let string = String::from_utf8(bytes)?;
                    let schema: Map<String, Value> = serde_json::from_str(&string)?;
                    // Verify the schema is valid; we are still storing the raw
                    // schema and recreate the validator in arion-lib as
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
                UpstreamBackend::Rest { .. } => validate_schema_fn(input_schema, "input")?,
                UpstreamBackend::McpServer { .. } => {
                    // input_schema is ignored for MCP backends
                    if input_schema.is_some() {
                        warn!("input_schema is ignored for MCP backends");
                    }
                    Map::new()
                },
            };

            let output_schema = match backend {
                UpstreamBackend::Rest { .. } => validate_schema_fn(output_schema, "output")?,
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
                namespace: intern_identifier(&namespace, "Tool.namespace")?,
                name: name.into(),
                description: description.into(),
                input_schema,
                output_schema,
                backend,
                rbac,
                embedding: EmbeddingVector(embedding),
                disclosure_mode,
            })
        }
    }

    impl TryFrom<ArionUpstreamBackend> for UpstreamBackend {
        type Error = GenericError;

        fn try_from(value: ArionUpstreamBackend) -> Result<Self, GenericError> {
            match value {
                ArionUpstreamBackend::RestBackend(be) => {
                    let cluster = be.cluster;
                    let cluster = required!(cluster)?;
                    let path = validate_rest_path(be.path)?;
                    let (body_template, body_json) = match be.body {
                        Some(ArionRestBody::BodyTemplate(template)) => {
                            let template: DataSource = template.try_into()?;
                            let template = String::from_utf8(template.to_bytes_blocking()?)?.into();
                            (Some(template), None)
                        },
                        Some(ArionRestBody::BodyJson(source)) => {
                            let source = required!(source).with_node("RestBackend.body_json")?;
                            (None, Some(source.into()))
                        },
                        None => (None, None),
                    };
                    Ok(UpstreamBackend::Rest {
                        method: http::Method::from_str(&be.method)?,
                        path: path.into(),
                        query_params: be.query_params.into_iter().map(TryInto::try_into).collect::<Result<_, _>>()?,
                        cluster: intern_identifier(&cluster, "RestBackend.cluster")?,
                        upstream_policy: Box::new(
                            be.upstream_policy.map(TryInto::try_into).transpose()?.unwrap_or_default(),
                        ),
                        body_template,
                        body_json,
                    })
                },
                ArionUpstreamBackend::McpServerBackend(be) => {
                    let transport = be.transport().into();
                    let path = validate_mcp_path(be.path, "McpServerBackend.path")?;
                    if be.cluster.is_empty() {
                        return Err(GenericError::from_msg("McpServerBackend.cluster must not be empty"));
                    }
                    Ok(UpstreamBackend::McpServer {
                        transport,
                        path: path.into(),
                        cluster: intern_identifier(&be.cluster, "McpServerBackend.cluster")?,
                    })
                },
            }
        }
    }

    impl TryFrom<ArionMcpQueryParams> for McpRestQueryParams {
        type Error = GenericError;

        fn try_from(arion: ArionMcpQueryParams) -> Result<Self, Self::Error> {
            let ArionMcpQueryParams { name, source } = arion;
            Ok(McpRestQueryParams {
                name: required!(name).with_node("QueryParam.name")?.into(),
                source: required!(source).with_node("QueryParam.source")?.into(),
            })
        }
    }

    impl TryFrom<ArionMcpServerInfo> for McpServerInfo {
        type Error = GenericError;
        fn try_from(arion: ArionMcpServerInfo) -> Result<Self, Self::Error> {
            if arion.name.is_empty() {
                return Err(GenericError::from_msg("McpServerInfo.name must not be empty"));
            }
            if arion.name.contains('/') {
                return Err(GenericError::from_msg(
                    "McpServerInfo.name must not contain '/' (used as xDS resource-id separator)",
                ));
            }
            Ok(McpServerInfo {
                name: intern_identifier(&arion.name, "McpServerInfo.name")?,
                version: arion.version.into(),
            })
        }
    }

    impl From<ArionAction> for Action {
        fn from(action: ArionAction) -> Self {
            match action {
                ArionAction::Allow => Action::Allow,
                ArionAction::Deny => Action::Deny,
            }
        }
    }

    impl TryFrom<ArionToolRbac> for McpToolRbac {
        type Error = GenericError;
        fn try_from(arion: ArionToolRbac) -> Result<Self, Self::Error> {
            let action = arion.action().into();
            let permissions = arion.permissions;
            if permissions.is_empty() {
                return Err(GenericError::from_msg("Tool RBAC must have at least one permission"));
            }

            let permissions = permissions.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;

            Ok(McpToolRbac { action, permissions })
        }
    }

    impl TryFrom<ArionPermission> for McpRbacPermission {
        type Error = GenericError;
        fn try_from(arion: ArionPermission) -> Result<Self, Self::Error> {
            let ArionPermission { permission_type } = arion;
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

    impl TryFrom<ArionToolSearch> for McpToolSearch {
        type Error = GenericError;
        fn try_from(arion: ArionToolSearch) -> Result<Self, Self::Error> {
            let ArionToolSearch { max_allowable_results, embeddings } = arion;
            let max_allowable_results = max_allowable_results
                .map(|value| {
                    NonZeroUsize::new(value as usize)
                        .map(NonZeroUsize::get)
                        .ok_or_else(|| GenericError::from_msg("max_allowable_results must be greater than zero"))
                })
                .transpose()?
                .unwrap_or_else(McpToolSearch::default_max_allowable_results);
            let embeddings = embeddings.map(TryInto::try_into).transpose()?;

            Ok(McpToolSearch { embeddings, max_allowable_results })
        }
    }

    impl TryFrom<ArionRemoteEmbeddings> for RemoteEmbeddings {
        type Error = GenericError;
        fn try_from(arion: ArionRemoteEmbeddings) -> Result<Self, Self::Error> {
            let ArionRemoteEmbeddings { cluster, model_id, path, timeout, dimensions, max_batch_size } = arion;
            let timeout = timeout
                .map(|d| -> Result<Duration, GenericError> {
                    let dur: RustType<Duration> = d.try_into()?;
                    Ok(dur.into_inner())
                })
                .transpose()?;
            RemoteEmbeddings {
                cluster: intern_identifier(&cluster, "RemoteEmbeddings.cluster")?,
                model_id: model_id.into(),
                path: path.into(),
                timeout,
                dimensions: dimensions as usize,
                max_batch_size: max_batch_size.map(|value| value as usize),
            }
            .normalized()
            .map_err(GenericError::from_msg)
        }
    }

    #[cfg(test)]
    mod tests {
        #![allow(clippy::assertions_on_result_states)]
        use super::*;
        use arion_data_plane_api::envoy_data_plane_api::arion::extensions::filters::http::mcp::mcp_gateway::v3::RestBackend as ArionRestBackend;
        use arion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::{
            DataSource as EnvoyDataSource, data_source::Specifier as EnvoySpecifier,
        };

        fn rest_backend(path: &str, query_params: Vec<ArionMcpQueryParams>) -> ArionUpstreamBackend {
            ArionUpstreamBackend::RestBackend(ArionRestBackend {
                method: "GET".to_owned(),
                path: path.to_owned(),
                query_params,
                cluster: "upstream".to_owned(),
                ..Default::default()
            })
        }

        fn gateway_with_mode(mode: Option<ArionMcpGatewayMode>) -> ArionMcpGateway {
            ArionMcpGateway {
                server_info: Some(ArionMcpServerInfo { name: "gateway".to_owned(), version: "1.0".to_owned() }),
                mode,
                ..Default::default()
            }
        }

        #[test]
        fn gateway_requires_an_explicit_mode() {
            let result: Result<McpGateway, _> = gateway_with_mode(None).try_into();
            assert!(result.unwrap_err().to_string().contains("mode"));
        }

        #[test]
        fn code_mode_conversion_applies_defaults_and_preserves_toolkits() {
            let toolkit = ArionToolkit {
                name: "support".to_owned(),
                description: "Support workflows".to_owned(),
                instructions: "Look up the customer first".to_owned(),
                tools: vec![ArionToolkitToolRef { namespace: "crm".to_owned(), name: "lookup".to_owned() }],
            };
            let code_mode = ArionCodeMode { toolkits: vec![toolkit], ..Default::default() };
            let parsed: McpGateway =
                gateway_with_mode(Some(ArionMcpGatewayMode::CodeMode(code_mode))).try_into().unwrap();

            assert_eq!(parsed.search, McpToolSearch::default());
            let McpGatewayMode::Code(mode) = parsed.mode else { panic!("expected CodeMode") };
            assert_eq!(mode, McpCodeMode { toolkits: mode.toolkits.clone(), ..McpCodeMode::default() });
            assert_eq!(mode.toolkits.len(), 1);
            let toolkit = mode.toolkits.first().expect("one toolkit");
            assert_eq!(toolkit.name.as_str(), "support");
            let tool = toolkit.tools.first().expect("one tool");
            assert_eq!(tool.namespace.as_str(), "crm");
            assert_eq!(tool.name.as_str(), "lookup");
        }

        #[test]
        fn dynamic_mcp_server_conversion_rejects_an_invalid_path() {
            let result: Result<DynamicMcpServer, _> = ArionDynamicMcpServer {
                namespace: "example".to_owned(),
                path: "localhost:3000/mcp".to_owned(),
                cluster: "mcp".to_owned(),
                ..Default::default()
            }
            .try_into();

            assert!(result.unwrap_err().to_string().contains("DynamicMcpServer.path"));
        }

        #[test]
        fn rest_backend_conversion_rejects_invalid_static_paths() {
            for path in ["", "/invalid path"] {
                let result: Result<UpstreamBackend, _> = rest_backend(path, Vec::new()).try_into();
                assert!(result.unwrap_err().to_string().contains("RestBackend.path"));
            }
        }

        #[test]
        fn rest_backend_conversion_rejects_incomplete_query_params() {
            for query_param in [
                ArionMcpQueryParams { name: String::new(), source: "id".to_owned() },
                ArionMcpQueryParams { name: "id".to_owned(), source: String::new() },
            ] {
                let result: Result<UpstreamBackend, _> = rest_backend("/lookup", vec![query_param]).try_into();
                assert!(result.unwrap_err().to_string().contains("QueryParam."));
            }
        }

        #[test]
        #[allow(clippy::indexing_slicing)]
        fn test_tool_rbac_config_parsing() {
            use arion_data_plane_api::envoy_data_plane_api::arion::extensions::filters::http::mcp::mcp_gateway::v3::{
                JwtClaimMatcher, Permission as ArionPermission, ToolRbac as ArionToolRbac, permission,
            };

            // Create a sample tool RBAC configuration
            let arion_rbac = ArionToolRbac {
                action: 0, // ALLOW
                permissions: vec![
                    ArionPermission {
                        permission_type: Some(permission::PermissionType::JwtClaim(JwtClaimMatcher {
                            field: "role".to_owned(),
                            value: "admin".to_owned(),
                        })),
                    },
                    ArionPermission {
                        permission_type: Some(permission::PermissionType::JwtClaim(JwtClaimMatcher {
                            field: "department".to_owned(),
                            value: "engineering".to_owned(),
                        })),
                    },
                ],
            };

            // Convert to our internal format
            let rbac: McpToolRbac = arion_rbac.try_into().unwrap();

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
            use arion_data_plane_api::envoy_data_plane_api::arion::extensions::filters::http::mcp::mcp_gateway::v3::{
                JwtClaimMatcher, Permission as ArionPermission, ToolRbac as ArionToolRbac, permission,
            };

            let arion_rbac = ArionToolRbac {
                action: 1, // DENY
                permissions: vec![ArionPermission {
                    permission_type: Some(permission::PermissionType::JwtClaim(JwtClaimMatcher {
                        field: "role".to_owned(),
                        value: "guest".to_owned(),
                    })),
                }],
            };

            let rbac: McpToolRbac = arion_rbac.try_into().unwrap();
            assert_eq!(rbac.action, Action::Deny);
        }

        #[test]
        fn test_tool_rbac_validation_errors() {
            use arion_data_plane_api::envoy_data_plane_api::arion::extensions::filters::http::mcp::mcp_gateway::v3::ToolRbac as ArionToolRbac;

            // Test empty permissions
            let arion_rbac = ArionToolRbac { action: 0, permissions: vec![] };

            let result: Result<McpToolRbac, _> = arion_rbac.try_into();
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("at least one permission"));
        }

        #[test]
        fn test_tool_search_round_trip() {
            let arion = ArionToolSearch {
                max_allowable_results: Some(9),
                embeddings: Some(ArionRemoteEmbeddings {
                    cluster: "embeddings".to_owned(),
                    model_id: "test-model".to_owned(),
                    path: REMOTE_EMBEDDINGS_PATH.to_owned(),
                    timeout: None,
                    dimensions: 384,
                    max_batch_size: Some(32),
                }),
            };
            let parsed: McpToolSearch = arion.try_into().unwrap();
            assert_eq!(parsed.max_allowable_results, 9);
            let embeddings = parsed.embeddings.expect("remote embeddings config");
            assert_eq!(embeddings.cluster.as_str(), "embeddings");
            assert_eq!(embeddings.model_id.as_str(), "test-model");
            assert_eq!(embeddings.path, REMOTE_EMBEDDINGS_PATH);
            assert_eq!(embeddings.dimensions, 384);
            assert_eq!(embeddings.max_batch_size, Some(32));
        }

        #[test]
        fn test_tool_search_empty_remote_cluster_is_rejected() {
            let result: Result<RemoteEmbeddings, _> = ArionRemoteEmbeddings {
                cluster: String::new(),
                model_id: "test-model".to_owned(),
                path: String::new(),
                timeout: None,
                dimensions: 384,
                max_batch_size: None,
            }
            .try_into();
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("cluster"));
        }

        fn remote(cluster: &str, model_id: &str, path: &str, dimensions: usize) -> RemoteEmbeddings {
            RemoteEmbeddings {
                cluster: cluster.to_interned_str(),
                model_id: model_id.into(),
                path: path.into(),
                timeout: None,
                dimensions,
                max_batch_size: None,
            }
        }

        #[test]
        fn normalized_prepends_leading_slash_and_defaults_empty_path() {
            assert_eq!(remote("c", "m", "v1/embeddings", 384).normalized().unwrap().path, REMOTE_EMBEDDINGS_PATH);
            assert_eq!(remote("c", "m", "", 384).normalized().unwrap().path, RemoteEmbeddings::default_path());
            assert_eq!(remote("c", "m", "/custom", 384).normalized().unwrap().path, "/custom");
        }

        #[test]
        fn normalized_rejects_a_zero_max_batch_size() {
            let mut config = remote("c", "m", "/p", 384);
            config.max_batch_size = Some(0);
            assert!(config.normalized().unwrap_err().contains("max_batch_size"));
        }

        #[test]
        fn normalized_rejects_empty_cluster_empty_model_and_zero_dimensions() {
            assert!(remote("", "m", "/p", 384).normalized().is_err());
            assert!(remote("c", "", "/p", 384).normalized().is_err());
            assert!(remote("c", "m", "/p", 0).normalized().is_err());
            assert!(remote("c", "m", "/p", 384).normalized().is_ok());
        }

        fn inline_data_source(content: &str) -> EnvoyDataSource {
            EnvoyDataSource { specifier: Some(EnvoySpecifier::InlineString(content.to_owned())), ..Default::default() }
        }

        fn open_api_source(spec: &str) -> ArionOpenApiToolSource {
            ArionOpenApiToolSource {
                spec: Some(inline_data_source(spec)),
                namespace: "pets".to_owned(),
                cluster: "backend".to_owned(),
                ..Default::default()
            }
        }

        #[test]
        fn open_api_source_conversion_expands_tools() {
            let spec = "servers:\n  - url: /v1\npaths:\n  /pets/{id}:\n    get: { operationId: getPet }\n";
            let converted: McpOpenApiToolSource = open_api_source(spec).try_into().unwrap();
            assert_eq!(converted.namespace.as_str(), "pets");
            assert_eq!(converted.tools.len(), 1);
            let tool = &converted.tools[0];
            assert_eq!(tool.name, "getPet");
            match &tool.backend {
                UpstreamBackend::Rest { path, cluster, body_json, .. } => {
                    assert_eq!(path, "/v1/pets/{{id | encode_path_segment}}");
                    assert_eq!(cluster.as_str(), "backend");
                    assert_eq!(*body_json, None);
                },
                other => panic!("expected REST backend: {other:?}"),
            }
        }

        #[test]
        fn open_api_source_conversion_rejects_missing_fields() {
            let valid = || open_api_source("paths:\n  /a:\n    get: { operationId: get_a }\n");
            for (invalid, field) in [
                (ArionOpenApiToolSource { spec: None, ..valid() }, "spec"),
                (ArionOpenApiToolSource { namespace: String::new(), ..valid() }, "OpenApiToolSource.namespace"),
                (ArionOpenApiToolSource { cluster: String::new(), ..valid() }, "OpenApiToolSource.cluster"),
            ] {
                let result: Result<McpOpenApiToolSource, _> = invalid.try_into();
                assert!(result.unwrap_err().to_string().contains(field));
            }
        }

        fn convert_rest_backend_body(body: Option<ArionRestBody>) -> Result<UpstreamBackend, GenericError> {
            ArionUpstreamBackend::RestBackend(ArionRestBackend {
                method: "POST".to_owned(),
                path: "/a".to_owned(),
                cluster: "upstream".to_owned(),
                body,
                ..Default::default()
            })
            .try_into()
        }

        #[test]
        fn rest_backend_body_conversion_handles_each_mode() {
            let converted =
                convert_rest_backend_body(Some(ArionRestBody::BodyTemplate(inline_data_source(r#"{"a": "{{a}}"}"#))))
                    .unwrap();
            assert!(matches!(converted, UpstreamBackend::Rest { body_template: Some(_), body_json: None, .. }));

            let converted = convert_rest_backend_body(Some(ArionRestBody::BodyJson("body".to_owned()))).unwrap();
            assert!(matches!(
                converted,
                UpstreamBackend::Rest { body_template: None, body_json: Some(source), .. } if source == "body"
            ));

            let converted = convert_rest_backend_body(None).unwrap();
            assert!(matches!(converted, UpstreamBackend::Rest { body_template: None, body_json: None, .. }));
        }

        #[test]
        fn rest_backend_body_conversion_rejects_an_empty_json_path() {
            let error = convert_rest_backend_body(Some(ArionRestBody::BodyJson(String::new()))).unwrap_err();
            assert!(error.to_string().contains("RestBackend.body_json"));
        }
    }
}
