use std::collections::HashMap;
use std::time::Duration;

use crate::config::network_filters::http_connection_manager::metadata_matcher::MetadataMatcher;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpGateway {
    #[serde(with = "http_serde_ext::header_name")]
    pub cluster_header: http::HeaderName,
    pub server_info: McpServerInfo,
    pub capabilities: McpCapabilities,
    pub supported_protocol_versions: Vec<String>,
    pub session: Option<McpSession>,
    pub tools: Vec<McpTool>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpServerInfo {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpCapabilities {
    pub tools: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpSession {
    pub enabled: bool,
    pub ttl: Duration,
    pub cache_client_init: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: InputSchema,
    pub rbac: McpRbac,
}

// todo(francesco): this is an Any object in protobuf definition and each tool
// defines its own input schema, need to find a way to deserialize it
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct InputSchema {
    pub r#type: String,
    pub properties: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpRbac {
    pub r#match: MetadataMatcher,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::{required, GenericError};

    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        Capabilities as OrionCapabilities, McpGateway as OrionMcpGateway, ServerInfo as OrionMcpServerInfo,
        SessionConfig as OrionSessionConfig, Tool as OrionTool,
    };

    impl TryFrom<OrionMcpGateway> for McpGateway {
        type Error = GenericError;
        fn try_from(orion: OrionMcpGateway) -> Result<Self, Self::Error> {
            let OrionMcpGateway {
                cluster_header,
                server_info,
                capabilities,
                supported_protocol_versions,
                session,
                tools,
                upstream_auth,
            } = orion;
            let server_info = required!(server_info)?;
            let capabilities = required!(capabilities)?;

            Ok(McpGateway {
                cluster_header: cluster_header.try_into()?,
                server_info: server_info.into(),
                capabilities: capabilities.into(),
                supported_protocol_versions,
                session: session.map(Into::into),
                tools: tools.into_iter().map(Into::into).collect(),
            })
        }
    }

    impl From<OrionMcpServerInfo> for McpServerInfo {
        fn from(orion: OrionMcpServerInfo) -> Self {
            McpServerInfo { name: orion.name, version: orion.version }
        }
    }

    impl From<OrionCapabilities> for McpCapabilities {
        fn from(orion: OrionCapabilities) -> Self {
            let tools = orion.tools.into_iter().map(|(k, v)| (k, v.value)).collect();
            McpCapabilities { tools }
        }
    }

    impl From<OrionSessionConfig> for McpSession {
        fn from(orion: OrionSessionConfig) -> Self {
            let OrionSessionConfig { enabled, ttl, cache_client_init } = orion;
            let duration = ttl.map(|x| Duration::new(x.seconds as u64, x.nanos as u32));
            McpSession { enabled, ttl: duration.unwrap_or(Duration::from_secs(3600)), cache_client_init }
        }
    }

    impl From<OrionTool> for McpTool {
        fn from(orion: OrionTool) -> Self {
            let OrionTool { name, description, input_schema, rbac, backend } = orion;
            McpTool { name, description, input_schema, rbac, backend }
        }
    }
}
