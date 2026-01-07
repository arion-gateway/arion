use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpGateway {
    pub server_info: McpServerInfo,
    pub supported_protocol_versions: Vec<String>,
    pub tools: Vec<McpTool>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpServerInfo {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    //pub input_schema: serde_json::Map<String, serde_json::Value>,
    //pub backend: McpBackend,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum McpBackend {
    Rest(RestBackend),
    //FunctionGraph,
    //Mcp,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct RestBackend {
    method: String,
    path: String,
    params: Vec<String>,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::{required, GenericError};

    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        McpGateway as OrionMcpGateway, ServerInfo as OrionMcpServerInfo, Tool as OrionTool,
    };

    impl TryFrom<OrionMcpGateway> for McpGateway {
        type Error = GenericError;
        fn try_from(orion: OrionMcpGateway) -> Result<Self, Self::Error> {
            let OrionMcpGateway { server_info, supported_protocol_versions, tools } = orion;
            let server_info = required!(server_info)?;
            //let capabilities = required!(capabilities)?;

            Ok(McpGateway {
                server_info: server_info.into(),
                supported_protocol_versions,
                tools: tools.into_iter().map(Into::into).collect(),
            })
        }
    }

    impl From<OrionMcpServerInfo> for McpServerInfo {
        fn from(orion: OrionMcpServerInfo) -> Self {
            McpServerInfo { name: orion.name, version: orion.version }
        }
    }

    impl From<OrionTool> for McpTool {
        fn from(orion: OrionTool) -> Self {
            let OrionTool { name, description, input_schema, backend } = orion;
            //McpTool { name, description, input_schema, backend: backend.into() }
            McpTool { name, description }
        }
    }
}
