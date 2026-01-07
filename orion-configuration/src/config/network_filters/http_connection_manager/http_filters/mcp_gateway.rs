use crate::config::core::DataSource;
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
    pub input_schema: serde_json::Value,
    pub backend: McpBackend,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpQueryParams {
    pub name: String,
    pub source: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum McpBackend {
    Rest { method: String, path: String, params: Vec<McpQueryParams> },
    FunctionGraph {},
    Mcp {},
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::{required, GenericError};

    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        tool::Backend as OrionMcpBackend, McpGateway as OrionMcpGateway, QueryParam as OrionMcpQueryParams,
        ServerInfo as OrionMcpServerInfo, Tool as OrionTool,
    };

    impl TryFrom<OrionMcpGateway> for McpGateway {
        type Error = GenericError;
        fn try_from(orion: OrionMcpGateway) -> Result<Self, Self::Error> {
            let OrionMcpGateway { server_info, supported_protocol_versions, tools } = orion;
            let server_info = required!(server_info)?;

            let tools = tools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;

            Ok(McpGateway {
                server_info: server_info.into(),
                supported_protocol_versions,
                //tools: tools.into_iter().map(TryInto::try_into)?.collect(),
                tools,
            })
        }
    }

    impl TryFrom<OrionTool> for McpTool {
        type Error = GenericError;

        fn try_from(orion: OrionTool) -> Result<Self, Self::Error> {
            let OrionTool { name, description, input_schema, backend } = orion;
            let input_schema: DataSource = required!(input_schema)?.try_into()?;
            let backend = required!(backend)?.into();

            let bytes = input_schema.to_bytes_blocking()?;
            let string = String::from_utf8(bytes)?;
            let input_schema = serde_json::from_str(&string)?;
            Ok(McpTool { name, description, input_schema, backend })
        }
    }

    impl From<OrionMcpBackend> for McpBackend {
        fn from(orion: OrionMcpBackend) -> Self {
            match orion {
                OrionMcpBackend::Rest(rest_backend) => McpBackend::Rest {
                    method: rest_backend.method,
                    path: rest_backend.path,
                    params: rest_backend.params.into_iter().map(Into::into).collect(),
                },
                OrionMcpBackend::FunctionGraph(_) => todo!(),
                OrionMcpBackend::McpServer(_) => todo!(),
            }
        }
    }

    impl From<OrionMcpQueryParams> for McpQueryParams {
        fn from(orion: OrionMcpQueryParams) -> Self {
            McpQueryParams { name: orion.name, source: orion.source }
        }
    }

    impl From<OrionMcpServerInfo> for McpServerInfo {
        fn from(orion: OrionMcpServerInfo) -> Self {
            McpServerInfo { name: orion.name, version: orion.version }
        }
    }
}
