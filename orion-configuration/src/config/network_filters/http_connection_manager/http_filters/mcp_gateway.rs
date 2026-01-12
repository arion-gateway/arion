use crate::config::core::DataSource;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ClusterHeader(#[serde(with = "http_serde_ext::header_name")] pub http::HeaderName);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpGateway {
    pub cluster_header: Option<ClusterHeader>,
    pub server_info: McpServerInfo,
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
    pub input_schema: Map<String, Value>,
    pub backend: McpBackend,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpBackend {
    pub cluster: String,
    pub r#async: bool,
    pub transcoding: McpTranscoding,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum McpTranscoding {
    Rest {
        #[serde(with = "http_serde_ext::method")]
        method: http::Method,
        path: String,
        query_params: Vec<McpRestQueryParams>,
    },
    FunctionGraph {},
    Mcp {},
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpRestQueryParams {
    pub name: String,
    pub source: String,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use std::str::FromStr;

    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::{required, GenericError};

    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        backend::Transcoding as OrionTranscoding, Backend as OrionMcpBackend, McpGateway as OrionMcpGateway,
        QueryParam as OrionMcpQueryParams, ServerInfo as OrionMcpServerInfo, Tool as OrionTool,
    };

    impl TryFrom<OrionMcpGateway> for McpGateway {
        type Error = GenericError;
        fn try_from(orion: OrionMcpGateway) -> Result<Self, Self::Error> {
            let OrionMcpGateway { cluster_header, server_info, tools } = orion;
            let server_info = required!(server_info)?;
            let cluster_header: Option<http::HeaderName> = cluster_header.map(TryInto::try_into).transpose()?;
            let cluster_header = cluster_header.map(ClusterHeader);

            let tools = tools.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
            Ok(McpGateway { cluster_header, server_info: server_info.into(), tools })
        }
    }

    impl TryFrom<OrionTool> for McpTool {
        type Error = GenericError;

        fn try_from(orion: OrionTool) -> Result<Self, Self::Error> {
            let OrionTool { name, description, input_schema, backend } = orion;
            let input_schema: DataSource = required!(input_schema)?.try_into()?;
            let backend = required!(backend)?.try_into()?;

            let bytes = input_schema.to_bytes_blocking()?;
            let string = String::from_utf8(bytes)?;
            let input_schema = serde_json::from_str(&string)?;
            Ok(McpTool { name, description, input_schema, backend })
        }
    }

    impl TryFrom<OrionMcpBackend> for McpBackend {
        type Error = GenericError;

        fn try_from(orion: OrionMcpBackend) -> Result<Self, GenericError> {
            let OrionMcpBackend { cluster, r#async, transcoding } = orion;
            let cluster = required!(cluster)?;
            let transcoding = required!(transcoding)?;

            match transcoding {
                OrionTranscoding::RestTranscoding(trans) => Ok(McpBackend {
                    cluster,
                    r#async,
                    transcoding: McpTranscoding::Rest {
                        method: http::Method::from_str(&trans.method)?,
                        path: trans.path,
                        query_params: trans.query_params.into_iter().map(Into::into).collect(),
                    },
                }),
                OrionTranscoding::FunctionGraphTranscoding(_) => todo!(),
                OrionTranscoding::McpServerTranscoding(_) => todo!(),
            }
        }
    }

    impl From<OrionMcpQueryParams> for McpRestQueryParams {
        fn from(orion: OrionMcpQueryParams) -> Self {
            McpRestQueryParams { name: orion.name, source: orion.source }
        }
    }

    impl From<OrionMcpServerInfo> for McpServerInfo {
        fn from(orion: OrionMcpServerInfo) -> Self {
            McpServerInfo { name: orion.name, version: orion.version }
        }
    }
}
