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
    pub backend: UpstreamBackend,
    pub rbac: Option<McpToolRbac>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum UpstreamBackend {
    Rest {
        #[serde(with = "http_serde_ext::method")]
        method: http::Method,
        path: String,
        query_params: Vec<McpRestQueryParams>,
        cluster: String,
        r#async: bool,
    },
    McpServer {
        transport: McpBackendTransportUpstream,
        url: String,
        cache_duration: Option<Duration>,
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
    pub name: String,
    pub source: String,
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

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use std::str::FromStr;

    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::core::RustType;
    use crate::config::{required, GenericError};

    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        mcp_server_backend::TransportUpstream as OrionTransportUpstream, tool::UpstreamBackend as OrionUpstreamBackend,
        McpGateway as OrionMcpGateway, QueryParam as OrionMcpQueryParams, ServerInfo as OrionMcpServerInfo,
        Tool as OrionTool,
    };

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
            let OrionTool { name, description, input_schema, upstream_backend, rbac } = orion;
            let input_schema: DataSource = required!(input_schema)?.try_into()?;
            let backend = required!(upstream_backend)?.try_into()?;

            let bytes = input_schema.to_bytes_blocking()?;
            let string = String::from_utf8(bytes)?;
            let input_schema = serde_json::from_str(&string)?;
            let rbac = rbac.map(TryInto::try_into).transpose()?;
            Ok(McpTool { name, description, input_schema, backend, rbac })
        }
    }

    impl TryFrom<OrionUpstreamBackend> for UpstreamBackend {
        type Error = GenericError;

        fn try_from(value: OrionUpstreamBackend) -> Result<Self, GenericError> {
            match value {
                OrionUpstreamBackend::RestBackend(be) => {
                    let cluster = be.cluster;
                    let cluster = required!(cluster)?;
                    Ok(UpstreamBackend::Rest {
                        method: http::Method::from_str(&be.method)?,
                        path: be.path,
                        query_params: be.query_params.into_iter().map(Into::into).collect(),
                        cluster,
                        r#async: be.r#async,
                    })
                },
                OrionUpstreamBackend::McpServerBackend(trans) => Ok(UpstreamBackend::McpServer {
                    transport: trans.transport().into(),
                    url: trans.url,
                    cache_duration: trans
                        .cache_duration
                        .map(|d| -> Result<Duration, GenericError> {
                            let dur: RustType<Duration> = d.try_into()?;
                            Ok(dur.into_inner())
                        })
                        .transpose()?,
                }),
                OrionUpstreamBackend::FunctionGraphBackend(_) => todo!(),
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

    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        permission, tool_rbac::Action as OrionAction, JwtClaimMatcher, JwtHeaderMatcher, Permission as OrionPermission,
        ToolRbac as OrionToolRbac,
    };

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
    }
}
