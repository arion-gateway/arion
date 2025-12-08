use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct McpGateway {
    #[serde(with = "http_serde_ext::header_name")]
    pub cluster_header: http::HeaderName,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    #![allow(deprecated)]
    use crate::config::GenericError;

    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::McpGateway as OrionMcpGateway;

    impl TryFrom<OrionMcpGateway> for McpGateway {
        type Error = GenericError;
        fn try_from(orion: OrionMcpGateway) -> Result<Self, Self::Error> {
            let OrionMcpGateway { cluster_header } = orion;

            Ok(McpGateway { cluster_header: cluster_header.try_into()? })
        }
    }
}
