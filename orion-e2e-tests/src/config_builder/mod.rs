// Copyright 2025 The kmesh Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod bootstrap;
mod cluster;
mod endpoint;
mod ext_proc;
mod filter_chain;
mod hcm;
mod health_check;
mod listener;
pub mod mcp_gateway;
pub mod presets;
mod rbac;
mod retry;
mod route;
mod route_config;
pub(crate) mod secret;
pub mod serialize;
mod tcp_proxy;
mod tls;
mod virtual_host;
pub mod xds;

pub use bootstrap::{BootstrapBuilder, LocalEmbeddingsServiceConfig};
pub use cluster::{Cluster, ClusterBuilder, HttpVersion, LbPolicy};
pub use endpoint::{Endpoint, EndpointBuilder, HealthStatus};
pub use ext_proc::ExtProcBuilder;
pub use filter_chain::{FilterChain, FilterChainBuilder};
pub use hcm::{CodecType, Hcm, HcmBuilder};
pub use health_check::{GrpcHealthCheckBuilder, HealthCheckMethod, HttpHealthCheckBuilder, TcpHealthCheckBuilder};
pub use listener::{Listener, ListenerBuilder};
pub use mcp_gateway::{
    dynamic_mcp_server_xds_resource, inline_string_data_source, mcp_gateway_any, mcp_gateway_http_filter,
    mcp_gateway_route_config, mcp_resource_id, mcp_tool_xds_resource, DynamicMcpServerBuilder, McpGatewayBuilder,
    McpGatewayHttpConfigBuilder, McpRestBackendBuilder, McpSemanticSearchBuilder, McpServerBackendBuilder,
    McpToolBuilder, McpToolRbacBuilder, McpTransport, DEFAULT_MCP_CLUSTER_HEADER, DEFAULT_MCP_FILTER_CHAIN_NAME,
    DEFAULT_MCP_ROUTE_CONFIG_NAME, DEFAULT_MCP_VHOST_NAME, MCP_DYNAMIC_SERVER_TYPE_URL, MCP_GATEWAY_FILTER_NAME,
    MCP_GATEWAY_TYPE_URL, MCP_TOOL_TYPE_URL,
};
pub use rbac::{HttpRbacBuilder, HttpRbacPolicyBuilder, NetworkRbacBuilder, NetworkRbacPolicyBuilder};
pub use retry::{RetryOn, RetryPolicy, RetryPolicyBuilder};
pub use route::{RedirectBuilder, Route, RouteBuilder};
pub use route_config::{RouteConfig, RouteConfigBuilder};
pub use secret::{Secret, SecretBuilder};
pub use tcp_proxy::TcpProxyBuilder;
pub use tls::{DownstreamTls, DownstreamTlsBuilder, TlsVersion, UpstreamTls, UpstreamTlsBuilder};
pub use virtual_host::{VirtualHost, VirtualHostBuilder};
