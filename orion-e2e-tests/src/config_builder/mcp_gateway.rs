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

use std::time::Duration;

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{data_source::Specifier, DataSource},
        extensions::filters::network::http_connection_manager::v3::{
            http_filter::ConfigType as HttpFilterConfigType, HttpFilter,
        },
        service::discovery::v3::Resource as XdsResource,
    },
    google::protobuf::{Any, Duration as ProstDuration},
    orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        mcp_server_backend, permission::PermissionType, tool, tool_rbac, DynamicMcpServer, FunctionGraphBackend,
        JwtClaimMatcher, JwtHeaderMatcher, McpGateway, McpServerBackend, Permission, QueryParam, RemoteEmbeddings,
        RestBackend, SemanticSearch, ServerInfo, SimilarityConfig, TdsSpecifier, Tool, ToolRbac,
    },
    prost::Message,
};

use super::{
    bootstrap::BootstrapBuilder, cluster::Cluster, cluster::ClusterBuilder, endpoint::EndpointBuilder,
    filter_chain::FilterChainBuilder, hcm::HcmBuilder, listener::ListenerBuilder, route::RouteBuilder,
    route_config::RouteConfigBuilder, virtual_host::VirtualHostBuilder,
};

pub const DEFAULT_MCP_CLUSTER_HEADER: &str = "x-mcp-target-cluster";
pub const DEFAULT_MCP_ROUTE_CONFIG_NAME: &str = "mcp_routes";
pub const DEFAULT_MCP_VHOST_NAME: &str = "mcp";
pub const DEFAULT_MCP_FILTER_CHAIN_NAME: &str = "main";

const DEFAULT_MCP_LISTENER_NAME: &str = "http";

pub const MCP_GATEWAY_FILTER_NAME: &str = "envoy.filters.http.mcp_gateway";
pub const MCP_GATEWAY_TYPE_URL: &str =
    "type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway";
pub const MCP_TOOL_TYPE_URL: &str = "type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.Tool";
pub const MCP_DYNAMIC_SERVER_TYPE_URL: &str =
    "type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.DynamicMcpServer";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum McpTransport {
    Sse,
    #[default]
    StreamableHttp,
}

impl McpTransport {
    fn to_proto(self) -> i32 {
        match self {
            Self::Sse => mcp_server_backend::TransportUpstream::Sse.into(),
            Self::StreamableHttp => mcp_server_backend::TransportUpstream::StreamableHttp.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct McpGatewayBuilder {
    proto: McpGateway,
}

impl McpGatewayBuilder {
    #[must_use]
    pub fn new(server_name: impl Into<String>, server_version: impl Into<String>) -> Self {
        Self {
            proto: McpGateway {
                cluster_header: Some(DEFAULT_MCP_CLUSTER_HEADER.to_owned()),
                server_info: Some(ServerInfo { name: server_name.into(), version: server_version.into() }),
                ..Default::default()
            },
        }
    }

    #[must_use]
    pub fn cluster_header(mut self, header: impl Into<String>) -> Self {
        self.proto.cluster_header = Some(header.into());
        self
    }

    #[must_use]
    pub fn without_cluster_header(mut self) -> Self {
        self.proto.cluster_header = None;
        self
    }

    #[must_use]
    pub fn tool(mut self, tool: impl Into<Tool>) -> Self {
        self.proto.tools.push(tool.into());
        self
    }

    #[must_use]
    pub fn tools<I, T>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Tool>,
    {
        self.proto.tools.extend(tools.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn dynamic_server(mut self, server: impl Into<DynamicMcpServer>) -> Self {
        self.proto.dynamic_mcp_servers.push(server.into());
        self
    }

    #[must_use]
    pub fn dynamic_servers<I, S>(mut self, servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<DynamicMcpServer>,
    {
        self.proto.dynamic_mcp_servers.extend(servers.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn tds(mut self, config_name: impl Into<String>) -> Self {
        self.proto.tds = Some(TdsSpecifier { config_name: config_name.into() });
        self
    }

    #[must_use]
    pub fn semantic_search(mut self, search: impl Into<SemanticSearch>) -> Self {
        self.proto.semantic_search_tool = Some(search.into());
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut McpGateway)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> McpGateway {
        self.proto
    }
}

impl From<McpGatewayBuilder> for McpGateway {
    fn from(builder: McpGatewayBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone)]
pub struct McpToolBuilder {
    proto: Tool,
}

impl McpToolBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self { proto: Tool { name: name.into(), description: description.into(), ..Default::default() } }
    }

    #[must_use]
    pub fn input_schema(mut self, schema: impl Into<DataSource>) -> Self {
        self.proto.input_schema = Some(schema.into());
        self
    }

    #[must_use]
    pub fn output_schema(mut self, schema: impl Into<DataSource>) -> Self {
        self.proto.output_schema = Some(schema.into());
        self
    }

    #[must_use]
    pub fn rest_backend(mut self, backend: impl Into<RestBackend>) -> Self {
        self.proto.upstream_backend = Some(tool::UpstreamBackend::RestBackend(backend.into()));
        self
    }

    #[must_use]
    pub fn mcp_server_backend(mut self, backend: impl Into<McpServerBackend>) -> Self {
        self.proto.upstream_backend = Some(tool::UpstreamBackend::McpServerBackend(backend.into()));
        self
    }

    #[must_use]
    pub fn function_graph_backend(mut self) -> Self {
        self.proto.upstream_backend = Some(tool::UpstreamBackend::FunctionGraphBackend(FunctionGraphBackend {}));
        self
    }

    #[must_use]
    pub fn rbac(mut self, rbac: impl Into<ToolRbac>) -> Self {
        self.proto.rbac = Some(rbac.into());
        self
    }

    #[must_use]
    pub fn embedding<I>(mut self, embedding: I) -> Self
    where
        I: IntoIterator<Item = f32>,
    {
        self.proto.embedding = embedding.into_iter().collect();
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut Tool)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> Tool {
        self.proto
    }
}

impl From<McpToolBuilder> for Tool {
    fn from(builder: McpToolBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone)]
pub struct McpRestBackendBuilder {
    proto: RestBackend,
}

impl McpRestBackendBuilder {
    #[must_use]
    pub fn new(cluster: impl Into<String>, method: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            proto: RestBackend {
                cluster: cluster.into(),
                method: method.into(),
                path: path.into(),
                ..Default::default()
            },
        }
    }

    #[must_use]
    pub fn query_param(mut self, name: impl Into<String>, source: impl Into<String>) -> Self {
        self.proto.query_params.push(QueryParam { name: name.into(), source: source.into() });
        self
    }

    #[must_use]
    pub fn query_params<I, N, S>(mut self, params: I) -> Self
    where
        I: IntoIterator<Item = (N, S)>,
        N: Into<String>,
        S: Into<String>,
    {
        self.proto
            .query_params
            .extend(params.into_iter().map(|(name, source)| QueryParam { name: name.into(), source: source.into() }));
        self
    }

    #[must_use]
    pub fn body_template(mut self, template: impl Into<DataSource>) -> Self {
        self.proto.body_template = Some(template.into());
        self
    }

    #[must_use]
    pub fn async_call(mut self, value: bool) -> Self {
        self.proto.r#async = value;
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut RestBackend)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> RestBackend {
        self.proto
    }
}

impl From<McpRestBackendBuilder> for RestBackend {
    fn from(builder: McpRestBackendBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone)]
pub struct McpServerBackendBuilder {
    proto: McpServerBackend,
}

impl McpServerBackendBuilder {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self { proto: McpServerBackend { transport: McpTransport::StreamableHttp.to_proto(), url: url.into() } }
    }

    #[must_use]
    pub fn sse(self) -> Self {
        self.transport(McpTransport::Sse)
    }

    #[must_use]
    pub fn streamable_http(self) -> Self {
        self.transport(McpTransport::StreamableHttp)
    }

    #[must_use]
    pub fn transport(mut self, transport: McpTransport) -> Self {
        self.proto.transport = transport.to_proto();
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut McpServerBackend)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> McpServerBackend {
        self.proto
    }
}

impl From<McpServerBackendBuilder> for McpServerBackend {
    fn from(builder: McpServerBackendBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone)]
pub struct DynamicMcpServerBuilder {
    proto: DynamicMcpServer,
}

impl DynamicMcpServerBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>, description: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            proto: DynamicMcpServer {
                name: name.into(),
                description: description.into(),
                transport: McpTransport::StreamableHttp.to_proto(),
                url: url.into(),
                ..Default::default()
            },
        }
    }

    #[must_use]
    pub fn sse(self) -> Self {
        self.transport(McpTransport::Sse)
    }

    #[must_use]
    pub fn streamable_http(self) -> Self {
        self.transport(McpTransport::StreamableHttp)
    }

    #[must_use]
    pub fn transport(mut self, transport: McpTransport) -> Self {
        self.proto.transport = transport.to_proto();
        self
    }

    #[must_use]
    pub fn cache_duration(mut self, duration: Duration) -> Self {
        self.proto.cache_duration = Some(ProstDuration {
            seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(duration.subsec_nanos()).unwrap_or(0),
        });
        self
    }

    #[must_use]
    pub fn rbac(mut self, rbac: impl Into<ToolRbac>) -> Self {
        self.proto.rbac = Some(rbac.into());
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut DynamicMcpServer)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> DynamicMcpServer {
        self.proto
    }
}

impl From<DynamicMcpServerBuilder> for DynamicMcpServer {
    fn from(builder: DynamicMcpServerBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone)]
pub struct McpToolRbacBuilder {
    proto: ToolRbac,
}

impl McpToolRbacBuilder {
    #[must_use]
    pub fn allow() -> Self {
        Self { proto: ToolRbac { action: tool_rbac::Action::Allow.into(), permissions: Vec::new() } }
    }

    #[must_use]
    pub fn deny() -> Self {
        Self { proto: ToolRbac { action: tool_rbac::Action::Deny.into(), permissions: Vec::new() } }
    }

    #[must_use]
    pub fn jwt_claim(self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.permission(Permission {
            permission_type: Some(PermissionType::JwtClaim(JwtClaimMatcher {
                field: field.into(),
                value: value.into(),
            })),
        })
    }

    #[must_use]
    pub fn jwt_header(self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.permission(Permission {
            permission_type: Some(PermissionType::JwtHeader(JwtHeaderMatcher {
                field: field.into(),
                value: value.into(),
            })),
        })
    }

    #[must_use]
    pub fn permission(mut self, permission: impl Into<Permission>) -> Self {
        self.proto.permissions.push(permission.into());
        self
    }

    #[must_use]
    pub fn permissions<I, P>(mut self, permissions: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<Permission>,
    {
        self.proto.permissions.extend(permissions.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut ToolRbac)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> ToolRbac {
        self.proto
    }
}

impl From<McpToolRbacBuilder> for ToolRbac {
    fn from(builder: McpToolRbacBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone)]
pub struct McpSemanticSearchBuilder {
    proto: SemanticSearch,
}

impl McpSemanticSearchBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { proto: SemanticSearch { similarity: Some(SimilarityConfig { top_k: 0 }), ..Default::default() } }
    }

    #[must_use]
    pub fn remote_embeddings(mut self, cluster: impl Into<String>, model_id: impl Into<String>) -> Self {
        self.proto.embeddings =
            Some(RemoteEmbeddings { cluster: cluster.into(), model_id: model_id.into(), ..Default::default() });
        self
    }

    #[must_use]
    pub fn embeddings_path(mut self, path: impl Into<String>) -> Self {
        self.proto.embeddings.get_or_insert_with(RemoteEmbeddings::default).path = path.into();
        self
    }

    #[must_use]
    pub fn embeddings_timeout(mut self, duration: Duration) -> Self {
        self.proto.embeddings.get_or_insert_with(RemoteEmbeddings::default).timeout = Some(ProstDuration {
            seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(duration.subsec_nanos()).unwrap_or(0),
        });
        self
    }

    #[must_use]
    pub fn embeddings_dimensions(mut self, dimensions: u32) -> Self {
        self.proto.embeddings.get_or_insert_with(RemoteEmbeddings::default).dimensions = dimensions;
        self
    }

    #[must_use]
    pub fn assisted_discovery(mut self, enabled: bool) -> Self {
        self.proto.enable_assisted_discovery = enabled;
        self
    }

    #[must_use]
    pub fn top_k(mut self, top_k: u32) -> Self {
        self.proto.similarity = Some(SimilarityConfig { top_k });
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut SemanticSearch)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> SemanticSearch {
        self.proto
    }
}

impl Default for McpSemanticSearchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl From<McpSemanticSearchBuilder> for SemanticSearch {
    fn from(builder: McpSemanticSearchBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone)]
struct McpGatewayJwtAuthConfig {
    jwks_inline: String,
    audiences: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct McpGatewayHttpConfigBuilder {
    gateway: McpGateway,
    listener_name: String,
    listener_port: u16,
    route_config_name: String,
    filter_chain_name: String,
    jwt_auth: Option<McpGatewayJwtAuthConfig>,
}

impl McpGatewayHttpConfigBuilder {
    #[must_use]
    pub fn new(gateway: impl Into<McpGateway>) -> Self {
        Self {
            gateway: gateway.into(),
            listener_name: DEFAULT_MCP_LISTENER_NAME.to_owned(),
            listener_port: 0,
            route_config_name: DEFAULT_MCP_ROUTE_CONFIG_NAME.to_owned(),
            filter_chain_name: DEFAULT_MCP_FILTER_CHAIN_NAME.to_owned(),
            jwt_auth: None,
        }
    }

    #[must_use]
    pub fn listener(mut self, name: impl Into<String>, port: u16) -> Self {
        self.listener_name = name.into();
        self.listener_port = port;
        self
    }

    #[must_use]
    pub fn route_config_name(mut self, name: impl Into<String>) -> Self {
        self.route_config_name = name.into();
        self
    }

    #[must_use]
    pub fn filter_chain_name(mut self, name: impl Into<String>) -> Self {
        self.filter_chain_name = name.into();
        self
    }

    #[must_use]
    pub fn jwt_auth<I, A>(mut self, jwks_inline: impl Into<String>, audiences: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Into<String>,
    {
        self.jwt_auth = Some(McpGatewayJwtAuthConfig {
            jwks_inline: jwks_inline.into(),
            audiences: audiences.into_iter().map(Into::into).collect(),
        });
        self
    }

    #[must_use]
    pub fn build_listener(self) -> ListenerBuilder {
        let route_cluster_header =
            self.gateway.cluster_header.clone().unwrap_or_else(|| DEFAULT_MCP_CLUSTER_HEADER.to_owned());
        let route_config = mcp_gateway_route_config(self.route_config_name, route_cluster_header);
        let mut hcm = HcmBuilder::new().route_config(route_config);

        if let Some(jwt_auth) = self.jwt_auth {
            hcm = hcm.with_jwt_auth(jwt_auth.jwks_inline, jwt_auth.audiences);
        }

        ListenerBuilder::new(self.listener_name)
            .port(self.listener_port)
            .filter_chain(FilterChainBuilder::new(self.filter_chain_name).hcm(hcm.mcp_gateway(self.gateway)))
    }

    #[must_use]
    pub fn build_bootstrap<C>(self, clusters: impl IntoIterator<Item = C>) -> BootstrapBuilder
    where
        C: Into<Cluster>,
    {
        BootstrapBuilder::new()
            .listener(self.build_listener())
            .cluster(ClusterBuilder::new("dummy").endpoint(EndpointBuilder::new("127.0.0.1", 1)))
            .clusters(clusters)
    }
}

#[must_use]
pub fn inline_string_data_source(value: impl Into<String>) -> DataSource {
    DataSource { specifier: Some(Specifier::InlineString(value.into())), watched_directory: None }
}

#[must_use]
pub fn mcp_gateway_any(config: impl Into<McpGateway>) -> Any {
    let config = config.into();
    Any { type_url: MCP_GATEWAY_TYPE_URL.to_owned(), value: config.encode_to_vec() }
}

#[must_use]
pub fn mcp_gateway_http_filter(config: impl Into<McpGateway>) -> HttpFilter {
    HttpFilter {
        name: MCP_GATEWAY_FILTER_NAME.to_owned(),
        config_type: Some(HttpFilterConfigType::TypedConfig(mcp_gateway_any(config))),
        ..Default::default()
    }
}

#[must_use]
pub fn mcp_gateway_route_config(
    route_config_name: impl Into<String>,
    cluster_header: impl Into<String>,
) -> RouteConfigBuilder {
    RouteConfigBuilder::new(route_config_name).virtual_host(
        VirtualHostBuilder::new(DEFAULT_MCP_VHOST_NAME)
            .route(RouteBuilder::new().match_prefix("/").cluster_header(cluster_header)),
    )
}

#[must_use]
pub fn mcp_resource_id(server_name: &str, config_name: &str, resource_name: &str) -> String {
    format!("{server_name}/{config_name}/{resource_name}")
}

#[must_use]
pub fn mcp_tool_xds_resource(resource_id: impl Into<String>, tool: &Tool) -> XdsResource {
    let any = Any { type_url: MCP_TOOL_TYPE_URL.to_owned(), value: tool.encode_to_vec() };
    XdsResource { name: resource_id.into(), resource: Some(any), ..Default::default() }
}

#[must_use]
pub fn dynamic_mcp_server_xds_resource(resource_id: impl Into<String>, server: &DynamicMcpServer) -> XdsResource {
    let any = Any { type_url: MCP_DYNAMIC_SERVER_TYPE_URL.to_owned(), value: server.encode_to_vec() };
    XdsResource { name: resource_id.into(), resource: Some(any), ..Default::default() }
}
