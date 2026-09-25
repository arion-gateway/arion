// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

use std::net::SocketAddr;

use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::rbac::v3::Rbac as NetworkRbac;

use super::bootstrap::BootstrapBuilder;
use super::cluster::ClusterBuilder;
use super::endpoint::EndpointBuilder;
use super::filter_chain::FilterChainBuilder;
use super::hcm::HcmBuilder;
use super::listener::ListenerBuilder;
use super::route::RouteBuilder;
use super::route_config::RouteConfigBuilder;
use super::tls::DownstreamTlsBuilder;
use super::virtual_host::VirtualHostBuilder;

#[must_use]
pub fn http_listener(name: impl Into<String>, port: u16) -> ListenerBuilder {
    let name_str: String = name.into();
    ListenerBuilder::new(&name_str)
        .port(port)
        .filter_chain(FilterChainBuilder::new(format!("{name_str}_filter_chain")).hcm(HcmBuilder::new().http1()))
}

#[must_use]
pub fn http_listener_auto(name: impl Into<String>) -> ListenerBuilder {
    http_listener(name, 0)
}

#[must_use]
pub fn https_listener(
    name: impl Into<String>,
    downstream_tls: DownstreamTlsBuilder,
    cluster_name: impl Into<String>,
) -> ListenerBuilder {
    let cluster_name = cluster_name.into();
    ListenerBuilder::new(name).port(0).filter_chain(
        FilterChainBuilder::new("main").downstream_tls(downstream_tls).hcm(default_hcm_for_cluster(cluster_name)),
    )
}

#[must_use]
pub fn http_filter_chain(name: impl Into<String>, cluster_name: impl Into<String>) -> FilterChainBuilder {
    FilterChainBuilder::new(name).hcm(default_hcm_for_cluster(cluster_name))
}

#[must_use]
pub fn http_filter_chain_with_network_rbac(
    name: impl Into<String>,
    cluster_name: impl Into<String>,
    rbac: impl Into<NetworkRbac>,
) -> FilterChainBuilder {
    FilterChainBuilder::new(name).network_rbac(rbac).hcm(default_hcm_for_cluster(cluster_name))
}

#[must_use]
pub fn https_sni_filter_chain(
    name: impl Into<String>,
    server_names: &[&str],
    downstream_tls: DownstreamTlsBuilder,
    cluster_name: impl Into<String>,
) -> FilterChainBuilder {
    FilterChainBuilder::new(name)
        .server_names(server_names)
        .downstream_tls(downstream_tls)
        .hcm(default_hcm_for_cluster(cluster_name))
}

#[must_use]
pub fn ext_proc_cluster(name: impl Into<String>, addr: SocketAddr) -> ClusterBuilder {
    ClusterBuilder::new(name).http2().endpoint(EndpointBuilder::from_socket_addr(addr))
}

#[must_use]
pub fn static_cluster(name: impl Into<String>, addr: SocketAddr) -> ClusterBuilder {
    ClusterBuilder::new(name).endpoint(EndpointBuilder::from_socket_addr(addr))
}

#[must_use]
pub fn static_cluster_multi<'a>(
    name: impl Into<String>,
    addrs: impl IntoIterator<Item = &'a SocketAddr>,
) -> ClusterBuilder {
    ClusterBuilder::new(name).endpoints(addrs.into_iter().map(|a| EndpointBuilder::from_socket_addr(*a)))
}

#[must_use]
pub fn default_route(cluster: impl Into<String>) -> RouteBuilder {
    RouteBuilder::new().match_prefix("/").cluster(cluster)
}

fn default_hcm_for_cluster(cluster_name: impl Into<String>) -> HcmBuilder {
    HcmBuilder::new().route_config(
        RouteConfigBuilder::new("routes")
            .virtual_host(VirtualHostBuilder::new("default").route(default_route(cluster_name))),
    )
}

#[must_use]
pub fn prefix_route(prefix: impl Into<String>, cluster: impl Into<String>) -> RouteBuilder {
    RouteBuilder::new().match_prefix(prefix).cluster(cluster)
}

#[must_use]
pub fn direct_response_route(prefix: impl Into<String>, status: u32, body: impl Into<String>) -> RouteBuilder {
    RouteBuilder::new().match_prefix(prefix).direct_response(status, body)
}

#[must_use]
pub fn simple_proxy(cluster_name: impl Into<String>, backend: SocketAddr) -> BootstrapBuilder {
    let cluster_name = cluster_name.into();

    let cluster = ClusterBuilder::new(&cluster_name).endpoint(EndpointBuilder::from_socket_addr(backend));

    let listener = ListenerBuilder::new("http").port(0).filter_chain(
        FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().route_config(
                RouteConfigBuilder::new("routes")
                    .virtual_host(VirtualHostBuilder::new("default").route(default_route(&cluster_name))),
            ),
        ),
    );

    BootstrapBuilder::new().listener(listener).cluster(cluster).admin("127.0.0.1", crate::TEST_ADMIN_PORT)
}

#[must_use]
pub fn routed_proxy<R, C>(
    routes: impl IntoIterator<Item = R>,
    clusters: impl IntoIterator<Item = C>,
) -> BootstrapBuilder
where
    R: Into<super::route::Route>,
    C: Into<super::cluster::Cluster>,
{
    let mut vhost = VirtualHostBuilder::new("default");
    for route in routes {
        vhost = vhost.route(route);
    }

    let listener = ListenerBuilder::new("http").port(0).filter_chain(
        FilterChainBuilder::new("main")
            .hcm(HcmBuilder::new().route_config(RouteConfigBuilder::new("routes").virtual_host(vhost))),
    );

    let mut bootstrap = BootstrapBuilder::new().listener(listener);
    for cluster in clusters {
        bootstrap = bootstrap.cluster(cluster);
    }

    bootstrap
}

#[must_use]
pub fn routed_proxy_no_clusters<R>(routes: impl IntoIterator<Item = R>) -> BootstrapBuilder
where
    R: Into<super::route::Route>,
{
    #[allow(clippy::unwrap_used)]
    let dummy =
        ClusterBuilder::new("_unused").endpoint(EndpointBuilder::from_socket_addr("127.0.0.1:1".parse().unwrap()));
    routed_proxy(routes, [dummy])
}

#[must_use]
pub fn routed_proxy_with_vhost<C>(vhost: VirtualHostBuilder, clusters: impl IntoIterator<Item = C>) -> BootstrapBuilder
where
    C: Into<super::cluster::Cluster>,
{
    let listener = ListenerBuilder::new("http").port(0).filter_chain(
        FilterChainBuilder::new("main")
            .hcm(HcmBuilder::new().route_config(RouteConfigBuilder::new("routes").virtual_host(vhost))),
    );

    let mut bootstrap = BootstrapBuilder::new().listener(listener);
    for cluster in clusters {
        bootstrap = bootstrap.cluster(cluster);
    }

    bootstrap
}

#[must_use]
pub fn routed_proxy_with_config<C>(
    route_config: RouteConfigBuilder,
    clusters: impl IntoIterator<Item = C>,
) -> BootstrapBuilder
where
    C: Into<super::cluster::Cluster>,
{
    let listener = ListenerBuilder::new("http")
        .port(0)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(route_config)));

    let mut bootstrap = BootstrapBuilder::new().listener(listener);
    for cluster in clusters {
        bootstrap = bootstrap.cluster(cluster);
    }

    bootstrap
}
