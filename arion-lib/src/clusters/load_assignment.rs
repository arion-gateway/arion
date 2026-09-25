// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
//
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
//
//

use std::time::Duration;
use triomphe::Arc;

use arion_configuration::config::cluster::{
    ClusterLoadAssignment as ClusterLoadAssignmentConfig, ExtendedLbPolicy, HealthStatus, HttpProtocolOptions,
    LbEndpoint as LbEndpointConfig, LbPolicy, LocalityLbEndpoints as LocalityLbEndpointsConfig, OverrideHostSource,
    StandardLbPolicy,
};
use http::uri::Authority;
use tracing::debug;
use typed_builder::TypedBuilder;
use webpki::types::ServerName;

use super::{
    balancers::{
        hash_policy::HashState, least::WeightedLeastRequestBalancer, maglev::MaglevBalancer,
        override_host::OverrideHostLoadBalancer, random::RandomBalancer, ring::RingHashBalancer,
        wrr::WeightedRoundRobinBalancer, Balancer, DefaultBalancer, EndpointWithAuthority, EndpointWithLoad,
        WeightedEndpoint,
    },
    health::{EndpointHealth, ValueUpdated},
};
use crate::{
    clusters::clusters_manager::{RoutingContext, RoutingRequirement},
    transport::{
        bind_device::BindDevice, connector::ConnectUsing, GrpcService, HttpChannel, HttpChannelBuilder, HttpChannels,
        TcpChannelConnector, UpstreamTransportSocketConfigurator,
    },
    Result,
};

#[derive(Debug, Clone)]
pub struct LbEndpoint {
    pub name: &'static str,
    pub connect_using: ConnectUsing,
    pub weight: u32,
    pub health_status: HealthStatus,
    http_channel: HttpChannel,
    tcp_channel: TcpChannelConnector,
}

impl PartialEq for LbEndpoint {
    fn eq(&self, other: &Self) -> bool {
        self.connect_using.authority() == other.connect_using.authority()
    }
}

impl WeightedEndpoint for LbEndpoint {
    fn weight(&self) -> u32 {
        self.weight
    }
}

impl EndpointWithAuthority for LbEndpoint {
    fn authority(&self) -> &Authority {
        self.connect_using.authority()
    }
}

impl Eq for LbEndpoint {}

impl PartialOrd for LbEndpoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for LbEndpoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.connect_using.authority().as_str().cmp(other.connect_using.authority().as_str())
    }
}

impl EndpointHealth for LbEndpoint {
    fn health(&self) -> HealthStatus {
        self.health_status
    }

    fn update_health(&mut self, health: HealthStatus) -> ValueUpdated {
        self.health_status.update_health(health)
    }
}

impl LbEndpoint {
    pub fn grpc_service(&self) -> Result<GrpcService> {
        GrpcService::try_new(self.http_channel.clone(), self.connect_using.authority().clone())
    }

    pub fn http_channel(&self) -> HttpChannel {
        self.http_channel.clone()
    }
}

#[derive(Debug, Clone)]
pub struct PartialLbEndpoint {
    pub connect_using: ConnectUsing,
    pub weight: u32,
    pub health_status: HealthStatus,
}

impl PartialLbEndpoint {
    fn new(value: &LbEndpoint) -> Self {
        PartialLbEndpoint {
            connect_using: value.connect_using.clone(),
            weight: value.weight,
            health_status: value.health_status,
        }
    }

    fn with_bind_device(mut self, bind_device: Option<BindDevice>) -> Self {
        self.connect_using = self.connect_using.with_bind_device(bind_device);
        self
    }

    fn with_connect_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.connect_using = self.connect_using.with_connect_timeout(timeout);
        self
    }

    fn with_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.connect_using = self.connect_using.with_idle_timeout(timeout);
        self
    }
}

impl EndpointWithLoad for LbEndpoint {
    fn http_load(&self) -> u32 {
        self.http_channel.load()
    }
}

#[derive(Debug, Clone, TypedBuilder)]
#[builder(build_method(vis="", name=prepare), field_defaults(setter(prefix = "with_")))]
struct LbEndpointBuilder {
    cluster_name: &'static str,
    endpoint: PartialLbEndpoint,
    http_protocol_options: HttpProtocolOptions,
    transport_socket: UpstreamTransportSocketConfigurator,
    #[builder(default)]
    server_name: Option<ServerName<'static>>,
}

impl LbEndpointBuilder {
    pub fn build(self) -> Result<Arc<LbEndpoint>> {
        let cluster_name = self.cluster_name;
        let PartialLbEndpoint { connect_using, weight, health_status } = self.endpoint;

        let builder = HttpChannelBuilder::new(connect_using.clone()).with_cluster_name(cluster_name);

        let maybe_tls_conf = self.transport_socket.tls_configurator();
        let builder = if let Some(server_name) = self.server_name {
            builder.with_tls(maybe_tls_conf.cloned()).with_server_name(server_name)
        } else {
            builder.with_tls(maybe_tls_conf.cloned())
        };
        let http_channel = builder.with_http_protocol_options(self.http_protocol_options).build()?;
        let tcp_channel = TcpChannelConnector::new(&connect_using, cluster_name, self.transport_socket.clone());

        Ok(Arc::new(LbEndpoint { name: cluster_name, connect_using, weight, health_status, http_channel, tcp_channel }))
    }
}

impl TryFrom<LbEndpointConfig> for PartialLbEndpoint {
    type Error = crate::Error;

    fn try_from(lb_endpoint: LbEndpointConfig) -> Result<Self> {
        let health_status = lb_endpoint.health_status;
        let address = lb_endpoint.address;
        let connect_using = ConnectUsing::from_address(&address, None, None, None)?;
        let weight = lb_endpoint.load_balancing_weight.into();
        Ok(PartialLbEndpoint { connect_using, weight, health_status })
    }
}

#[derive(Debug, Clone, Default)]
pub struct LocalityLbEndpoints {
    pub name: &'static str,
    pub endpoints: Vec<Arc<LbEndpoint>>,
    pub priority: u32,
    pub healthy_endpoints: u32,
    pub total_endpoints: u32,
    pub transport_socket: UpstreamTransportSocketConfigurator,
    pub http_protocol_options: HttpProtocolOptions,
    pub connection_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
}
impl LocalityLbEndpoints {
    fn rebuild(self) -> Result<Self> {
        let endpoints = self
            .endpoints
            .into_iter()
            .map(|e| {
                LbEndpointBuilder::builder()
                    .with_cluster_name(self.name)
                    .with_http_protocol_options(self.http_protocol_options.clone())
                    .with_transport_socket(self.transport_socket.clone())
                    .with_endpoint(PartialLbEndpoint::new(&e))
                    .prepare()
                    .build()
            })
            .collect::<Result<_>>()?;

        Ok(Self { endpoints, ..self })
    }
}

#[derive(Debug, Clone, Default)]
pub struct PartialLocalityLbEndpoints {
    endpoints: Vec<PartialLbEndpoint>,
    pub priority: u32,
}
#[derive(Debug, Clone, Default, TypedBuilder)]
#[builder(build_method(vis="", name=prepare), field_defaults(setter(prefix = "with_")))]
pub struct LocalityLbEndpointsBuilder {
    cluster_name: &'static str,
    bind_device: Option<BindDevice>,
    endpoints: PartialLocalityLbEndpoints,
    http_protocol_options: HttpProtocolOptions,
    transport_socket: UpstreamTransportSocketConfigurator,
    server_name: Option<ServerName<'static>>,
    connection_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
}

impl LocalityLbEndpointsBuilder {
    pub fn build(self) -> Result<LocalityLbEndpoints> {
        let cluster_name = self.cluster_name;
        let PartialLocalityLbEndpoints { endpoints, priority } = self.endpoints;

        let endpoints: Vec<Arc<LbEndpoint>> = endpoints
            .into_iter()
            .map(|e| {
                let server_name = self.transport_socket.tls_configurator().and(self.server_name.clone());
                let e = e
                    .with_bind_device(self.bind_device.clone())
                    .with_connect_timeout(self.connection_timeout)
                    .with_idle_timeout(self.idle_timeout);

                LbEndpointBuilder::builder()
                    .with_endpoint(e)
                    .with_cluster_name(cluster_name)
                    .with_transport_socket(self.transport_socket.clone())
                    .with_server_name(server_name)
                    .with_http_protocol_options(self.http_protocol_options.clone())
                    .prepare()
                    .build()
            })
            .collect::<Result<_>>()?;

        let total_endpoints_usize = endpoints.len();
        let healthy_endpoints_usize = endpoints.iter().filter(|e| e.health_status.is_healthy()).count();

        let (Ok(total_endpoints), Ok(healthy_endpoints)) =
            (u32::try_from(total_endpoints_usize), u32::try_from(healthy_endpoints_usize))
        else {
            return Err("Too many endpoints".into());
        };

        // we divide by 100 because we multiply by 100 later to calculate a percentage
        if healthy_endpoints > u32::MAX / 100 {
            return Err("Too many endpoints".into());
        }

        Ok(LocalityLbEndpoints {
            name: cluster_name,
            endpoints,
            priority,
            healthy_endpoints,
            total_endpoints,
            transport_socket: self.transport_socket,
            http_protocol_options: self.http_protocol_options,
            connection_timeout: self.connection_timeout,
            idle_timeout: self.idle_timeout,
        })
    }
}

impl TryFrom<LocalityLbEndpointsConfig> for PartialLocalityLbEndpoints {
    type Error = crate::Error;

    fn try_from(value: LocalityLbEndpointsConfig) -> Result<Self> {
        let endpoints = value.lb_endpoints.into_iter().map(PartialLbEndpoint::try_from).collect::<Result<_>>()?;
        let priority = value.priority;
        Ok(PartialLocalityLbEndpoints { priority, endpoints })
    }
}

#[derive(Debug, Clone)]
pub enum BalancerType {
    RoundRobin(DefaultBalancer<WeightedRoundRobinBalancer<LbEndpoint>, LbEndpoint>),
    Random(DefaultBalancer<RandomBalancer<LbEndpoint>, LbEndpoint>),
    LeastRequests(DefaultBalancer<WeightedLeastRequestBalancer<LbEndpoint>, LbEndpoint>),
    RingHash(DefaultBalancer<RingHashBalancer<LbEndpoint>, LbEndpoint>),
    Maglev(DefaultBalancer<MaglevBalancer<LbEndpoint>, LbEndpoint>),
    OverrideHost(OverrideHostLoadBalancer),
}

impl BalancerType {
    pub fn update_health(&mut self, endpoint: &LbEndpoint, health: HealthStatus) -> Result<ValueUpdated> {
        match self {
            BalancerType::RoundRobin(balancer) => balancer.update_health(endpoint, health),
            BalancerType::Random(balancer) => balancer.update_health(endpoint, health),
            BalancerType::LeastRequests(balancer) => balancer.update_health(endpoint, health),
            BalancerType::RingHash(balancer) => balancer.update_health(endpoint, health),
            BalancerType::Maglev(balancer) => balancer.update_health(endpoint, health),
            BalancerType::OverrideHost(balancer) => balancer.update_health(endpoint, health),
        }
    }
    pub(crate) fn next_item(&mut self, hash: Option<u64>) -> Option<&LbEndpoint> {
        match self {
            BalancerType::RoundRobin(balancer) => balancer.next_item(hash),
            BalancerType::Random(balancer) => balancer.next_item(hash),
            BalancerType::LeastRequests(balancer) => balancer.next_item(hash),
            BalancerType::RingHash(balancer) => balancer.next_item(hash),
            BalancerType::Maglev(balancer) => balancer.next_item(hash),
            BalancerType::OverrideHost(balancer) => balancer.next_item(hash),
        }
    }

    pub(crate) fn requires_hash(&self) -> bool {
        match self {
            BalancerType::RingHash(_) | BalancerType::Maglev(_) => true,
            BalancerType::OverrideHost(balancer) => balancer.fallback_requires_hash(),
            _ => false,
        }
    }

    fn rebuild(self, endpoints: &[LocalityLbEndpoints]) -> Self {
        match self {
            BalancerType::OverrideHost(balancer) => BalancerType::OverrideHost(balancer.rebuild(endpoints)),
            other => other,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ClusterLoadAssignment {
    cluster_name: &'static str,
    pub transport_socket: UpstreamTransportSocketConfigurator,
    protocol_options: HttpProtocolOptions,
    balancer: BalancerType,
    pub endpoints: Vec<LocalityLbEndpoints>,
}

#[derive(Debug, Clone)]
pub struct PartialClusterLoadAssignment {
    endpoints: Vec<PartialLocalityLbEndpoints>,
}

impl ClusterLoadAssignment {
    pub fn get_routing_requirements(&self) -> RoutingRequirement {
        if let BalancerType::OverrideHost(balancer) = &self.balancer {
            RoutingRequirement::OverrideHost {
                header: balancer.header(),
                fallback_requires_hash: balancer.fallback_requires_hash(),
            }
        } else if self.balancer.requires_hash() {
            RoutingRequirement::Hash
        } else {
            RoutingRequirement::None
        }
    }

    pub fn get_http_channel(&mut self, context: RoutingContext) -> Result<HttpChannels> {
        match context {
            RoutingContext::OverrideHost { header, fallback_hash } => {
                let hash = fallback_hash.and_then(HashState::compute);
                if let BalancerType::OverrideHost(balancer) = &mut self.balancer {
                    balancer.select_override(header, hash)
                } else {
                    let endpoint = self.balancer.next_item(hash).ok_or("No active endpoint")?;
                    Ok(HttpChannels::Single(endpoint.http_channel.clone()))
                }
            },
            RoutingContext::Hash(hash_state) => {
                let endpoint = self.balancer.next_item(hash_state.compute()).ok_or("No active endpoint")?;
                Ok(HttpChannels::Single(endpoint.http_channel.clone()))
            },
            _ => {
                let endpoint = self.balancer.next_item(None).ok_or("No active endpoint")?;
                Ok(HttpChannels::Single(endpoint.http_channel.clone()))
            },
        }
    }

    pub fn get_tcp_channel(&mut self) -> Result<TcpChannelConnector> {
        let endpoint = self.balancer.next_item(None).ok_or("No active endpoint")?;
        Ok(endpoint.tcp_channel.clone())
    }

    pub fn get_grpc_channel(&mut self) -> Result<GrpcService> {
        let endpoint = self.balancer.next_item(None).ok_or("No active endpoint")?;
        endpoint.grpc_service()
    }

    pub fn all_http_channels(&self) -> Vec<(Authority, HttpChannel)> {
        self.all_endpoints_iter()
            .map(|endpoint| (endpoint.authority().clone(), endpoint.http_channel.clone()))
            .collect()
    }

    pub fn all_tcp_channels(&self) -> Vec<(Authority, TcpChannelConnector)> {
        self.all_endpoints_iter().map(|endpoint| (endpoint.authority().clone(), endpoint.tcp_channel.clone())).collect()
    }

    pub fn try_all_grpc_channels(&self) -> Vec<Result<(Authority, GrpcService)>> {
        self.all_endpoints_iter()
            .map(|endpoint| endpoint.grpc_service().map(|channel| (endpoint.authority().clone(), channel)))
            .collect()
    }

    pub fn update_endpoint_health(&mut self, authority: &http::uri::Authority, health: HealthStatus) {
        for locality in &self.endpoints {
            locality.endpoints.iter().filter(|endpoint| endpoint.authority() == authority).for_each(|endpoint| {
                if let Err(err) = self.balancer.update_health(endpoint, health) {
                    debug!("Could not update endpoint health: {}", err);
                }
            });
        }
    }

    pub fn rebuild(self) -> Result<Self> {
        let endpoints: Vec<_> = self
            .endpoints
            .into_iter()
            .map(|mut e| {
                e.transport_socket = self.transport_socket.clone();
                e.rebuild()
            })
            .collect::<Result<Vec<_>>>()?;
        let balancer = self.balancer.rebuild(&endpoints);
        Ok(Self {
            cluster_name: self.cluster_name,
            transport_socket: self.transport_socket,
            protocol_options: self.protocol_options,
            balancer,
            endpoints,
        })
    }

    fn all_endpoints_iter(&self) -> impl Iterator<Item = &LbEndpoint> {
        self.endpoints.iter().flat_map(|locality_endpoints| &locality_endpoints.endpoints).map(Arc::as_ref)
    }
}

#[derive(Debug, Clone, TypedBuilder)]
#[builder(build_method(vis="pub(crate)", name=prepare), field_defaults(setter(prefix = "with_")))]
pub struct ClusterLoadAssignmentBuilder {
    cluster_name: &'static str,
    cla: PartialClusterLoadAssignment,
    bind_device: Option<BindDevice>,
    #[builder(default)]
    protocol_options: Option<HttpProtocolOptions>,
    lb_policy: LbPolicy,
    transport_socket: UpstreamTransportSocketConfigurator,
    #[builder(default)]
    server_name: Option<ServerName<'static>>,
    #[builder(default)]
    connection_timeout: Option<Duration>,
    #[builder(default)]
    idle_timeout: Option<Duration>,
}

impl ClusterLoadAssignmentBuilder {
    pub fn build(self) -> Result<ClusterLoadAssignment> {
        let cluster_name = self.cluster_name;
        let protocol_options = self.protocol_options.unwrap_or_default();

        let PartialClusterLoadAssignment { endpoints } = self.cla;

        let endpoints = endpoints
            .into_iter()
            .map(|e| {
                let server_name = self.transport_socket.tls_configurator().and(self.server_name.clone());

                LocalityLbEndpointsBuilder::builder()
                    .with_cluster_name(cluster_name)
                    .with_endpoints(e)
                    .with_bind_device(self.bind_device.clone())
                    .with_connection_timeout(self.connection_timeout)
                    .with_idle_timeout(self.idle_timeout)
                    .with_transport_socket(self.transport_socket.clone())
                    .with_server_name(server_name)
                    .with_http_protocol_options(protocol_options.clone())
                    .prepare()
                    .build()
            })
            .collect::<Result<Vec<_>>>()?;

        let balancer = match &self.lb_policy {
            LbPolicy::Standard(policy) => Self::build_balancer_from_policy(*policy, &endpoints),
            LbPolicy::Extended(ExtendedLbPolicy::OverrideHost(config)) => {
                let header = match &config.override_host_source {
                    OverrideHostSource::Header { name } => name.clone(),
                    OverrideHostSource::Metadata { .. } => {
                        return Err("Metadata override host source is not supported".into());
                    },
                };
                let fallback = Self::build_balancer_from_policy(config.fallback_policy, &endpoints);
                BalancerType::OverrideHost(OverrideHostLoadBalancer::new(header, fallback, &endpoints))
            },
        };

        Ok(ClusterLoadAssignment {
            cluster_name,
            protocol_options,
            balancer,
            transport_socket: self.transport_socket,
            endpoints,
        })
    }

    fn build_balancer_from_policy(policy: StandardLbPolicy, endpoints: &[LocalityLbEndpoints]) -> BalancerType {
        match policy {
            StandardLbPolicy::Random | StandardLbPolicy::ClusterProvided => {
                BalancerType::Random(DefaultBalancer::from_slice(endpoints))
            },
            StandardLbPolicy::RoundRobin => BalancerType::RoundRobin(DefaultBalancer::from_slice(endpoints)),
            StandardLbPolicy::LeastRequest => BalancerType::LeastRequests(DefaultBalancer::from_slice(endpoints)),
            StandardLbPolicy::RingHash => BalancerType::RingHash(DefaultBalancer::from_slice(endpoints)),
            StandardLbPolicy::Maglev => BalancerType::Maglev(DefaultBalancer::from_slice(endpoints)),
        }
    }
}

impl TryFrom<ClusterLoadAssignmentConfig> for PartialClusterLoadAssignment {
    type Error = crate::Error;
    fn try_from(cla: ClusterLoadAssignmentConfig) -> Result<Self> {
        let endpoints: Vec<_> =
            cla.endpoints.into_iter().map(PartialLocalityLbEndpoints::try_from).collect::<Result<_>>()?;

        if endpoints.is_empty() {
            return Err("At least one locality must be specified".into());
        }

        Ok(Self { endpoints })
    }
}

#[cfg(test)]
mod test {
    use http::uri::Authority;

    use super::LbEndpoint;
    use crate::{
        clusters::health::HealthStatus,
        transport::{
            bind_device::BindDevice, connector::ConnectUsing, HttpChannelBuilder, TcpChannelConnector,
            UpstreamTransportSocketConfigurator,
        },
    };

    impl LbEndpoint {
        /// This function is used by unit tests in other modules
        pub fn new(
            authority: Authority,
            cluster_name: &'static str,
            bind_device: Option<BindDevice>,
            weight: u32,
            health_status: HealthStatus,
        ) -> Self {
            let connect_using =
                ConnectUsing::Socket { authority, bind_device, connect_timeout: None, idle_timeout: None };
            let http_channel =
                HttpChannelBuilder::new(connect_using.clone()).with_cluster_name(cluster_name).build().unwrap();
            let tcp_channel = TcpChannelConnector::new(
                &connect_using,
                "test_cluster",
                UpstreamTransportSocketConfigurator::default(),
            );

            Self { name: "Cluster", connect_using, weight, health_status, http_channel, tcp_channel }
        }
    }
}
