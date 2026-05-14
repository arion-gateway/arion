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

use std::net::SocketAddr;
use std::time::Duration;

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::{
            cluster::v3::{
                circuit_breakers::Thresholds as EnvoyThresholds,
                cluster::{
                    ClusterDiscoveryType, DiscoveryType, LbConfig, LbPolicy as EnvoyLbPolicy, OriginalDstLbConfig,
                },
                load_balancing_policy::Policy,
                CircuitBreakers as EnvoyCircuitBreakers, Cluster as EnvoyCluster, LoadBalancingPolicy,
            },
            core::v3::{
                transport_socket::ConfigType as TransportSocketConfigType, HealthCheck as ProtoHealthCheck,
                Http1ProtocolOptions, Http2ProtocolOptions, TransportSocket, TypedExtensionConfig,
            },
            endpoint::v3::{ClusterLoadAssignment, LbEndpoint, LocalityLbEndpoints},
        },
        extensions::load_balancing_policies::{
            override_host::v3::{override_host::OverrideHostSource, OverrideHost},
            random::v3::Random as EnvoyRandom,
            round_robin::v3::RoundRobin as EnvoyRoundRobin,
        },
    },
    google::protobuf::{Any, Duration as ProtoDuration, UInt32Value},
    prost::Message,
};

use super::{
    endpoint::EndpointBuilder,
    health_check::{GrpcHealthCheckBuilder, HttpHealthCheckBuilder, TcpHealthCheckBuilder},
    tls::UpstreamTls,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LbPolicy {
    #[default]
    RoundRobin,
    Random,
    LeastRequest,
    RingHash,
    Maglev,
}

impl LbPolicy {
    fn to_proto(self) -> i32 {
        match self {
            Self::RoundRobin => EnvoyLbPolicy::RoundRobin.into(),
            Self::Random => EnvoyLbPolicy::Random.into(),
            Self::LeastRequest => EnvoyLbPolicy::LeastRequest.into(),
            Self::RingHash => EnvoyLbPolicy::RingHash.into(),
            Self::Maglev => EnvoyLbPolicy::Maglev.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HttpVersion {
    #[default]
    Http1,
    Http2,
}

#[derive(Debug, Clone)]
pub struct ClusterBuilder {
    proto: EnvoyCluster,
    http_version: HttpVersion,
}

impl ClusterBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            proto: EnvoyCluster {
                name: name.clone(),
                cluster_discovery_type: Some(ClusterDiscoveryType::Type(DiscoveryType::Static.into())),
                lb_policy: EnvoyLbPolicy::RoundRobin.into(),
                load_assignment: Some(ClusterLoadAssignment {
                    cluster_name: name,
                    endpoints: vec![LocalityLbEndpoints::default()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            http_version: HttpVersion::default(),
        }
    }

    #[must_use]
    pub fn with_endpoint(name: impl Into<String>, addr: SocketAddr) -> Self {
        Self::new(name).endpoint(EndpointBuilder::from_socket_addr(addr))
    }

    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<LbEndpoint>) -> Self {
        self.ensure_load_assignment();
        if let Some(la) = self.proto.load_assignment.as_mut() {
            if la.endpoints.is_empty() {
                la.endpoints.push(LocalityLbEndpoints::default());
            }
            la.endpoints[0].lb_endpoints.push(endpoint.into());
        }
        self
    }

    #[must_use]
    pub fn endpoints<I, E>(mut self, endpoints: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<LbEndpoint>,
    {
        for ep in endpoints {
            self = self.endpoint(ep);
        }
        self
    }

    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.proto.connect_timeout =
            Some(ProtoDuration { seconds: timeout.as_secs() as i64, nanos: timeout.subsec_nanos() as i32 });
        self
    }

    #[must_use]
    pub fn lb_policy(mut self, policy: LbPolicy) -> Self {
        self.proto.lb_policy = policy.to_proto();
        self
    }

    #[must_use]
    pub fn round_robin(self) -> Self {
        self.lb_policy(LbPolicy::RoundRobin)
    }

    #[must_use]
    pub fn random(self) -> Self {
        self.lb_policy(LbPolicy::Random)
    }

    #[must_use]
    pub fn least_request(self) -> Self {
        self.lb_policy(LbPolicy::LeastRequest)
    }

    #[must_use]
    pub fn ring_hash(self) -> Self {
        self.lb_policy(LbPolicy::RingHash)
    }

    #[must_use]
    pub fn maglev(self) -> Self {
        self.lb_policy(LbPolicy::Maglev)
    }

    #[must_use]
    pub fn override_host(mut self, header_name: &str, fallback_policy: LbPolicy) -> Self {
        let fallback_typed_config = Self::build_lb_policy_typed_config(fallback_policy);
        let fallback_lb_policy = LoadBalancingPolicy {
            policies: vec![Policy {
                typed_extension_config: Some(TypedExtensionConfig {
                    name: "fallback".to_owned(),
                    typed_config: Some(fallback_typed_config),
                }),
            }],
        };

        let override_host = OverrideHost {
            override_host_sources: vec![OverrideHostSource { header: header_name.to_owned(), metadata: None }],
            fallback_policy: Some(fallback_lb_policy),
        };

        let override_host_typed_config = Any {
            type_url: "type.googleapis.com/envoy.extensions.load_balancing_policies.override_host.v3.OverrideHost".to_owned(),
            value: override_host.encode_to_vec(),
        };

        self.proto.load_balancing_policy = Some(LoadBalancingPolicy {
            policies: vec![Policy {
                typed_extension_config: Some(TypedExtensionConfig {
                    name: "override_host".to_owned(),
                    typed_config: Some(override_host_typed_config),
                }),
            }],
        });

        self
    }

    fn build_lb_policy_typed_config(policy: LbPolicy) -> Any {
        match policy {
            LbPolicy::RoundRobin => Any {
                type_url: "type.googleapis.com/envoy.extensions.load_balancing_policies.round_robin.v3.RoundRobin".to_owned(),
                value: EnvoyRoundRobin::default().encode_to_vec(),
            },
            LbPolicy::Random => Any {
                type_url: "type.googleapis.com/envoy.extensions.load_balancing_policies.random.v3.Random".to_owned(),
                value: EnvoyRandom::default().encode_to_vec(),
            },
            LbPolicy::LeastRequest => {
                use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::load_balancing_policies::least_request::v3::LeastRequest;
                Any {
                    type_url:
                        "type.googleapis.com/envoy.extensions.load_balancing_policies.least_request.v3.LeastRequest".to_owned(),
                    value: LeastRequest::default().encode_to_vec(),
                }
            },
            LbPolicy::RingHash => {
                use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::load_balancing_policies::ring_hash::v3::RingHash;
                Any {
                    type_url: "type.googleapis.com/envoy.extensions.load_balancing_policies.ring_hash.v3.RingHash".to_owned(),
                    value: RingHash::default().encode_to_vec(),
                }
            },
            LbPolicy::Maglev => {
                use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::load_balancing_policies::maglev::v3::Maglev;
                Any {
                    type_url: "type.googleapis.com/envoy.extensions.load_balancing_policies.maglev.v3.Maglev".to_owned(),
                    value: Maglev::default().encode_to_vec(),
                }
            },
        }
    }

    #[must_use]
    pub fn eds(mut self) -> Self {
        self.proto.cluster_discovery_type = Some(ClusterDiscoveryType::Type(DiscoveryType::Eds.into()));
        self.proto.load_assignment = None;
        self
    }

    #[must_use]
    pub fn original_dst_via_header(mut self, header_name: &str) -> Self {
        self.proto.cluster_discovery_type = Some(ClusterDiscoveryType::Type(DiscoveryType::OriginalDst.into()));
        self.proto.lb_policy = EnvoyLbPolicy::ClusterProvided.into();
        self.proto.load_assignment = None;
        self.proto.lb_config = Some(LbConfig::OriginalDstLbConfig(OriginalDstLbConfig {
            use_http_header: true,
            http_header_name: header_name.to_owned(),
            upstream_port_override: None,
            metadata_key: None,
        }));
        self
    }

    #[must_use]
    pub fn original_dst_via_default_header(self) -> Self {
        self.original_dst_via_header("")
    }

    #[must_use]
    pub fn original_dst_port_override(mut self, port: u16) -> Self {
        if let Some(LbConfig::OriginalDstLbConfig(ref mut config)) = self.proto.lb_config {
            config.upstream_port_override = Some(UInt32Value { value: u32::from(port) });
        }
        self
    }

    #[must_use]
    pub fn locality_endpoints<I, E>(mut self, priority: u32, endpoints: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<LbEndpoint>,
    {
        self.ensure_load_assignment();
        if let Some(la) = self.proto.load_assignment.as_mut() {
            let lb_endpoints: Vec<LbEndpoint> = endpoints.into_iter().map(Into::into).collect();
            if let Some(existing) = la.endpoints.iter_mut().find(|e| e.priority == priority) {
                existing.lb_endpoints.extend(lb_endpoints);
            } else {
                la.endpoints.push(LocalityLbEndpoints { priority, lb_endpoints, ..Default::default() });
            }
        }
        self
    }

    #[must_use]
    pub fn health_check(mut self, health_check: impl Into<ProtoHealthCheck>) -> Self {
        self.proto.health_checks.push(health_check.into());
        self
    }

    #[must_use]
    pub fn http_health_check(self, path: &str, interval: Duration, timeout: Duration) -> Self {
        self.health_check(HttpHealthCheckBuilder::new(path, interval, timeout))
    }

    #[must_use]
    pub fn tcp_health_check(self, interval: Duration, timeout: Duration) -> Self {
        self.health_check(TcpHealthCheckBuilder::new(interval, timeout))
    }

    #[must_use]
    pub fn grpc_health_check(self, interval: Duration, timeout: Duration) -> Self {
        self.health_check(GrpcHealthCheckBuilder::new(interval, timeout))
    }

    #[must_use]
    pub fn http1(mut self) -> Self {
        self.http_version = HttpVersion::Http1;
        self
    }

    #[must_use]
    pub fn http2(mut self) -> Self {
        self.http_version = HttpVersion::Http2;
        self
    }

    #[must_use]
    pub fn upstream_tls(mut self, tls: impl Into<UpstreamTls>) -> Self {
        let tls_proto = tls.into();
        let transport_socket = TransportSocket {
            name: "envoy.transport_sockets.tls".to_owned(),
            config_type: Some(TransportSocketConfigType::TypedConfig(Any {
                type_url: "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext".to_owned(),
                value: tls_proto.encode_to_vec(),
            })),
        };
        self.proto.transport_socket = Some(transport_socket);
        self
    }

    #[must_use]
    pub fn circuit_breaker_max_requests(mut self, max: u32) -> Self {
        self.ensure_default_circuit_breaker_threshold().max_requests = Some(UInt32Value { value: max });
        self
    }

    #[must_use]
    pub fn circuit_breaker_max_connections(mut self, max: u32) -> Self {
        self.ensure_default_circuit_breaker_threshold().max_connections = Some(UInt32Value { value: max });
        self
    }

    #[must_use]
    pub fn circuit_breaker_max_retries(mut self, max: u32) -> Self {
        self.ensure_default_circuit_breaker_threshold().max_retries = Some(UInt32Value { value: max });
        self
    }

    #[must_use]
    pub fn circuit_breaker_threshold(mut self, threshold: EnvoyThresholds) -> Self {
        let cb = self.proto.circuit_breakers.get_or_insert_with(EnvoyCircuitBreakers::default);
        cb.thresholds.push(threshold);
        self
    }

    fn ensure_default_circuit_breaker_threshold(&mut self) -> &mut EnvoyThresholds {
        let cb = self.proto.circuit_breakers.get_or_insert_with(EnvoyCircuitBreakers::default);
        if let Some(idx) = cb.thresholds.iter().position(|t| t.priority == 0) {
            return &mut cb.thresholds[idx];
        }
        cb.thresholds.push(EnvoyThresholds { priority: 0, ..Default::default() });
        let len = cb.thresholds.len();
        &mut cb.thresholds[len - 1]
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyCluster)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(mut self) -> EnvoyCluster {
        self.apply_http_protocol_options();
        self.proto
    }

    fn ensure_load_assignment(&mut self) {
        if self.proto.load_assignment.is_none() {
            self.proto.load_assignment = Some(ClusterLoadAssignment {
                cluster_name: self.proto.name.clone(),
                endpoints: vec![LocalityLbEndpoints::default()],
                ..Default::default()
            });
        }
    }

    fn apply_http_protocol_options(&mut self) {
        use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::upstreams::http::v3::{
            http_protocol_options::{
                explicit_http_config::ProtocolConfig, ExplicitHttpConfig, UpstreamProtocolOptions,
            },
            HttpProtocolOptions,
        };

        let protocol_config = match self.http_version {
            HttpVersion::Http1 => ProtocolConfig::HttpProtocolOptions(Http1ProtocolOptions::default()),
            HttpVersion::Http2 => ProtocolConfig::Http2ProtocolOptions(Http2ProtocolOptions::default()),
        };

        let explicit_config =
            UpstreamProtocolOptions::ExplicitHttpConfig(ExplicitHttpConfig { protocol_config: Some(protocol_config) });

        let http_options =
            HttpProtocolOptions { upstream_protocol_options: Some(explicit_config), ..Default::default() };

        let any = Any {
            type_url: "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions".to_owned(),
            value: http_options.encode_to_vec(),
        };

        self.proto
            .typed_extension_protocol_options
            .insert("envoy.extensions.upstreams.http.v3.HttpProtocolOptions".to_owned(), any);
    }
}

impl From<ClusterBuilder> for EnvoyCluster {
    fn from(builder: ClusterBuilder) -> Self {
        builder.build()
    }
}

pub type Cluster = EnvoyCluster;
