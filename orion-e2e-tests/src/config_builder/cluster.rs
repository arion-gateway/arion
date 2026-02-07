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
    envoy::config::{
        cluster::v3::{
            cluster::{ClusterDiscoveryType, DiscoveryType, LbPolicy as EnvoyLbPolicy},
            Cluster as EnvoyCluster,
        },
        core::v3::{
            transport_socket::ConfigType as TransportSocketConfigType, Http1ProtocolOptions, Http2ProtocolOptions,
            TransportSocket,
        },
        endpoint::v3::{ClusterLoadAssignment, LbEndpoint, LocalityLbEndpoints},
    },
    google::protobuf::{Any, Duration as ProtoDuration},
    prost::Message,
};

use super::{endpoint::EndpointBuilder, tls::UpstreamTls};

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
            name: "envoy.transport_sockets.tls".to_string(),
            config_type: Some(TransportSocketConfigType::TypedConfig(Any {
                type_url: "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext"
                    .to_string(),
                value: tls_proto.encode_to_vec(),
            })),
        };
        self.proto.transport_socket = Some(transport_socket);
        self
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
