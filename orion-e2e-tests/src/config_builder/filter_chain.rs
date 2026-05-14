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

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::{
            core::v3::{transport_socket::ConfigType as TransportSocketConfigType, TransportSocket},
            listener::v3::{filter::ConfigType, Filter, FilterChain as EnvoyFilterChain, FilterChainMatch},
        },
        extensions::filters::network::{
            connection_limit::v3::ConnectionLimit as EnvoyConnectionLimit,
            http_connection_manager::v3::HttpConnectionManager, ratelimit::v3::RateLimit as NetworkRateLimit,
            rbac::v3::Rbac as NetworkRbac, tcp_proxy::v3::TcpProxy,
        },
    },
    google::protobuf::{Any, Duration as ProtoDuration, UInt32Value, UInt64Value},
    prost::Message,
};

use super::{hcm::Hcm, tls::DownstreamTls};

#[derive(Debug, Clone)]
pub struct FilterChainBuilder {
    proto: EnvoyFilterChain,
}

impl FilterChainBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { proto: EnvoyFilterChain { name: name.into(), ..Default::default() } }
    }

    #[must_use]
    pub fn hcm(mut self, hcm: impl Into<Hcm>) -> Self {
        let hcm_proto: HttpConnectionManager = hcm.into();
        let hcm_any = Any {
            type_url:
                "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager"
                    .into(),
            value: hcm_proto.encode_to_vec(),
        };

        self.proto.filters.push(Filter {
            name: "envoy.filters.network.http_connection_manager".into(),
            config_type: Some(ConfigType::TypedConfig(hcm_any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn tcp_proxy(mut self, tcp_proxy: impl Into<TcpProxy>) -> Self {
        let tcp_proxy_proto: TcpProxy = tcp_proxy.into();
        let tcp_proxy_any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy".into(),
            value: tcp_proxy_proto.encode_to_vec(),
        };

        self.proto.filters.push(Filter {
            name: "envoy.filters.network.tcp_proxy".into(),
            config_type: Some(ConfigType::TypedConfig(tcp_proxy_any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn connection_limit(mut self, max_connections: u64, delay: Option<std::time::Duration>) -> Self {
        let proto = EnvoyConnectionLimit {
            stat_prefix: "cx_limit".into(),
            max_connections: Some(UInt64Value { value: max_connections }),
            delay: delay.map(|d| ProtoDuration { seconds: d.as_secs() as i64, nanos: d.subsec_nanos() as i32 }),
            runtime_enabled: None,
        };
        let any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.network.connection_limit.v3.ConnectionLimit".into(),
            value: proto.encode_to_vec(),
        };
        self.proto.filters.push(Filter {
            name: "envoy.filters.network.connection_limit".into(),
            config_type: Some(ConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn network_global_rate_limit(mut self, rl: impl Into<NetworkRateLimit>) -> Self {
        let proto: NetworkRateLimit = rl.into();
        let any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.network.ratelimit.v3.RateLimit".into(),
            value: proto.encode_to_vec(),
        };
        self.proto.filters.push(Filter {
            name: "envoy.filters.network.ratelimit".into(),
            config_type: Some(ConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn network_rbac(mut self, rbac: impl Into<NetworkRbac>) -> Self {
        let rbac_proto: NetworkRbac = rbac.into();
        let rbac_any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.network.rbac.v3.RBAC".into(),
            value: rbac_proto.encode_to_vec(),
        };

        self.proto.filters.push(Filter {
            name: "envoy.filters.network.rbac".into(),
            config_type: Some(ConfigType::TypedConfig(rbac_any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn downstream_tls(mut self, tls: impl Into<DownstreamTls>) -> Self {
        let tls_proto = tls.into();
        let transport_socket = TransportSocket {
            name: "envoy.transport_sockets.tls".to_string(),
            config_type: Some(TransportSocketConfigType::TypedConfig(Any {
                type_url: "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext"
                    .to_string(),
                value: tls_proto.encode_to_vec(),
            })),
        };
        self.proto.transport_socket = Some(transport_socket);
        self
    }

    #[must_use]
    pub fn server_names(mut self, names: &[&str]) -> Self {
        self.ensure_filter_chain_match();
        if let Some(ref mut m) = self.proto.filter_chain_match {
            m.server_names = names.iter().map(|s| (*s).to_string()).collect();
        }
        self
    }

    #[must_use]
    pub fn server_name(self, name: impl Into<String>) -> Self {
        let name_str = name.into();
        self.server_names(&[name_str.as_str()])
    }

    #[must_use]
    pub fn destination_port(mut self, port: u32) -> Self {
        self.ensure_filter_chain_match();
        if let Some(ref mut m) = self.proto.filter_chain_match {
            m.destination_port = Some(UInt32Value { value: port });
        }
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyFilterChain)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyFilterChain {
        self.proto
    }

    fn ensure_filter_chain_match(&mut self) {
        if self.proto.filter_chain_match.is_none() {
            self.proto.filter_chain_match = Some(FilterChainMatch::default());
        }
    }
}

impl From<FilterChainBuilder> for EnvoyFilterChain {
    fn from(builder: FilterChainBuilder) -> Self {
        builder.build()
    }
}

pub type FilterChain = EnvoyFilterChain;
