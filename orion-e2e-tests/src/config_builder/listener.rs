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

use std::net::{IpAddr, Ipv4Addr};

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::{
            core::v3::{address::Address as AddressType, socket_address::PortSpecifier, Address, SocketAddress},
            listener::v3::{listener_filter::ConfigType, FilterChain, Listener as EnvoyListener, ListenerFilter},
        },
        extensions::filters::listener::{
            local_ratelimit::v3::LocalRateLimit as EnvoyListenerLocalRateLimit, tls_inspector::v3::TlsInspector,
        },
        r#type::v3::TokenBucket as EnvoyTokenBucket,
    },
    google::protobuf::{Any, Duration as ProtoDuration, UInt32Value},
    prost::Message,
};

#[derive(Debug, Clone)]
pub struct ListenerBuilder {
    proto: EnvoyListener,
    ip_address: IpAddr,
    port: u16,
}

impl ListenerBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            proto: EnvoyListener { name: name.into(), ..Default::default() },
            ip_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
        }
    }

    #[must_use]
    pub fn with_port(name: impl Into<String>, port: u16) -> Self {
        Self::new(name).port(port)
    }

    #[must_use]
    pub fn address(mut self, address: impl Into<IpAddr>) -> Self {
        self.ip_address = address.into();
        self
    }

    #[must_use]
    pub fn bind_all(mut self) -> Self {
        self.ip_address = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        self
    }

    #[must_use]
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    #[must_use]
    pub fn filter_chain(mut self, fc: impl Into<FilterChain>) -> Self {
        self.proto.filter_chains.push(fc.into());
        self
    }

    #[must_use]
    pub fn filter_chains<I, F>(mut self, chains: I) -> Self
    where
        I: IntoIterator<Item = F>,
        F: Into<FilterChain>,
    {
        self.proto.filter_chains.extend(chains.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn with_tls_inspector(mut self) -> Self {
        let tls_inspector = TlsInspector::default();
        let listener_filter = ListenerFilter {
            name: "envoy.filters.listener.tls_inspector".to_owned(),
            config_type: Some(ConfigType::TypedConfig(Any {
                type_url: "type.googleapis.com/envoy.extensions.filters.listener.tls_inspector.v3.TlsInspector"
                    .to_owned(),
                value: tls_inspector.encode_to_vec(),
            })),
            ..Default::default()
        };
        self.proto.listener_filters.push(listener_filter);
        self
    }

    #[must_use]
    pub fn listener_local_rate_limit(
        mut self,
        stat_prefix: impl Into<String>,
        max_tokens: u32,
        tokens_per_fill: u32,
        fill_interval_secs: u64,
    ) -> Self {
        let token_bucket = EnvoyTokenBucket {
            max_tokens,
            tokens_per_fill: Some(UInt32Value { value: tokens_per_fill }),
            #[allow(clippy::cast_possible_wrap, reason = "fill_interval_secs is a config value that fits i64")]
            fill_interval: Some(ProtoDuration { seconds: fill_interval_secs as i64, nanos: 0 }),
        };
        let local_ratelimit = EnvoyListenerLocalRateLimit {
            stat_prefix: stat_prefix.into(),
            token_bucket: Some(token_bucket),
            runtime_enabled: None,
        };
        let listener_filter = ListenerFilter {
            name: "envoy.filters.listener.local_ratelimit".to_owned(),
            config_type: Some(ConfigType::TypedConfig(Any {
                type_url: "type.googleapis.com/envoy.extensions.filters.listener.local_ratelimit.v3.LocalRateLimit"
                    .to_owned(),
                value: local_ratelimit.encode_to_vec(),
            })),
            ..Default::default()
        };
        self.proto.listener_filters.push(listener_filter);
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyListener)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(mut self) -> EnvoyListener {
        let socket_address = SocketAddress {
            address: self.ip_address.to_string(),
            port_specifier: Some(PortSpecifier::PortValue(u32::from(self.port))),
            ..Default::default()
        };

        self.proto.address = Some(Address { address: Some(AddressType::SocketAddress(socket_address)) });

        self.proto
    }

    #[must_use]
    pub fn get_port(&self) -> u16 {
        self.port
    }
}

impl From<ListenerBuilder> for EnvoyListener {
    fn from(builder: ListenerBuilder) -> Self {
        builder.build()
    }
}

pub type Listener = EnvoyListener;
