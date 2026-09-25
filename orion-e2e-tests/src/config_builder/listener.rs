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

use std::net::{IpAddr, Ipv4Addr};

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::{
            accesslog::v3::{access_log::ConfigType as AccessLogConfigType, AccessLog as EnvoyAccessLog},
            core::v3::{
                address::Address as AddressType, socket_address::PortSpecifier,
                substitution_format_string::Format as SubstitutionFormat, Address, SocketAddress,
                SubstitutionFormatString,
            },
            listener::v3::{
                listener::{InternalListenerConfig, ListenerSpecifier},
                listener_filter::ConfigType,
                FilterChain, Listener as EnvoyListener, ListenerFilter,
            },
        },
        extensions::{
            access_loggers::file::v3::{
                file_access_log::AccessLogFormat as FileAccessLogFormat, FileAccessLog as EnvoyFileAccessLog,
            },
            filters::listener::{
                local_ratelimit::v3::LocalRateLimit as EnvoyListenerLocalRateLimit,
                proxy_protocol::v3::ProxyProtocol as EnvoyProxyProtocol, tls_inspector::v3::TlsInspector,
            },
        },
        r#type::v3::TokenBucket as EnvoyTokenBucket,
    },
    google::protobuf::{Any, Duration as ProtoDuration, UInt32Value},
    prost::Message,
};

#[derive(Debug, Clone, Default)]
pub struct ProxyProtocolConfig {
    pub allow_requests_without_proxy_protocol: bool,
    pub disallowed_versions: Vec<ProxyProtocolVersion>,
    pub pass_through_tlvs: Option<ProxyProtocolPassThroughTlvs>,
    pub stat_prefix: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyProtocolVersion {
    V1,
    V2,
}

#[derive(Debug, Clone)]
pub struct ProxyProtocolPassThroughTlvs {
    pub match_all: bool,
    pub tlv_types: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListenerKind {
    Socket,
    Internal,
}

#[derive(Debug, Clone)]
pub struct ListenerBuilder {
    proto: EnvoyListener,
    ip_address: IpAddr,
    port: u16,
    kind: ListenerKind,
}

impl ListenerBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            proto: EnvoyListener { name: name.into(), ..Default::default() },
            ip_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            kind: ListenerKind::Socket,
        }
    }

    #[must_use]
    pub fn internal(mut self) -> Self {
        self.kind = ListenerKind::Internal;
        self
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
    pub fn with_proxy_protocol(self) -> Self {
        self.with_proxy_protocol_config(ProxyProtocolConfig::default())
    }

    #[must_use]
    pub fn with_proxy_protocol_config(mut self, config: ProxyProtocolConfig) -> Self {
        use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::{
            proxy_protocol_pass_through_tl_vs::PassTlVsMatchType as EnvoyPassTlvsMatchType,
            ProxyProtocolPassThroughTlVs as EnvoyPassThroughTlvs,
        };

        let disallowed_versions = config
            .disallowed_versions
            .iter()
            .map(|v| match v {
                ProxyProtocolVersion::V1 => 0,
                ProxyProtocolVersion::V2 => 1,
            })
            .collect();

        let pass_through_tlvs = config.pass_through_tlvs.as_ref().map(|p| EnvoyPassThroughTlvs {
            match_type: if p.match_all {
                EnvoyPassTlvsMatchType::IncludeAll as i32
            } else {
                EnvoyPassTlvsMatchType::Include as i32
            },
            tlv_type: p.tlv_types.iter().copied().map(u32::from).collect(),
        });

        let pp = EnvoyProxyProtocol {
            rules: vec![],
            allow_requests_without_proxy_protocol: config.allow_requests_without_proxy_protocol,
            pass_through_tlvs,
            disallowed_versions,
            stat_prefix: config.stat_prefix.unwrap_or_default(),
            ..Default::default()
        };

        let listener_filter = ListenerFilter {
            name: "envoy.filters.listener.proxy_protocol".to_owned(),
            config_type: Some(ConfigType::TypedConfig(Any {
                type_url: "type.googleapis.com/envoy.extensions.filters.listener.proxy_protocol.v3.ProxyProtocol"
                    .to_owned(),
                value: pp.encode_to_vec(),
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
    #[allow(deprecated)]
    pub fn access_log_file(self, path: impl Into<String>, text_format: impl Into<String>) -> Self {
        let file_access_log = EnvoyFileAccessLog {
            path: path.into(),
            access_log_format: Some(FileAccessLogFormat::LogFormat(SubstitutionFormatString {
                format: Some(SubstitutionFormat::TextFormat(text_format.into())),
                ..Default::default()
            })),
        };

        let typed_config = Any {
            type_url: "type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog".into(),
            value: file_access_log.encode_to_vec(),
        };

        let access_log = EnvoyAccessLog {
            name: "envoy.access_loggers.file".into(),
            config_type: Some(AccessLogConfigType::TypedConfig(typed_config)),
            ..Default::default()
        };

        self.with_proto(move |proto| proto.access_log.push(access_log))
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyListener)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(mut self) -> EnvoyListener {
        match self.kind {
            ListenerKind::Socket => {
                let socket_address = SocketAddress {
                    address: self.ip_address.to_string(),
                    port_specifier: Some(PortSpecifier::PortValue(u32::from(self.port))),
                    ..Default::default()
                };
                self.proto.address = Some(Address { address: Some(AddressType::SocketAddress(socket_address)) });
            },
            ListenerKind::Internal => {
                self.proto.address = None;
                self.proto.listener_specifier = Some(ListenerSpecifier::InternalListener(InternalListenerConfig {}));
            },
        }
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
