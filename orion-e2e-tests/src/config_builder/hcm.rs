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
        config::{
            accesslog::v3::{access_log::ConfigType as AccessLogConfigType, AccessLog as EnvoyAccessLog},
            core::v3::{
                config_source::ConfigSourceSpecifier, substitution_format_string::Format as SubstitutionFormat,
                AggregatedConfigSource, ConfigSource, SubstitutionFormatString,
            },
            route::v3::RouteConfiguration,
        },
        extensions::{
            access_loggers::file::v3::{
                file_access_log::AccessLogFormat as FileAccessLogFormat, FileAccessLog as EnvoyFileAccessLog,
            },
            filters::{
                http::{
                    ext_proc::v3::ExternalProcessor as EnvoyExternalProcessor,
                    jwt_authn::v3::JwtAuthentication as EnvoyJwtAuthentication,
                    local_ratelimit::v3::LocalRateLimit as EnvoyLocalRateLimit,
                    {rbac::v3::Rbac as HttpRbac, router::v3::Router},
                },
                network::http_connection_manager::v3::{
                    http_connection_manager::{CodecType as ProtoCodecType, RouteSpecifier, Tracing as EnvoyTracing},
                    http_filter::ConfigType as HttpFilterConfigType,
                    HttpConnectionManager as EnvoyHcm, HttpFilter, Rds,
                },
            },
        },
    },
    google::protobuf::{Any, BoolValue},
    orion::extensions::filters::http::user_rate_limit::v3::UserRateLimiter as OrionUserRateLimiter,
    prost::Message,
};

use orion_data_plane_api::envoy_data_plane_api::envoy::r#type::v3::Percent;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CodecType {
    #[default]
    Auto,
    Http1,
    Http2,
}

impl CodecType {
    fn to_proto(self) -> i32 {
        match self {
            Self::Auto => ProtoCodecType::Auto.into(),
            Self::Http1 => ProtoCodecType::Http1.into(),
            Self::Http2 => ProtoCodecType::Http2.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HcmBuilder {
    proto: EnvoyHcm,
}

impl HcmBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { proto: EnvoyHcm { stat_prefix: "ingress_http".into(), ..Default::default() } }
    }

    #[must_use]
    pub fn codec(mut self, codec: CodecType) -> Self {
        self.proto.codec_type = codec.to_proto();
        self
    }

    #[must_use]
    pub fn http1(self) -> Self {
        self.codec(CodecType::Http1)
    }

    #[must_use]
    pub fn http2(self) -> Self {
        self.codec(CodecType::Http2)
    }

    #[must_use]
    pub fn auto(self) -> Self {
        self.codec(CodecType::Auto)
    }

    #[must_use]
    pub fn stat_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.proto.stat_prefix = prefix.into();
        self
    }

    #[must_use]
    pub fn route_config(mut self, config: impl Into<RouteConfiguration>) -> Self {
        self.proto.route_specifier = Some(RouteSpecifier::RouteConfig(config.into()));
        self
    }

    #[must_use]
    pub fn rds(mut self, route_config_name: impl Into<String>) -> Self {
        self.proto.route_specifier = Some(RouteSpecifier::Rds(Rds {
            route_config_name: route_config_name.into(),
            config_source: Some(ConfigSource {
                config_source_specifier: Some(ConfigSourceSpecifier::Ads(AggregatedConfigSource {})),
                ..Default::default()
            }),
        }));
        self
    }

    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.proto.request_timeout = Some(super::duration_to_proto(timeout));
        self
    }

    #[must_use]
    pub fn generate_request_id(mut self, generate: bool) -> Self {
        self.proto.generate_request_id = Some(BoolValue { value: generate });
        self
    }

    #[must_use]
    pub fn preserve_external_request_id(mut self, preserve: bool) -> Self {
        self.proto.preserve_external_request_id = preserve;
        self
    }

    #[must_use]
    pub fn always_set_request_id_in_response(mut self, always_set: bool) -> Self {
        self.proto.always_set_request_id_in_response = always_set;
        self
    }

    /// Enable tracing on this HCM with the given sampling rates (0-100).
    ///
    /// `None` means 100% sampling (the Envoy default when unset).
    #[must_use]
    pub fn tracing(
        mut self,
        client_sampling: Option<u32>,
        random_sampling: Option<u32>,
        overall_sampling: Option<u32>,
    ) -> Self {
        self.proto.tracing = Some(EnvoyTracing {
            client_sampling: client_sampling.map(|v| Percent { value: f64::from(v) }),
            random_sampling: random_sampling.map(|v| Percent { value: f64::from(v) }),
            overall_sampling: overall_sampling.map(|v| Percent { value: f64::from(v) }),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn ext_proc(mut self, ext_proc: impl Into<EnvoyExternalProcessor>) -> Self {
        let proto: EnvoyExternalProcessor = ext_proc.into();
        let any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor".into(),
            value: proto.encode_to_vec(),
        };
        self.proto.http_filters.push(HttpFilter {
            name: "envoy.filters.http.ext_proc".into(),
            config_type: Some(HttpFilterConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyHcm)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn http_rbac(mut self, rbac: impl Into<HttpRbac>) -> Self {
        let rbac_proto: HttpRbac = rbac.into();
        let rbac_any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.rbac.v3.RBAC".into(),
            value: rbac_proto.encode_to_vec(),
        };

        self.proto.http_filters.push(HttpFilter {
            name: "envoy.filters.http.rbac".into(),
            config_type: Some(HttpFilterConfigType::TypedConfig(rbac_any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn local_rate_limit(mut self, local_rl: impl Into<EnvoyLocalRateLimit>) -> Self {
        let proto: EnvoyLocalRateLimit = local_rl.into();
        let any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.local_ratelimit.v3.LocalRateLimit".into(),
            value: proto.encode_to_vec(),
        };
        self.proto.http_filters.push(HttpFilter {
            name: "envoy.filters.http.local_ratelimit".into(),
            config_type: Some(HttpFilterConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn cedar_policy(mut self, builder: super::cedar_policy::CedarPolicyBuilder) -> Self {
        let any: Any = builder.into();
        self.proto.http_filters.push(HttpFilter {
            name: "orion.filters.http.cedar".into(),
            config_type: Some(HttpFilterConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn jwt_authn(mut self, jwt: impl Into<EnvoyJwtAuthentication>) -> Self {
        let proto: EnvoyJwtAuthentication = jwt.into();
        let any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.jwt_authn.v3.JwtAuthentication".to_owned(),
            value: proto.encode_to_vec(),
        };
        self.proto.http_filters.push(HttpFilter {
            name: "envoy.filters.http.jwt_authn".into(),
            config_type: Some(HttpFilterConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn user_rate_limit(mut self, user_rl: impl Into<OrionUserRateLimiter>) -> Self {
        let proto: OrionUserRateLimiter = user_rl.into();
        let any = Any {
            type_url: "type.googleapis.com/orion.extensions.filters.http.user_rate_limit.v3.UserRateLimiter".into(),
            value: proto.encode_to_vec(),
        };
        self.proto.http_filters.push(HttpFilter {
            name: "orion.filters.http.user_rate_limit".into(),
            config_type: Some(HttpFilterConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn wasm(
        mut self,
        wasm: impl Into<orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::wasm::v3::Wasm>,
    ) -> Self {
        let proto = wasm.into();
        let any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.wasm.v3.Wasm".into(),
            value: proto.encode_to_vec(),
        };
        self.proto.http_filters.push(HttpFilter {
            name: "orion.filters.http.wasm".into(),
            config_type: Some(HttpFilterConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn upgrade_websocket(mut self) -> Self {
        use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::http_connection_manager::UpgradeConfig;
        self.proto.upgrade_configs.push(UpgradeConfig { upgrade_type: "websocket".into(), ..Default::default() });
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
    pub fn build(mut self) -> EnvoyHcm {
        self.add_router_filter();
        self.proto
    }

    fn add_router_filter(&mut self) {
        let router = Router::default();
        let router_any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router".into(),
            value: router.encode_to_vec(),
        };

        self.proto.http_filters.push(HttpFilter {
            name: "envoy.filters.http.router".into(),
            config_type: Some(
                orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::http_filter::ConfigType::TypedConfig(
                    router_any,
                ),
            ),
            ..Default::default()
        });
    }
}

impl From<HcmBuilder> for EnvoyHcm {
    fn from(builder: HcmBuilder) -> Self {
        builder.build()
    }
}

pub type Hcm = EnvoyHcm;
