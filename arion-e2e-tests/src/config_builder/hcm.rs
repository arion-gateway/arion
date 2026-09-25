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

use std::time::Duration;

use arion_data_plane_api::envoy_data_plane_api::{
    arion::extensions::filters::http::user_rate_limit::v3::UserRateLimiter as ArionUserRateLimiter,
    envoy::{
        config::{
            accesslog::v3::{access_log::ConfigType as AccessLogConfigType, AccessLog as EnvoyAccessLog},
            common::mutation_rules::v3::{
                header_mutation::{Action as MutationAction, RemoveOnMatch},
                HeaderMutation as EnvoyHeaderMutation,
            },
            core::v3::{
                config_source::ConfigSourceSpecifier, substitution_format_string::Format as SubstitutionFormat,
                AggregatedConfigSource, ConfigSource, HeaderValue as EnvoyHeaderValue,
                HeaderValueOption as EnvoyHeaderValueOption, SubstitutionFormatString, TypedExtensionConfig,
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
            http::early_header_mutation::header_mutation::v3::HeaderMutation as EnvoyEarlyHeaderMutationConfig,
        },
        r#type::matcher::v3::{string_matcher::MatchPattern, StringMatcher},
    },
    google::protobuf::{Any, BoolValue},
    prost::Message,
};

use arion_data_plane_api::envoy_data_plane_api::envoy::r#type::v3::Percent;

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
    early_mutations: Vec<EnvoyHeaderMutation>,
}

impl HcmBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            proto: EnvoyHcm { stat_prefix: "ingress_http".into(), ..Default::default() },
            early_mutations: Vec::new(),
        }
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

    /// Adds an early header mutation removing the named request header.
    /// Early mutations run before any other HCM header processing (Envoy parity).
    #[must_use]
    pub fn early_remove_header(mut self, name: impl Into<String>) -> Self {
        self.early_mutations.push(EnvoyHeaderMutation { action: Some(MutationAction::Remove(name.into())) });
        self
    }

    /// Adds an early header mutation appending (or adding) a request header.
    #[must_use]
    pub fn early_append_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.early_mutations.push(EnvoyHeaderMutation {
            action: Some(MutationAction::Append(EnvoyHeaderValueOption {
                header: Some(EnvoyHeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
                ..Default::default()
            })),
        });
        self
    }

    /// Adds an early header mutation removing every request header whose name
    /// starts with `prefix`.
    #[must_use]
    pub fn early_remove_on_match_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.early_mutations.push(EnvoyHeaderMutation {
            action: Some(MutationAction::RemoveOnMatch(RemoveOnMatch {
                key_matcher: Some(StringMatcher {
                    match_pattern: Some(MatchPattern::Prefix(prefix.into())),
                    ..Default::default()
                }),
            })),
        });
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
            name: "arion.filters.http.cedar".into(),
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
    pub fn user_rate_limit(mut self, user_rl: impl Into<ArionUserRateLimiter>) -> Self {
        let proto: ArionUserRateLimiter = user_rl.into();
        let any = Any {
            type_url: "type.googleapis.com/arion.extensions.filters.http.user_rate_limit.v3.UserRateLimiter".into(),
            value: proto.encode_to_vec(),
        };
        self.proto.http_filters.push(HttpFilter {
            name: "arion.filters.http.user_rate_limit".into(),
            config_type: Some(HttpFilterConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn wasm(
        mut self,
        wasm: impl Into<arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::wasm::v3::Wasm>,
    ) -> Self {
        let proto = wasm.into();
        let any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.wasm.v3.Wasm".into(),
            value: proto.encode_to_vec(),
        };
        self.proto.http_filters.push(HttpFilter {
            name: "arion.filters.http.wasm".into(),
            config_type: Some(HttpFilterConfigType::TypedConfig(any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn upgrade_websocket(mut self) -> Self {
        use arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::http_connection_manager::UpgradeConfig;
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
    pub fn with_http_filters(mut self, filters: Vec<HttpFilter>) -> Self {
        // Insert filters before the router filter (which should be last)
        let router_pos = self.proto.http_filters.iter().position(|f| f.name == "envoy.filters.http.router");

        if let Some(pos) = router_pos {
            for (i, filter) in filters.into_iter().enumerate() {
                self.proto.http_filters.insert(pos + i, filter);
            }
        } else {
            self.proto.http_filters.extend(filters);
        }
        self
    }

    #[must_use]
    pub fn mcp_gateway(
        self,
        gateway: impl Into<
            arion_data_plane_api::envoy_data_plane_api::arion::extensions::filters::http::mcp::mcp_gateway::v3::McpGateway,
        >,
    ) -> Self {
        self.with_http_filters(vec![super::mcp_gateway::mcp_gateway_http_filter(gateway)])
    }

    #[must_use]
    pub fn with_jwt_auth(self, jwks_inline: impl Into<String>, audiences: Vec<String>) -> Self {
        self.with_jwt_auth_config(jwks_inline, audiences, &[], false)
    }

    /// JWT auth with optional `claim_to_headers` and `clear_route_cache`.
    ///
    /// When `clear_route_cache` is true, a successful authentication rematches the
    /// request so later routes can select on headers copied from JWT claims.
    #[must_use]
    pub fn with_jwt_auth_config(
        self,
        jwks_inline: impl Into<String>,
        audiences: Vec<String>,
        claim_to_headers: &[(&str, &str)],
        clear_route_cache: bool,
    ) -> Self {
        use arion_data_plane_api::envoy_data_plane_api::envoy::config::route::v3::RouteMatch;
        use arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::{
            jwt_provider, jwt_requirement, requirement_rule, JwtAuthentication, JwtClaimToHeader, JwtHeader,
            JwtProvider, JwtRequirement, RequirementRule,
        };

        let jwks_string = jwks_inline.into();

        let provider = JwtProvider {
            issuer: "https://auth.example.com".to_owned(),
            audiences,
            payload_in_metadata: "jwt_payload".to_owned(),
            header_in_metadata: "jwt_header".to_owned(),
            from_headers: vec![JwtHeader {
                name: "Authorization".to_owned(),
                value_prefix: "Bearer ".to_owned(),
            }],
            from_params: vec![],
            from_cookies: vec![],
            forward: false,
            forward_payload_header: String::new(),
            pad_forward_payload_header: false,
            jwks_source_specifier: Some(jwt_provider::JwksSourceSpecifier::LocalJwks(
                arion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::DataSource {
                    specifier: Some(
                        arion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::data_source::Specifier::InlineString(
                            jwks_string,
                        ),
                    ),
                    watched_directory: None,
                },
            )),
            subjects: None,
            require_expiration: false,
            max_lifetime: None,
            normalize_payload_in_metadata: None,
            failed_status_in_metadata: String::new(),
            clock_skew_seconds: 60,
            claim_to_headers: claim_to_headers
                .iter()
                .map(|(header_name, claim_name)| JwtClaimToHeader {
                    header_name: (*header_name).to_owned(),
                    claim_name: (*claim_name).to_owned(),
                })
                .collect(),
            clear_route_cache,
            jwt_cache_config: None,
        };

        let mut providers = std::collections::HashMap::new();
        providers.insert("oauth_provider".to_owned(), provider);

        let jwt_auth = JwtAuthentication {
            providers,
            rules: vec![RequirementRule {
                r#match: Some(RouteMatch {
                    path_specifier: Some(
                        arion_data_plane_api::envoy_data_plane_api::envoy::config::route::v3::route_match::PathSpecifier::Prefix(
                            "/".to_owned(),
                        ),
                    ),
                    ..Default::default()
                }),
                requirement_type: Some(
                    requirement_rule::RequirementType::Requires(
                        JwtRequirement {
                            requires_type: Some(
                                jwt_requirement::RequiresType::ProviderName(
                                    "oauth_provider".to_owned(),
                                ),
                            ),
                        },
                    ),
                ),
            }],
            requirement_map: std::collections::HashMap::new(),
            filter_state_rules: None,
            bypass_cors_preflight: false,
            strip_failure_response: false,
            stat_prefix: String::new(),
        };

        let jwt_any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.jwt_authn.v3.JwtAuthentication".into(),
            value: jwt_auth.encode_to_vec(),
        };

        let jwt_filter = HttpFilter {
            name: "envoy.filters.http.jwt_authn".into(),
            config_type: Some(
                arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::http_filter::ConfigType::TypedConfig(
                    jwt_any,
                ),
            ),
            ..Default::default()
        };

        self.with_http_filters(vec![jwt_filter])
    }

    #[must_use]
    pub fn build(mut self) -> EnvoyHcm {
        self.add_router_filter();
        self.add_early_header_mutation_extension();
        self.proto
    }

    fn add_early_header_mutation_extension(&mut self) {
        if self.early_mutations.is_empty() {
            return;
        }
        let config = EnvoyEarlyHeaderMutationConfig { mutations: std::mem::take(&mut self.early_mutations) };
        let any = Any {
            type_url:
                "type.googleapis.com/envoy.extensions.http.early_header_mutation.header_mutation.v3.HeaderMutation"
                    .into(),
            value: config.encode_to_vec(),
        };
        self.proto.early_header_mutation_extensions.push(TypedExtensionConfig {
            name: "early_header_mutation".into(),
            typed_config: Some(any),
            ..Default::default()
        });
    }

    fn add_router_filter(&mut self) {
        // Only add router if not already present
        if self.proto.http_filters.iter().any(|f| f.name == "envoy.filters.http.router") {
            return;
        }

        let router = Router::default();
        let router_any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router".into(),
            value: router.encode_to_vec(),
        };

        self.proto.http_filters.push(HttpFilter {
            name: "envoy.filters.http.router".into(),
            config_type: Some(
                arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::http_filter::ConfigType::TypedConfig(
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
