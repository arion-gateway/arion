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
            core::v3::{header_value_option::HeaderAppendAction, HeaderValue, HeaderValueOption},
            route::v3::{
                header_matcher::HeaderMatchSpecifier,
                query_parameter_matcher::QueryParameterMatchSpecifier,
                redirect_action::{PathRewriteSpecifier, RedirectResponseCode, SchemeRewriteSpecifier},
                route::Action,
                route_action::ClusterSpecifier,
                route_match::PathSpecifier,
                weighted_cluster::ClusterWeight,
                DirectResponseAction, HeaderMatcher, QueryParameterMatcher, RedirectAction, RetryPolicy,
                Route as EnvoyRoute, RouteAction, RouteMatch, WeightedCluster,
            },
        },
        r#type::{
            matcher::v3::{string_matcher::MatchPattern, RegexMatcher, StringMatcher},
            v3::Int64Range,
        },
    },
    google::protobuf::{BoolValue, Duration as ProtoDuration, UInt32Value},
};

#[derive(Debug, Clone, Default)]
pub struct RouteBuilder {
    proto: EnvoyRoute,
}

impl RouteBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            proto: EnvoyRoute {
                r#match: Some(RouteMatch {
                    path_specifier: Some(PathSpecifier::Prefix("/".into())),
                    ..Default::default()
                }),
                action: Some(Action::Route(RouteAction::default())),
                ..Default::default()
            },
        }
    }

    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.proto.name = name.into();
        self
    }

    #[must_use]
    pub fn match_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.path_specifier = Some(PathSpecifier::Prefix(prefix.into()));
        }
        self
    }

    #[must_use]
    pub fn match_exact(mut self, path: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.path_specifier = Some(PathSpecifier::Path(path.into()));
        }
        self
    }

    #[must_use]
    pub fn match_regex(mut self, pattern: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.path_specifier =
                Some(PathSpecifier::SafeRegex(RegexMatcher { regex: pattern.into(), ..Default::default() }));
        }
        self
    }

    #[must_use]
    pub fn match_path_separated_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.path_specifier = Some(PathSpecifier::PathSeparatedPrefix(prefix.into()));
        }
        self
    }

    #[must_use]
    pub fn case_insensitive(mut self) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.case_sensitive = Some(BoolValue { value: false });
        }
        self
    }

    #[must_use]
    pub fn match_header_exact(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.headers.push(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::ExactMatch(value.into())),
                ..Default::default()
            });
        }
        self
    }

    #[must_use]
    pub fn match_header_present(mut self, name: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.headers.push(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::PresentMatch(true)),
                ..Default::default()
            });
        }
        self
    }

    #[must_use]
    pub fn match_header_prefix(mut self, name: impl Into<String>, prefix: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.headers.push(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::StringMatch(StringMatcher {
                    match_pattern: Some(MatchPattern::Prefix(prefix.into())),
                    ..Default::default()
                })),
                ..Default::default()
            });
        }
        self
    }

    #[must_use]
    pub fn match_header_suffix(mut self, name: impl Into<String>, suffix: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.headers.push(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::SuffixMatch(suffix.into())),
                ..Default::default()
            });
        }
        self
    }

    #[must_use]
    pub fn match_header_contains(mut self, name: impl Into<String>, substring: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.headers.push(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::ContainsMatch(substring.into())),
                ..Default::default()
            });
        }
        self
    }

    #[must_use]
    pub fn match_header_regex(mut self, name: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.headers.push(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::StringMatch(StringMatcher {
                    match_pattern: Some(MatchPattern::SafeRegex(RegexMatcher {
                        regex: pattern.into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
                ..Default::default()
            });
        }
        self
    }

    #[must_use]
    pub fn match_header_range(mut self, name: impl Into<String>, start: i64, end: i64) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.headers.push(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::RangeMatch(Int64Range { start, end })),
                ..Default::default()
            });
        }
        self
    }

    #[must_use]
    pub fn match_header_absent(mut self, name: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.headers.push(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::PresentMatch(true)),
                invert_match: true,
                ..Default::default()
            });
        }
        self
    }

    #[must_use]
    pub fn match_query_exact(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.query_parameters.push(QueryParameterMatcher {
                name: name.into(),
                query_parameter_match_specifier: Some(QueryParameterMatchSpecifier::StringMatch(StringMatcher {
                    match_pattern: Some(MatchPattern::Exact(value.into())),
                    ..Default::default()
                })),
            });
        }
        self
    }

    #[must_use]
    pub fn match_query_regex(mut self, name: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.query_parameters.push(QueryParameterMatcher {
                name: name.into(),
                query_parameter_match_specifier: Some(QueryParameterMatchSpecifier::StringMatch(StringMatcher {
                    match_pattern: Some(MatchPattern::SafeRegex(RegexMatcher {
                        regex: pattern.into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            });
        }
        self
    }

    #[must_use]
    pub fn match_query_present(mut self, name: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.query_parameters.push(QueryParameterMatcher {
                name: name.into(),
                query_parameter_match_specifier: Some(QueryParameterMatchSpecifier::PresentMatch(true)),
            });
        }
        self
    }

    #[must_use]
    pub fn match_query_absent(mut self, name: impl Into<String>) -> Self {
        self.ensure_match();
        if let Some(ref mut m) = self.proto.r#match {
            m.query_parameters.push(QueryParameterMatcher {
                name: name.into(),
                query_parameter_match_specifier: Some(QueryParameterMatchSpecifier::PresentMatch(false)),
            });
        }
        self
    }

    #[must_use]
    pub fn match_method(self, method: impl Into<String>) -> Self {
        self.match_header_exact(":method", method)
    }

    #[must_use]
    pub fn cluster(mut self, cluster: impl Into<String>) -> Self {
        self.proto.action = Some(Action::Route(RouteAction {
            cluster_specifier: Some(ClusterSpecifier::Cluster(cluster.into())),
            ..Default::default()
        }));
        self
    }

    #[must_use]
    pub fn weighted_clusters(mut self, clusters: &[(&str, u32)]) -> Self {
        let cluster_weights: Vec<ClusterWeight> = clusters
            .iter()
            .map(|(name, weight)| ClusterWeight {
                name: (*name).to_string(),
                weight: Some(UInt32Value { value: *weight }),
                ..Default::default()
            })
            .collect();

        self.proto.action = Some(Action::Route(RouteAction {
            cluster_specifier: Some(ClusterSpecifier::WeightedClusters(WeightedCluster {
                clusters: cluster_weights,
                ..Default::default()
            })),
            ..Default::default()
        }));
        self
    }

    #[must_use]
    pub fn cluster_header(mut self, header_name: impl Into<String>) -> Self {
        self.proto.action = Some(Action::Route(RouteAction {
            cluster_specifier: Some(ClusterSpecifier::ClusterHeader(header_name.into())),
            ..Default::default()
        }));
        self
    }

    #[must_use]
    pub fn redirect(mut self, builder: RedirectBuilder) -> Self {
        self.proto.action = Some(Action::Redirect(builder.build()));
        self
    }

    #[must_use]
    pub fn direct_response(mut self, status: u32, body: impl Into<String>) -> Self {
        use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::{data_source::Specifier, DataSource};
        self.proto.action = Some(Action::DirectResponse(DirectResponseAction {
            status,
            body: Some(DataSource { specifier: Some(Specifier::InlineString(body.into())), watched_directory: None }),
        }));
        self
    }

    #[must_use]
    pub fn direct_response_empty(mut self, status: u32) -> Self {
        self.proto.action = Some(Action::DirectResponse(DirectResponseAction { status, body: None }));
        self
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        if let Some(Action::Route(ref mut route_action)) = self.proto.action {
            route_action.timeout =
                Some(ProtoDuration { seconds: timeout.as_secs() as i64, nanos: timeout.subsec_nanos() as i32 });
        }
        self
    }

    #[must_use]
    pub fn retry_policy(mut self, policy: impl Into<RetryPolicy>) -> Self {
        if let Some(Action::Route(ref mut route_action)) = self.proto.action {
            route_action.retry_policy = Some(policy.into());
        }
        self
    }

    #[must_use]
    pub fn add_request_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.request_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn add_request_header_if_absent(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.request_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            append_action: HeaderAppendAction::AddIfAbsent as i32,
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn overwrite_request_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.request_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn remove_request_header(mut self, name: impl Into<String>) -> Self {
        self.proto.request_headers_to_remove.push(name.into());
        self
    }

    #[must_use]
    pub fn add_response_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.response_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn add_response_header_if_absent(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.response_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            append_action: HeaderAppendAction::AddIfAbsent as i32,
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn overwrite_response_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.response_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn remove_response_header(mut self, name: impl Into<String>) -> Self {
        self.proto.response_headers_to_remove.push(name.into());
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyRoute)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyRoute {
        self.proto
    }

    fn ensure_match(&mut self) {
        if self.proto.r#match.is_none() {
            self.proto.r#match = Some(RouteMatch::default());
        }
    }
}

impl From<RouteBuilder> for EnvoyRoute {
    fn from(builder: RouteBuilder) -> Self {
        builder.build()
    }
}

pub type Route = EnvoyRoute;

#[derive(Debug, Clone, Default)]
pub struct RedirectBuilder {
    proto: RedirectAction,
}

impl RedirectBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { proto: RedirectAction::default() }
    }

    #[must_use]
    pub fn status_301(mut self) -> Self {
        self.proto.response_code = RedirectResponseCode::MovedPermanently as i32;
        self
    }

    #[must_use]
    pub fn status_302(mut self) -> Self {
        self.proto.response_code = RedirectResponseCode::Found as i32;
        self
    }

    #[must_use]
    pub fn status_303(mut self) -> Self {
        self.proto.response_code = RedirectResponseCode::SeeOther as i32;
        self
    }

    #[must_use]
    pub fn status_307(mut self) -> Self {
        self.proto.response_code = RedirectResponseCode::TemporaryRedirect as i32;
        self
    }

    #[must_use]
    pub fn status_308(mut self) -> Self {
        self.proto.response_code = RedirectResponseCode::PermanentRedirect as i32;
        self
    }

    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.proto.host_redirect = host.into();
        self
    }

    #[must_use]
    pub fn port(mut self, port: u32) -> Self {
        self.proto.port_redirect = port;
        self
    }

    #[must_use]
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.proto.path_rewrite_specifier = Some(PathRewriteSpecifier::PathRedirect(path.into()));
        self
    }

    #[must_use]
    pub fn prefix_rewrite(mut self, prefix: impl Into<String>) -> Self {
        self.proto.path_rewrite_specifier = Some(PathRewriteSpecifier::PrefixRewrite(prefix.into()));
        self
    }

    #[must_use]
    pub fn https(mut self) -> Self {
        self.proto.scheme_rewrite_specifier = Some(SchemeRewriteSpecifier::HttpsRedirect(true));
        self
    }

    #[must_use]
    pub fn scheme(mut self, scheme: impl Into<String>) -> Self {
        self.proto.scheme_rewrite_specifier = Some(SchemeRewriteSpecifier::SchemeRedirect(scheme.into()));
        self
    }

    #[must_use]
    pub fn strip_query(mut self) -> Self {
        self.proto.strip_query = true;
        self
    }

    #[must_use]
    pub fn build(self) -> RedirectAction {
        self.proto
    }
}

impl From<RedirectBuilder> for RedirectAction {
    fn from(builder: RedirectBuilder) -> Self {
        builder.build()
    }
}
