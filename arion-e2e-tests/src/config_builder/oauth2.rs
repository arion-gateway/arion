// Copyright 2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use arion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{http_uri::HttpUpstreamType, HttpUri},
        extensions::{
            filters::http::oauth2::v3::{o_auth2_credentials::TokenFormation, OAuth2, OAuth2Config, OAuth2Credentials},
            transport_sockets::tls::v3::SdsSecretConfig,
        },
        r#type::matcher::v3::{
            path_matcher::Rule as PathMatcherRule, string_matcher::MatchPattern, PathMatcher, StringMatcher,
        },
    },
    google::protobuf::{Any, BoolValue},
    prost::Message,
};

use super::duration_to_proto;

#[derive(Debug, Clone)]
pub struct OAuth2Builder {
    token_endpoint_uri: String,
    token_endpoint_cluster: String,
    token_endpoint_timeout: Duration,
    authorization_endpoint: String,
    client_id: String,
    token_secret_name: String,
    hmac_secret_name: String,
    redirect_uri: String,
    redirect_path: String,
    signout_path: String,
    forward_bearer_token: bool,
    auth_scopes: Vec<String>,
}

impl OAuth2Builder {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        token_endpoint_uri: impl Into<String>,
        token_endpoint_cluster: impl Into<String>,
        authorization_endpoint: impl Into<String>,
        client_id: impl Into<String>,
        token_secret_name: impl Into<String>,
        hmac_secret_name: impl Into<String>,
        redirect_uri: impl Into<String>,
    ) -> Self {
        Self {
            token_endpoint_uri: token_endpoint_uri.into(),
            token_endpoint_cluster: token_endpoint_cluster.into(),
            token_endpoint_timeout: Duration::from_secs(5),
            authorization_endpoint: authorization_endpoint.into(),
            client_id: client_id.into(),
            token_secret_name: token_secret_name.into(),
            hmac_secret_name: hmac_secret_name.into(),
            redirect_uri: redirect_uri.into(),
            redirect_path: "/callback".to_owned(),
            signout_path: "/signout".to_owned(),
            forward_bearer_token: true,
            auth_scopes: vec!["user".to_owned()],
        }
    }

    #[must_use]
    pub fn redirect_path(mut self, path: impl Into<String>) -> Self {
        self.redirect_path = path.into();
        self
    }

    #[must_use]
    pub fn signout_path(mut self, path: impl Into<String>) -> Self {
        self.signout_path = path.into();
        self
    }

    #[must_use]
    pub fn forward_bearer_token(mut self, forward: bool) -> Self {
        self.forward_bearer_token = forward;
        self
    }

    #[must_use]
    pub fn auth_scope(mut self, scope: impl Into<String>) -> Self {
        self.auth_scopes.push(scope.into());
        self
    }

    #[must_use]
    pub fn build(self) -> OAuth2 {
        let redirect_path_matcher = PathMatcher {
            rule: Some(PathMatcherRule::Path(StringMatcher {
                ignore_case: false,
                match_pattern: Some(MatchPattern::Exact(self.redirect_path)),
            })),
        };

        let signout_path = PathMatcher {
            rule: Some(PathMatcherRule::Path(StringMatcher {
                ignore_case: false,
                match_pattern: Some(MatchPattern::Exact(self.signout_path)),
            })),
        };

        let token_endpoint = HttpUri {
            uri: self.token_endpoint_uri,
            http_upstream_type: Some(HttpUpstreamType::Cluster(self.token_endpoint_cluster)),
            timeout: Some(duration_to_proto(self.token_endpoint_timeout)),
        };

        let credentials = OAuth2Credentials {
            client_id: self.client_id,
            token_secret: Some(SdsSecretConfig { name: self.token_secret_name, sds_config: None }),
            token_formation: Some(TokenFormation::HmacSecret(SdsSecretConfig {
                name: self.hmac_secret_name,
                sds_config: None,
            })),
            cookie_names: None,
            cookie_domain: String::new(),
        };

        let config = OAuth2Config {
            token_endpoint: Some(token_endpoint),
            retry_policy: None,
            authorization_endpoint: self.authorization_endpoint,
            end_session_endpoint: String::new(),
            credentials: Some(credentials),
            redirect_uri: self.redirect_uri,
            redirect_path_matcher: Some(redirect_path_matcher),
            signout_path: Some(signout_path),
            forward_bearer_token: self.forward_bearer_token,
            preserve_authorization_header: false,
            pass_through_matcher: vec![],
            auth_scopes: self.auth_scopes,
            resources: vec![],
            auth_type: 0,
            use_refresh_token: Some(BoolValue { value: true }),
            default_expires_in: None,
            deny_redirect_matcher: vec![],
            default_refresh_token_expires_in: None,
            disable_id_token_set_cookie: false,
            disable_access_token_set_cookie: false,
            disable_refresh_token_set_cookie: false,
            cookie_configs: None,
            stat_prefix: String::new(),
            csrf_token_expires_in: None,
            code_verifier_token_expires_in: None,
            disable_token_encryption: false,
        };

        OAuth2 { config: Some(config) }
    }
}

impl From<OAuth2Builder> for OAuth2 {
    fn from(builder: OAuth2Builder) -> Self {
        builder.build()
    }
}

impl From<OAuth2Builder> for Any {
    fn from(builder: OAuth2Builder) -> Self {
        let proto = builder.build();
        Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.oauth2.v3.OAuth2".into(),
            value: proto.encode_to_vec(),
        }
    }
}
