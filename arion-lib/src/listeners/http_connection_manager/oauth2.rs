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

use triomphe::Arc;

use crate::{
    listeners::http_filters::{FilterDecision, FilterFactory},
    ArionRequestBody,
};
use arion_configuration::config::network_filters::http_connection_manager::http_filters::oauth2::OAuth2Config;
use http::Request;
use tracing::{debug, info};

pub struct OAuth2FilterBuilder {
    config: OAuth2Config,
}

impl OAuth2FilterBuilder {
    pub fn new(config: OAuth2Config) -> Self {
        OAuth2FilterBuilder { config }
    }

    pub fn build(self) -> OAuth2Filter {
        debug!(target: "oauth2", "creating new OAuth2 filter");
        let inner = Arc::new(OAuth2FilterInner {
            config: self.config,
        });
        OAuth2Filter { inner }
    }
}

#[derive(Debug)]
pub struct OAuth2FilterInner {
    pub config: OAuth2Config,
}

#[derive(Debug)]
pub struct OAuth2Filter {
    pub inner: Arc<OAuth2FilterInner>,
}

impl FilterFactory for OAuth2Filter {
    fn new_from(&self) -> Self {
        OAuth2Filter { inner: Arc::clone(&self.inner) }
    }
}

impl Clone for OAuth2Filter {
    fn clone(&self) -> Self {
        OAuth2Filter { inner: Arc::clone(&self.inner) }
    }
}

impl OAuth2Filter {
    #[allow(dead_code)]
    pub fn new(config: OAuth2Config) -> Self {
        OAuth2FilterBuilder::new(config).build()
    }

    #[allow(dead_code)]
    pub fn config(&self) -> &OAuth2Config {
        &self.inner.config
    }

    #[allow(clippy::unused_async)]
    pub async fn apply_request(&mut self, _req: &mut Request<ArionRequestBody>) -> FilterDecision {
        info!(target: "oauth2", "OAuth2 filter applying request with config: {:?}", self.inner.config);
        FilterDecision::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arion_configuration::config::{
        core::HttpUri,
        network_filters::http_connection_manager::{
            http_filters::oauth2::{CookieNames, OAuth2Credentials},
            route::{PathMatcher, PathSpecifier},
        },
    };
    use std::time::Duration;

    fn sample_config() -> OAuth2Config {
        OAuth2Config {
            token_endpoint: HttpUri {
                uri: "https://auth.example.com/oauth/token".into(),
                cluster: "oauth_cluster".into(),
                timeout: Duration::from_secs(5),
            },
            retry_policy: None,
            authorization_endpoint: "https://auth.example.com/oauth/authorize".into(),
            end_session_endpoint: None,
            credentials: OAuth2Credentials {
                client_id: "client-id".into(),
                token_secret: "token-secret".into(),
                hmac_secret: "hmac-secret".into(),
                cookie_names: CookieNames::default(),
                cookie_domain: None,
            },
            redirect_uri: "https://example.com/callback".into(),
            redirect_path_matcher: PathMatcher {
                specifier: PathSpecifier::Exact("/callback".into()),
                ignore_case: false,
            },
            signout_path: PathMatcher {
                specifier: PathSpecifier::Exact("/signout".into()),
                ignore_case: false,
            },
            forward_bearer_token: false,
            preserve_authorization_header: false,
            pass_through_matcher: vec![],
            auth_scopes: vec!["user".into()],
            resources: vec![],
            auth_type: Default::default(),
            use_refresh_token: true,
            default_expires_in: Duration::ZERO,
            deny_redirect_matcher: vec![],
            default_refresh_token_expires_in: Duration::from_secs(604_800),
            disable_id_token_set_cookie: false,
            disable_access_token_set_cookie: false,
            disable_refresh_token_set_cookie: false,
            cookie_configs: Default::default(),
            stat_prefix: "oauth".into(),
            csrf_token_expires_in: Duration::from_secs(600),
            code_verifier_token_expires_in: Duration::from_secs(600),
            disable_token_encryption: false,
        }
    }

    #[tokio::test]
    async fn test_oauth2_filter_creation_and_apply_request() {
        let config = sample_config();
        let mut filter = OAuth2Filter::new(config.clone());
        assert_eq!(filter.config(), &config);

        let cloned = filter.new_from();
        assert_eq!(cloned.config(), &config);

        let mut req = Request::builder()
            .uri("https://example.com/api/test")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        assert!(matches!(decision, FilterDecision::Continue));
    }
}
