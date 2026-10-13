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

use arion_interner::{InternedStr, StringInterner};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::config::{
    core::HttpUri,
    network_filters::http_connection_manager::{RetryPolicy, header_matcher::HeaderMatcher, route::PathMatcher},
};

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OAuth2Config {
    pub token_endpoint: HttpUri,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub retry_policy: Option<RetryPolicy>,
    pub authorization_endpoint: SmolStr,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub end_session_endpoint: Option<SmolStr>,
    pub credentials: OAuth2Credentials,
    pub redirect_uri: SmolStr,
    pub redirect_path_matcher: PathMatcher,
    pub signout_path: PathMatcher,
    #[serde(default)]
    pub forward_bearer_token: bool,
    #[serde(default)]
    pub preserve_authorization_header: bool,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Default::default")]
    pub pass_through_matcher: Vec<HeaderMatcher>,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "default_auth_scopes")]
    pub auth_scopes: Vec<SmolStr>,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Default::default")]
    pub resources: Vec<SmolStr>,
    #[serde(default)]
    pub auth_type: AuthType,
    #[serde(default = "default_true")]
    pub use_refresh_token: bool,
    #[serde(with = "humantime_serde", default)]
    pub default_expires_in: Duration,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Default::default")]
    pub deny_redirect_matcher: Vec<HeaderMatcher>,
    #[serde(with = "humantime_serde", default = "default_refresh_token_lifetime")]
    pub default_refresh_token_expires_in: Duration,
    #[serde(default)]
    pub disable_id_token_set_cookie: bool,
    #[serde(default)]
    pub disable_access_token_set_cookie: bool,
    #[serde(default)]
    pub disable_refresh_token_set_cookie: bool,
    #[serde(default)]
    pub cookie_configs: CookieConfigs,
    #[serde(default = "default_stat_prefix")]
    pub stat_prefix: InternedStr,
    #[serde(with = "humantime_serde", default = "default_token_lifetime")]
    pub csrf_token_expires_in: Duration,
    #[serde(with = "humantime_serde", default = "default_token_lifetime")]
    pub code_verifier_token_expires_in: Duration,
    #[serde(default)]
    pub disable_token_encryption: bool,
}

const fn default_true() -> bool {
    true
}

fn default_auth_scopes() -> Vec<SmolStr> {
    vec!["user".into()]
}

const fn default_refresh_token_lifetime() -> Duration {
    Duration::from_secs(604_800) // 7 days
}

const fn default_token_lifetime() -> Duration {
    Duration::from_secs(600) // 10 minutes
}

fn default_stat_prefix() -> InternedStr {
    "oauth".to_interned_str()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OAuth2Credentials {
    pub client_id: SmolStr,
    pub token_secret: SmolStr,
    pub hmac_secret: SmolStr,
    #[serde(default)]
    pub cookie_names: CookieNames,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub cookie_domain: Option<SmolStr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CookieNames {
    #[serde(default = "default_bearer_token")]
    pub bearer_token: SmolStr,
    #[serde(default = "default_oauth_hmac")]
    pub oauth_hmac: SmolStr,
    #[serde(default = "default_oauth_expires")]
    pub oauth_expires: SmolStr,
    #[serde(default = "default_id_token")]
    pub id_token: SmolStr,
    #[serde(default = "default_refresh_token")]
    pub refresh_token: SmolStr,
    #[serde(default = "default_oauth_nonce")]
    pub oauth_nonce: SmolStr,
    #[serde(default = "default_code_verifier")]
    pub code_verifier: SmolStr,
}

impl Default for CookieNames {
    fn default() -> Self {
        Self {
            bearer_token: default_bearer_token(),
            oauth_hmac: default_oauth_hmac(),
            oauth_expires: default_oauth_expires(),
            id_token: default_id_token(),
            refresh_token: default_refresh_token(),
            oauth_nonce: default_oauth_nonce(),
            code_verifier: default_code_verifier(),
        }
    }
}

fn default_bearer_token() -> SmolStr {
    "BearerToken".into()
}

fn default_oauth_hmac() -> SmolStr {
    "OauthHMAC".into()
}

fn default_oauth_expires() -> SmolStr {
    "OauthExpires".into()
}

fn default_id_token() -> SmolStr {
    "IdToken".into()
}

fn default_refresh_token() -> SmolStr {
    "RefreshToken".into()
}

fn default_oauth_nonce() -> SmolStr {
    "OauthNonce".into()
}

fn default_code_verifier() -> SmolStr {
    "OauthCodeVerifier".into()
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    #[default]
    UrlEncodedBody,
    BasicAuth,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CookieSameSite {
    #[default]
    Disabled,
    Strict,
    Lax,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CookieConfig {
    #[serde(default)]
    pub same_site: CookieSameSite,
    #[serde(default = "default_cookie_path")]
    pub path: SmolStr,
    #[serde(default)]
    pub partitioned: bool,
}

fn default_cookie_path() -> SmolStr {
    "/".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CookieConfigs {
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub bearer_token_cookie_config: Option<CookieConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub oauth_hmac_cookie_config: Option<CookieConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub oauth_expires_cookie_config: Option<CookieConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub id_token_cookie_config: Option<CookieConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub refresh_token_cookie_config: Option<CookieConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub oauth_nonce_cookie_config: Option<CookieConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub code_verifier_cookie_config: Option<CookieConfig>,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use super::*;

    use crate::config::{common::*, core::RustType};
    use arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::oauth2::v3::{
        CookieConfig as EnvoyCookieConfig, CookieConfigs as EnvoyCookieConfigs, OAuth2 as EnvoyOAuth2,
        OAuth2Config as EnvoyOAuth2Config, OAuth2Credentials as EnvoyOAuth2Credentials,
        cookie_config::SameSite as EnvoySameSite,
        o_auth2_config::AuthType as EnvoyAuthType,
        o_auth2_credentials::{CookieNames as EnvoyCookieNames, TokenFormation as EnvoyTokenFormation},
    };

    impl TryFrom<EnvoyOAuth2> for OAuth2Config {
        type Error = GenericError;
        fn try_from(value: EnvoyOAuth2) -> Result<Self, Self::Error> {
            let EnvoyOAuth2 { config } = value;
            let config = required!(config).with_node("config")?;
            config.try_into()
        }
    }

    impl TryFrom<EnvoyOAuth2Config> for OAuth2Config {
        type Error = GenericError;
        fn try_from(value: EnvoyOAuth2Config) -> Result<Self, Self::Error> {
            let EnvoyOAuth2Config {
                token_endpoint,
                retry_policy,
                authorization_endpoint,
                end_session_endpoint,
                credentials,
                redirect_uri,
                redirect_path_matcher,
                signout_path,
                forward_bearer_token,
                preserve_authorization_header,
                pass_through_matcher,
                auth_scopes,
                resources,
                auth_type,
                use_refresh_token,
                default_expires_in,
                deny_redirect_matcher,
                default_refresh_token_expires_in,
                disable_id_token_set_cookie,
                disable_access_token_set_cookie,
                disable_refresh_token_set_cookie,
                cookie_configs,
                stat_prefix,
                csrf_token_expires_in,
                code_verifier_token_expires_in,
                disable_token_encryption,
            } = value;

            if forward_bearer_token && preserve_authorization_header {
                return Err(GenericError::from_msg(
                    "forward_bearer_token and preserve_authorization_header cannot both be set to true",
                ));
            }

            let token_endpoint = required!(token_endpoint).with_node("token_endpoint")?.try_into()?;
            let retry_policy = retry_policy.map(TryInto::try_into).transpose().with_node("retry_policy")?;
            let authorization_endpoint = required!(authorization_endpoint).with_node("authorization_endpoint")?;
            let end_session_endpoint = (!end_session_endpoint.is_empty()).then(|| end_session_endpoint.into());
            let credentials = required!(credentials).with_node("credentials")?.try_into()?;
            let redirect_uri = required!(redirect_uri).with_node("redirect_uri")?;
            let redirect_path_matcher =
                required!(redirect_path_matcher).with_node("redirect_path_matcher")?.try_into()?;
            let signout_path = required!(signout_path).with_node("signout_path")?.try_into()?;

            let pass_through_matcher = convert_vec!(pass_through_matcher).with_node("pass_through_matcher")?;
            let deny_redirect_matcher = convert_vec!(deny_redirect_matcher).with_node("deny_redirect_matcher")?;

            let auth_scopes = if auth_scopes.is_empty() {
                vec!["user".into()]
            } else {
                auth_scopes.into_iter().map(SmolStr::from).collect()
            };

            let resources = resources.into_iter().map(SmolStr::from).collect();

            let auth_type = match EnvoyAuthType::try_from(auth_type).map_err(|e| {
                GenericError::from_msg_with_cause(format!("invalid auth_type enum value {auth_type}"), e)
            })? {
                EnvoyAuthType::UrlEncodedBody => AuthType::UrlEncodedBody,
                EnvoyAuthType::BasicAuth => AuthType::BasicAuth,
            };

            let use_refresh_token = use_refresh_token.map(|v| v.value).unwrap_or(true);

            let default_expires_in = default_expires_in
                .map(TryInto::try_into)
                .transpose()?
                .map(RustType::<Duration>::into_inner)
                .unwrap_or(Duration::ZERO);

            let default_refresh_token_expires_in = default_refresh_token_expires_in
                .map(TryInto::try_into)
                .transpose()?
                .map(RustType::<Duration>::into_inner)
                .unwrap_or_else(default_refresh_token_lifetime);

            let csrf_token_expires_in = csrf_token_expires_in
                .map(TryInto::try_into)
                .transpose()?
                .map(RustType::<Duration>::into_inner)
                .unwrap_or_else(default_token_lifetime);

            let code_verifier_token_expires_in = code_verifier_token_expires_in
                .map(TryInto::try_into)
                .transpose()?
                .map(RustType::<Duration>::into_inner)
                .unwrap_or_else(default_token_lifetime);

            let cookie_configs = cookie_configs.map(TryInto::try_into).transpose()?.unwrap_or_default();
            let stat_prefix = if stat_prefix.is_empty() { default_stat_prefix() } else { stat_prefix.into() };

            Ok(Self {
                token_endpoint,
                retry_policy,
                authorization_endpoint: authorization_endpoint.into(),
                end_session_endpoint,
                credentials,
                redirect_uri: redirect_uri.into(),
                redirect_path_matcher,
                signout_path,
                forward_bearer_token,
                preserve_authorization_header,
                pass_through_matcher,
                auth_scopes,
                resources,
                auth_type,
                use_refresh_token,
                default_expires_in,
                deny_redirect_matcher,
                default_refresh_token_expires_in,
                disable_id_token_set_cookie,
                disable_access_token_set_cookie,
                disable_refresh_token_set_cookie,
                cookie_configs,
                stat_prefix,
                csrf_token_expires_in,
                code_verifier_token_expires_in,
                disable_token_encryption,
            })
        }
    }

    impl TryFrom<EnvoyOAuth2Credentials> for OAuth2Credentials {
        type Error = GenericError;
        fn try_from(value: EnvoyOAuth2Credentials) -> Result<Self, Self::Error> {
            let EnvoyOAuth2Credentials { client_id, token_secret, cookie_names, cookie_domain, token_formation } =
                value;

            let client_id = required!(client_id).with_node("client_id")?;
            let token_secret = required!(token_secret).with_node("token_secret")?;
            let token_secret_name = token_secret.name;
            let token_secret_name = required!(token_secret_name).with_node("name")?;

            let token_formation = required!(token_formation).with_node("token_formation")?;
            let hmac_secret = match token_formation {
                EnvoyTokenFormation::HmacSecret(hmac_secret) => {
                    let hmac_secret_name = hmac_secret.name;
                    let hmac_secret_name = required!(hmac_secret_name).with_node("name")?;
                    SmolStr::from(hmac_secret_name)
                },
            };

            let cookie_names = cookie_names.map(CookieNames::from).unwrap_or_default();
            let cookie_domain = (!cookie_domain.is_empty()).then(|| cookie_domain.into());

            Ok(Self {
                client_id: client_id.into(),
                token_secret: token_secret_name.into(),
                hmac_secret,
                cookie_names,
                cookie_domain,
            })
        }
    }

    impl From<EnvoyCookieNames> for CookieNames {
        fn from(value: EnvoyCookieNames) -> Self {
            let defaults = CookieNames::default();
            Self {
                bearer_token: if value.bearer_token.is_empty() {
                    defaults.bearer_token
                } else {
                    value.bearer_token.into()
                },
                oauth_hmac: if value.oauth_hmac.is_empty() { defaults.oauth_hmac } else { value.oauth_hmac.into() },
                oauth_expires: if value.oauth_expires.is_empty() {
                    defaults.oauth_expires
                } else {
                    value.oauth_expires.into()
                },
                id_token: if value.id_token.is_empty() { defaults.id_token } else { value.id_token.into() },
                refresh_token: if value.refresh_token.is_empty() {
                    defaults.refresh_token
                } else {
                    value.refresh_token.into()
                },
                oauth_nonce: if value.oauth_nonce.is_empty() { defaults.oauth_nonce } else { value.oauth_nonce.into() },
                code_verifier: if value.code_verifier.is_empty() {
                    defaults.code_verifier
                } else {
                    value.code_verifier.into()
                },
            }
        }
    }

    impl TryFrom<EnvoyCookieConfig> for CookieConfig {
        type Error = GenericError;
        fn try_from(value: EnvoyCookieConfig) -> Result<Self, Self::Error> {
            let EnvoyCookieConfig { same_site, path, partitioned } = value;
            let same_site = match EnvoySameSite::try_from(same_site).map_err(|e| {
                GenericError::from_msg_with_cause(format!("invalid same_site enum value {same_site}"), e)
            })? {
                EnvoySameSite::Disabled => CookieSameSite::Disabled,
                EnvoySameSite::Strict => CookieSameSite::Strict,
                EnvoySameSite::Lax => CookieSameSite::Lax,
                EnvoySameSite::None => CookieSameSite::None,
            };
            let path = if path.is_empty() { default_cookie_path() } else { path.into() };
            Ok(Self { same_site, path, partitioned })
        }
    }

    impl TryFrom<EnvoyCookieConfigs> for CookieConfigs {
        type Error = GenericError;
        fn try_from(value: EnvoyCookieConfigs) -> Result<Self, Self::Error> {
            let EnvoyCookieConfigs {
                bearer_token_cookie_config,
                oauth_hmac_cookie_config,
                oauth_expires_cookie_config,
                id_token_cookie_config,
                refresh_token_cookie_config,
                oauth_nonce_cookie_config,
                code_verifier_cookie_config,
            } = value;

            Ok(Self {
                bearer_token_cookie_config: bearer_token_cookie_config.map(TryInto::try_into).transpose()?,
                oauth_hmac_cookie_config: oauth_hmac_cookie_config.map(TryInto::try_into).transpose()?,
                oauth_expires_cookie_config: oauth_expires_cookie_config.map(TryInto::try_into).transpose()?,
                id_token_cookie_config: id_token_cookie_config.map(TryInto::try_into).transpose()?,
                refresh_token_cookie_config: refresh_token_cookie_config.map(TryInto::try_into).transpose()?,
                oauth_nonce_cookie_config: oauth_nonce_cookie_config.map(TryInto::try_into).transpose()?,
                code_verifier_cookie_config: code_verifier_cookie_config.map(TryInto::try_into).transpose()?,
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use arion_data_plane_api::envoy_data_plane_api::envoy::{
            config::core::v3::{HttpUri as EnvoyHttpUri, http_uri::HttpUpstreamType},
            extensions::transport_sockets::tls::v3::SdsSecretConfig as EnvoySdsSecretConfig,
            r#type::matcher::v3::{
                PathMatcher as EnvoyTypePathMatcher, StringMatcher as EnvoyStringMatcher,
                path_matcher::Rule as EnvoyPathMatcherRule, string_matcher::MatchPattern as EnvoyStringMatcherPattern,
            },
        };
        use arion_data_plane_api::envoy_data_plane_api::google::protobuf::Duration as EnvoyDuration;

        fn sample_envoy_oauth2_config() -> EnvoyOAuth2Config {
            EnvoyOAuth2Config {
                token_endpoint: Some(EnvoyHttpUri {
                    uri: "https://auth.example.com/oauth/token".to_owned(),
                    http_upstream_type: Some(HttpUpstreamType::Cluster("oauth_cluster".to_owned())),
                    timeout: Some(EnvoyDuration { seconds: 5, nanos: 0 }),
                }),
                retry_policy: None,
                authorization_endpoint: "https://auth.example.com/oauth/authorize".to_owned(),
                end_session_endpoint: "https://auth.example.com/oauth/logout".to_owned(),
                credentials: Some(EnvoyOAuth2Credentials {
                    client_id: "test-client-id".to_owned(),
                    token_secret: Some(EnvoySdsSecretConfig { name: "client-secret-sds".to_owned(), sds_config: None }),
                    token_formation: Some(EnvoyTokenFormation::HmacSecret(EnvoySdsSecretConfig {
                        name: "hmac-secret-sds".to_owned(),
                        sds_config: None,
                    })),
                    cookie_names: None,
                    cookie_domain: "example.com".to_owned(),
                }),
                redirect_uri: "https://app.example.com/callback".to_owned(),
                redirect_path_matcher: Some(EnvoyTypePathMatcher {
                    rule: Some(EnvoyPathMatcherRule::Path(EnvoyStringMatcher {
                        ignore_case: false,
                        match_pattern: Some(EnvoyStringMatcherPattern::Exact("/callback".to_owned())),
                    })),
                }),
                signout_path: Some(EnvoyTypePathMatcher {
                    rule: Some(EnvoyPathMatcherRule::Path(EnvoyStringMatcher {
                        ignore_case: false,
                        match_pattern: Some(EnvoyStringMatcherPattern::Exact("/signout".to_owned())),
                    })),
                }),
                forward_bearer_token: true,
                preserve_authorization_header: false,
                pass_through_matcher: vec![],
                auth_scopes: vec!["openid".to_owned(), "profile".to_owned()],
                resources: vec!["https://api.example.com".to_owned()],
                auth_type: EnvoyAuthType::BasicAuth as i32,
                use_refresh_token: None,
                default_expires_in: None,
                deny_redirect_matcher: vec![],
                default_refresh_token_expires_in: None,
                disable_id_token_set_cookie: false,
                disable_access_token_set_cookie: false,
                disable_refresh_token_set_cookie: false,
                cookie_configs: None,
                stat_prefix: "custom_oauth".to_owned(),
                csrf_token_expires_in: None,
                code_verifier_token_expires_in: None,
                disable_token_encryption: false,
            }
        }

        #[test]
        fn test_valid_oauth2_config_conversion() {
            let envoy_cfg = sample_envoy_oauth2_config();
            let cfg = OAuth2Config::try_from(envoy_cfg).unwrap();

            assert_eq!(cfg.token_endpoint.uri, "https://auth.example.com/oauth/token");
            assert_eq!(cfg.token_endpoint.cluster, "oauth_cluster");
            assert_eq!(cfg.token_endpoint.timeout, Duration::from_secs(5));
            assert_eq!(cfg.authorization_endpoint, "https://auth.example.com/oauth/authorize");
            assert_eq!(cfg.end_session_endpoint.as_deref(), Some("https://auth.example.com/oauth/logout"));
            assert_eq!(cfg.credentials.client_id, "test-client-id");
            assert_eq!(cfg.credentials.token_secret, "client-secret-sds");
            assert_eq!(cfg.credentials.hmac_secret, "hmac-secret-sds");
            assert_eq!(cfg.credentials.cookie_domain.as_deref(), Some("example.com"));
            assert_eq!(cfg.credentials.cookie_names.bearer_token, "BearerToken");
            assert_eq!(cfg.redirect_uri, "https://app.example.com/callback");
            assert_eq!(cfg.auth_scopes, vec![SmolStr::from("openid"), SmolStr::from("profile")]);
            assert_eq!(cfg.resources, vec![SmolStr::from("https://api.example.com")]);
            assert_eq!(cfg.auth_type, AuthType::BasicAuth);
            assert!(cfg.use_refresh_token);
            assert_eq!(cfg.default_expires_in, Duration::ZERO);
            assert_eq!(cfg.default_refresh_token_expires_in, Duration::from_secs(604_800));
            assert_eq!(cfg.csrf_token_expires_in, Duration::from_secs(600));
            assert_eq!(cfg.code_verifier_token_expires_in, Duration::from_secs(600));
            assert_eq!(cfg.stat_prefix.as_str(), "custom_oauth");
            assert!(cfg.forward_bearer_token);
            assert!(!cfg.preserve_authorization_header);
        }

        #[test]
        fn test_mutual_exclusivity_validation() {
            let mut envoy_cfg = sample_envoy_oauth2_config();
            envoy_cfg.forward_bearer_token = true;
            envoy_cfg.preserve_authorization_header = true;

            let result = OAuth2Config::try_from(envoy_cfg);
            result.unwrap_err();
        }

        #[test]
        fn test_default_auth_scopes() {
            let mut envoy_cfg = sample_envoy_oauth2_config();
            envoy_cfg.auth_scopes = vec![];

            let cfg = OAuth2Config::try_from(envoy_cfg).unwrap();
            assert_eq!(cfg.auth_scopes, vec![SmolStr::from("user")]);
        }

        #[test]
        fn test_top_level_oauth2_wrapper() {
            let envoy_filter = EnvoyOAuth2 { config: Some(sample_envoy_oauth2_config()) };

            let cfg = OAuth2Config::try_from(envoy_filter).unwrap();
            assert_eq!(cfg.credentials.client_id, "test-client-id");
        }

        #[test]
        fn test_sds_config_present_is_accepted() {
            use arion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::ConfigSource as EnvoyConfigSource;

            let mut envoy_cfg = sample_envoy_oauth2_config();
            if let Some(creds) = &mut envoy_cfg.credentials {
                if let Some(ts) = &mut creds.token_secret {
                    ts.sds_config = Some(EnvoyConfigSource::default());
                }
                if let Some(EnvoyTokenFormation::HmacSecret(hmac)) = &mut creds.token_formation {
                    hmac.sds_config = Some(EnvoyConfigSource::default());
                }
            }

            let cfg = OAuth2Config::try_from(envoy_cfg).unwrap();
            assert_eq!(cfg.credentials.token_secret, "client-secret-sds");
            assert_eq!(cfg.credentials.hmac_secret, "hmac-secret-sds");
        }
    }
}
