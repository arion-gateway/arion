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

use std::{
    borrow::Cow,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use aws_lc_rs::{aead, constant_time, digest, hmac};
use base64::{
    prelude::{BASE64_STANDARD, BASE64_URL_SAFE_NO_PAD},
    Engine,
};
use http::{header, uri::PathAndQuery, HeaderMap, HeaderValue, Request, StatusCode, Version};
use oauth2::{basic::BasicClient, AuthUrl, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge, RedirectUrl, Scope};
use rand::RngCore;
use serde::Deserialize;
use smol_str::SmolStr;
use tracing::{debug, error, warn};
use triomphe::Arc;
use url::Url;

use crate::{
    body::response_flags::ResponseFlags,
    event_error::EventFailure,
    listeners::{
        http_filters::{FilterDecision, FilterFactory},
        synthetic_http_response::SyntheticHttpResponse,
    },
    ArionRequestBody,
};
use arion_configuration::config::network_filters::http_connection_manager::http_filters::oauth2::{
    AuthType, CookieConfig, CookieNames, CookieSameSite, OAuth2Config,
};
use arion_format::{
    context::{DownstreamContext, SocketAddrContext},
    uri_formatter::UriFormatter,
};

#[derive(Debug, thiserror::Error)]
enum OauthClientError {
    #[error("Oauth2 reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("TokenEndpointError: {0}")]
    TokenEndpointError(http::StatusCode),
}

#[derive(Deserialize)]
struct TokenEndpointResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
}

pub struct OAuth2FilterInner {
    pub config: OAuth2Config,
    hmac_key: hmac::Key,
    token_cipher: aead::LessSafeKey,
    pub redirect_uri_formatter: UriFormatter,
    pub client: reqwest::Client,
}

impl std::fmt::Debug for OAuth2FilterInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2FilterInner")
            .field("config", &self.config)
            .field("hmac_key", &"<redacted>")
            .field("token_cipher", &"<redacted>")
            .field("redirect_uri_formatter", &self.redirect_uri_formatter)
            .field("client", &self.client)
            .finish()
    }
}

#[derive(Debug)]
pub struct OAuth2Filter {
    pub inner: Arc<OAuth2FilterInner>,
}

pub struct OAuth2FilterBuilder {
    config: OAuth2Config,
}

impl OAuth2FilterBuilder {
    pub fn new(config: OAuth2Config) -> Self {
        OAuth2FilterBuilder { config }
    }

    pub fn build(self) -> crate::Result<OAuth2Filter> {
        debug!(target: "oauth2", "creating new OAuth2 filter");

        let redirect_uri_formatter = UriFormatter::try_new(self.config.redirect_uri.as_str())
            .map_err(|err| format!("invalid OAuth2 redirect_uri formatter: {err}"))?;
        AuthUrl::new(self.config.authorization_endpoint.to_string())
            .map_err(|err| format!("invalid OAuth2 authorization_endpoint: {err}"))?;
        oauth2::TokenUrl::new(self.config.token_endpoint.uri.to_string())
            .map_err(|err| format!("invalid OAuth2 token_endpoint URI: {err}"))?;

        let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build()?;
        let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, self.config.credentials.hmac_secret.as_bytes());
        let token_key_bytes = digest::digest(&digest::SHA256, self.config.credentials.hmac_secret.as_bytes());
        let unbound_key = aead::UnboundKey::new(&aead::AES_256_GCM, token_key_bytes.as_ref())
            .map_err(|err| format!("failed to initialize OAuth2 AES-256-GCM key: {err}"))?;
        let token_cipher = aead::LessSafeKey::new(unbound_key);

        let inner = Arc::new(OAuth2FilterInner {
            config: self.config,
            hmac_key,
            token_cipher,
            redirect_uri_formatter,
            client,
        });
        Ok(OAuth2Filter { inner })
    }
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
    pub fn try_new(config: OAuth2Config) -> crate::Result<Self> {
        OAuth2FilterBuilder::new(config).build()
    }

    #[allow(dead_code)]
    pub fn config(&self) -> &OAuth2Config {
        &self.inner.config
    }

    pub async fn apply_request(&mut self, req: &mut Request<ArionRequestBody>) -> FilterDecision {
        let config = &self.inner.config;

        // 1. Pass-through matcher: bypass authentication if configured headers match.
        for matcher in &config.pass_through_matcher {
            if matcher.request_matches(req) {
                debug!(target: "oauth2", "request matched pass_through_matcher, bypassing oauth");
                return FilterDecision::Continue;
            }
        }

        let Some(host) = extract_host(req) else {
            warn!(target: "oauth2", "missing Host header or authority in request");
            let resp =
                SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into()).into_response(req.version());
            return FilterDecision::DirectResponse(Box::new(resp));
        };
        let path = req.uri().path();

        // 2. Signout path: clear authentication cookies and redirect to end_session_endpoint or /.
        if let Some(pq) = req.uri().path_and_query() {
            if config.signout_path.matches(pq).matched() {
                debug!(target: "oauth2", "request matched signout_path: {path}");
                return self.handle_signout(req);
            }
        }

        // 3. Callback / redirect path matcher: handle OAuth2 provider authorization code response.
        if let Some(pq) = req.uri().path_and_query() {
            if config.redirect_path_matcher.matches(pq).matched() {
                debug!(target: "oauth2", "request matched redirect_path_matcher: {path}");
                let is_https = request_is_https(req);
                let query = req.uri().query().unwrap_or_default().to_owned();
                let headers = req.headers().clone();
                return self.handle_callback(req.version(), is_https, &query, &headers, host).await;
            }
        }

        // 4. Verify the complete signed session on every request. The HMAC payload matches Envoy:
        // domain, expiration, access token, ID token, and refresh token, separated by newlines.
        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        let authenticated = {
            let session = extract_session_cookies(req.headers(), &config.credentials.cookie_names);
            if let (Some(hmac_value), Some(expires)) = (session.hmac, session.expires) {
                if let Ok(expires_at) = expires.parse::<u64>() {
                    if expires_at > now_secs {
                        let mut access_buf = [0_u8; 1024];
                        let mut id_buf = [0_u8; 1024];
                        let mut refresh_buf = [0_u8; 1024];

                        let access_token = self.decrypt_token_cookie(session.access_token, &mut access_buf);
                        let id_token = self.decrypt_token_cookie(session.id_token, &mut id_buf);
                        let refresh_token = self.decrypt_token_cookie(session.refresh_token, &mut refresh_buf);
                        let domain = hmac_domain(config.credentials.cookie_domain.as_deref(), host);
                        if verify_session_hmac(
                            &self.inner.hmac_key,
                            domain,
                            expires,
                            access_token.as_deref(),
                            id_token.as_deref(),
                            refresh_token.as_deref(),
                            hmac_value,
                        ) {
                            Some(access_token.as_deref().and_then(|token| self.bearer_header(req, token)))
                        } else {
                            warn!(target: "oauth2", "invalid OAuth session HMAC for domain: {domain}");
                            None
                        }
                    } else {
                        debug!(target: "oauth2", "oauth session expired at {expires_at}, now {now_secs}");
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(authorization) = authenticated {
            if let Some(authorization) = authorization {
                req.headers_mut().insert(header::AUTHORIZATION, authorization);
            }
            return FilterDecision::Continue;
        }

        // 5. Unauthenticated request: check deny_redirect_matcher.
        for matcher in &config.deny_redirect_matcher {
            if matcher.request_matches(req) {
                debug!(target: "oauth2", "request matched deny_redirect_matcher, returning 401");
                let resp = SyntheticHttpResponse::unauthorized(EventFailure::DirectResponse.into())
                    .into_response(req.version());
                return FilterDecision::DirectResponse(Box::new(resp));
            }
        }

        // 6. Initiate OAuth2 authorization code flow: 302 redirect to authorization_endpoint.
        self.handle_unauthenticated(req)
    }

    fn bearer_header(&self, req: &Request<ArionRequestBody>, token: &str) -> Option<HeaderValue> {
        if !self.inner.config.forward_bearer_token
            || (self.inner.config.preserve_authorization_header && req.headers().contains_key(header::AUTHORIZATION))
        {
            return None;
        }
        HeaderValue::from_str(&format!("Bearer {token}")).ok()
    }

    fn format_redirect_uri(&self, req: &Request<ArionRequestBody>) -> Option<SmolStr> {
        let ctx = DownstreamContext {
            request: req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        };
        let redirect_uri = self.inner.redirect_uri_formatter.format(&ctx);
        let parsed = Url::parse(redirect_uri.as_str()).ok()?;
        (matches!(parsed.scheme(), "http" | "https")
            && parsed.host_str().is_some()
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.fragment().is_none())
        .then_some(redirect_uri)
    }

    fn authorization_url(&self, redirect_uri: &str, state: String, pkce_challenge: PkceCodeChallenge) -> Option<Url> {
        let config = &self.inner.config;
        let client = BasicClient::new(ClientId::new(config.credentials.client_id.to_string()))
            .set_client_secret(ClientSecret::new(config.credentials.token_secret.to_string()))
            .set_auth_uri(AuthUrl::new(config.authorization_endpoint.to_string()).ok()?)
            .set_redirect_uri(RedirectUrl::new(redirect_uri.to_owned()).ok()?);
        let request =
            config.auth_scopes.iter().fold(client.authorize_url(|| CsrfToken::new(state)), |request, scope| {
                request.add_scope(Scope::new(scope.to_string()))
            });
        Some(request.set_pkce_challenge(pkce_challenge).url().0)
    }

    fn decrypt_token_cookie<'a, 'b>(
        &self,
        value: Option<&'a str>,
        buf: &'b mut [u8],
    ) -> Option<Cow<'b, str>>
    where
        'a: 'b,
    {
        let value = value?;
        if self.inner.config.disable_token_encryption {
            return Some(Cow::Borrowed(value));
        }
        decrypt_cookie_value(&self.inner.token_cipher, value, buf)
    }

    fn encrypt_token_cookie(&self, value: &str) -> Option<String> {
        if self.inner.config.disable_token_encryption {
            return Some(value.to_owned());
        }
        encrypt_cookie_value(&self.inner.token_cipher, value)
    }

    fn handle_signout(&self, req: &Request<ArionRequestBody>) -> FilterDecision {
        let config = &self.inner.config;
        let cnames = &config.credentials.cookie_names;
        let cdomain = config.credentials.cookie_domain.as_deref();
        let is_https = request_is_https(req);

        let mut resp = SyntheticHttpResponse::custom_error(
            StatusCode::FOUND,
            None,
            EventFailure::DirectResponse.into(),
            ResponseFlags::default(),
        )
        .into_response(req.version());

        let target_url = config.end_session_endpoint.as_deref().unwrap_or("/");
        if let Ok(loc) = HeaderValue::from_str(target_url) {
            resp.headers_mut().insert(header::LOCATION, loc);
        }
        add_oauth_response_headers(resp.headers_mut());

        // A cookie is only deleted when its name, domain, and path match the original cookie.
        for (name, cookie_config) in [
            (cnames.bearer_token.as_str(), config.cookie_configs.bearer_token_cookie_config.as_ref()),
            (cnames.oauth_hmac.as_str(), config.cookie_configs.oauth_hmac_cookie_config.as_ref()),
            (cnames.oauth_expires.as_str(), config.cookie_configs.oauth_expires_cookie_config.as_ref()),
            (cnames.id_token.as_str(), config.cookie_configs.id_token_cookie_config.as_ref()),
            (cnames.refresh_token.as_str(), config.cookie_configs.refresh_token_cookie_config.as_ref()),
            (cnames.oauth_nonce.as_str(), config.cookie_configs.oauth_nonce_cookie_config.as_ref()),
            (cnames.code_verifier.as_str(), config.cookie_configs.code_verifier_cookie_config.as_ref()),
        ] {
            append_expired_cookie(resp.headers_mut(), name, cookie_config, cdomain, is_https);
        }

        FilterDecision::DirectResponse(Box::new(resp))
    }

    fn handle_unauthenticated(&self, req: &Request<ArionRequestBody>) -> FilterDecision {
        let config = &self.inner.config;
        let cnames = &config.credentials.cookie_names;
        let cdomain = config.credentials.cookie_domain.as_deref();
        let is_https = request_is_https(req);

        let Some(redirect_uri) = self.format_redirect_uri(req) else {
            warn!(target: "oauth2", "redirect_uri formatter produced an invalid absolute HTTP(S) URL");
            let mut resp =
                SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into()).into_response(req.version());
            add_oauth_response_headers(resp.headers_mut());
            return FilterDecision::DirectResponse(Box::new(resp));
        };

        let flow_id = random_urlsafe_token(16);
        let csrf_token = CsrfToken::new_random();
        let state = format!("{flow_id}.{}", csrf_token.secret());
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
        let nonce_cookie_name = flow_cookie_name(cnames.oauth_nonce.as_str(), &flow_id);
        let verifier_cookie_name = flow_cookie_name(cnames.code_verifier.as_str(), &flow_id);
        let original_target = local_redirect_target(req.uri().path_and_query().map(PathAndQuery::as_str));
        let issued_at = unix_timestamp_secs();
        let transaction_cookie = encode_transaction_cookie(
            &self.inner.hmac_key,
            &flow_id,
            csrf_token.secret(),
            issued_at,
            original_target,
            redirect_uri.as_str(),
        );
        let verifier_payload = format!("{issued_at}.{}", pkce_verifier.secret());
        let Some(verifier_cookie) = encrypt_cookie_value(&self.inner.token_cipher, &verifier_payload) else {
            error!(target: "oauth2", "failed to encrypt PKCE verifier cookie");
            let mut resp = SyntheticHttpResponse::internal_server_error(
                EventFailure::DirectResponse.into(),
                ResponseFlags::default(),
            )
            .into_response(req.version());
            add_oauth_response_headers(resp.headers_mut());
            return FilterDecision::DirectResponse(Box::new(resp));
        };

        let auth_url = match self.authorization_url(&redirect_uri, state, pkce_challenge) {
            Some(url) => url,
            None => {
                error!(target: "oauth2", "failed to build OAuth authorization URL");
                let mut resp = SyntheticHttpResponse::internal_server_error(
                    EventFailure::DirectResponse.into(),
                    ResponseFlags::default(),
                )
                .into_response(req.version());
                add_oauth_response_headers(resp.headers_mut());
                return FilterDecision::DirectResponse(Box::new(resp));
            },
        };

        let nonce_header = format_cookie(
            &nonce_cookie_name,
            &transaction_cookie,
            Some(config.csrf_token_expires_in.as_secs()),
            config.cookie_configs.oauth_nonce_cookie_config.as_ref(),
            cdomain,
            is_https,
        );
        let verifier_header = format_cookie(
            &verifier_cookie_name,
            &verifier_cookie,
            Some(config.code_verifier_token_expires_in.as_secs()),
            config.cookie_configs.code_verifier_cookie_config.as_ref(),
            cdomain,
            is_https,
        );
        let (Some(nonce_header), Some(verifier_header)) = (nonce_header, verifier_header) else {
            error!(target: "oauth2", "OAuth transaction cookie could not be serialized");
            let mut resp = SyntheticHttpResponse::internal_server_error(
                EventFailure::DirectResponse.into(),
                ResponseFlags::default(),
            )
            .into_response(req.version());
            add_oauth_response_headers(resp.headers_mut());
            return FilterDecision::DirectResponse(Box::new(resp));
        };

        let mut resp = SyntheticHttpResponse::custom_error(
            StatusCode::FOUND,
            None,
            EventFailure::DirectResponse.into(),
            ResponseFlags::default(),
        )
        .into_response(req.version());
        match HeaderValue::from_str(auth_url.as_str()) {
            Ok(location) => resp.headers_mut().insert(header::LOCATION, location),
            Err(err) => {
                error!(target: "oauth2", "OAuth authorization URL cannot be represented as a header: {err}");
                let mut error_resp = SyntheticHttpResponse::internal_server_error(
                    EventFailure::DirectResponse.into(),
                    ResponseFlags::default(),
                )
                .into_response(req.version());
                add_oauth_response_headers(error_resp.headers_mut());
                return FilterDecision::DirectResponse(Box::new(error_resp));
            },
        };
        add_oauth_response_headers(resp.headers_mut());
        resp.headers_mut().append(header::SET_COOKIE, nonce_header);
        resp.headers_mut().append(header::SET_COOKIE, verifier_header);

        FilterDecision::DirectResponse(Box::new(resp))
    }

    fn callback_failure(&self, version: Version, status: StatusCode, is_https: bool, flow_id: &str) -> FilterDecision {
        let config = &self.inner.config;
        let mut resp = SyntheticHttpResponse::custom_error(
            status,
            None,
            EventFailure::DirectResponse.into(),
            ResponseFlags::default(),
        )
        .into_response(version);
        add_oauth_response_headers(resp.headers_mut());
        clear_transaction_cookies(resp.headers_mut(), config, flow_id, is_https);
        FilterDecision::DirectResponse(Box::new(resp))
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_callback(
        &self,
        version: Version,
        is_https: bool,
        query: &str,
        headers: &HeaderMap,
        host: &str,
    ) -> FilterDecision {
        let config = &self.inner.config;
        let cnames = &config.credentials.cookie_names;
        let cdomain = config.credentials.cookie_domain.as_deref();
        let parameters = match parse_callback_query(query) {
            Ok(parameters) => parameters,
            Err(()) => {
                warn!(target: "oauth2", "invalid or ambiguous OAuth callback query");
                let mut resp =
                    SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into()).into_response(version);
                add_oauth_response_headers(resp.headers_mut());
                return FilterDecision::DirectResponse(Box::new(resp));
            },
        };
        let Some(state) = parameters.state.as_deref() else {
            warn!(target: "oauth2", "callback request is missing state");
            let mut resp =
                SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into()).into_response(version);
            add_oauth_response_headers(resp.headers_mut());
            return FilterDecision::DirectResponse(Box::new(resp));
        };
        let Some((flow_id, nonce)) = split_callback_state(state) else {
            warn!(target: "oauth2", "callback request contains malformed state");
            let mut resp =
                SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into()).into_response(version);
            add_oauth_response_headers(resp.headers_mut());
            return FilterDecision::DirectResponse(Box::new(resp));
        };
        let nonce_cookie_name = flow_cookie_name(cnames.oauth_nonce.as_str(), flow_id);
        let Some(transaction) = verify_transaction_cookie(
            &self.inner.hmac_key,
            state,
            extract_cookie(headers, &nonce_cookie_name),
            config.csrf_token_expires_in.as_secs(),
        ) else {
            warn!(target: "oauth2", "callback CSRF state validation failed");
            let mut resp =
                SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into()).into_response(version);
            add_oauth_response_headers(resp.headers_mut());
            return FilterDecision::DirectResponse(Box::new(resp));
        };
        debug_assert_eq!(transaction.flow_id, flow_id);
        debug_assert_eq!(transaction.nonce, nonce);

        if parameters.error.is_some() {
            warn!(target: "oauth2", "oauth provider returned an authorization error");
            return self.callback_failure(version, StatusCode::UNAUTHORIZED, is_https, flow_id);
        }
        let Some(code) = parameters.code.as_deref() else {
            warn!(target: "oauth2", "callback request is missing code");
            return self.callback_failure(version, StatusCode::BAD_REQUEST, is_https, flow_id);
        };
        let verifier_cookie_name = flow_cookie_name(cnames.code_verifier.as_str(), flow_id);
        let mut verifier_buf = [0_u8; 256];
        let Some(code_verifier) = extract_cookie(headers, &verifier_cookie_name)
            .and_then(|value| decrypt_cookie_value(&self.inner.token_cipher, value, &mut verifier_buf))
            .and_then(|value| verify_code_verifier(&value, config.code_verifier_token_expires_in.as_secs()))
        else {
            warn!(target: "oauth2", "callback PKCE verifier validation failed");
            return self.callback_failure(version, StatusCode::BAD_REQUEST, is_https, flow_id);
        };

        let token_resp = match self.exchange_code(code, &transaction.redirect_uri, &code_verifier).await {
            Ok(resp) => resp,
            Err(err) => {
                error!(target: "oauth2", "failed to exchange authorization code for token: {err}");
                return self.callback_failure(version, StatusCode::BAD_GATEWAY, is_https, flow_id);
            },
        };
        let now_secs = unix_timestamp_secs();
        let expires_in = token_resp.expires_in.unwrap_or_else(|| {
            let configured = config.default_expires_in.as_secs();
            if configured == 0 {
                3600
            } else {
                configured
            }
        });
        let Some(expires_at) = now_secs.checked_add(expires_in) else {
            error!(target: "oauth2", "OAuth token expiration overflows Unix timestamp");
            return self.callback_failure(version, StatusCode::INTERNAL_SERVER_ERROR, is_https, flow_id);
        };
        let mut expires_buffer = itoa::Buffer::new();
        let expires = expires_buffer.format(expires_at);
        let access_token = (!config.disable_access_token_set_cookie).then_some(token_resp.access_token.as_str());
        let id_token = (!config.disable_id_token_set_cookie).then_some(token_resp.id_token.as_deref()).flatten();
        let refresh_token = (config.use_refresh_token && !config.disable_refresh_token_set_cookie)
            .then_some(token_resp.refresh_token.as_deref())
            .flatten();
        let access_cookie = access_token.and_then(|token| self.encrypt_token_cookie(token));
        let id_cookie = id_token.and_then(|token| self.encrypt_token_cookie(token));
        let refresh_cookie = refresh_token.and_then(|token| self.encrypt_token_cookie(token));
        if (access_token.is_some() && access_cookie.is_none())
            || (id_token.is_some() && id_cookie.is_none())
            || (refresh_token.is_some() && refresh_cookie.is_none())
        {
            error!(target: "oauth2", "failed to encrypt OAuth token cookie");
            return self.callback_failure(version, StatusCode::INTERNAL_SERVER_ERROR, is_https, flow_id);
        }
        let domain = hmac_domain(cdomain, host);
        let hmac_val =
            compute_session_hmac(&self.inner.hmac_key, domain, expires, access_token, id_token, refresh_token);

        let mut resp = SyntheticHttpResponse::custom_error(
            StatusCode::FOUND,
            None,
            EventFailure::DirectResponse.into(),
            ResponseFlags::default(),
        )
        .into_response(version);
        let Ok(location) = HeaderValue::from_str(&transaction.target) else {
            return self.callback_failure(version, StatusCode::INTERNAL_SERVER_ERROR, is_https, flow_id);
        };
        resp.headers_mut().insert(header::LOCATION, location);
        add_oauth_response_headers(resp.headers_mut());

        if let Some(token) = access_cookie {
            append_cookie(
                resp.headers_mut(),
                cnames.bearer_token.as_str(),
                &token,
                Some(expires_in),
                config.cookie_configs.bearer_token_cookie_config.as_ref(),
                cdomain,
                is_https,
            );
        }
        append_cookie(
            resp.headers_mut(),
            cnames.oauth_expires.as_str(),
            expires,
            Some(expires_in),
            config.cookie_configs.oauth_expires_cookie_config.as_ref(),
            cdomain,
            is_https,
        );
        append_cookie(
            resp.headers_mut(),
            cnames.oauth_hmac.as_str(),
            &hmac_val,
            Some(expires_in),
            config.cookie_configs.oauth_hmac_cookie_config.as_ref(),
            cdomain,
            is_https,
        );
        if let Some(token) = id_cookie {
            append_cookie(
                resp.headers_mut(),
                cnames.id_token.as_str(),
                &token,
                Some(expires_in),
                config.cookie_configs.id_token_cookie_config.as_ref(),
                cdomain,
                is_https,
            );
        }
        if let Some(token) = refresh_cookie {
            append_cookie(
                resp.headers_mut(),
                cnames.refresh_token.as_str(),
                &token,
                Some(config.default_refresh_token_expires_in.as_secs()),
                config.cookie_configs.refresh_token_cookie_config.as_ref(),
                cdomain,
                is_https,
            );
        }
        clear_transaction_cookies(resp.headers_mut(), config, flow_id, is_https);

        FilterDecision::DirectResponse(Box::new(resp))
    }

    async fn exchange_code(
        &self,
        code: &str,
        redirect_uri: &str,
        code_verifier: &str,
    ) -> Result<TokenEndpointResponse, OauthClientError> {
        let config = &self.inner.config;
        let (body, basic_auth) = {
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            form.append_pair("grant_type", "authorization_code");
            form.append_pair("code", code);
            form.append_pair("redirect_uri", redirect_uri);
            form.append_pair("code_verifier", code_verifier);
            let basic_auth = match config.auth_type {
                AuthType::UrlEncodedBody => {
                    form.append_pair("client_id", config.credentials.client_id.as_str());
                    form.append_pair("client_secret", config.credentials.token_secret.as_str());
                    None
                },
                AuthType::BasicAuth => {
                    let credentials = format!("{}:{}", config.credentials.client_id, config.credentials.token_secret);
                    Some(format!("Basic {}", BASE64_STANDARD.encode(credentials.as_bytes())))
                },
            };
            (form.finish(), basic_auth)
        };
        let req_builder = self
            .inner
            .client
            .post(config.token_endpoint.uri.as_str())
            .timeout(config.token_endpoint.timeout)
            .header(header::CONTENT_TYPE.as_str(), "application/x-www-form-urlencoded")
            .body(body);
        let req_builder = match basic_auth {
            Some(value) => req_builder.header(header::AUTHORIZATION.as_str(), value),
            None => req_builder,
        };

        let resp = req_builder.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            debug!(target: "oauth2", "token endpoint returned status {status}");
            return Err(OauthClientError::TokenEndpointError(status));
        }

        Ok(resp.json::<TokenEndpointResponse>().await?)
    }
}

// === Helper Functions ===

fn local_redirect_target(target: Option<&str>) -> &str {
    let Some(target) = target else {
        return "/";
    };

    let is_local_path = target.starts_with('/')
        && !target.starts_with("//")
        && !target.contains('\\')
        && PathAndQuery::from_str(target).is_ok();

    is_local_path.then_some(target).unwrap_or("/")
}

const TRANSACTION_COOKIE_VERSION: &str = "v1";
const ENCRYPTED_COOKIE_PREFIX: &str = "gcm.";
const AEAD_NONCE_LEN: usize = 12;

struct OAuthTransaction {
    flow_id: String,
    nonce: String,
    target: String,
    redirect_uri: String,
}

#[derive(Default)]
struct CallbackParameters {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

fn random_urlsafe_token(bytes: usize) -> String {
    let mut random = vec![0_u8; bytes];
    rand::thread_rng().fill_bytes(&mut random);
    BASE64_URL_SAFE_NO_PAD.encode(random)
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn flow_cookie_name(base_name: &str, flow_id: &str) -> String {
    format!("{base_name}.{flow_id}")
}

fn transaction_tag(key: &hmac::Key, payload: &str) -> hmac::Tag {
    let mut context = hmac::Context::with_key(key);
    context.update(b"arion.oauth2.transaction.v1\0");
    context.update(payload.as_bytes());
    context.sign()
}

fn encode_transaction_cookie(
    key: &hmac::Key,
    flow_id: &str,
    nonce: &str,
    issued_at: u64,
    target: &str,
    redirect_uri: &str,
) -> String {
    let target = BASE64_URL_SAFE_NO_PAD.encode(target);
    let redirect_uri = BASE64_URL_SAFE_NO_PAD.encode(redirect_uri);
    let payload = format!("{TRANSACTION_COOKIE_VERSION}.{flow_id}.{nonce}.{issued_at}.{target}.{redirect_uri}");
    let signature = BASE64_URL_SAFE_NO_PAD.encode(transaction_tag(key, &payload).as_ref());
    format!("{payload}.{signature}")
}

fn split_callback_state(state: &str) -> Option<(&str, &str)> {
    let (flow_id, nonce) = state.split_once('.')?;
    (!flow_id.is_empty()
        && !nonce.is_empty()
        && flow_id.len() <= 64
        && nonce.len() <= 256
        && flow_id.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        && nonce.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
    .then_some((flow_id, nonce))
}

fn verify_transaction_cookie(
    key: &hmac::Key,
    state: &str,
    cookie: Option<&str>,
    max_age_secs: u64,
) -> Option<OAuthTransaction> {
    let (state_flow_id, state_nonce) = split_callback_state(state)?;
    let cookie = cookie?;
    let (payload, signature) = cookie.rsplit_once('.')?;
    let signature = BASE64_URL_SAFE_NO_PAD.decode(signature).ok()?;
    let expected = transaction_tag(key, payload);
    if signature.len() != expected.as_ref().len()
        || constant_time::verify_slices_are_equal(expected.as_ref(), &signature).is_err()
    {
        return None;
    }

    let mut fields = payload.split('.');
    let (Some(version), Some(flow_id), Some(nonce), Some(issued_at), Some(target), Some(redirect_uri), None) =
        (fields.next(), fields.next(), fields.next(), fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return None;
    };
    if version != TRANSACTION_COOKIE_VERSION
        || constant_time::verify_slices_are_equal(flow_id.as_bytes(), state_flow_id.as_bytes()).is_err()
        || constant_time::verify_slices_are_equal(nonce.as_bytes(), state_nonce.as_bytes()).is_err()
    {
        return None;
    }
    let issued_at = issued_at.parse::<u64>().ok()?;
    let now = unix_timestamp_secs();
    if issued_at > now || now.checked_sub(issued_at)? > max_age_secs {
        return None;
    }
    let target = String::from_utf8(BASE64_URL_SAFE_NO_PAD.decode(target).ok()?).ok()?;
    let redirect_uri = String::from_utf8(BASE64_URL_SAFE_NO_PAD.decode(redirect_uri).ok()?).ok()?;
    if local_redirect_target(Some(&target)) != target || !is_valid_redirect_uri(&redirect_uri) {
        return None;
    }

    Some(OAuthTransaction { flow_id: flow_id.to_owned(), nonce: nonce.to_owned(), target, redirect_uri })
}

fn is_valid_redirect_uri(value: &str) -> bool {
    let Ok(parsed) = Url::parse(value) else {
        return false;
    };
    matches!(parsed.scheme(), "http" | "https")
        && parsed.host_str().is_some()
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.fragment().is_none()
}

fn verify_code_verifier(payload: &str, max_age_secs: u64) -> Option<String> {
    let (issued_at, verifier) = payload.split_once('.')?;
    let issued_at = issued_at.parse::<u64>().ok()?;
    let now = unix_timestamp_secs();
    (issued_at <= now
        && now.checked_sub(issued_at)? <= max_age_secs
        && (43..=128).contains(&verifier.len())
        && verifier.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')))
    .then(|| verifier.to_owned())
}

fn strict_form_decode(value: &str) -> Option<String> {
    fn hex_value(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            },
            b'%' => {
                let high = *bytes.get(index + 1)?;
                let low = *bytes.get(index + 2)?;
                decoded.push((hex_value(high)? << 4) | hex_value(low)?);
                index += 3;
            },
            byte => {
                decoded.push(byte);
                index += 1;
            },
        }
    }
    String::from_utf8(decoded).ok()
}

fn parse_callback_query(query: &str) -> Result<CallbackParameters, ()> {
    let mut params = CallbackParameters::default();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_name, raw_value) = pair.split_once('=').ok_or(())?;
        let name = strict_form_decode(raw_name).ok_or(())?;
        let value = strict_form_decode(raw_value).ok_or(())?;
        let slot = match name.as_str() {
            "code" => &mut params.code,
            "state" => &mut params.state,
            "error" => &mut params.error,
            "error_description" => continue,
            _ => continue,
        };
        if slot.replace(value).is_some() {
            return Err(());
        }
    }
    Ok(params)
}

fn encrypt_cookie_value(key: &aead::LessSafeKey, value: &str) -> Option<String> {
    let mut nonce = [0_u8; AEAD_NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce);
    let mut ciphertext = value.as_bytes().to_vec();
    key.seal_in_place_append_tag(aead::Nonce::assume_unique_for_key(nonce), aead::Aad::empty(), &mut ciphertext)
        .ok()?;
    let mut payload = Vec::with_capacity(nonce.len() + ciphertext.len());
    payload.extend_from_slice(&nonce);
    payload.extend_from_slice(&ciphertext);
    Some(format!("{ENCRYPTED_COOKIE_PREFIX}{}", BASE64_URL_SAFE_NO_PAD.encode(payload)))
}

fn decrypt_cookie_value<'b>(key: &aead::LessSafeKey, value: &str, buf: &'b mut [u8]) -> Option<Cow<'b, str>> {
    let encoded = value.strip_prefix(ENCRYPTED_COOKIE_PREFIX)?;
    if let Ok(decoded_len) = BASE64_URL_SAFE_NO_PAD.decode_slice(encoded.as_bytes(), buf) {
        if decoded_len <= AEAD_NONCE_LEN {
            return None;
        }
        let (nonce_bytes, ciphertext) = buf[..decoded_len].split_at_mut(AEAD_NONCE_LEN);
        let nonce: [u8; AEAD_NONCE_LEN] = nonce_bytes.try_into().ok()?;
        let plaintext = key
            .open_in_place(aead::Nonce::assume_unique_for_key(nonce), aead::Aad::empty(), ciphertext)
            .ok()?;
        std::str::from_utf8(plaintext).ok().map(Cow::Borrowed)
    } else {
        let mut payload = BASE64_URL_SAFE_NO_PAD.decode(encoded).ok()?;
        if payload.len() <= AEAD_NONCE_LEN {
            return None;
        }
        let nonce: [u8; AEAD_NONCE_LEN] = payload[..AEAD_NONCE_LEN].try_into().ok()?;
        let plaintext = key
            .open_in_place(aead::Nonce::assume_unique_for_key(nonce), aead::Aad::empty(), &mut payload[AEAD_NONCE_LEN..])
            .ok()?;
        std::str::from_utf8(plaintext).ok().map(|s| Cow::Owned(s.to_owned()))
    }
}

fn extract_cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    for value in headers.get_all(header::COOKIE) {
        let Ok(cookie_header) = value.to_str() else { continue };
        for pair in cookie_header.split(';') {
            let Some((cookie_name, cookie_value)) = pair.split_once('=') else { continue };
            if cookie_name.trim() == name {
                return Some(cookie_value.trim());
            }
        }
    }
    None
}

fn extract_host(req: &Request<ArionRequestBody>) -> Option<&str> {
    if let Some(host) = req.headers().get(header::HOST).and_then(|value| value.to_str().ok()) {
        return Some(host);
    }
    req.uri().authority().map(http::uri::Authority::as_str)
}

struct SessionCookies<'a> {
    access_token: Option<&'a str>,
    hmac: Option<&'a str>,
    expires: Option<&'a str>,
    id_token: Option<&'a str>,
    refresh_token: Option<&'a str>,
}

fn extract_session_cookies<'a>(headers: &'a HeaderMap, cookie_names: &CookieNames) -> SessionCookies<'a> {
    let mut cookies =
        SessionCookies { access_token: None, hmac: None, expires: None, id_token: None, refresh_token: None };

    for value in headers.get_all(header::COOKIE) {
        let Ok(cookie_header) = value.to_str() else { continue };
        for pair in cookie_header.split(';') {
            let Some((name, value)) = pair.split_once('=') else { continue };
            let name = name.trim();
            let value = value.trim();
            if name == cookie_names.bearer_token.as_str() {
                cookies.access_token = Some(value);
            } else if name == cookie_names.oauth_hmac.as_str() {
                cookies.hmac = Some(value);
            } else if name == cookie_names.oauth_expires.as_str() {
                cookies.expires = Some(value);
            } else if name == cookie_names.id_token.as_str() {
                cookies.id_token = Some(value);
            } else if name == cookie_names.refresh_token.as_str() {
                cookies.refresh_token = Some(value);
            }
        }
    }

    cookies
}

#[inline]
fn hmac_domain<'a>(cookie_domain: Option<&'a str>, host: &'a str) -> &'a str {
    cookie_domain.filter(|domain| !domain.is_empty()).unwrap_or(host)
}

fn session_hmac_tag(
    key: &hmac::Key,
    domain: &str,
    expires: &str,
    access_token: Option<&str>,
    id_token: Option<&str>,
    refresh_token: Option<&str>,
) -> hmac::Tag {
    // Envoy separates the five fields with newlines and does not terminate the final field.
    // Incremental updates avoid allocating a concatenated payload on the request path.
    let mut context = hmac::Context::with_key(key);
    context.update(domain.as_bytes());
    context.update(b"\n");
    context.update(expires.as_bytes());
    context.update(b"\n");
    context.update(access_token.unwrap_or_default().as_bytes());
    context.update(b"\n");
    context.update(id_token.unwrap_or_default().as_bytes());
    context.update(b"\n");
    context.update(refresh_token.unwrap_or_default().as_bytes());
    context.sign()
}

fn compute_session_hmac(
    key: &hmac::Key,
    domain: &str,
    expires: &str,
    access_token: Option<&str>,
    id_token: Option<&str>,
    refresh_token: Option<&str>,
) -> String {
    BASE64_STANDARD.encode(session_hmac_tag(key, domain, expires, access_token, id_token, refresh_token).as_ref())
}

fn verify_session_hmac(
    key: &hmac::Key,
    domain: &str,
    expires: &str,
    access_token: Option<&str>,
    id_token: Option<&str>,
    refresh_token: Option<&str>,
    signature: &str,
) -> bool {
    let tag = session_hmac_tag(key, domain, expires, access_token, id_token, refresh_token);
    let mut decoded = [0_u8; 64];
    let Ok(decoded_len) = BASE64_STANDARD.decode_slice(signature, &mut decoded) else {
        return false;
    };

    if decoded_len == tag.as_ref().len()
        && constant_time::verify_slices_are_equal(tag.as_ref(), &decoded[..decoded_len]).is_ok()
    {
        return true;
    }

    // Envoy accepts cookies emitted by versions that encoded the lowercase hexadecimal digest
    // before Base64 encoding it.
    if decoded_len != tag.as_ref().len() * 2 {
        return false;
    }
    let mut expected_legacy = [0_u8; 64];
    for (byte, hex) in tag.as_ref().iter().zip(expected_legacy.chunks_exact_mut(2)) {
        hex[0] = HEX_LOWER[(byte >> 4) as usize];
        hex[1] = HEX_LOWER[(byte & 0x0f) as usize];
    }
    constant_time::verify_slices_are_equal(&expected_legacy, &decoded[..decoded_len]).is_ok()
}

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

fn request_is_https(req: &Request<ArionRequestBody>) -> bool {
    req.uri().scheme_str() == Some("https")
        || req.headers().get("x-forwarded-proto").is_some_and(|value| value == "https")
}

fn add_oauth_response_headers(headers: &mut HeaderMap) {
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(http::HeaderName::from_static("referrer-policy"), HeaderValue::from_static("no-referrer"));
}

fn append_cookie(
    headers: &mut HeaderMap,
    name: &str,
    value: &str,
    max_age: Option<u64>,
    config: Option<&CookieConfig>,
    domain: Option<&str>,
    is_secure: bool,
) {
    if let Some(cookie) = format_cookie(name, value, max_age, config, domain, is_secure) {
        headers.append(header::SET_COOKIE, cookie);
    }
}

fn append_expired_cookie(
    headers: &mut HeaderMap,
    name: &str,
    config: Option<&CookieConfig>,
    domain: Option<&str>,
    is_secure: bool,
) {
    append_cookie(headers, name, "", Some(0), config, domain, is_secure);
}

fn clear_transaction_cookies(headers: &mut HeaderMap, config: &OAuth2Config, flow_id: &str, is_https: bool) {
    let names = &config.credentials.cookie_names;
    let domain = config.credentials.cookie_domain.as_deref();
    append_expired_cookie(
        headers,
        &flow_cookie_name(names.oauth_nonce.as_str(), flow_id),
        config.cookie_configs.oauth_nonce_cookie_config.as_ref(),
        domain,
        is_https,
    );
    append_expired_cookie(
        headers,
        &flow_cookie_name(names.code_verifier.as_str(), flow_id),
        config.cookie_configs.code_verifier_cookie_config.as_ref(),
        domain,
        is_https,
    );
}

#[allow(clippy::too_many_arguments)]
fn format_cookie(
    name: &str,
    value: &str,
    max_age: Option<u64>,
    config: Option<&CookieConfig>,
    domain: Option<&str>,
    is_secure: bool,
) -> Option<HeaderValue> {
    let path = config.map_or("/", |c| c.path.as_str());
    let domain_len = domain.map_or(0, |d| if d.is_empty() { 0 } else { d.len() + 9 });
    let estimated_cap = name.len() + 1 + value.len() + 7 + path.len() + domain_len + 64;

    let mut buf = String::with_capacity(estimated_cap);
    buf.push_str(name);
    buf.push('=');
    buf.push_str(value);
    buf.push_str("; Path=");
    buf.push_str(path);
    buf.push_str("; HttpOnly");

    if is_secure {
        buf.push_str("; Secure");
    }

    if let Some(d) = domain {
        if !d.is_empty() {
            buf.push_str("; Domain=");
            buf.push_str(d);
        }
    }

    if let Some(age) = max_age {
        buf.push_str("; Max-Age=");
        let mut itoa_buf = itoa::Buffer::new();
        buf.push_str(itoa_buf.format(age));
    }

    if let Some(cfg) = config {
        match cfg.same_site {
            CookieSameSite::Strict => buf.push_str("; SameSite=Strict"),
            CookieSameSite::Lax => buf.push_str("; SameSite=Lax"),
            CookieSameSite::None => buf.push_str("; SameSite=None"),
            CookieSameSite::Disabled => {},
        }
        if cfg.partitioned {
            buf.push_str("; Partitioned");
        }
    } else {
        buf.push_str("; SameSite=Lax");
    }

    HeaderValue::try_from(buf.into_bytes()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use arion_configuration::config::{
        core::{HttpUri, StringMatcher},
        network_filters::http_connection_manager::{
            header_matcher::HeaderMatcher,
            http_filters::oauth2::{CookieConfigs, OAuth2Credentials},
            route::{PathMatcher, PathSpecifier},
        },
    };

    fn sample_config() -> OAuth2Config {
        OAuth2Config {
            token_endpoint: HttpUri {
                uri: "https://auth.example.com/oauth/token".into(),
                cluster: "oauth_cluster".into(),
                timeout: Duration::from_secs(5),
            },
            retry_policy: None,
            authorization_endpoint: "https://auth.example.com/oauth/authorize".into(),
            end_session_endpoint: Some("https://auth.example.com/oauth/logout".into()),
            credentials: OAuth2Credentials {
                client_id: "client-id".into(),
                token_secret: "token-secret".into(),
                hmac_secret: "hmac-secret-test-key".into(),
                cookie_names: CookieNames::default(),
                cookie_domain: None,
            },
            redirect_uri: "%REQ(x-forwarded-proto)%://%REQ(:authority)%/callback".into(),
            redirect_path_matcher: PathMatcher {
                specifier: PathSpecifier::Exact("/callback".into()),
                ignore_case: false,
            },
            signout_path: PathMatcher { specifier: PathSpecifier::Exact("/signout".into()), ignore_case: false },
            forward_bearer_token: true,
            preserve_authorization_header: false,
            pass_through_matcher: vec![],
            auth_scopes: vec!["openid".into(), "profile".into()],
            resources: vec![],
            auth_type: AuthType::default(),
            use_refresh_token: true,
            default_expires_in: Duration::from_secs(3600),
            deny_redirect_matcher: vec![],
            default_refresh_token_expires_in: Duration::from_secs(604_800),
            disable_id_token_set_cookie: false,
            disable_access_token_set_cookie: false,
            disable_refresh_token_set_cookie: false,
            cookie_configs: CookieConfigs::default(),
            stat_prefix: "oauth".into(),
            csrf_token_expires_in: Duration::from_secs(600),
            code_verifier_token_expires_in: Duration::from_secs(600),
            disable_token_encryption: false,
        }
    }

    #[tokio::test]
    async fn test_unauthenticated_request_triggers_redirect_with_dynamic_uri() {
        let config = sample_config();
        let mut filter = OAuth2Filter::try_new(config).unwrap();

        let mut req = Request::builder()
            .uri("/protected/resource")
            .header("Host", "gateway.example.com")
            .header("x-forwarded-proto", "https")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        match decision {
            FilterDecision::DirectResponse(resp) => {
                assert_eq!(resp.status(), StatusCode::FOUND);
                let location = resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
                eprintln!("LOCATION IS: {location}");
                assert!(location.starts_with("https://auth.example.com/oauth/authorize?"));
                // Notice the dynamic redirect_uri formatting from request headers!
                assert!(
                    location.contains("https%3A%2F%2Fgateway%2Eexample%2Ecom%2Fcallback")
                        || location.contains("https%3A%2F%2Fgateway.example.com%2Fcallback")
                );
                assert!(location.contains("client_id=client%2Did") || location.contains("client_id=client-id"));
                assert!(resp.headers().contains_key(header::SET_COOKIE));
            },
            _ => panic!("expected 302 DirectResponse"),
        }
    }

    #[tokio::test]
    async fn test_deny_redirect_matcher_returns_401() {
        let mut config = sample_config();
        config.deny_redirect_matcher = vec![HeaderMatcher {
            header_name: "x-deny-redirect".parse().unwrap(),
            header_matcher: StringMatcher::new("true"),
            invert_match: false,
            treat_missing_header_as_empty: false,
        }];

        let mut filter = OAuth2Filter::try_new(config).unwrap();
        let mut req = Request::builder()
            .uri("/api/data")
            .header("Host", "gateway.example.com")
            .header("x-deny-redirect", "true")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        match decision {
            FilterDecision::DirectResponse(resp) => {
                assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
            },
            _ => panic!("expected 401 Unauthorized"),
        }
    }

    #[tokio::test]
    async fn test_pass_through_matcher_bypasses_oauth() {
        let mut config = sample_config();
        config.pass_through_matcher = vec![HeaderMatcher {
            header_name: "x-internal-service".parse().unwrap(),
            header_matcher: StringMatcher::new("true"),
            invert_match: false,
            treat_missing_header_as_empty: false,
        }];

        let mut filter = OAuth2Filter::try_new(config).unwrap();
        let mut req = Request::builder()
            .uri("/api/data")
            .header("Host", "gateway.example.com")
            .header("x-internal-service", "true")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        assert!(matches!(decision, FilterDecision::Continue));
    }

    #[tokio::test]
    async fn test_authenticated_session_verifies_complete_envoy_payload() {
        let mut config = sample_config();
        config.disable_token_encryption = true;
        let mut filter = OAuth2Filter::try_new(config.clone()).unwrap();

        let host = "gateway.example.com:8443";
        let expires_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600;
        let expires = expires_at.to_string();
        let access_token = "my-jwt-access-token";
        let id_token = "my-id-token";
        let refresh_token = "my-refresh-token";
        let key = hmac::Key::new(hmac::HMAC_SHA256, config.credentials.hmac_secret.as_bytes());
        let hmac_value =
            compute_session_hmac(&key, host, &expires, Some(access_token), Some(id_token), Some(refresh_token));

        let cookie_header = format!(
            "BearerToken={access_token}; IdToken={id_token}; RefreshToken={refresh_token}; OauthExpires={expires}; OauthHMAC={hmac_value}"
        );
        let mut request = Request::builder()
            .uri("/api/data")
            .header("Host", host)
            .header("Cookie", cookie_header)
            .body(ArionRequestBody::default())
            .unwrap();

        assert!(matches!(filter.apply_request(&mut request).await, FilterDecision::Continue));
        assert_eq!(request.headers().get(header::AUTHORIZATION).unwrap(), "Bearer my-jwt-access-token");

        let mut tampered_request = Request::builder()
            .uri("/api/data")
            .header("Host", host)
            .header(
                "Cookie",
                format!(
                    "BearerToken=tampered; IdToken={id_token}; RefreshToken={refresh_token}; OauthExpires={expires}; OauthHMAC={hmac_value}"
                ),
            )
            .body(ArionRequestBody::default())
            .unwrap();
        assert!(matches!(filter.apply_request(&mut tampered_request).await, FilterDecision::DirectResponse(_)));
    }

    #[tokio::test]
    async fn test_signout_flow_clears_cookies_and_redirects() {
        let config = sample_config();
        let mut filter = OAuth2Filter::try_new(config).unwrap();

        let mut req = Request::builder()
            .uri("/signout")
            .header("Host", "gateway.example.com")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        match decision {
            FilterDecision::DirectResponse(resp) => {
                assert_eq!(resp.status(), StatusCode::FOUND);
                assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "https://auth.example.com/oauth/logout");
                let cookies: Vec<_> = resp.headers().get_all(header::SET_COOKIE).iter().collect();
                assert!(!cookies.is_empty());
                for c in cookies {
                    assert!(c.to_str().unwrap().contains("Max-Age=0"));
                }
            },
            _ => panic!("expected 302 DirectResponse for signout"),
        }
    }

    #[tokio::test]
    async fn test_signout_clears_cookies_with_their_configured_path() {
        let mut config = sample_config();
        config.cookie_configs.bearer_token_cookie_config =
            Some(CookieConfig { same_site: CookieSameSite::Lax, path: "/protected".into(), partitioned: false });
        let mut filter = OAuth2Filter::try_new(config).unwrap();
        let mut req = Request::builder()
            .uri("/signout")
            .header("Host", "gateway.example.com")
            .header("x-forwarded-proto", "https")
            .body(ArionRequestBody::default())
            .unwrap();

        let FilterDecision::DirectResponse(resp) = filter.apply_request(&mut req).await else {
            panic!("expected signout response");
        };
        let bearer_clear = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .find_map(|value| value.to_str().ok().filter(|cookie| cookie.starts_with("BearerToken=")));
        let bearer_clear = bearer_clear.expect("BearerToken deletion cookie");
        assert!(bearer_clear.contains("Path=/protected"));
        assert!(bearer_clear.contains("Max-Age=0"));
    }

    #[test]
    fn test_format_cookie() {
        let hv = format_cookie("my_cookie", "my_val", Some(3600), None, Some("example.com"), true).unwrap();
        let s = hv.to_str().unwrap();
        assert!(s.contains("my_cookie=my_val"));
        assert!(s.contains("Path=/"));
        assert!(s.contains("HttpOnly"));
        assert!(s.contains("Secure"));
        assert!(s.contains("Domain=example.com"));
        assert!(s.contains("Max-Age=3600"));
        assert!(s.contains("SameSite=Lax"));
    }

    #[test]
    fn test_extract_session_cookies_single_pass() {
        let cnames = CookieNames::default();
        let mut headers = HeaderMap::new();

        headers.insert(
            header::COOKIE,
            "theme=dark; BearerToken=tok123; tracking_id=abc; OauthHMAC=sig456; OauthExpires=789; other=xyz"
                .parse()
                .unwrap(),
        );

        let cookies = extract_session_cookies(&headers, &cnames);
        assert_eq!(cookies.access_token, Some("tok123"));
        assert_eq!(cookies.hmac, Some("sig456"));
        assert_eq!(cookies.expires, Some("789"));
        assert_eq!(cookies.id_token, None);
        assert_eq!(cookies.refresh_token, None);

        let mut headers2 = HeaderMap::new();
        headers2.insert(header::COOKIE, "BearerToken=tok123; IdToken=id; RefreshToken=refresh".parse().unwrap());
        let cookies = extract_session_cookies(&headers2, &cnames);
        assert_eq!(cookies.access_token, Some("tok123"));
        assert_eq!(cookies.hmac, None);
        assert_eq!(cookies.expires, None);
        assert_eq!(cookies.id_token, Some("id"));
        assert_eq!(cookies.refresh_token, Some("refresh"));
    }

    #[test]
    fn test_session_hmac_matches_envoy_payload_format() {
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"hmac-secret");
        let expected = BASE64_STANDARD
            .encode(hmac::sign(&key, b"gateway.example.com:8443\n1234567890\naccess-token\nid-token\nrefresh-token"));
        let actual = compute_session_hmac(
            &key,
            "gateway.example.com:8443",
            "1234567890",
            Some("access-token"),
            Some("id-token"),
            Some("refresh-token"),
        );
        assert_eq!(actual, expected);
        assert!(verify_session_hmac(
            &key,
            "gateway.example.com:8443",
            "1234567890",
            Some("access-token"),
            Some("id-token"),
            Some("refresh-token"),
            &actual,
        ));

        let tag = hmac::sign(&key, b"gateway.example.com:8443\n1234567890\naccess-token\nid-token\nrefresh-token");
        let mut legacy_hex = [0_u8; 64];
        for (byte, hex) in tag.as_ref().iter().zip(legacy_hex.chunks_exact_mut(2)) {
            hex[0] = HEX_LOWER[(byte >> 4) as usize];
            hex[1] = HEX_LOWER[(byte & 0x0f) as usize];
        }
        let legacy = BASE64_STANDARD.encode(legacy_hex);
        assert!(verify_session_hmac(
            &key,
            "gateway.example.com:8443",
            "1234567890",
            Some("access-token"),
            Some("id-token"),
            Some("refresh-token"),
            &legacy,
        ));
    }

    #[test]
    fn test_hmac_domain_uses_cookie_domain_or_unmodified_authority() {
        assert_eq!(hmac_domain(None, "example.com:8080"), "example.com:8080");
        assert_eq!(hmac_domain(Some(".example.com"), "example.com:8080"), ".example.com");
        assert_eq!(hmac_domain(Some(""), "example.com:8080"), "example.com:8080");
    }

    #[test]
    fn transaction_cookie_binds_state_to_local_target_and_redirect_uri() {
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"hmac-secret");
        let state = "flow_id.nonce";
        let cookie = encode_transaction_cookie(
            &key,
            "flow_id",
            "nonce",
            unix_timestamp_secs(),
            "/dashboard?tab=profile",
            "https://gateway.example.com/callback",
        );
        let transaction = verify_transaction_cookie(&key, state, Some(&cookie), 600).expect("valid transaction");
        assert_eq!(transaction.target, "/dashboard?tab=profile");
        assert_eq!(transaction.redirect_uri, "https://gateway.example.com/callback");
    }

    #[test]
    fn transaction_cookie_rejects_tampering_mismatch_and_non_local_target() {
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"hmac-secret");
        let cookie = encode_transaction_cookie(
            &key,
            "flow_id",
            "nonce",
            unix_timestamp_secs(),
            "/dashboard",
            "https://gateway.example.com/callback",
        );
        assert!(verify_transaction_cookie(&key, "flow_id.other", Some(&cookie), 600).is_none());
        assert!(verify_transaction_cookie(&key, "flow_id.nonce", None, 600).is_none());
        assert!(verify_transaction_cookie(&key, "flow_id.nonce", Some(&format!("{cookie}x")), 600).is_none());

        let unsafe_cookie = encode_transaction_cookie(
            &key,
            "flow_id",
            "nonce",
            unix_timestamp_secs(),
            "//evil.example",
            "https://gateway.example.com/callback",
        );
        assert!(verify_transaction_cookie(&key, "flow_id.nonce", Some(&unsafe_cookie), 600).is_none());
    }

    #[test]
    fn encrypted_cookie_round_trip_rejects_tampering() {
        let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &[7_u8; 32]).unwrap();
        let key = aead::LessSafeKey::new(unbound);
        let encrypted = encrypt_cookie_value(&key, "access-token").expect("encryption succeeds");
        assert_ne!(encrypted, "access-token");
        let mut buf = [0_u8; 256];
        assert_eq!(decrypt_cookie_value(&key, &encrypted, &mut buf).as_deref(), Some("access-token"));
        assert_eq!(decrypt_cookie_value(&key, &format!("{encrypted}x"), &mut buf).as_deref(), None);
    }

    #[test]
    fn code_verifier_enforces_server_side_expiration_and_format() {
        let valid = format!("{}.{}", unix_timestamp_secs(), "a".repeat(43));
        assert!(verify_code_verifier(&valid, 600).is_some());
        assert!(verify_code_verifier("0.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 600).is_none());
        assert!(verify_code_verifier("1.invalid", 600).is_none());
    }

    #[test]
    fn callback_parser_rejects_duplicate_critical_parameters() {
        assert!(parse_callback_query("code=first&code=second&state=flow.nonce").is_err());
        assert!(parse_callback_query("code=valid&state=first&state=second").is_err());
        assert!(parse_callback_query("code=%GG&state=flow.nonce").is_err());
    }

    #[tokio::test]
    async fn test_callback_with_oauth_error_without_valid_state_returns_bad_request() {
        let config = sample_config();
        let mut filter = OAuth2Filter::try_new(config).unwrap();

        let mut req = Request::builder()
            .uri("/callback?error=access_denied&error_description=user+denied+consent&state=test")
            .header("Host", "gateway.example.com")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        match decision {
            FilterDecision::DirectResponse(resp) => {
                assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            },
            _ => panic!("expected 400 Bad Request when IdP error has no valid transaction state"),
        }
    }

    #[tokio::test]
    async fn test_callback_with_invalid_percent_encoding_returns_bad_request() {
        let config = sample_config();
        let mut filter = OAuth2Filter::try_new(config).unwrap();

        let mut req = Request::builder()
            .uri("/callback?code=%FF%FF&state=test")
            .header("Host", "gateway.example.com")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        match decision {
            FilterDecision::DirectResponse(resp) => {
                assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            },
            _ => panic!("expected 400 Bad Request on invalid percent encoded query"),
        }
    }

    #[tokio::test]
    async fn test_missing_host_returns_bad_request() {
        let config = sample_config();
        let mut filter = OAuth2Filter::try_new(config).unwrap();

        let mut req = Request::builder().uri("/protected").body(ArionRequestBody::default()).unwrap();

        let decision = filter.apply_request(&mut req).await;
        match decision {
            FilterDecision::DirectResponse(resp) => {
                assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            },
            _ => panic!("expected 400 Bad Request when Host header is missing"),
        }
    }
}
