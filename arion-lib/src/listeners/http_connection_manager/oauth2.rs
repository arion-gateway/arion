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
    sync::{LazyLock, Once},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{prelude::BASE64_STANDARD, Engine};
use http::{header, HeaderMap, HeaderValue, Request, StatusCode, Version};
use papaya::HashMap as PapayaMap;
use rand::Rng;
use aws_lc_rs::hmac;
use serde::Deserialize;
use smol_str::SmolStr;
use tracing::{debug, error, warn};
use triomphe::Arc;

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
    AuthType, CookieConfig, CookieSameSite, OAuth2Config,
};
use arion_format::{
    context::{DownstreamContext, SocketAddrContext},
    uri_formatter::UriFormatter,
};

/// Global concurrent cache storing validated HMAC signatures to avoid repeated cryptographic
/// computation and allocations on subsequent requests.
///
/// Key: HMAC signature string (`SmolStr`).
/// Value: Expiration timestamp in epoch seconds (`u64`).
static OAUTH_SESSION_CACHE: LazyLock<PapayaMap<SmolStr, u64, ahash::RandomState>> =
    LazyLock::new(|| PapayaMap::with_hasher(ahash::RandomState::new()));

static CLEANER_ONCE: Once = Once::new();
static CLEANER_PERIOD: Duration = Duration::from_secs(30);

fn ensure_cleaner_started() {
    CLEANER_ONCE.call_once(|| {
        tokio::spawn(async {
            loop {
                pingora_timeout::sleep(CLEANER_PERIOD).await;
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
                OAUTH_SESSION_CACHE.pin().retain(|_k, &expiry| expiry > now);
            }
        });
    });
}

static OAUTH_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to build oauth reqwest client")
});

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

#[derive(Debug)]
pub struct OAuth2FilterInner {
    pub config: OAuth2Config,
    pub redirect_uri_formatter: UriFormatter,
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

    pub fn build(self) -> OAuth2Filter {
        debug!(target: "oauth2", "creating new OAuth2 filter");

        let redirect_uri_formatter = UriFormatter::try_new(self.config.redirect_uri.as_str())
            .unwrap_or_else(|_| UriFormatter::try_new("").unwrap());

        let inner = Arc::new(OAuth2FilterInner {
            config: self.config,
            redirect_uri_formatter,
        });
        OAuth2Filter { inner }
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
    pub fn new(config: OAuth2Config) -> Self {
        OAuth2FilterBuilder::new(config).build()
    }

    #[allow(dead_code)]
    pub fn config(&self) -> &OAuth2Config {
        &self.inner.config
    }

    pub async fn apply_request(&mut self, req: &mut Request<ArionRequestBody>) -> FilterDecision {
        ensure_cleaner_started();
        let config = &self.inner.config;

        // 1. Pass-through matcher: bypass authentication if configured headers match.
        for matcher in &config.pass_through_matcher {
            if matcher.request_matches(req) {
                debug!(target: "oauth2", "request matched pass_through_matcher, bypassing oauth");
                return FilterDecision::Continue;
            }
        }

        let host = extract_host(req);
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
                let (is_https, redirect_uri, query, version) = {
                    let ctx = DownstreamContext {
                        request: req,
                        request_head_size: 0,
                        trace_id: None,
                        server_name: None,
                        socket_address: SocketAddrContext::default(),
                    };
                    let redirect_uri = self.inner.redirect_uri_formatter.format(&ctx);
                    let is_https = req.uri().scheme_str() == Some("https")
                        || req.headers().get("x-forwarded-proto").is_some_and(|v| v == "https");
                    let query = req.uri().query().unwrap_or_default().to_string();
                    (is_https, redirect_uri, query, req.version())
                };
                return self.handle_callback(version, is_https, redirect_uri, &query, &host).await;
            }
        }

        // 4. Hot path: check for existing valid session cookies.
        let cookie_names = &config.credentials.cookie_names;
        let (bearer_token, oauth_hmac, oauth_expires) = {
            let cookies = extract_cookies(req.headers());
            let tok = cookies
                .iter()
                .find(|(k, _)| *k == cookie_names.bearer_token.as_str())
                .map(|(_, v)| SmolStr::new(*v));
            let hmac_cookie = cookies
                .iter()
                .find(|(k, _)| *k == cookie_names.oauth_hmac.as_str())
                .map(|(_, v)| SmolStr::new(*v));
            let exp_cookie = cookies
                .iter()
                .find(|(k, _)| *k == cookie_names.oauth_expires.as_str())
                .map(|(_, v)| SmolStr::new(*v));
            (tok, hmac_cookie, exp_cookie)
        };

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        if let (Some(token), Some(hmac_val), Some(expires_str)) = (&bearer_token, &oauth_hmac, &oauth_expires) {
            if let Ok(expires_at) = expires_str.parse::<u64>() {
                if expires_at > now_secs {
                    // Check fast-path papaya session cache.
                    let cache = OAUTH_SESSION_CACHE.pin();
                    if let Some(&cached_expiry) = cache.get(hmac_val.as_str()) {
                        if cached_expiry > now_secs {
                            self.inject_bearer_token(req, token.as_str());
                            return FilterDecision::Continue;
                        }
                    }

                    // Cache miss: verify HMAC signature.
                    let expected_data = format!("{host}:{expires_at}");
                    if verify_hmac(config.credentials.hmac_secret.as_str(), &expected_data, hmac_val.as_str()) {
                        cache.insert(hmac_val.clone(), expires_at);
                        self.inject_bearer_token(req, token.as_str());
                        return FilterDecision::Continue;
                    }
                    warn!(target: "oauth2", "invalid HMAC signature for host: {host}");
                } else {
                    debug!(target: "oauth2", "oauth session expired at {expires_at}, now {now_secs}");
                }
            }
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

    fn inject_bearer_token(&self, req: &mut Request<ArionRequestBody>, token: &str) {
        if self.inner.config.forward_bearer_token {
            if self.inner.config.preserve_authorization_header && req.headers().contains_key(header::AUTHORIZATION) {
                return;
            }
            if let Ok(hv) = HeaderValue::from_str(&format!("Bearer {token}")) {
                req.headers_mut().insert(header::AUTHORIZATION, hv);
            }
        }
    }

    fn handle_signout(&self, req: &Request<ArionRequestBody>) -> FilterDecision {
        let config = &self.inner.config;
        let cnames = &config.credentials.cookie_names;
        let cdomain = config.credentials.cookie_domain.as_deref();

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

        // Clear all cookies
        for name in [
            cnames.bearer_token.as_str(),
            cnames.oauth_hmac.as_str(),
            cnames.oauth_expires.as_str(),
            cnames.id_token.as_str(),
            cnames.refresh_token.as_str(),
            cnames.oauth_nonce.as_str(),
        ] {
            let cookie_str = format_cookie(name, "", Some(0), None, cdomain, false);
            if let Ok(hv) = HeaderValue::from_str(&cookie_str) {
                resp.headers_mut().append(header::SET_COOKIE, hv);
            }
        }

        FilterDecision::DirectResponse(Box::new(resp))
    }

    fn handle_unauthenticated(&self, req: &Request<ArionRequestBody>) -> FilterDecision {
        let config = &self.inner.config;
        let cnames = &config.credentials.cookie_names;
        let cdomain = config.credentials.cookie_domain.as_deref();

        // Evaluate redirect_uri on-demand with request context
        let ctx = DownstreamContext {
            request: req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        };
        let redirect_uri = self.inner.redirect_uri_formatter.format(&ctx);

        // Generate random CSRF nonce
        let nonce: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();

        // State encodes the original path and query
        let original_target = req.uri().path_and_query().map_or("/", |pq| pq.as_str());
        let state = percent_encode_str(original_target);
        let encoded_redirect_uri = percent_encode_str(redirect_uri.as_str());
        let scopes = config.auth_scopes.join(" ");
        let encoded_scopes = percent_encode_str(&scopes);

        let auth_url = format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&scope={}&state={}",
            config.authorization_endpoint,
            percent_encode_str(config.credentials.client_id.as_str()),
            encoded_redirect_uri,
            encoded_scopes,
            state
        );

        let mut resp = SyntheticHttpResponse::custom_error(
            StatusCode::FOUND,
            None,
            EventFailure::DirectResponse.into(),
            ResponseFlags::default(),
        )
        .into_response(req.version());

        if let Ok(loc) = HeaderValue::from_str(&auth_url) {
            resp.headers_mut().insert(header::LOCATION, loc);
        }

        // Set CSRF nonce cookie
        let is_https = req.uri().scheme_str() == Some("https")
            || req.headers().get("x-forwarded-proto").is_some_and(|v| v == "https");
        let nonce_cookie = format_cookie(
            cnames.oauth_nonce.as_str(),
            &nonce,
            Some(config.csrf_token_expires_in.as_secs()),
            config.cookie_configs.oauth_nonce_cookie_config.as_ref(),
            cdomain,
            is_https,
        );

        if let Ok(hv) = HeaderValue::from_str(&nonce_cookie) {
            resp.headers_mut().append(header::SET_COOKIE, hv);
        }

        FilterDecision::DirectResponse(Box::new(resp))
    }

    async fn handle_callback(
        &self,
        version: Version,
        is_https: bool,
        redirect_uri: SmolStr,
        query: &str,
        host: &str,
    ) -> FilterDecision {
        let config = &self.inner.config;
        let cnames = &config.credentials.cookie_names;
        let cdomain = config.credentials.cookie_domain.as_deref();

        let mut code = None;
        let mut state = None;

        for pair in query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                if k == "code" {
                    code = Some(percent_decode_str(v));
                } else if k == "state" {
                    state = Some(percent_decode_str(v));
                }
            }
        }

        let Some(code_val) = code else {
            warn!(target: "oauth2", "callback request missing 'code' parameter");
            let resp = SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into())
                .into_response(version);
            return FilterDecision::DirectResponse(Box::new(resp));
        };

        // Exchange authorization code for access token via token_endpoint
        let token_result = self.exchange_code(&code_val, &redirect_uri).await;
        let token_resp = match token_result {
            Ok(resp) => resp,
            Err(e) => {
                error!(target: "oauth2", "failed to exchange code for token: {e}");
                let resp = SyntheticHttpResponse::internal_server_error(
                    EventFailure::DirectResponse.into(),
                    ResponseFlags::default(),
                )
                .into_response(version);
                return FilterDecision::DirectResponse(Box::new(resp));
            },
        };

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let expires_in = token_resp.expires_in.unwrap_or_else(|| {
            if config.default_expires_in.as_secs() > 0 {
                config.default_expires_in.as_secs()
            } else {
                3600
            }
        });
        let expires_at = now_secs + expires_in;

        // Compute HMAC signature for host + expires
        let hmac_payload = format!("{host}:{expires_at}");
        let hmac_val = compute_hmac(config.credentials.hmac_secret.as_str(), &hmac_payload);

        // Insert into fast session cache
        OAUTH_SESSION_CACHE.pin().insert(SmolStr::new(&hmac_val), expires_at);

        // Prepare redirect to target URL (state or /)
        let target_url = state.as_deref().unwrap_or("/");

        let mut resp = SyntheticHttpResponse::custom_error(
            StatusCode::FOUND,
            None,
            EventFailure::DirectResponse.into(),
            ResponseFlags::default(),
        )
        .into_response(version);

        if let Ok(loc) = HeaderValue::from_str(target_url) {
            resp.headers_mut().insert(header::LOCATION, loc);
        }

        // Set BearerToken cookie
        if !config.disable_access_token_set_cookie {
            let cookie = format_cookie(
                cnames.bearer_token.as_str(),
                &token_resp.access_token,
                Some(expires_in),
                config.cookie_configs.bearer_token_cookie_config.as_ref(),
                cdomain,
                is_https,
            );
            if let Ok(hv) = HeaderValue::from_str(&cookie) {
                resp.headers_mut().append(header::SET_COOKIE, hv);
            }
        }

        // Set OauthExpires cookie
        let exp_cookie = format_cookie(
            cnames.oauth_expires.as_str(),
            &expires_at.to_string(),
            Some(expires_in),
            config.cookie_configs.oauth_expires_cookie_config.as_ref(),
            cdomain,
            is_https,
        );
        if let Ok(hv) = HeaderValue::from_str(&exp_cookie) {
            resp.headers_mut().append(header::SET_COOKIE, hv);
        }

        // Set OauthHMAC cookie
        let hmac_cookie = format_cookie(
            cnames.oauth_hmac.as_str(),
            &hmac_val,
            Some(expires_in),
            config.cookie_configs.oauth_hmac_cookie_config.as_ref(),
            cdomain,
            is_https,
        );
        if let Ok(hv) = HeaderValue::from_str(&hmac_cookie) {
            resp.headers_mut().append(header::SET_COOKIE, hv);
        }

        // Set IdToken cookie if present and enabled
        if !config.disable_id_token_set_cookie {
            if let Some(id_tok) = &token_resp.id_token {
                let id_cookie = format_cookie(
                    cnames.id_token.as_str(),
                    id_tok,
                    Some(expires_in),
                    config.cookie_configs.id_token_cookie_config.as_ref(),
                    cdomain,
                    is_https,
                );
                if let Ok(hv) = HeaderValue::from_str(&id_cookie) {
                    resp.headers_mut().append(header::SET_COOKIE, hv);
                }
            }
        }

        // Set RefreshToken cookie if present and enabled
        if !config.disable_refresh_token_set_cookie && config.use_refresh_token {
            if let Some(ref_tok) = &token_resp.refresh_token {
                let ref_exp = config.default_refresh_token_expires_in.as_secs();
                let ref_cookie = format_cookie(
                    cnames.refresh_token.as_str(),
                    ref_tok,
                    Some(ref_exp),
                    config.cookie_configs.refresh_token_cookie_config.as_ref(),
                    cdomain,
                    is_https,
                );
                if let Ok(hv) = HeaderValue::from_str(&ref_cookie) {
                    resp.headers_mut().append(header::SET_COOKIE, hv);
                }
            }
        }

        // Clear OauthNonce cookie
        let clear_nonce = format_cookie(cnames.oauth_nonce.as_str(), "", Some(0), None, cdomain, false);
        if let Ok(hv) = HeaderValue::from_str(&clear_nonce) {
            resp.headers_mut().append(header::SET_COOKIE, hv);
        }

        FilterDecision::DirectResponse(Box::new(resp))
    }

    async fn exchange_code(&self, code: &str, redirect_uri: &str) -> Result<TokenEndpointResponse, String> {
        let config = &self.inner.config;
        let uri = config.token_endpoint.uri.as_str();

        let req_builder = OAUTH_HTTP_CLIENT
            .post(uri)
            .timeout(config.token_endpoint.timeout);

        let req_builder = match config.auth_type {
            AuthType::UrlEncodedBody => {
                let body = format!(
                    "grant_type=authorization_code&code={}&redirect_uri={}&client_id={}&client_secret={}",
                    percent_encode_str(code),
                    percent_encode_str(redirect_uri),
                    percent_encode_str(config.credentials.client_id.as_str()),
                    percent_encode_str(config.credentials.token_secret.as_str()),
                );
                req_builder
                    .header(header::CONTENT_TYPE.as_str(), "application/x-www-form-urlencoded")
                    .body(body)
            },
            AuthType::BasicAuth => {
                let body = format!(
                    "grant_type=authorization_code&code={}&redirect_uri={}",
                    percent_encode_str(code),
                    percent_encode_str(redirect_uri),
                );
                let creds = format!("{}:{}", config.credentials.client_id, config.credentials.token_secret);
                let auth_header = format!("Basic {}", BASE64_STANDARD.encode(creds.as_bytes()));
                req_builder
                    .header(header::AUTHORIZATION.as_str(), auth_header)
                    .header(header::CONTENT_TYPE.as_str(), "application/x-www-form-urlencoded")
                    .body(body)
            },
        };

        let resp = req_builder.send().await.map_err(|e| format!("request error: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("token endpoint returned status {status}: {body}"));
        }

        resp.json::<TokenEndpointResponse>()
            .await
            .map_err(|e| format!("failed to decode json response: {e}"))
    }
}

// === Helper Functions ===

fn extract_host(req: &Request<ArionRequestBody>) -> String {
    if let Some(host) = req.headers().get(header::HOST).and_then(|v| v.to_str().ok()) {
        return host.split(':').next().unwrap_or(host).to_string();
    }
    if let Some(auth) = req.uri().authority() {
        return auth.host().to_string();
    }
    "localhost".to_string()
}

fn extract_cookies<'a>(headers: &'a HeaderMap) -> Vec<(&'a str, &'a str)> {
    let mut cookies = Vec::new();
    for hv in headers.get_all(header::COOKIE) {
        if let Ok(cookie_str) = hv.to_str() {
            for pair in cookie_str.split(';') {
                let pair = pair.trim();
                if let Some((k, v)) = pair.split_once('=') {
                    cookies.push((k.trim(), v.trim()));
                }
            }
        }
    }
    cookies
}

fn compute_hmac(secret: &str, data: &str) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let sig = hmac::sign(&key, data.as_bytes());
    BASE64_STANDARD.encode(sig.as_ref())
}

fn verify_hmac(secret: &str, data: &str, signature_b64: &str) -> bool {
    let Ok(sig_bytes) = BASE64_STANDARD.decode(signature_b64) else {
        return false;
    };
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    hmac::verify(&key, data.as_bytes(), &sig_bytes).is_ok()
}

fn format_cookie(
    name: &str,
    value: &str,
    max_age: Option<u64>,
    config: Option<&CookieConfig>,
    domain: Option<&str>,
    is_secure: bool,
) -> String {
    let path = config.map_or("/", |c| c.path.as_str());
    let mut parts = Vec::with_capacity(7);
    parts.push(format!("{name}={value}"));
    parts.push(format!("Path={path}"));
    parts.push("HttpOnly".to_string());

    if is_secure {
        parts.push("Secure".to_string());
    }

    if let Some(d) = domain {
        if !d.is_empty() {
            parts.push(format!("Domain={d}"));
        }
    }

    if let Some(age) = max_age {
        parts.push(format!("Max-Age={age}"));
    }

    if let Some(cfg) = config {
        match cfg.same_site {
            CookieSameSite::Strict => parts.push("SameSite=Strict".to_string()),
            CookieSameSite::Lax => parts.push("SameSite=Lax".to_string()),
            CookieSameSite::None => parts.push("SameSite=None".to_string()),
            CookieSameSite::Disabled => {},
        }
        if cfg.partitioned {
            parts.push("Partitioned".to_string());
        }
    } else {
        parts.push("SameSite=Lax".to_string());
    }

    parts.join("; ")
}

fn percent_encode_str(input: &str) -> String {
    percent_encoding::utf8_percent_encode(input, percent_encoding::NON_ALPHANUMERIC).to_string()
}

fn percent_decode_str(input: &str) -> String {
    percent_encoding::percent_decode_str(input)
        .decode_utf8_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arion_configuration::config::{
        core::{HttpUri, StringMatcher},
        network_filters::http_connection_manager::{
            header_matcher::HeaderMatcher,
            http_filters::oauth2::{CookieNames, OAuth2Credentials},
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
            signout_path: PathMatcher {
                specifier: PathSpecifier::Exact("/signout".into()),
                ignore_case: false,
            },
            forward_bearer_token: true,
            preserve_authorization_header: false,
            pass_through_matcher: vec![],
            auth_scopes: vec!["openid".into(), "profile".into()],
            resources: vec![],
            auth_type: Default::default(),
            use_refresh_token: true,
            default_expires_in: Duration::from_secs(3600),
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
    async fn test_unauthenticated_request_triggers_redirect_with_dynamic_uri() {
        let config = sample_config();
        let mut filter = OAuth2Filter::new(config);

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
                eprintln!("LOCATION IS: {}", location);
                assert!(location.starts_with("https://auth.example.com/oauth/authorize?"));
                // Notice the dynamic redirect_uri formatting from request headers!
                assert!(location.contains("https%3A%2F%2Fgateway%2Eexample%2Ecom%2Fcallback") || location.contains("https%3A%2F%2Fgateway.example.com%2Fcallback"));
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

        let mut filter = OAuth2Filter::new(config);
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

        let mut filter = OAuth2Filter::new(config);
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
    async fn test_authenticated_fast_path_with_session_cache_and_bearer_forwarding() {
        let config = sample_config();
        let mut filter = OAuth2Filter::new(config.clone());

        let host = "gateway.example.com";
        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let expires_at = now_secs + 3600;
        let token = "my-jwt-access-token";

        let hmac_payload = format!("{host}:{expires_at}");
        let hmac_val = compute_hmac(config.credentials.hmac_secret.as_str(), &hmac_payload);

        let cookie_header = format!(
            "BearerToken={token}; OauthExpires={expires_at}; OauthHMAC={hmac_val}"
        );

        let mut req = Request::builder()
            .uri("/api/data")
            .header("Host", host)
            .header("Cookie", cookie_header)
            .body(ArionRequestBody::default())
            .unwrap();

        // First pass: validates HMAC and populates papaya cache
        let decision = filter.apply_request(&mut req).await;
        assert!(matches!(decision, FilterDecision::Continue));
        assert_eq!(
            req.headers().get(header::AUTHORIZATION).unwrap(),
            "Bearer my-jwt-access-token"
        );

        // Verify cache entry exists in papaya
        assert!(OAUTH_SESSION_CACHE.pin().get(hmac_val.as_str()).is_some());

        // Second pass: fast-path hit directly from papaya cache (~20ns)
        let mut req2 = Request::builder()
            .uri("/api/data-2")
            .header("Host", host)
            .header("Cookie", format!("BearerToken={token}; OauthExpires={expires_at}; OauthHMAC={hmac_val}"))
            .body(ArionRequestBody::default())
            .unwrap();

        let decision2 = filter.apply_request(&mut req2).await;
        assert!(matches!(decision2, FilterDecision::Continue));
        assert_eq!(
            req2.headers().get(header::AUTHORIZATION).unwrap(),
            "Bearer my-jwt-access-token"
        );
    }

    #[tokio::test]
    async fn test_signout_flow_clears_cookies_and_redirects() {
        let config = sample_config();
        let mut filter = OAuth2Filter::new(config);

        let mut req = Request::builder()
            .uri("/signout")
            .header("Host", "gateway.example.com")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        match decision {
            FilterDecision::DirectResponse(resp) => {
                assert_eq!(resp.status(), StatusCode::FOUND);
                assert_eq!(
                    resp.headers().get(header::LOCATION).unwrap(),
                    "https://auth.example.com/oauth/logout"
                );
                let cookies: Vec<_> = resp.headers().get_all(header::SET_COOKIE).iter().collect();
                assert!(!cookies.is_empty());
                for c in cookies {
                    assert!(c.to_str().unwrap().contains("Max-Age=0"));
                }
            },
            _ => panic!("expected 302 DirectResponse for signout"),
        }
    }
}
