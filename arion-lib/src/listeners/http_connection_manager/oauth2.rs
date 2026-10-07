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
    sync::{LazyLock, Once},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};

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
    AuthType, CookieConfig, CookieNames, CookieSameSite, OAuth2Config,
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

        let Some(host) = extract_host(req) else {
            warn!(target: "oauth2", "missing Host header or authority in request");
            let resp = SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into())
                .into_response(req.version());
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
                return self.handle_callback(version, is_https, redirect_uri, &query, host).await;
            }
        }

        // 4. Hot path: check for existing valid session cookies.
        let cookie_names = &config.credentials.cookie_names;
        let (bearer_token, oauth_hmac, oauth_expires) =
            extract_session_cookies(req.headers(), cookie_names);

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        if let (Some(tok), Some(hmac_val), Some(expires_str)) = (bearer_token, oauth_hmac, oauth_expires) {
            if let Ok(expires_at) = expires_str.parse::<u64>() {
                if expires_at > now_secs {
                    // Check fast-path papaya session cache.
                    let cache = OAUTH_SESSION_CACHE.pin();
                    if let Some(&cached_expiry) = cache.get(hmac_val) {
                        if cached_expiry > now_secs {
                            let token = SmolStr::new(tok);
                            self.inject_bearer_token(req, &token);
                            return FilterDecision::Continue;
                        }
                    }

                    // Cache miss: verify HMAC signature.
                    let expected_data = format!("{host}:{expires_at}");
                    if verify_hmac(config.credentials.hmac_secret.as_str(), &expected_data, hmac_val) {
                        cache.insert(SmolStr::new(hmac_val), expires_at);
                        let token = SmolStr::new(tok);
                        self.inject_bearer_token(req, &token);
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
            if let Some(hv) = format_cookie(name, "", Some(0), None, cdomain, false) {
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
        let state = utf8_percent_encode(original_target, NON_ALPHANUMERIC);
        let encoded_redirect_uri = utf8_percent_encode(redirect_uri.as_str(), NON_ALPHANUMERIC);
        let scopes = config.auth_scopes.join(" ");
        let encoded_scopes = utf8_percent_encode(&scopes, NON_ALPHANUMERIC);
        let encoded_client_id = utf8_percent_encode(config.credentials.client_id.as_str(), NON_ALPHANUMERIC);

        let auth_url = format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&scope={}&state={}",
            config.authorization_endpoint,
            encoded_client_id,
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
        if let Some(hv) = format_cookie(
            cnames.oauth_nonce.as_str(),
            &nonce,
            Some(config.csrf_token_expires_in.as_secs()),
            config.cookie_configs.oauth_nonce_cookie_config.as_ref(),
            cdomain,
            is_https,
        ) {
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

        let mut code: Option<Cow<'_, str>> = None;
        let mut state: Option<Cow<'_, str>> = None;

        for pair in query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                if k == "code" {
                    let decoded = match percent_decode_str(v).decode_utf8() {
                        Ok(d) => d,
                        Err(e) => {
                            warn!(target: "oauth2", "invalid percent-encoded utf-8 in 'code': {e}");
                            let resp = SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into())
                                .into_response(version);
                            return FilterDecision::DirectResponse(Box::new(resp));
                        }
                    };
                    code = Some(decoded);
                } else if k == "state" {
                    let decoded = match percent_decode_str(v).decode_utf8() {
                        Ok(d) => d,
                        Err(e) => {
                            warn!(target: "oauth2", "invalid percent-encoded utf-8 in 'state': {e}");
                            let resp = SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into())
                                .into_response(version);
                            return FilterDecision::DirectResponse(Box::new(resp));
                        }
                    };
                    state = Some(decoded);
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
            if let Some(hv) = format_cookie(
                cnames.bearer_token.as_str(),
                &token_resp.access_token,
                Some(expires_in),
                config.cookie_configs.bearer_token_cookie_config.as_ref(),
                cdomain,
                is_https,
            ) {
                resp.headers_mut().append(header::SET_COOKIE, hv);
            }
        }

        // Set OauthExpires cookie
        let mut exp_itoa = itoa::Buffer::new();
        let exp_str = exp_itoa.format(expires_at);
        if let Some(hv) = format_cookie(
            cnames.oauth_expires.as_str(),
            exp_str,
            Some(expires_in),
            config.cookie_configs.oauth_expires_cookie_config.as_ref(),
            cdomain,
            is_https,
        ) {
            resp.headers_mut().append(header::SET_COOKIE, hv);
        }

        // Set OauthHMAC cookie
        if let Some(hv) = format_cookie(
            cnames.oauth_hmac.as_str(),
            &hmac_val,
            Some(expires_in),
            config.cookie_configs.oauth_hmac_cookie_config.as_ref(),
            cdomain,
            is_https,
        ) {
            resp.headers_mut().append(header::SET_COOKIE, hv);
        }

        // Set IdToken cookie if present and enabled
        if !config.disable_id_token_set_cookie {
            if let Some(id_tok) = &token_resp.id_token {
                if let Some(hv) = format_cookie(
                    cnames.id_token.as_str(),
                    id_tok,
                    Some(expires_in),
                    config.cookie_configs.id_token_cookie_config.as_ref(),
                    cdomain,
                    is_https,
                ) {
                    resp.headers_mut().append(header::SET_COOKIE, hv);
                }
            }
        }

        // Set RefreshToken cookie if present and enabled
        if !config.disable_refresh_token_set_cookie && config.use_refresh_token {
            if let Some(ref_tok) = &token_resp.refresh_token {
                let ref_exp = config.default_refresh_token_expires_in.as_secs();
                if let Some(hv) = format_cookie(
                    cnames.refresh_token.as_str(),
                    ref_tok,
                    Some(ref_exp),
                    config.cookie_configs.refresh_token_cookie_config.as_ref(),
                    cdomain,
                    is_https,
                ) {
                    resp.headers_mut().append(header::SET_COOKIE, hv);
                }
            }
        }

        // Clear OauthNonce cookie
        if let Some(hv) = format_cookie(cnames.oauth_nonce.as_str(), "", Some(0), None, cdomain, false) {
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
                    utf8_percent_encode(code, NON_ALPHANUMERIC),
                    utf8_percent_encode(redirect_uri, NON_ALPHANUMERIC),
                    utf8_percent_encode(config.credentials.client_id.as_str(), NON_ALPHANUMERIC),
                    utf8_percent_encode(config.credentials.token_secret.as_str(), NON_ALPHANUMERIC),
                );
                req_builder
                    .header(header::CONTENT_TYPE.as_str(), "application/x-www-form-urlencoded")
                    .body(body)
            },
            AuthType::BasicAuth => {
                let body = format!(
                    "grant_type=authorization_code&code={}&redirect_uri={}",
                    utf8_percent_encode(code, NON_ALPHANUMERIC),
                    utf8_percent_encode(redirect_uri, NON_ALPHANUMERIC),
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

fn extract_host(req: &Request<ArionRequestBody>) -> Option<&str> {
    if let Some(host) = req.headers().get(header::HOST).and_then(|v| v.to_str().ok()) {
        return Some(strip_port(host));
    }
    req.uri().host()
}

#[inline]
fn strip_port(host: &str) -> &str {
    if let Some(rest) = host.strip_prefix('[') {
        if let Some(bracket_end) = rest.find(']') {
            &host[..=bracket_end + 1]
        } else {
            host
        }
    } else {
        host.split_once(':').map_or(host, |(h, _port)| h)
    }
}

fn extract_session_cookies<'a>(
    headers: &'a HeaderMap,
    cookie_names: &CookieNames,
) -> (Option<&'a str>, Option<&'a str>, Option<&'a str>) {
    let mut bearer_token = None;
    let mut oauth_hmac = None;
    let mut oauth_expires = None;

    for hv in headers.get_all(header::COOKIE) {
        let Ok(cookie_str) = hv.to_str() else { continue };
        for pair in cookie_str.split(';') {
            if let Some((k, v)) = pair.split_once('=') {
                let k = k.trim();
                let v = v.trim();
                if k == cookie_names.bearer_token.as_str() {
                    bearer_token = Some(v);
                } else if k == cookie_names.oauth_hmac.as_str() {
                    oauth_hmac = Some(v);
                } else if k == cookie_names.oauth_expires.as_str() {
                    oauth_expires = Some(v);
                }

                if bearer_token.is_some() && oauth_hmac.is_some() && oauth_expires.is_some() {
                    return (bearer_token, oauth_hmac, oauth_expires);
                }
            }
        }
    }

    (bearer_token, oauth_hmac, oauth_expires)
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
) -> Option<HeaderValue> {
    let path = config.map_or("/", |c| c.path.as_str());
    let domain_len = domain.map_or(0, |d| if !d.is_empty() { d.len() + 9 } else { 0 });
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
    use arion_configuration::config::{
        core::{HttpUri, StringMatcher},
        network_filters::http_connection_manager::{
            header_matcher::HeaderMatcher,
            http_filters::oauth2::OAuth2Credentials,
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

        let (tok, hmac, exp) = extract_session_cookies(&headers, &cnames);
        assert_eq!(tok, Some("tok123"));
        assert_eq!(hmac, Some("sig456"));
        assert_eq!(exp, Some("789"));

        let mut headers2 = HeaderMap::new();
        headers2.insert(
            header::COOKIE,
            "BearerToken=tok123; other=xyz".parse().unwrap(),
        );
        let (tok2, hmac2, exp2) = extract_session_cookies(&headers2, &cnames);
        assert_eq!(tok2, Some("tok123"));
        assert_eq!(hmac2, None);
        assert_eq!(exp2, None);
    }

    #[test]
    fn test_extract_host_and_strip_port() {
        assert_eq!(strip_port("example.com"), "example.com");
        assert_eq!(strip_port("example.com:8080"), "example.com");
        assert_eq!(strip_port("127.0.0.1:3000"), "127.0.0.1");
        assert_eq!(strip_port("[::1]:8080"), "[::1]");
        assert_eq!(strip_port("[::1]"), "[::1]");
        assert_eq!(strip_port("[2001:db8::1]:443"), "[2001:db8::1]");
    }

    #[tokio::test]
    async fn test_callback_with_invalid_percent_encoding_returns_bad_request() {
        let config = sample_config();
        let mut filter = OAuth2Filter::new(config);

        let mut req = Request::builder()
            .uri("/callback?code=%FF%FF&state=test")
            .header("Host", "gateway.example.com")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        match decision {
            FilterDecision::DirectResponse(resp) => {
                assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            }
            _ => panic!("expected 400 Bad Request on invalid percent encoded query"),
        }
    }

    #[tokio::test]
    async fn test_missing_host_returns_bad_request() {
        let config = sample_config();
        let mut filter = OAuth2Filter::new(config);

        let mut req = Request::builder()
            .uri("/protected")
            .body(ArionRequestBody::default())
            .unwrap();

        let decision = filter.apply_request(&mut req).await;
        match decision {
            FilterDecision::DirectResponse(resp) => {
                assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            }
            _ => panic!("expected 400 Bad Request when Host header is missing"),
        }
    }
}
