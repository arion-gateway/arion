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

//! `Auth2` authentication filter e2e integration tests.
//!
//! Covers:
//! - Unauthenticated 302 redirection to authorization endpoint with state & nonce cookies.
//! - Authorization code exchange at /callback via upstream token cluster.
//! - HMAC cookie generation, per-request verification, and session cookie tampering rejection.
//! - Bearer token injection (`Authorization: Bearer <token>`) towards upstream backend.
//! - Signout path clearing session cookies.
//! - Callback validation error when code parameter is missing.

#![allow(clippy::expect_used, reason = "test infrastructure - panicking on setup failure is intentional")]

use std::path::PathBuf;

use arion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, OAuth2Builder,
    RouteBuilder, RouteConfigBuilder, VirtualHostBuilder,
};
use arion_e2e_tests::{
    ArionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient, TestResponse,
    cleanup_config_file,
};
use http::StatusCode;

fn extract_cookie_value(response: &TestResponse, name: &str) -> Option<String> {
    extract_cookie_with_name(response, |cookie_name| cookie_name == name).map(|(_, value)| value)
}

fn extract_cookie_with_name(response: &TestResponse, predicate: impl Fn(&str) -> bool) -> Option<(String, String)> {
    for cookie_header in response.header_all("set-cookie") {
        let first_part = cookie_header.split(';').next()?.trim();
        if let Some((name, value)) = first_part.split_once('=') {
            if predicate(name.trim()) {
                return Some((name.trim().to_owned(), value.trim().to_owned()));
            }
        }
    }
    None
}

fn extract_query_parameter<'a>(url: &'a str, name: &str) -> Option<&'a str> {
    url.split_once('?')?.1.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then_some(value)
    })
}

struct OAuth2TestHarness {
    backend: TestBackend,
    oauth_server: TestBackend,
    _arion: ArionInstance,
    client: TestClient,
    config_path: PathBuf,
}

impl OAuth2TestHarness {
    async fn start() -> Self {
        let backend = TestBackend::start().await.expect("start backend");
        let oauth_server = TestBackend::start().await.expect("start oauth server");

        backend.set_default_response(PreConfiguredResponse::with_body("upstream response ok")).await;

        let token_json = r#"{"access_token":"mock_access_token_xyz_42","token_type":"Bearer","expires_in":3600,"refresh_token":"mock_refresh_token_abc","id_token":"mock_id_token_def"}"#;
        oauth_server
            .set_default_response(
                PreConfiguredResponse::with_body(token_json).header("content-type", "application/json"),
            )
            .await;

        let token_endpoint_uri = format!("http://{}/oauth/token", oauth_server.addr());
        let authorization_endpoint = format!("http://{}/oauth/authorize", oauth_server.addr());

        let oauth2 = OAuth2Builder::new(
            token_endpoint_uri,
            "oauth_cluster",
            authorization_endpoint,
            "test-client-id",
            "test-token-secret",
            "test-hmac-signing-key-secret-12345",
            "http://127.0.0.1:0/callback", // Arion dynamic redirect
        )
        .redirect_path("/callback")
        .signout_path("/signout")
        .forward_bearer_token(true);

        let route_config = RouteConfigBuilder::new("main_routes").virtual_host(
            VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
        );

        let hcm = HcmBuilder::new().http1().oauth2(oauth2).route_config(route_config);

        let bootstrap = BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm)))
            .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())))
            .cluster(
                ClusterBuilder::new("oauth_cluster").endpoint(EndpointBuilder::from_socket_addr(oauth_server.addr())),
            );

        let config_path = bootstrap.build_to_temp().expect("build config to temp");
        let arion =
            ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn arion");
        let client = TestClient::new(arion.listener_addr().expect("listener address"));

        Self { backend, oauth_server, _arion: arion, client, config_path }
    }
}

impl Drop for OAuth2TestHarness {
    fn drop(&mut self) {
        cleanup_config_file(&self.config_path);
    }
}

#[tokio::test]
#[ignore]
async fn test_oauth2_unauthenticated_request_redirects_to_auth() {
    let harness = OAuth2TestHarness::start().await;

    // Unauthenticated request to /protected-api
    let resp = harness.client.get("/protected-api").await.expect("request");
    resp.assert_status(StatusCode::FOUND);

    // Assert Location header contains OAuth2 authorization URL parameters
    let location = resp.header("location").expect("location header");
    assert!(location.contains("/oauth/authorize"), "location must contain authorize endpoint: {location}");
    assert!(location.contains("client_id="), "location must contain client_id: {location}");
    let state = extract_query_parameter(location, "state").expect("authorization redirect state");
    assert!(!state.is_empty(), "authorization redirect state must not be empty");

    assert!(location.contains("code_challenge="), "location must contain the PKCE challenge: {location}");
    assert!(location.contains("code_challenge_method=S256"), "location must request PKCE S256: {location}");
    // The transaction is per-flow so concurrent browser tabs do not overwrite one another.
    let nonce_cookie = extract_cookie_with_name(&resp, |name| name.starts_with("OauthNonce."));
    assert!(nonce_cookie.is_some(), "response must set a per-flow OauthNonce cookie");
    let verifier_cookie = extract_cookie_with_name(&resp, |name| name.starts_with("OauthCodeVerifier."));
    assert!(verifier_cookie.is_some(), "response must set a per-flow PKCE verifier cookie");
}

#[tokio::test]
#[ignore]
async fn test_oauth2_full_authentication_flow_and_bearer_injection() {
    let mut harness = OAuth2TestHarness::start().await;

    // Step 1: Initial unauthenticated request
    let resp1 = harness.client.get("/api/data").await.expect("initial request");
    resp1.assert_status(StatusCode::FOUND);

    let (nonce_cookie_name, nonce_cookie) =
        extract_cookie_with_name(&resp1, |name| name.starts_with("OauthNonce.")).expect("OauthNonce flow cookie");
    let (verifier_cookie_name, verifier_cookie) =
        extract_cookie_with_name(&resp1, |name| name.starts_with("OauthCodeVerifier."))
            .expect("OauthCodeVerifier flow cookie");
    let authorization_location = resp1.header("location").expect("authorization redirect location");
    let state = extract_query_parameter(authorization_location, "state").expect("authorization redirect state");

    // Step 2: The IdP must reflect the opaque state received in the authorization request.
    let callback_path = format!("/callback?code=mockauthcode999&state={state}");
    let callback_req = RequestBuilder::get(callback_path)
        .header("cookie", format!("{nonce_cookie_name}={nonce_cookie}; {verifier_cookie_name}={verifier_cookie}"));

    let resp2 = harness.client.send(callback_req).await.expect("callback request");
    resp2.assert_status(StatusCode::FOUND);

    // Verify redirected back to the original destination
    resp2.assert_header("location", "/api/data");

    // Verify OAuth token exchange occurred on the oauth_server backend
    let oauth_req = harness.oauth_server.await_request().await.expect("token exchange request to oauth server");
    assert_eq!(oauth_req.method, http::Method::POST);
    assert_eq!(oauth_req.path(), "/oauth/token");
    let oauth_body = oauth_req.body_str().expect("body utf8");
    assert!(oauth_body.contains("grant_type=authorization_code"));
    assert!(oauth_body.contains("code=mockauthcode999"));
    assert!(oauth_body.contains("code_verifier="));
    assert!(oauth_body.contains("client_id=test"));

    // Extract session cookies generated by Arion
    let bearer_token = extract_cookie_value(&resp2, "BearerToken").expect("BearerToken cookie");
    let id_token = extract_cookie_value(&resp2, "IdToken").expect("IdToken cookie");
    let refresh_token = extract_cookie_value(&resp2, "RefreshToken").expect("RefreshToken cookie");
    let oauth_hmac = extract_cookie_value(&resp2, "OauthHMAC").expect("OauthHMAC cookie");
    let oauth_expires = extract_cookie_value(&resp2, "OauthExpires").expect("OauthExpires cookie");

    assert!(bearer_token.starts_with("gcm."), "access token cookie must be encrypted");
    assert!(id_token.starts_with("gcm."), "ID token cookie must be encrypted");
    assert!(refresh_token.starts_with("gcm."), "refresh token cookie must be encrypted");
    assert_ne!(oauth_hmac, "");
    assert_ne!(oauth_expires, "");

    // Step 3: Access protected resource using the session cookies
    let session_cookies = [
        ("BearerToken", bearer_token.as_str()),
        ("IdToken", id_token.as_str()),
        ("RefreshToken", refresh_token.as_str()),
        ("OauthHMAC", oauth_hmac.as_str()),
        ("OauthExpires", oauth_expires.as_str()),
    ];
    let cookie_header =
        session_cookies.iter().map(|(name, value)| format!("{name}={value}")).collect::<Vec<_>>().join("; ");

    let auth_req = RequestBuilder::get("/api/data").header("cookie", &cookie_header);
    let resp3 = harness.client.send(auth_req).await.expect("authenticated request");
    resp3.assert_status(StatusCode::OK);
    resp3.assert_body("upstream response ok");

    // Verify that Arion injected the Bearer token when proxying to the upstream backend!
    let upstream_req = harness.backend.await_request().await.expect("upstream request");
    assert_eq!(
        upstream_req.header("authorization"),
        Some("Bearer mock_access_token_xyz_42"),
        "Arion must forward Bearer token header to upstream"
    );

    // Step 4: Subsequent requests must verify the complete signed session again.
    let repeat_req = RequestBuilder::get("/api/data").header("cookie", &cookie_header);
    let resp4 = harness.client.send(repeat_req).await.expect("repeat authenticated request");
    resp4.assert_status(StatusCode::OK);
    resp4.assert_body("upstream response ok");

    // A previously accepted signature must not authorize modified session fields.
    for (changed_name, changed_value) in [
        ("BearerToken", "tampered_access_token"),
        ("IdToken", "tampered_id_token"),
        ("RefreshToken", "tampered_refresh_token"),
        ("OauthHMAC", "invalid_signature"),
        ("OauthExpires", "9999999999"),
    ] {
        let tampered_cookies = session_cookies
            .iter()
            .map(|&(name, value)| {
                let value = if name == changed_name { changed_value } else { value };
                format!("{name}={value}")
            })
            .collect::<Vec<_>>()
            .join("; ");
        let req = RequestBuilder::get("/api/data").header("cookie", tampered_cookies);
        let resp = harness.client.send(req).await.expect("tampered session request");
        assert_eq!(resp.status, StatusCode::FOUND, "tampering with {changed_name} must require authentication");
        assert!(resp.header("location").expect("authorization redirect").contains("/oauth/authorize"));
    }

    for omitted_name in ["BearerToken", "IdToken", "RefreshToken"] {
        let incomplete_cookies = session_cookies
            .iter()
            .filter(|(name, _)| *name != omitted_name)
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");
        let req = RequestBuilder::get("/api/data").header("cookie", incomplete_cookies);
        let resp = harness.client.send(req).await.expect("incomplete session request");
        assert_eq!(resp.status, StatusCode::FOUND, "omitting {omitted_name} must require authentication");
        assert!(resp.header("location").expect("authorization redirect").contains("/oauth/authorize"));
    }
}

#[tokio::test]
#[ignore]
async fn test_oauth2_callback_missing_code_returns_400() {
    let harness = OAuth2TestHarness::start().await;

    // Hit /callback without ?code= parameter
    let resp = harness.client.get("/callback").await.expect("request");
    resp.assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[ignore]
async fn test_oauth2_signout_clears_cookies() {
    let harness = OAuth2TestHarness::start().await;

    // Call /signout with active session cookies
    let req = RequestBuilder::get("/signout")
        .header("cookie", "BearerToken=some_token; OauthHMAC=sig; OauthExpires=9999999999");
    let resp = harness.client.send(req).await.expect("signout request");

    // Assert redirect
    resp.assert_status(StatusCode::FOUND);

    // Assert cookies are cleared
    let set_cookies = resp.header_all("set-cookie");
    let clears_bearer = set_cookies
        .iter()
        .any(|c| c.contains("BearerToken=;") || c.contains("BearerToken=\"\"") || c.contains("Max-Age=0"));
    let clears_hmac = set_cookies
        .iter()
        .any(|c| c.contains("OauthHMAC=;") || c.contains("OauthHMAC=\"\"") || c.contains("Max-Age=0"));

    assert!(clears_bearer, "Signout must clear BearerToken cookie: {set_cookies:?}");
    assert!(clears_hmac, "Signout must clear OauthHMAC cookie: {set_cookies:?}");
}
