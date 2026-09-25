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

//! JWT authn filter e2e tests.
//!
//! Covers `claim_to_headers` + `clear_route_cache`: the request is first
//! matched without the claim header, JWT copies `sub` onto `x-jwt-sub`, and
//! a rematch can then select a more specific route.

#![allow(clippy::expect_used, reason = "test infrastructure — panicking on setup failure is intentional")]

use std::net::SocketAddr;
use std::path::PathBuf;

use arion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder, RouteConfigBuilder,
    VirtualHostBuilder,
};
use arion_e2e_tests::{
    cleanup_config_file, generate_jwt_token, ArionInstance, JwtKeyPair, PreConfiguredResponse, RequestBuilder,
    SpawnOptions, TestBackend, TestClient, TestJwtClaims, TestResponse,
};
use http::StatusCode;

const JWT_AUDIENCE: &str = "mcp-gateway";
const ADMIN_SUBJECT: &str = "admin";
const USER_SUBJECT: &str = "user";

struct JwtRerouteHarness {
    admin: TestBackend,
    fallback: TestBackend,
    arion: ArionInstance,
    client: TestClient,
    jwt_keys: JwtKeyPair,
    config_path: PathBuf,
}

impl JwtRerouteHarness {
    async fn start(clear_route_cache: bool) -> Self {
        let admin = TestBackend::start().await.expect("admin backend start");
        let fallback = TestBackend::start().await.expect("fallback backend start");
        admin.set_default_response(PreConfiguredResponse::with_body("admin")).await;
        fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

        let jwt_keys = JwtKeyPair::generate();
        let bootstrap =
            jwt_claim_header_proxy(jwt_keys.get_jwks_inline(), clear_route_cache, admin.addr(), fallback.addr());
        let config_path = bootstrap.build_to_temp().expect("build config");
        let arion =
            ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn arion");
        let client = TestClient::new(arion.listener_addr().expect("listener addr"));

        Self { admin, fallback, arion, client, jwt_keys, config_path }
    }

    fn bearer_for(&self, subject: &str) -> String {
        let claims = TestJwtClaims::new(subject, subject).with_audience(JWT_AUDIENCE);
        generate_jwt_token(&claims, &self.jwt_keys.private_key).expect("encode jwt")
    }

    async fn get_with_jwt(&self, token: &str) -> TestResponse {
        self.client
            .send(RequestBuilder::get("/test").header("authorization", format!("Bearer {token}")))
            .await
            .expect("request")
    }

    fn shutdown(self) {
        self.arion.shutdown();
        cleanup_config_file(&self.config_path);
    }
}

fn jwt_claim_header_proxy(
    jwks_inline: String,
    clear_route_cache: bool,
    admin_addr: SocketAddr,
    fallback_addr: SocketAddr,
) -> BootstrapBuilder {
    BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new()
                        .http1()
                        .with_jwt_auth_config(
                            jwks_inline,
                            vec![JWT_AUDIENCE.to_owned()],
                            &[("x-jwt-sub", "sub")],
                            clear_route_cache,
                        )
                        .route_config(
                            RouteConfigBuilder::new("routes").virtual_host(
                                VirtualHostBuilder::new("default")
                                    .route(
                                        RouteBuilder::new()
                                            .match_prefix("/")
                                            .match_header_exact("x-jwt-sub", ADMIN_SUBJECT)
                                            .cluster("admin"),
                                    )
                                    .route(RouteBuilder::new().match_prefix("/").cluster("fallback")),
                            ),
                        ),
                ),
            ),
        )
        .cluster(presets::static_cluster("admin", admin_addr))
        .cluster(presets::static_cluster("fallback", fallback_addr))
}

#[tokio::test]
#[ignore]
async fn test_jwt_clear_route_cache_reroutes_on_claim_header() {
    let mut harness = JwtRerouteHarness::start(true).await;

    let admin_token = harness.bearer_for(ADMIN_SUBJECT);
    let response = harness.get_with_jwt(&admin_token).await;
    response.assert_status(StatusCode::OK);
    response.assert_body("admin");

    let captured = harness.admin.await_request().await.expect("admin backend request");
    assert_eq!(captured.header("x-jwt-sub"), Some(ADMIN_SUBJECT));

    let user_token = harness.bearer_for(USER_SUBJECT);
    let response = harness.get_with_jwt(&user_token).await;
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");

    let captured = harness.fallback.await_request().await.expect("fallback backend request");
    assert_eq!(captured.header("x-jwt-sub"), Some(USER_SUBJECT));

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_jwt_without_clear_route_cache_keeps_initial_route() {
    let mut harness = JwtRerouteHarness::start(false).await;

    let admin_token = harness.bearer_for(ADMIN_SUBJECT);
    let response = harness.get_with_jwt(&admin_token).await;
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");

    let captured = harness.fallback.await_request().await.expect("fallback backend request");
    assert_eq!(
        captured.header("x-jwt-sub"),
        Some(ADMIN_SUBJECT),
        "claim header is still injected; only rematch is skipped"
    );

    harness.shutdown();
}
