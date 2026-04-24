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

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, LocalRateLimitBuilder,
    RouteBuilder, RouteConfigBuilder, UserRateLimiterBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient};

fn simple_route_config() -> RouteConfigBuilder {
    RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    )
}

#[tokio::test]
#[test_log::test]
#[ignore]
async fn test_listener_local_rate_limit_enforced() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http")
                .port(0)
                .listener_local_rate_limit("listener_rate_limit", 2, 1, 10)
                .filter_chain(
                    FilterChainBuilder::new("main").hcm(HcmBuilder::new().http1().route_config(simple_route_config())),
                ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let addr = orion.listener_addr().unwrap();

    let client1 = TestClient::new(addr);
    let response = client1.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let client2 = TestClient::new(addr);
    let response = client2.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let client3 = TestClient::new(addr);
    let result = client3.get("/test").await;
    assert!(result.is_err(), "Expected network error when listener rate limit exceeded (3rd connection)");

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_listener_local_rate_limit_refill() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http")
                .port(0)
                .listener_local_rate_limit("listener_rate_limit", 1, 1, 2)
                .filter_chain(
                    FilterChainBuilder::new("main").hcm(HcmBuilder::new().http1().route_config(simple_route_config())),
                ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let addr = orion.listener_addr().unwrap();

    // Instantiating multiple clients otherwise a single client uses connection pooling
    // and does not reopen connection between requests
    let client1 = TestClient::new(addr);
    let response = client1.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let client2 = TestClient::new(addr);
    let result = client2.get("/test").await;
    assert!(result.is_err(), "Expected network error when listener rate limit exceeded");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let client3 = TestClient::new(addr);
    let response = client3.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_hcm_local_rate_limit_enforced() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(3, 1, 10);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for _ in 0..3 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("OK");
    }

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_hcm_local_rate_limit_custom_status_code() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(503).token_bucket(1, 1, 10);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::SERVICE_UNAVAILABLE);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_hcm_local_rate_limit_refill() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(1, 1, 2);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    tokio::time::sleep(Duration::from_secs(3)).await;

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_per_route_local_rate_limit_override() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let hcm_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(10, 1, 10);

    let route_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("route_rate_limit").status_code(429).token_bucket(2, 1, 10);

    let routes = RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default")
            .route(
                RouteBuilder::new()
                    .match_prefix("/limited")
                    .cluster("backend")
                    .local_rate_limit_override(route_rate_limit),
            )
            .route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().local_rate_limit(hcm_rate_limit).route_config(routes)),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/limited/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/limited/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/limited/test").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    for _ in 0..10 {
        let response = client.get("/other").await.unwrap();
        response.assert_status(StatusCode::OK);
    }

    let response = client.get("/other").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_user_rate_limiter_missing_header() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let user_rate_limiter = UserRateLimiterBuilder::new()
        .stat_prefix("user_rate_limiter")
        .user_id_header("x-user-id")
        .status_code(429)
        .add_user_limit_local(Some("alice"), 2, 1, 10);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().user_rate_limit(user_rate_limiter).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_user_rate_limiter_simple_rate_limit() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let user_rate_limiter = UserRateLimiterBuilder::new()
        .stat_prefix("user_rate_limiter")
        .user_id_header("x-user-id")
        .status_code(429)
        .add_user_limit_simple(Some("alice"), 2, 1);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().user_rate_limit(user_rate_limiter).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for _ in 0..2 {
        let response = client.send(RequestBuilder::get("/test").header("x-user-id", "alice")).await.unwrap();
        response.assert_status(StatusCode::OK);
    }

    let response = client.send(RequestBuilder::get("/test").header("x-user-id", "alice")).await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_user_rate_limiter_custom_header() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let user_rate_limiter = UserRateLimiterBuilder::new()
        .stat_prefix("user_rate_limiter")
        .user_id_header("authorization")
        .status_code(429)
        .add_user_limit_local(Some("test4_bearer_token"), 2, 1, 10);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().user_rate_limit(user_rate_limiter).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for _ in 0..2 {
        let response =
            client.send(RequestBuilder::get("/test").header("authorization", "test4_bearer_token")).await.unwrap();
        response.assert_status(StatusCode::OK);
    }

    let response =
        client.send(RequestBuilder::get("/test").header("authorization", "test4_bearer_token")).await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_rate_limit_applies_before_routing() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(1, 1, 10);

    let routes = RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default")
            .route(RouteBuilder::new().match_prefix("/nonexistent").direct_response_empty(404))
            .route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(routes)),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/nonexistent").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    orion.shutdown();
}
