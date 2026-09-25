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

use std::net::SocketAddr;
use std::time::Duration;

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    presets, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient, XdsEnabledHarness,
};

#[tokio::test]
#[ignore]
async fn test_prefix_match_basic() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("api-backend")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/api").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/api").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("api-backend");
    backend.await_request().await.unwrap();

    let response = client.get("/api/v1").await.unwrap();
    response.assert_status(StatusCode::OK);
    backend.await_request().await.unwrap();

    let response = client.get("/api/users/123").await.unwrap();
    response.assert_status(StatusCode::OK);
    backend.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_prefix_match_no_match() {
    let backend = TestBackend::start().await.unwrap();

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/api").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/application").await.unwrap();
    response.assert_status(StatusCode::NOT_FOUND);

    let response = client.get("/other").await.unwrap();
    response.assert_status(StatusCode::NOT_FOUND);

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_exact_match_basic() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("exact")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_exact("/health").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/health").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("exact");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_exact_match_no_match() {
    let backend = TestBackend::start().await.unwrap();

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_exact("/health").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/health/").await.unwrap();
    response.assert_status(StatusCode::NOT_FOUND);

    let response = client.get("/healthcheck").await.unwrap();
    response.assert_status(StatusCode::NOT_FOUND);

    let response = client.get("/health/status").await.unwrap();
    response.assert_status(StatusCode::NOT_FOUND);

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_regex_match_basic() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("regex")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_regex("/api/v[0-9]+/.*").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/api/v1/users").await.unwrap();
    response.assert_status(StatusCode::OK);
    backend.await_request().await.unwrap();

    let response = client.get("/api/v2/orders").await.unwrap();
    response.assert_status(StatusCode::OK);
    backend.await_request().await.unwrap();

    let response = client.get("/api/v123/items").await.unwrap();
    response.assert_status(StatusCode::OK);
    backend.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_regex_match_no_match() {
    let backend = TestBackend::start().await.unwrap();

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_regex("/api/v[0-9]+/.*").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/api/vX/users").await.unwrap();
    response.assert_status(StatusCode::NOT_FOUND);

    let response = client.get("/api/v1").await.unwrap();
    response.assert_status(StatusCode::NOT_FOUND);

    let response = client.get("/other/v1/users").await.unwrap();
    response.assert_status(StatusCode::NOT_FOUND);

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_path_separated_prefix() {
    let mut api_backend = TestBackend::start().await.unwrap();
    let mut fallback_backend = TestBackend::start().await.unwrap();
    api_backend.set_default_response(PreConfiguredResponse::with_body("api")).await;
    fallback_backend.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_path_separated_prefix("/api").cluster("api"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [
            presets::static_cluster("api", api_backend.addr()),
            presets::static_cluster("fallback", fallback_backend.addr()),
        ],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/api").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("api");
    api_backend.await_request().await.unwrap();

    let response = client.get("/api/users").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("api");
    api_backend.await_request().await.unwrap();

    let response = client.get("/apikey").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback_backend.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_case_insensitive_prefix() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("matched")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/API").case_insensitive().cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/api/users").await.unwrap();
    response.assert_status(StatusCode::OK);
    backend.await_request().await.unwrap();

    let response = client.get("/API/users").await.unwrap();
    response.assert_status(StatusCode::OK);
    backend.await_request().await.unwrap();

    let response = client.get("/Api/Users").await.unwrap();
    response.assert_status(StatusCode::OK);
    backend.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_first_match_wins() {
    let mut first_backend = TestBackend::start().await.unwrap();
    first_backend.set_default_response(PreConfiguredResponse::with_body("first")).await;

    let dummy_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/api").cluster("first"),
            RouteBuilder::new().match_prefix("/api").cluster("second"),
        ],
        [presets::static_cluster("first", first_backend.addr()), presets::static_cluster("second", dummy_addr)],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/api/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("first");
    first_backend.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_path_matchers_when_configured_over_xds() {
    let mut backend_api = TestBackend::start().await.unwrap();
    let mut backend_other = TestBackend::start().await.unwrap();
    backend_api.set_default_response(PreConfiguredResponse::with_body("api")).await;
    backend_other.set_default_response(PreConfiguredResponse::with_body("other")).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster_api =
        ClusterBuilder::new("backend-api").endpoint(EndpointBuilder::from_socket_addr(backend_api.addr())).build();
    let cluster_other =
        ClusterBuilder::new("backend-other").endpoint(EndpointBuilder::from_socket_addr(backend_other.addr())).build();
    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().rds("routes")))
        .build();
    let route_config = RouteConfigBuilder::new("routes")
        .virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/api").cluster("backend-api"))
                .route(RouteBuilder::new().match_prefix("/").cluster("backend-other")),
        )
        .build();

    harness.push_cluster(&cluster_api).await.unwrap();
    harness.push_cluster(&cluster_other).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();
    harness.push_route_config(&route_config).await.unwrap();

    let client = TestClient::new(listener_addr);

    let response = client.get("/api/users").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("api");
    backend_api.await_request().await.unwrap();

    let response = client.get("/other").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("other");
    backend_other.await_request().await.unwrap();

    let updated_route_config = RouteConfigBuilder::new("routes")
        .virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/v2").cluster("backend-api"))
                .route(RouteBuilder::new().match_prefix("/").cluster("backend-other")),
        )
        .build();

    harness.push_route_config(&updated_route_config).await.unwrap();

    let client = TestClient::new(listener_addr);

    // Wait for the updated route config to propagate: with the new config, /api/users no
    // longer matches /api -> backend-api, so it falls through to / -> backend-other ("other").
    // With the old config it still returns "api", making this our propagation sentinel.
    pingora::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get("/api/users").await {
                if response.body_str() == Some("other") {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for updated XDS path route config to propagate");

    // Drain backend queues accumulated during the polling loop.
    while backend_other.try_recv_request().is_some() {}
    while backend_api.try_recv_request().is_some() {}

    let response = client.get("/api/users").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("other");
    backend_other.await_request().await.unwrap();

    let response = client.get("/v2/users").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("api");
    backend_api.await_request().await.unwrap();

    let response = client.get("/other").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("other");
    backend_other.await_request().await.unwrap();

    harness.shutdown();
}
