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
async fn test_query_exact_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_query_exact("version", "2").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test?version=2").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_query_exact_no_match() {
    let matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_query_exact("version", "2").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test?version=1").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    let response = client.get("/test?version=22").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_query_present_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_query_present("debug").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test?debug=true").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.get("/test?debug=false").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.get("/test?debug=").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_query_present_missing() {
    let matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_query_present("debug").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    let response = client.get("/test?other=value").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_query_absent_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_query_absent("nocache").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.get("/test?other=value").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.get("/test?nocache=1").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_query_regex_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_query_regex("id", "^[0-9]+$").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test?id=12345").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.get("/test?id=abc").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    let response = client.get("/test?id=123abc").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_multiple_query_params_and() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new()
                .match_prefix("/")
                .match_query_exact("type", "admin")
                .match_query_present("token")
                .cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test?type=admin&token=abc123").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.get("/test?type=admin").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    let response = client.get("/test?token=abc123").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    let response = client.get("/test?type=user&token=abc123").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_url_encoded_values() {
    let mut matched = TestBackend::start().await.unwrap();
    let fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_query_exact("name", "hello world").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test?name=hello%20world").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_query_with_path_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/api").match_query_exact("version", "v2").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/api/test?version=v2").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.get("/other?version=v2").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    let response = client.get("/api/test?version=v1").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_query_matchers_when_configured_over_xds() {
    let mut backend_v1 = TestBackend::start().await.unwrap();
    let mut backend_v2 = TestBackend::start().await.unwrap();
    backend_v1.set_default_response(PreConfiguredResponse::with_body("v1")).await;
    backend_v2.set_default_response(PreConfiguredResponse::with_body("v2")).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster_v1 =
        ClusterBuilder::new("backend-v1").endpoint(EndpointBuilder::from_socket_addr(backend_v1.addr())).build();
    let cluster_v2 =
        ClusterBuilder::new("backend-v2").endpoint(EndpointBuilder::from_socket_addr(backend_v2.addr())).build();
    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().rds("routes")))
        .build();
    let route_config = RouteConfigBuilder::new("routes")
        .virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/").match_query_exact("version", "1").cluster("backend-v1"))
                .route(RouteBuilder::new().match_prefix("/").cluster("backend-v2")),
        )
        .build();

    harness.push_cluster(&cluster_v1).await.unwrap();
    harness.push_cluster(&cluster_v2).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();
    harness.push_route_config(&route_config).await.unwrap();

    let client = TestClient::new(listener_addr);

    let response = client.get("/test?version=1").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("v1");
    backend_v1.await_request().await.unwrap();

    let response = client.get("/test?version=2").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("v2");
    backend_v2.await_request().await.unwrap();

    let updated_route_config = RouteConfigBuilder::new("routes")
        .virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/").match_query_exact("api", "v2").cluster("backend-v2"))
                .route(RouteBuilder::new().match_prefix("/").cluster("backend-v1")),
        )
        .build();

    harness.push_route_config(&updated_route_config).await.unwrap();

    let client = TestClient::new(listener_addr);

    // Wait for the updated route config to propagate: with the new config, a plain request
    // (no query params) falls through to backend-v1 ("v1") instead of backend-v2 ("v2").
    // This distinguishes new config from old and serves as our propagation sentinel.
    pingora::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get("/test").await {
                if response.body_str() == Some("v1") {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for updated XDS query route config to propagate");

    // Drain backend queues accumulated during the polling loop.
    while backend_v1.try_recv_request().is_some() {}
    while backend_v2.try_recv_request().is_some() {}

    let response = client.get("/test?version=1").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("v1");
    backend_v1.await_request().await.unwrap();

    let response = client.get("/test?api=v2").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("v2");
    backend_v2.await_request().await.unwrap();

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("v1");
    backend_v1.await_request().await.unwrap();

    let response = client.get("/test?version=2").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("v1");
    backend_v1.await_request().await.unwrap();

    harness.shutdown();
}
