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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    presets, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RedirectBuilder,
    RouteBuilder, RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient, XdsEnabledHarness,
};

#[tokio::test]
#[ignore]
async fn test_cluster_routing_basic() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("backend")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("backend");
    backend.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_cluster_missing() {
    let backend = TestBackend::start().await.unwrap();

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").cluster("nonexistent")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_weighted_clusters_basic() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("backend1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("backend2")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").weighted_clusters(&[("backend1", 50), ("backend2", 50)])],
        [presets::static_cluster("backend1", backend1.addr()), presets::static_cluster("backend2", backend2.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let mut counts: HashMap<String, u32> = HashMap::new();
    for _ in 0..20 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_string();
        *counts.entry(body).or_insert(0) += 1;
    }

    assert!(counts.contains_key("backend1"), "Expected traffic to backend1");
    assert!(counts.contains_key("backend2"), "Expected traffic to backend2");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_weighted_clusters_single() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("backend")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").weighted_clusters(&[("backend", 100)])],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    for _ in 0..5 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("backend");
        backend.await_request().await.unwrap();
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_cluster_header_routing() {
    let mut backend1 = TestBackend::start().await.unwrap();
    let mut backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("backend1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("backend2")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").cluster_header("x-cluster")],
        [presets::static_cluster("backend1", backend1.addr()), presets::static_cluster("backend2", backend2.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("x-cluster", "backend1")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("backend1");
    backend1.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("x-cluster", "backend2")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("backend2");
    backend2.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_cluster_header_missing() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("backend")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").cluster_header("x-cluster")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_redirect_302_basic() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/old")
        .redirect(RedirectBuilder::new().status_302().path("/new"))]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/old").await.unwrap();
    response.assert_status(StatusCode::FOUND);
    response.assert_header("location", "/new");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_redirect_301_permanent() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/old")
        .redirect(RedirectBuilder::new().status_301().path("/new"))]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/old").await.unwrap();
    response.assert_status(StatusCode::MOVED_PERMANENTLY);
    response.assert_header("location", "/new");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_redirect_307_temporary() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/old")
        .redirect(RedirectBuilder::new().status_307().path("/new"))]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/old").await.unwrap();
    response.assert_status(StatusCode::TEMPORARY_REDIRECT);
    response.assert_header("location", "/new");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_redirect_308_permanent() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/old")
        .redirect(RedirectBuilder::new().status_308().path("/new"))]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/old").await.unwrap();
    response.assert_status(StatusCode::PERMANENT_REDIRECT);
    response.assert_header("location", "/new");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_redirect_host_rewrite() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/")
        .redirect(RedirectBuilder::new().status_302().scheme("http").host("example.com").path("/redirected"))]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/path").await.unwrap();
    response.assert_status(StatusCode::FOUND);
    let location = response.header("location").expect("Missing location header");
    assert!(location.contains("example.com"), "Expected host in location, got: {}", location);
    assert!(location.contains("/redirected"), "Expected path in location, got: {}", location);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_redirect_path_rewrite() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/v1")
        .redirect(RedirectBuilder::new().status_302().prefix_rewrite("/v2"))]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/v1/users").await.unwrap();
    response.assert_status(StatusCode::FOUND);
    response.assert_header("location", "/v2/users");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_redirect_https_upgrade() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/")
        .redirect(RedirectBuilder::new().status_301().https().host("example.com").path("/secure"))]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/path").await.unwrap();
    response.assert_status(StatusCode::MOVED_PERMANENTLY);
    let location = response.header("location").expect("Missing location header");
    assert!(location.starts_with("https://"), "Expected https scheme, got: {}", location);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_redirect_strip_query() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/old")
        .redirect(RedirectBuilder::new().status_302().path("/new").strip_query())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/old?foo=bar&baz=qux").await.unwrap();
    response.assert_status(StatusCode::FOUND);
    let location = response.header("location").expect("Missing location header");
    assert!(!location.contains('?'), "Expected query to be stripped, got: {}", location);
    assert!(location.contains("/new"), "Expected path /new, got: {}", location);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_redirect_preserve_query() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/old")
        .redirect(RedirectBuilder::new().status_302().path("/new"))]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/old?foo=bar").await.unwrap();
    response.assert_status(StatusCode::FOUND);
    let location = response.header("location").expect("Missing location header");
    assert!(location.contains("foo=bar"), "Expected query to be preserved, got: {}", location);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_direct_response_with_body() {
    let bootstrap = presets::routed_proxy_no_clusters([RouteBuilder::new()
        .match_prefix("/")
        .direct_response(200, "Hello, World!")]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/anything").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("Hello, World!");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_direct_response_empty() {
    let bootstrap =
        presets::routed_proxy_no_clusters([RouteBuilder::new().match_prefix("/").direct_response_empty(204)]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/anything").await.unwrap();
    response.assert_status(StatusCode::NO_CONTENT);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_direct_response_error_status() {
    let bootstrap = presets::routed_proxy_no_clusters([
        RouteBuilder::new().match_prefix("/forbidden").direct_response(403, "Forbidden"),
        RouteBuilder::new().match_prefix("/error").direct_response(500, "Internal Server Error"),
    ]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/forbidden").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);
    response.assert_body("Forbidden");

    let response = client.get("/error").await.unwrap();
    response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
    response.assert_body("Internal Server Error");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_direct_response_with_path_match() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("backend")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/health").direct_response(200, "OK"),
            RouteBuilder::new().match_prefix("/").cluster("backend"),
        ],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/health").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("OK");

    let response = client.get("/api/users").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("backend");
    backend.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_direct_response_with_header_match() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("backend")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new()
                .match_prefix("/")
                .match_header_exact("x-mock", "true")
                .direct_response(200, "mocked response"),
            RouteBuilder::new().match_prefix("/").cluster("backend"),
        ],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/api").header("x-mock", "true")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("mocked response");

    let response = client.get("/api").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("backend");
    backend.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_direct_response_fallback() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("backend")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/api").cluster("backend"),
            RouteBuilder::new().match_prefix("/").direct_response(404, "Not Found"),
        ],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/api/users").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("backend");
    backend.await_request().await.unwrap();

    let response = client.get("/unknown").await.unwrap();
    response.assert_status(StatusCode::NOT_FOUND);
    response.assert_body("Not Found");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_routing_actions_when_configured_over_xds() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("from-backend")).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).build();
    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().rds("routes")))
        .build();
    let route_config = RouteConfigBuilder::new("routes")
        .virtual_host(
            VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
        )
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.push_route_config(&route_config).await.unwrap();
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    let client = TestClient::new(listener_addr);
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("from-backend");
    backend.await_request().await.unwrap();

    let updated_route_config = RouteConfigBuilder::new("routes")
        .virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/").direct_response(200, "Direct from Orion")),
        )
        .build();

    harness.push_route_config(&updated_route_config).await.unwrap();

    let client = TestClient::new(listener_addr);
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("Direct from Orion");

    harness.shutdown();
}
