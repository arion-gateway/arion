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

use arion_e2e_tests::config_builder::{
    presets, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use arion_e2e_tests::{
    cleanup_config_file, ArionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
    XdsEnabledHarness,
};
use http::StatusCode;

#[tokio::test]
#[ignore]
async fn test_header_exact_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_header_exact("x-version", "v2").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("x-version", "v2")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_exact_no_match() {
    let matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_header_exact("x-version", "v2").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("x-version", "v1")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("x-version", "V2")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_present_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_header_present("authorization").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("authorization", "Bearer token")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("authorization", "")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_present_missing() {
    let matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_header_present("authorization").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_absent_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_header_absent("x-debug").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("x-debug", "true")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_prefix_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new()
                .match_prefix("/")
                .match_header_prefix("content-type", "application/")
                .cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("content-type", "application/json")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("content-type", "text/plain")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_suffix_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_header_suffix("content-type", "/json").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("content-type", "application/json")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("content-type", "application/xml")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_contains_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_header_contains("user-agent", "Chrome").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response =
        client.send(RequestBuilder::get("/test").header("user-agent", "Mozilla/5.0 Chrome/91")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("user-agent", "Mozilla/5.0 Firefox")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_regex_match() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_header_regex("x-custom-header", "[0-9]+").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("x-custom-header", "5508400440000")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response =
        client.send(RequestBuilder::get("/test").header("x-custom-header", "not-matching-IDX")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_multiple_headers_and_logic() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new()
                .match_prefix("/")
                .match_header_exact("x-version", "v2")
                .match_header_contains("authorization", "tok")
                .cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client
        .send(RequestBuilder::get("/test").header("x-version", "v2").header("authorization", "token"))
        .await
        .unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("x-version", "v2")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("authorization", "token")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_case_sensitivity() {
    let mut matched = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    matched.set_default_response(PreConfiguredResponse::with_body("matched")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/").match_header_exact("X-Version", "v2").cluster("matched"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [presets::static_cluster("matched", matched.addr()), presets::static_cluster("fallback", fallback.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("x-version", "v2")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("X-VERSION", "v2")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("matched");
    matched.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("x-version", "V2")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_pseudo_header_method() {
    let mut get_backend = TestBackend::start().await.unwrap();
    let mut post_backend = TestBackend::start().await.unwrap();
    let mut fallback = TestBackend::start().await.unwrap();
    get_backend.set_default_response(PreConfiguredResponse::with_body("get")).await;
    post_backend.set_default_response(PreConfiguredResponse::with_body("post")).await;
    fallback.set_default_response(PreConfiguredResponse::with_body("fallback")).await;

    let bootstrap = presets::routed_proxy(
        [
            RouteBuilder::new().match_prefix("/api").match_method("GET").cluster("get"),
            RouteBuilder::new().match_prefix("/api").match_method("POST").cluster("post"),
            RouteBuilder::new().match_prefix("/").cluster("fallback"),
        ],
        [
            presets::static_cluster("get", get_backend.addr()),
            presets::static_cluster("post", post_backend.addr()),
            presets::static_cluster("fallback", fallback.addr()),
        ],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/api/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("get");
    get_backend.await_request().await.unwrap();

    let response = client.post("/api/test", "body").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("post");
    post_backend.await_request().await.unwrap();

    let response = client.put("/api/test", "body").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("fallback");
    fallback.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_matchers_when_configured_over_xds() {
    let mut backend_a = TestBackend::start().await.unwrap();
    let mut backend_b = TestBackend::start().await.unwrap();
    backend_a.set_default_response(PreConfiguredResponse::with_body("A")).await;
    backend_b.set_default_response(PreConfiguredResponse::with_body("B")).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster_a =
        ClusterBuilder::new("backend-a").endpoint(EndpointBuilder::from_socket_addr(backend_a.addr())).build();
    let cluster_b =
        ClusterBuilder::new("backend-b").endpoint(EndpointBuilder::from_socket_addr(backend_b.addr())).build();
    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().rds("routes")))
        .build();
    let route_config = RouteConfigBuilder::new("routes")
        .virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/").match_header_exact("x-route", "a").cluster("backend-a"))
                .route(RouteBuilder::new().match_prefix("/").cluster("backend-b")),
        )
        .build();

    harness.push_cluster(&cluster_a).await.unwrap();
    harness.push_cluster(&cluster_b).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.arion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();
    harness.push_route_config(&route_config).await.unwrap();

    let client = TestClient::new(listener_addr);
    let response = client.send(RequestBuilder::get("/test").header("x-route", "a")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("A");
    backend_a.await_request().await.unwrap();

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("B");
    backend_b.await_request().await.unwrap();

    let updated_route_config = RouteConfigBuilder::new("routes")
        .virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/").match_header_exact("x-route", "b").cluster("backend-b"))
                .route(RouteBuilder::new().match_prefix("/").cluster("backend-a")),
        )
        .build();

    harness.push_route_config(&updated_route_config).await.unwrap();

    let client = TestClient::new(listener_addr);

    // Wait for the updated route config to propagate: with the new config, a plain request
    // (no x-route header) falls through to backend-a ("A") instead of backend-b ("B").
    // This distinguishes new config from old and serves as our propagation sentinel.
    pingora::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get("/test").await {
                if response.body_str() == Some("A") {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for updated XDS header route config to propagate");

    // Drain backend queues accumulated during the polling loop.
    while backend_a.try_recv_request().is_some() {}
    while backend_b.try_recv_request().is_some() {}

    let response = client.send(RequestBuilder::get("/test").header("x-route", "a")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("A");
    backend_a.await_request().await.unwrap();

    let response = client.send(RequestBuilder::get("/test").header("x-route", "b")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("B");
    backend_b.await_request().await.unwrap();

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("A");
    backend_a.await_request().await.unwrap();

    harness.shutdown();
}
