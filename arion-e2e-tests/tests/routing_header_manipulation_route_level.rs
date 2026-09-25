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
async fn test_request_add_header_basic() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").add_request_header("x-added", "value").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-added"), Some("value"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_request_add_header_if_absent_when_missing() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").add_request_header_if_absent("x-added", "default").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-added"), Some("default"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_request_add_header_if_absent_when_present() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").add_request_header_if_absent("x-added", "default").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("x-added", "original")).await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-added"), Some("original"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_request_overwrite_header() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").overwrite_request_header("x-version", "v2").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("x-version", "v1")).await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-version"), Some("v2"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_request_add_multiple_headers() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new()
            .match_prefix("/")
            .add_request_header("x-first", "one")
            .add_request_header("x-second", "two")
            .add_request_header("x-third", "three")
            .cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-first"), Some("one"));
    assert_eq!(captured.header("x-second"), Some("two"));
    assert_eq!(captured.header("x-third"), Some("three"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_request_remove_header_basic() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").remove_request_header("x-remove-me").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("x-remove-me", "gone")).await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert!(captured.header("x-remove-me").is_none());

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_request_remove_header_missing() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").remove_request_header("x-nonexistent").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_request_remove_multiple_headers() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new()
            .match_prefix("/")
            .remove_request_header("x-first")
            .remove_request_header("x-second")
            .cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client
        .send(RequestBuilder::get("/test").header("x-first", "one").header("x-second", "two").header("x-keep", "keep"))
        .await
        .unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert!(captured.header("x-first").is_none());
    assert!(captured.header("x-second").is_none());
    assert_eq!(captured.header("x-keep"), Some("keep"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_request_remove_then_add_same() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new()
            .match_prefix("/")
            .remove_request_header("x-header")
            .add_request_header("x-header", "new-value")
            .cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("x-header", "old-value")).await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-header"), Some("new-value"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_add_header_basic() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").add_response_header("x-added", "value").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-added", "value");

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_add_header_if_absent_when_missing() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").add_response_header_if_absent("x-added", "default").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-added", "default");

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_add_header_if_absent_when_present() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK").header("x-existing", "from-backend")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new()
            .match_prefix("/")
            .add_response_header_if_absent("x-existing", "default")
            .cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-existing", "from-backend");

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_overwrite_header() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK").header("x-version", "v1")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").overwrite_response_header("x-version", "v2").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-version", "v2");

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_add_multiple_headers() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new()
            .match_prefix("/")
            .add_response_header("x-first", "one")
            .add_response_header("x-second", "two")
            .add_response_header("x-third", "three")
            .cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-first", "one");
    response.assert_header("x-second", "two");
    response.assert_header("x-third", "three");

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_remove_header_basic() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK").header("x-remove-me", "gone")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").remove_response_header("x-remove-me").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    assert!(response.header("x-remove-me").is_none());

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_remove_header_missing() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").remove_response_header("x-nonexistent").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_remove_multiple_headers() {
    let mut backend = TestBackend::start().await.unwrap();
    backend
        .set_default_response(
            PreConfiguredResponse::with_body("OK")
                .header("x-first", "one")
                .header("x-second", "two")
                .header("x-keep", "keep"),
        )
        .await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new()
            .match_prefix("/")
            .remove_response_header("x-first")
            .remove_response_header("x-second")
            .cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    assert!(response.header("x-first").is_none());
    assert!(response.header("x-second").is_none());
    response.assert_header("x-keep", "keep");

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_remove_then_add_same() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK").header("x-header", "old-value")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new()
            .match_prefix("/")
            .remove_response_header("x-header")
            .add_response_header("x-header", "new-value")
            .cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-header", "new-value");

    backend.await_request().await.unwrap();

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_both_request_and_response_headers() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new()
            .match_prefix("/")
            .add_request_header("x-request", "from-route")
            .add_response_header("x-response", "from-route")
            .cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-response", "from-route");

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-request"), Some("from-route"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_request_header_not_in_response() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").add_request_header("x-request-only", "value").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    assert!(response.header("x-request-only").is_none());

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-request-only"), Some("value"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_response_header_not_in_request() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").add_response_header("x-response-only", "value").cluster("backend")],
        [presets::static_cluster("backend", backend.addr())],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-response-only", "value");

    let captured = backend.await_request().await.unwrap();
    assert!(captured.header("x-response-only").is_none());

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_header_manipulation_route_level_when_configured_over_xds() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

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
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/").add_response_header("x-version", "v1").cluster("backend")),
        )
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.arion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();
    harness.push_route_config(&route_config).await.unwrap();

    let client = TestClient::new(listener_addr);

    pingora::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get("/test").await {
                if response.status == StatusCode::OK && response.header("x-version") == Some("v1") {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for initial RDS route");

    while backend.try_recv_request().is_some() {}

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-version", "v1");
    backend.await_request().await.unwrap();

    let updated_route_config = RouteConfigBuilder::new("routes")
        .virtual_host(
            VirtualHostBuilder::new("default").route(
                RouteBuilder::new()
                    .match_prefix("/")
                    .add_response_header("x-version", "v2")
                    .add_response_header("x-source", "arion-proxy")
                    .cluster("backend"),
            ),
        )
        .build();

    harness.push_route_config(&updated_route_config).await.unwrap();

    let client = TestClient::new(listener_addr);

    pingora::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get("/test").await {
                if response.status == StatusCode::OK
                    && response.header("x-version") == Some("v2")
                    && response.header("x-source") == Some("arion-proxy")
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for updated RDS route");

    while backend.try_recv_request().is_some() {}

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-version", "v2");
    response.assert_header("x-source", "arion-proxy");

    harness.shutdown();
}
