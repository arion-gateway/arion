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

//! Integration tests for `ORIGINAL_DST` clusters.
//!
//! `ORIGINAL_DST` clusters route requests to dynamically-determined destinations
//! based on the `x-envoy-original-dst-host` HTTP header (or a custom header).

use std::time::Duration;

use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, ClusterBuilder, RouteBuilder};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};

const DEFAULT_DST_HEADER: &str = "x-envoy-original-dst-host";

fn dest_header(backend: &TestBackend) -> String {
    format!("127.0.0.1:{}", backend.addr().port())
}

#[tokio::test]
#[ignore]
async fn test_original_dst_routes_to_header_destination() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let cluster = ClusterBuilder::new("backend").original_dst_via_default_header().build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for _ in 0..10 {
        let response =
            client.send(RequestBuilder::get("/test").header(DEFAULT_DST_HEADER, dest_header(&backend1))).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b1");
    }

    for _ in 0..10 {
        let response =
            client.send(RequestBuilder::get("/test").header(DEFAULT_DST_HEADER, dest_header(&backend2))).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b2");
    }

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_original_dst_custom_header_name() {
    const CUSTOM_HEADER: &str = "x-custom-dst";

    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let cluster = ClusterBuilder::new("backend").original_dst_via_header(CUSTOM_HEADER).build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for _ in 0..10 {
        let response =
            client.send(RequestBuilder::get("/test").header(CUSTOM_HEADER, dest_header(&backend1))).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b1");
    }

    for _ in 0..10 {
        let response =
            client.send(RequestBuilder::get("/test").header(CUSTOM_HEADER, dest_header(&backend2))).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b2");
    }

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_original_dst_port_override() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("overridden")).await;

    let cluster = ClusterBuilder::new("backend")
        .original_dst_via_default_header()
        .original_dst_port_override(backend.addr().port())
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response =
        client.send(RequestBuilder::get("/test").header(DEFAULT_DST_HEADER, "127.0.0.1:9999")).await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("overridden");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_original_dst_multiple_destinations() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend").original_dst_via_default_header().build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for (backend, expected_body) in [(&backend1, "b1"), (&backend2, "b2"), (&backend3, "b3")] {
        let response =
            client.send(RequestBuilder::get("/test").header(DEFAULT_DST_HEADER, dest_header(backend))).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body(expected_body);
    }

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_original_dst_missing_header_returns_error() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("should not reach")).await;

    let cluster = ClusterBuilder::new("backend").original_dst_via_default_header().build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    assert!(
        response.status == StatusCode::SERVICE_UNAVAILABLE
            || response.status == StatusCode::INTERNAL_SERVER_ERROR
            || response.status == StatusCode::BAD_GATEWAY,
        "Expected error status (503, 500, or 502), got {}",
        response.status
    );

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_original_dst_invalid_header_returns_error() {
    let cluster = ClusterBuilder::new("backend").original_dst_via_default_header().build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let invalid_values = ["not-a-valid-authority", ":::invalid:::", ""];

    for invalid_value in invalid_values {
        let response =
            client.send(RequestBuilder::get("/test").header(DEFAULT_DST_HEADER, invalid_value)).await.unwrap();
        assert!(
            response.status == StatusCode::SERVICE_UNAVAILABLE
                || response.status == StatusCode::INTERNAL_SERVER_ERROR
                || response.status == StatusCode::BAD_GATEWAY
                || response.status == StatusCode::BAD_REQUEST,
            "Expected error status for invalid value '{}', got {}",
            invalid_value,
            response.status
        );
    }

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_original_dst_connect_timeout() {
    let cluster = ClusterBuilder::new("backend")
        .original_dst_via_default_header()
        .connect_timeout(Duration::from_millis(100))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    // Use TEST-NET-1 (RFC 5737) - packets are dropped, causing connect timeout
    let start = std::time::Instant::now();
    let response =
        client.send(RequestBuilder::get("/test").header(DEFAULT_DST_HEADER, "192.0.2.1:12345")).await.unwrap();
    let elapsed = start.elapsed();

    assert!(
        response.status == StatusCode::SERVICE_UNAVAILABLE || response.status == StatusCode::GATEWAY_TIMEOUT,
        "Expected 503 or 504 on connect timeout, got {}",
        response.status
    );

    assert!(
        elapsed < Duration::from_millis(500),
        "Request should have timed out quickly (~100ms), but took {elapsed:?}"
    );

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_original_dst_connection_refused() {
    let cluster = ClusterBuilder::new("backend").original_dst_via_default_header().build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response =
        client.send(RequestBuilder::get("/test").header(DEFAULT_DST_HEADER, "127.0.0.1:59999")).await.unwrap();

    response.assert_status(StatusCode::SERVICE_UNAVAILABLE);

    orion.shutdown();
    cleanup_config_file(&config_path);
}
