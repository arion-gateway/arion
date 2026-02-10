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

use std::collections::HashSet;

use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, ClusterBuilder, EndpointBuilder, LbPolicy, RouteBuilder};
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient};

const OVERRIDE_HEADER: &str = "x-override-host";

#[tokio::test]
#[ignore]
async fn test_override_host_routes_to_specified_backend() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend")
        .override_host(OVERRIDE_HEADER, LbPolicy::RoundRobin)
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend3.addr()))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let override_value = format!("127.0.0.1:{}", backend2.addr().port());
    for _ in 0..10 {
        let response =
            client.send(RequestBuilder::get("/test").header(OVERRIDE_HEADER, &override_value)).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b2");
    }

    let override_value = format!("127.0.0.1:{}", backend3.addr().port());
    for _ in 0..10 {
        let response =
            client.send(RequestBuilder::get("/test").header(OVERRIDE_HEADER, &override_value)).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b3");
    }

    let override_value = format!("127.0.0.1:{}", backend1.addr().port());
    for _ in 0..10 {
        let response =
            client.send(RequestBuilder::get("/test").header(OVERRIDE_HEADER, &override_value)).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b1");
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_override_host_missing_header_uses_fallback() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend")
        .override_host(OVERRIDE_HEADER, LbPolicy::RoundRobin)
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend3.addr()))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let mut seen_backends: HashSet<String> = HashSet::new();
    for _ in 0..9 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        seen_backends.insert(response.body_str().unwrap_or("").to_string());
    }

    assert!(seen_backends.contains("b1"), "Expected traffic to b1 via fallback");
    assert!(seen_backends.contains("b2"), "Expected traffic to b2 via fallback");
    assert!(seen_backends.contains("b3"), "Expected traffic to b3 via fallback");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_override_host_invalid_header_uses_fallback() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let cluster = ClusterBuilder::new("backend")
        .override_host(OVERRIDE_HEADER, LbPolicy::RoundRobin)
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let invalid_values = vec!["not-a-valid-authority", ":::invalid:::", ""];

    for invalid_value in invalid_values {
        let mut seen_backends: HashSet<String> = HashSet::new();
        for _ in 0..6 {
            let response =
                client.send(RequestBuilder::get("/test").header(OVERRIDE_HEADER, invalid_value)).await.unwrap();
            response.assert_status(StatusCode::OK);
            seen_backends.insert(response.body_str().unwrap_or("").to_string());
        }
        assert!(
            seen_backends.len() > 1 && seen_backends.contains("b1") && seen_backends.contains("b2"),
            "Expected fallback behavior for invalid header value: '{invalid_value}'"
        );
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_override_host_unknown_host_uses_fallback() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let cluster = ClusterBuilder::new("backend")
        .override_host(OVERRIDE_HEADER, LbPolicy::RoundRobin)
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let unknown_host = "127.0.0.1:59999";

    let mut seen_backends: HashSet<String> = HashSet::new();
    for _ in 0..6 {
        let response = client.send(RequestBuilder::get("/test").header(OVERRIDE_HEADER, unknown_host)).await.unwrap();
        response.assert_status(StatusCode::OK);
        seen_backends.insert(response.body_str().unwrap_or("").to_string());
    }

    assert!(seen_backends.contains("b1"), "Expected traffic to b1 via fallback");
    assert!(seen_backends.contains("b2"), "Expected traffic to b2 via fallback");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_override_host_multiple_hosts_uses_first() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let cluster = ClusterBuilder::new("backend")
        .override_host(OVERRIDE_HEADER, LbPolicy::RoundRobin)
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let override_value = format!("127.0.0.1:{},127.0.0.1:{}", backend2.addr().port(), backend1.addr().port());

    for _ in 0..10 {
        let response =
            client.send(RequestBuilder::get("/test").header(OVERRIDE_HEADER, &override_value)).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b2");
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_override_host_multiple_hosts_uses_next_available_on_failover() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let backend1_addr = backend1.addr();
    drop(backend1);

    let cluster = ClusterBuilder::new("backend")
        .override_host(OVERRIDE_HEADER, LbPolicy::RoundRobin)
        .endpoint(EndpointBuilder::from_socket_addr(backend1_addr))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let override_value = format!("127.0.0.1:{},127.0.0.1:{}", backend1_addr.port(), backend2.addr().port());

    for _ in 0..10 {
        let response =
            client.send(RequestBuilder::get("/test").header(OVERRIDE_HEADER, &override_value)).await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b2");
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
