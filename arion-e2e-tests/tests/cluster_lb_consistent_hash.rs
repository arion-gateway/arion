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

use std::collections::HashSet;

use arion_e2e_tests::config_builder::{presets, ClusterBuilder, EndpointBuilder, RouteBuilder};
use arion_e2e_tests::{
    cleanup_config_file, ArionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};
use http::StatusCode;

#[tokio::test]
#[ignore]
async fn test_ring_hash_header_routing() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend")
        .ring_hash()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend3.addr()))
        .build();

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").cluster("backend").hash_policy_header("x-hash-key")],
        [cluster],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(arion.listener_addr().unwrap());

    let mut first_response = String::new();
    for i in 0..10 {
        let response = client.send(RequestBuilder::get("/test").header("x-hash-key", "user-123")).await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        if i == 0 {
            first_response = body.clone();
        }
        assert_eq!(body, first_response, "Same hash key should route to same backend");
    }

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_ring_hash_different_keys() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend")
        .ring_hash()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend3.addr()))
        .build();

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").cluster("backend").hash_policy_header("x-hash-key")],
        [cluster],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(arion.listener_addr().unwrap());

    let mut backends_hit: HashSet<String> = HashSet::new();
    for i in 0..100 {
        let key = format!("unique-key-{i}");
        let response = client.send(RequestBuilder::get("/test").header("x-hash-key", &key)).await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        backends_hit.insert(body);
    }

    assert!(
        backends_hit.len() >= 2,
        "Different keys should distribute across multiple backends, but only hit {} backends",
        backends_hit.len()
    );

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_maglev_header_routing() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend")
        .maglev()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend3.addr()))
        .build();

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").cluster("backend").hash_policy_header("x-hash-key")],
        [cluster],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(arion.listener_addr().unwrap());

    let mut first_response = String::new();
    for i in 0..10 {
        let response = client.send(RequestBuilder::get("/test").header("x-hash-key", "user-456")).await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        if i == 0 {
            first_response = body.clone();
        }
        assert_eq!(body, first_response, "Same hash key should route to same backend with Maglev");
    }

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_maglev_different_keys() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend")
        .maglev()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend3.addr()))
        .build();

    let bootstrap = presets::routed_proxy(
        [RouteBuilder::new().match_prefix("/").cluster("backend").hash_policy_header("x-hash-key")],
        [cluster],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(arion.listener_addr().unwrap());

    let mut backends_hit: HashSet<String> = HashSet::new();
    for i in 0..100 {
        let key = format!("maglev-key-{i}");
        let response = client.send(RequestBuilder::get("/test").header("x-hash-key", &key)).await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        backends_hit.insert(body);
    }

    assert!(
        backends_hit.len() >= 2,
        "Different keys should distribute across multiple backends with Maglev, but only hit {} backends",
        backends_hit.len()
    );

    arion.shutdown();
    cleanup_config_file(&config_path);
}
