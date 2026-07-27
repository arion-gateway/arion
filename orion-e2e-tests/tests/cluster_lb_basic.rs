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
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, ClusterBuilder, EndpointBuilder, RouteBuilder};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient,
};

#[tokio::test]
#[ignore]
async fn test_round_robin_distribution() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend")
        .round_robin()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend3.addr()))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let mut responses: Vec<String> = Vec::new();
    for _ in 0..9 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        responses.push(response.body_str().unwrap_or("").to_owned());
    }

    let b1_count = responses.iter().filter(|r| *r == "b1").count();
    let b2_count = responses.iter().filter(|r| *r == "b2").count();
    let b3_count = responses.iter().filter(|r| *r == "b3").count();

    assert_eq!(b1_count, 3, "Expected 3 requests to b1, got {b1_count}");
    assert_eq!(b2_count, 3, "Expected 3 requests to b2, got {b2_count}");
    assert_eq!(b3_count, 3, "Expected 3 requests to b3, got {b3_count}");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_round_robin_weighted() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend")
        .round_robin()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()).weight(1))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()).weight(2))
        .endpoint(EndpointBuilder::from_socket_addr(backend3.addr()).weight(3))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let mut counts: HashMap<String, u32> = HashMap::new();
    let total_requests = 120;
    for _ in 0..total_requests {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        *counts.entry(body).or_insert(0) += 1;
    }

    let b1 = f64::from(*counts.get("b1").unwrap_or(&0));
    let b2 = f64::from(*counts.get("b2").unwrap_or(&0));
    let b3 = f64::from(*counts.get("b3").unwrap_or(&0));

    let expected_b1 = f64::from(total_requests) / 6.0;
    let expected_b2 = f64::from(total_requests) * 2.0 / 6.0;
    let expected_b3 = f64::from(total_requests) * 3.0 / 6.0;

    let tolerance = 0.3;
    assert!((b1 - expected_b1).abs() / expected_b1 < tolerance, "b1: expected ~{expected_b1:.0}, got {b1:.0}");
    assert!((b2 - expected_b2).abs() / expected_b2 < tolerance, "b2: expected ~{expected_b2:.0}, got {b2:.0}");
    assert!((b3 - expected_b3).abs() / expected_b3 < tolerance, "b3: expected ~{expected_b3:.0}, got {b3:.0}");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_random_distribution() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    let backend3 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend3.set_default_response(PreConfiguredResponse::with_body("b3")).await;

    let cluster = ClusterBuilder::new("backend")
        .random()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend3.addr()))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let mut counts: HashMap<String, u32> = HashMap::new();
    let total_requests = 90;
    for _ in 0..total_requests {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        *counts.entry(body).or_insert(0) += 1;
    }

    assert!(counts.contains_key("b1"), "Expected some traffic to b1");
    assert!(counts.contains_key("b2"), "Expected some traffic to b2");
    assert!(counts.contains_key("b3"), "Expected some traffic to b3");

    let b1 = *counts.get("b1").unwrap_or(&0);
    let b2 = *counts.get("b2").unwrap_or(&0);
    let b3 = *counts.get("b3").unwrap_or(&0);
    let expected = total_requests / 3;
    let tolerance = expected / 2;

    assert!(b1.abs_diff(expected) < tolerance, "b1 count {b1} too far from expected {expected}");
    assert!(b2.abs_diff(expected) < tolerance, "b2 count {b2} too far from expected {expected}");
    assert!(b3.abs_diff(expected) < tolerance, "b3 count {b3} too far from expected {expected}");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_least_request_prefers_idle() {
    let backend1 = TestBackend::start_with_capacity(500).await.unwrap();
    let backend2 = TestBackend::start_with_capacity(500).await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("fast")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("slow").delay(Duration::from_millis(200))).await;

    let cluster = ClusterBuilder::new("backend")
        .least_request()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = Arc::new(TestClient::new(orion.listener_addr().unwrap()));

    let mut all_handles = Vec::new();

    for wave in 0..10 {
        let handles: Vec<_> = (0..50)
            .map(|_| {
                let client = Arc::clone(&client);
                tokio::spawn(async move { client.get("/test").await })
            })
            .collect();

        all_handles.extend(handles);

        if wave < 9 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let results = join_all(all_handles).await;

    let mut counts: HashMap<String, u32> = HashMap::new();
    for result in results {
        if let Ok(Ok(response)) = result {
            let response: orion_e2e_tests::TestResponse = response;
            response.assert_status(StatusCode::OK);
            let body = response.body_str().unwrap_or("").to_owned();
            *counts.entry(body).or_insert(0) += 1;
        }
    }

    let fast_count = *counts.get("fast").unwrap_or(&0);
    let slow_count = *counts.get("slow").unwrap_or(&0);

    assert!(
        fast_count > slow_count,
        "Expected fast backend ({fast_count}) to receive more requests than slow backend ({slow_count})"
    );

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_endpoint_connection_refused() {
    let cluster =
        ClusterBuilder::new("backend").round_robin().endpoint(EndpointBuilder::new("127.0.0.1", 59999)).build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::SERVICE_UNAVAILABLE);

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_connect_timeout() {
    // Use TEST-NET-1 (RFC 5737) - packets are dropped, causing connect timeout
    let cluster = ClusterBuilder::new("backend")
        .round_robin()
        .connect_timeout(Duration::from_millis(100))
        .endpoint(EndpointBuilder::new("192.0.2.1", 12345))
        .build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let start = std::time::Instant::now();
    let response = client.get("/test").await.unwrap();
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
