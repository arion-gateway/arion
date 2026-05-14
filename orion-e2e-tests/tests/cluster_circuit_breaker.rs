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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use http::StatusCode;
use orion_data_plane_api::envoy_data_plane_api::{
    envoy::config::cluster::v3::circuit_breakers::Thresholds, google::protobuf::UInt32Value,
};
use orion_e2e_tests::config_builder::{
    presets, ClusterBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RetryPolicyBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient, XdsEnabledHarness};

#[tokio::test]
#[ignore]
async fn test_circuit_breaker_max_requests_overflow() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("ok").delay(Duration::from_secs(2))).await;

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr()).circuit_breaker_max_requests(1).build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = Arc::new(TestClient::new(orion.listener_addr().unwrap()));

    let handles: Vec<_> = (0..5)
        .map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.get("/test").await })
        })
        .collect();

    let results: Vec<_> = join_all(handles).await.into_iter().filter_map(|r| r.ok()).filter_map(|r| r.ok()).collect();

    let ok_count = results.iter().filter(|r| r.status == StatusCode::OK).count();
    let overflow_count = results.iter().filter(|r| r.status == StatusCode::SERVICE_UNAVAILABLE).count();

    assert_eq!(ok_count, 1, "Expected exactly 1 request to succeed, got {ok_count}");
    assert_eq!(overflow_count, 4, "Expected 4 overflow responses, got {overflow_count}");

    for r in results.iter().filter(|r| r.status == StatusCode::SERVICE_UNAVAILABLE) {
        assert_eq!(r.header("x-envoy-overloaded"), Some("true"), "Missing x-envoy-overloaded header");
        assert!(r.body.is_empty(), "Circuit breaker overflow body should be empty");
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_circuit_breaker_under_limit_all_succeed() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("ok")).await;

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr()).circuit_breaker_max_requests(10).build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for i in 0..10 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("ok");
        assert!(
            response.header("x-envoy-overloaded").is_none(),
            "Request {i} should not have x-envoy-overloaded header"
        );
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_circuit_breaker_recovery_after_drain() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("ok").delay(Duration::from_millis(500))).await;

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr()).circuit_breaker_max_requests(1).build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = Arc::new(TestClient::new(orion.listener_addr().unwrap()));

    // Fire request A (holds the slot for ~500ms)
    let client_a = Arc::clone(&client);
    let handle_a = tokio::spawn(async move { client_a.get("/test").await });

    // Wait for A to be in-flight
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Request B should be denied (A still in-flight)
    let response_b = client.get("/test").await.unwrap();
    response_b.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response_b.header("x-envoy-overloaded"), Some("true"));

    // Wait for A to complete
    let response_a = handle_a.await.unwrap().unwrap();
    response_a.assert_status(StatusCode::OK);

    // Request C should succeed (counter decremented)
    let response_c = client.get("/test").await.unwrap();
    response_c.assert_status(StatusCode::OK);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_circuit_breaker_default_config_allows_traffic() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("ok")).await;

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr()).build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for _ in 0..20 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_circuit_breaker_exact_boundary() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("ok").delay(Duration::from_secs(2))).await;

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr()).circuit_breaker_max_requests(3).build();

    let bootstrap = presets::routed_proxy([RouteBuilder::new().match_prefix("/").cluster("backend")], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = Arc::new(TestClient::new(orion.listener_addr().unwrap()));

    let handles: Vec<_> = (0..6)
        .map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.get("/test").await })
        })
        .collect();

    let results: Vec<_> = join_all(handles).await.into_iter().filter_map(|r| r.ok()).filter_map(|r| r.ok()).collect();

    let ok_count = results.iter().filter(|r| r.status == StatusCode::OK).count();
    let overflow_count = results.iter().filter(|r| r.status == StatusCode::SERVICE_UNAVAILABLE).count();

    assert_eq!(ok_count, 3, "Expected exactly 3 requests to succeed, got {ok_count}");
    assert_eq!(overflow_count, 3, "Expected 3 overflow responses, got {overflow_count}");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_circuit_breaker_max_retries_zero_prevents_retries() {
    let mut backend = TestBackend::start().await.unwrap();
    backend
        .set_default_response(
            PreConfiguredResponse::with_status(StatusCode::SERVICE_UNAVAILABLE).delay(Duration::from_millis(50)),
        )
        .await;

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr()).circuit_breaker_max_retries(0).build();

    let retry_policy = RetryPolicyBuilder::new().on_5xx().num_retries(3);
    let route = RouteBuilder::new().match_prefix("/").cluster("backend");
    let vhost = VirtualHostBuilder::new("default").route(route).retry_policy(retry_policy);

    let bootstrap = presets::routed_proxy_with_vhost(vhost, [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::SERVICE_UNAVAILABLE);

    // Backend should receive exactly 1 request (no retries due to max_retries=0)
    let _req1 = backend.await_request().await.unwrap();
    assert!(backend.try_recv_request().is_none(), "Should not have received any retry requests");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_circuit_breaker_max_retries_permits_retry_attempts() {
    let mut backend = TestBackend::start().await.unwrap();
    backend
        .set_default_response(
            PreConfiguredResponse::with_status(StatusCode::SERVICE_UNAVAILABLE).delay(Duration::from_millis(50)),
        )
        .await;

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr()).circuit_breaker_max_retries(3).build();

    let retry_policy = RetryPolicyBuilder::new().on_5xx().num_retries(3);
    let route = RouteBuilder::new().match_prefix("/").cluster("backend");
    let vhost = VirtualHostBuilder::new("default").route(route).retry_policy(retry_policy);

    let bootstrap = presets::routed_proxy_with_vhost(vhost, [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::SERVICE_UNAVAILABLE);

    // With max_retries=3, the circuit breaker should permit retries.
    // Backend should receive more than 1 request (original + at least one retry).
    let _req1 = backend.await_request().await.unwrap();
    assert!(backend.try_recv_request().is_some(), "Expected at least one retry request");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_circuit_breaker_high_priority_independent() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("ok").delay(Duration::from_secs(2))).await;

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr())
        .circuit_breaker_max_requests(1)
        .circuit_breaker_threshold(Thresholds {
            priority: 1, // HIGH
            max_requests: Some(UInt32Value { value: 5 }),
            ..Default::default()
        })
        .build();

    let default_route = RouteBuilder::new().match_prefix("/default").cluster("backend");
    let high_route = RouteBuilder::new().match_prefix("/high").cluster("backend").with_proto(|r| {
        if let Some(orion_data_plane_api::envoy_data_plane_api::envoy::config::route::v3::route::Action::Route(
            ref mut ra,
        )) = r.action
        {
            ra.priority = 1; // HIGH
        }
    });

    let bootstrap = presets::routed_proxy([high_route, default_route], [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = Arc::new(TestClient::new(orion.listener_addr().unwrap()));

    // Fire 3 concurrent requests to /default — expect 1 OK, 2 denied
    let default_handles: Vec<_> = (0..3)
        .map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.get("/default/test").await })
        })
        .collect();

    // Fire 3 concurrent requests to /high — all should succeed (limit is 5)
    let high_handles: Vec<_> = (0..3)
        .map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.get("/high/test").await })
        })
        .collect();

    let default_results: Vec<_> =
        join_all(default_handles).await.into_iter().filter_map(|r| r.ok()).filter_map(|r| r.ok()).collect();
    let high_results: Vec<_> =
        join_all(high_handles).await.into_iter().filter_map(|r| r.ok()).filter_map(|r| r.ok()).collect();

    let default_ok = default_results.iter().filter(|r| r.status == StatusCode::OK).count();
    let default_denied = default_results.iter().filter(|r| r.status == StatusCode::SERVICE_UNAVAILABLE).count();
    assert_eq!(default_ok, 1, "Expected 1 default request to succeed, got {default_ok}");
    assert_eq!(default_denied, 2, "Expected 2 default requests denied, got {default_denied}");

    let high_ok = high_results.iter().filter(|r| r.status == StatusCode::OK).count();
    assert_eq!(high_ok, 3, "Expected all 3 high-priority requests to succeed, got {high_ok}");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_circuit_breaker_xds_config() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("ok").delay(Duration::from_secs(2))).await;

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr()).circuit_breaker_max_requests(1).build();

    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.expect("Failed to push cluster");
    harness.push_listener(&listener).await.expect("Failed to push listener");

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.expect("Listener not ready");

    let client = Arc::new(TestClient::new(listener_addr));

    let handles: Vec<_> = (0..3)
        .map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.get("/test").await })
        })
        .collect();

    let results: Vec<_> = join_all(handles).await.into_iter().filter_map(|r| r.ok()).filter_map(|r| r.ok()).collect();

    let ok_count = results.iter().filter(|r| r.status == StatusCode::OK).count();
    let overflow_count = results.iter().filter(|r| r.status == StatusCode::SERVICE_UNAVAILABLE).count();

    assert_eq!(ok_count, 1, "Expected 1 request to succeed via xDS, got {ok_count}");
    assert_eq!(overflow_count, 2, "Expected 2 overflow responses via xDS, got {overflow_count}");

    for r in results.iter().filter(|r| r.status == StatusCode::SERVICE_UNAVAILABLE) {
        assert_eq!(r.header("x-envoy-overloaded"), Some("true"));
    }

    harness.shutdown();
}
