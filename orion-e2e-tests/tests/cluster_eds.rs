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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, HealthStatus, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{PreConfiguredResponse, TestBackend, TestClient, XdsEnabledHarness};

#[tokio::test]
#[ignore]
async fn test_eds_initial_endpoints() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("from-eds")).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend").eds().build();

    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.push_endpoints("backend", &[EndpointBuilder::from_socket_addr(backend.addr()).build()]).await.unwrap();

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    let client = TestClient::new(listener_addr);
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("from-eds");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_eds_add_endpoint() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend").eds().build();

    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.push_endpoints("backend", &[EndpointBuilder::from_socket_addr(backend1.addr()).build()]).await.unwrap();

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    let client = TestClient::new(listener_addr);
    for _ in 0..5 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b1");
    }

    harness
        .push_endpoints(
            "backend",
            &[
                EndpointBuilder::from_socket_addr(backend1.addr()).build(),
                EndpointBuilder::from_socket_addr(backend2.addr()).build(),
            ],
        )
        .await
        .unwrap();

    let mut counts: HashMap<String, u32> = HashMap::new();
    for _ in 0..20 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        *counts.entry(body).or_insert(0) += 1;
    }

    assert!(counts.contains_key("b1"), "Expected traffic to b1");
    assert!(counts.contains_key("b2"), "Expected traffic to newly added b2");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_eds_remove_endpoint() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend").eds().build();

    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness
        .push_endpoints(
            "backend",
            &[
                EndpointBuilder::from_socket_addr(backend1.addr()).build(),
                EndpointBuilder::from_socket_addr(backend2.addr()).build(),
            ],
        )
        .await
        .unwrap();

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    let client = TestClient::new(listener_addr);

    let mut counts: HashMap<String, u32> = HashMap::new();
    for _ in 0..10 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        *counts.entry(body).or_insert(0) += 1;
    }
    assert!(counts.contains_key("b1") && counts.contains_key("b2"), "Both backends should receive traffic initially");

    harness.push_endpoints("backend", &[EndpointBuilder::from_socket_addr(backend1.addr()).build()]).await.unwrap();

    for _ in 0..10 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b1");
    }

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_eds_weight_update() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend").eds().build();

    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness
        .push_endpoints(
            "backend",
            &[
                EndpointBuilder::from_socket_addr(backend1.addr()).weight(1).build(),
                EndpointBuilder::from_socket_addr(backend2.addr()).weight(1).build(),
            ],
        )
        .await
        .unwrap();

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    let client = TestClient::new(listener_addr);

    let mut initial_counts: HashMap<String, u32> = HashMap::new();
    for _ in 0..20 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        *initial_counts.entry(body).or_insert(0) += 1;
    }
    let initial_b1 = *initial_counts.get("b1").unwrap_or(&0);
    let initial_b2 = *initial_counts.get("b2").unwrap_or(&0);
    assert!(
        (6..=14).contains(&initial_b1) && (6..=14).contains(&initial_b2),
        "With equal weights, both backends should receive roughly equal traffic. b1={initial_b1}, b2={initial_b2}"
    );

    harness
        .push_endpoints(
            "backend",
            &[
                EndpointBuilder::from_socket_addr(backend1.addr()).weight(1).build(),
                EndpointBuilder::from_socket_addr(backend2.addr()).weight(9).build(),
            ],
        )
        .await
        .unwrap();

    let mut counts: HashMap<String, u32> = HashMap::new();
    let total_requests = 100;
    for _ in 0..total_requests {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        let body = response.body_str().unwrap_or("").to_owned();
        *counts.entry(body).or_insert(0) += 1;
    }

    let b1_count = *counts.get("b1").unwrap_or(&0);
    let b2_count = *counts.get("b2").unwrap_or(&0);

    assert!(
        b2_count >= b1_count * 2,
        "b2 (weight 9) should receive significantly more traffic than b1 (weight 1). b1={b1_count}, b2={b2_count}"
    );

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_eds_health_status_unhealthy() {
    let backend1 = TestBackend::start().await.unwrap();
    let backend2 = TestBackend::start().await.unwrap();
    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend").eds().build();

    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness
        .push_endpoints(
            "backend",
            &[
                EndpointBuilder::from_socket_addr(backend1.addr()).health_status(HealthStatus::Unhealthy).build(),
                EndpointBuilder::from_socket_addr(backend2.addr()).build(),
            ],
        )
        .await
        .unwrap();

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    let client = TestClient::new(listener_addr);
    for _ in 0..10 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("b2");
    }

    harness.shutdown();
}
