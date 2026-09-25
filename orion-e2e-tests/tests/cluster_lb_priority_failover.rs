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
    ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, HealthStatus, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{PreConfiguredResponse, TestBackend, TestClient, XdsEnabledHarness};

#[tokio::test]
#[ignore]
async fn test_priority_zero_receives_all_traffic() {
    let p0_backend = TestBackend::start().await.unwrap();
    let p1_backend = TestBackend::start().await.unwrap();
    p0_backend.set_default_response(PreConfiguredResponse::with_body("p0")).await;
    p1_backend.set_default_response(PreConfiguredResponse::with_body("p1")).await;

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

    let p0_endpoints = vec![EndpointBuilder::from_socket_addr(p0_backend.addr()).build()];
    let p1_endpoints = vec![EndpointBuilder::from_socket_addr(p1_backend.addr()).build()];
    harness.push_endpoints_with_priorities("backend", &[(0, p0_endpoints), (1, p1_endpoints)]).await.unwrap();

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    let client = TestClient::new(listener_addr);
    for _ in 0..10 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("p0");
    }

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_priority_failover_on_unhealthy() {
    let p0_backend = TestBackend::start().await.unwrap();
    let p1_backend = TestBackend::start().await.unwrap();
    p0_backend.set_default_response(PreConfiguredResponse::with_body("p0")).await;
    p1_backend.set_default_response(PreConfiguredResponse::with_body("p1")).await;

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

    let p0_endpoints = vec![EndpointBuilder::from_socket_addr(p0_backend.addr()).build()];
    let p1_endpoints = vec![EndpointBuilder::from_socket_addr(p1_backend.addr()).build()];
    harness.push_endpoints_with_priorities("backend", &[(0, p0_endpoints), (1, p1_endpoints)]).await.unwrap();

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    let client = TestClient::new(listener_addr);
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("p0");

    let p0_unhealthy =
        vec![EndpointBuilder::from_socket_addr(p0_backend.addr()).health_status(HealthStatus::Unhealthy).build()];
    let p1_healthy = vec![EndpointBuilder::from_socket_addr(p1_backend.addr()).build()];
    harness.push_endpoints_with_priorities("backend", &[(0, p0_unhealthy), (1, p1_healthy)]).await.unwrap();

    for _ in 0..5 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("p1");
    }

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_priority_recovery() {
    let p0_backend = TestBackend::start().await.unwrap();
    let p1_backend = TestBackend::start().await.unwrap();
    p0_backend.set_default_response(PreConfiguredResponse::with_body("p0")).await;
    p1_backend.set_default_response(PreConfiguredResponse::with_body("p1")).await;

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

    let p0_unhealthy =
        vec![EndpointBuilder::from_socket_addr(p0_backend.addr()).health_status(HealthStatus::Unhealthy).build()];
    let p1_healthy = vec![EndpointBuilder::from_socket_addr(p1_backend.addr()).build()];
    harness.push_endpoints_with_priorities("backend", &[(0, p0_unhealthy), (1, p1_healthy)]).await.unwrap();

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    let client = TestClient::new(listener_addr);
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("p1");

    let p0_healthy = vec![EndpointBuilder::from_socket_addr(p0_backend.addr()).build()];
    let p1_healthy = vec![EndpointBuilder::from_socket_addr(p1_backend.addr()).build()];
    harness.push_endpoints_with_priorities("backend", &[(0, p0_healthy), (1, p1_healthy)]).await.unwrap();

    for _ in 0..5 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("p0");
    }

    harness.shutdown();
}
