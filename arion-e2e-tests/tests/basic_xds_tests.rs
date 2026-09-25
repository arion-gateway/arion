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

use pingora::prelude::fast_timeout::fast_timeout;
use std::net::SocketAddr;
use std::time::Duration;

use arion_e2e_tests::config_builder::{
    ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder, RouteConfigBuilder,
    VirtualHostBuilder,
};
use arion_e2e_tests::{PreConfiguredResponse, TestBackend, TestClient, XdsEnabledHarness};
use http::StatusCode;

#[tokio::test]
#[ignore]
async fn test_dynamic_xds_config() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from xDS backend!")).await;

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).build();
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

    harness.arion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.expect("Listener not ready");

    let client = TestClient::new(listener_addr);

    fast_timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get("/test").await {
                if response.status == StatusCode::OK {
                    if let Some(body) = response.body_str() {
                        if body == "Hello from xDS backend!" {
                            break;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for initial RDS route");

    while backend.try_recv_request().is_some() {}

    let response = client.get("/test").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Hello from xDS backend!");

    let captured_request = backend.await_request().await.expect("No request received by backend");
    assert_eq!(captured_request.path(), "/test");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_dynamic_config_update() {
    let mut backend1 = TestBackend::start().await.expect("Failed to start backend1");
    let mut backend2 = TestBackend::start().await.expect("Failed to start backend2");

    backend1.set_default_response(PreConfiguredResponse::with_body("Response from backend1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("Response from backend2")).await;

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster1 = ClusterBuilder::new("backend1").endpoint(EndpointBuilder::from_socket_addr(backend1.addr())).build();
    let cluster2 = ClusterBuilder::new("backend2").endpoint(EndpointBuilder::from_socket_addr(backend2.addr())).build();

    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(
            FilterChainBuilder::new("main").hcm(
                HcmBuilder::new().route_config(
                    RouteConfigBuilder::new("routes").virtual_host(
                        VirtualHostBuilder::new("default")
                            .route(RouteBuilder::new().match_prefix("/api").cluster("backend1"))
                            .route(RouteBuilder::new().match_prefix("/service").cluster("backend2")),
                    ),
                ),
            ),
        )
        .build();

    harness.push_cluster(&cluster1).await.expect("Failed to push cluster1");
    harness.push_listener(&listener).await.expect("Failed to push listener");
    harness.arion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.expect("Listener not ready");

    let client = TestClient::new(listener_addr);

    fast_timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get("/api/test").await {
                if response.status == StatusCode::OK {
                    if let Some(body) = response.body_str() {
                        if body == "Response from backend1" {
                            break;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for initial cluster1");

    while backend1.try_recv_request().is_some() {}

    let response = client.get("/api/test").await.expect("Failed to send request to /api");
    response.assert_status(StatusCode::OK);
    response.assert_body("Response from backend1");

    let response = client.get("/service/test").await.expect("Failed to send request to /service");
    response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);

    harness.push_cluster(&cluster2).await.expect("Failed to push cluster2");

    // Wait for cluster2 to become active — XDS cluster push is async and Arion may not
    // have applied it yet by the time push_cluster returns.
    fast_timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get("/service/test").await {
                if response.status == StatusCode::OK {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for cluster2 to become available after XDS push");

    while backend2.try_recv_request().is_some() {}

    let response = client.get("/service/test").await.expect("Failed to send request to /service after cluster add");
    response.assert_status(StatusCode::OK);
    response.assert_body("Response from backend2");

    harness.shutdown();
}
