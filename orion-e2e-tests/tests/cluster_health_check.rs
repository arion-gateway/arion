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
use std::time::Duration;

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    ClusterBuilder, EndpointBuilder, FilterChainBuilder, GrpcHealthCheckBuilder, HcmBuilder, HttpHealthCheckBuilder,
    ListenerBuilder, RouteBuilder, RouteConfigBuilder, TcpHealthCheckBuilder, TcpProxyBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    GrpcTestBackend, GrpcTestClient, PreConfiguredResponse, TcpTestBackend, TcpTestClient, TestBackend, TestClient,
    XdsEnabledHarness,
};

#[tokio::test]
#[ignore]
async fn test_http_health_check_excludes_unhealthy() {
    let mut healthy_backend = TestBackend::start().await.unwrap();
    let mut unhealthy_backend = TestBackend::start().await.unwrap();

    healthy_backend.set_default_response(PreConfiguredResponse::with_body("healthy")).await;
    unhealthy_backend.set_default_response(PreConfiguredResponse::with_status(StatusCode::SERVICE_UNAVAILABLE)).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend")
        .round_robin()
        .endpoint(EndpointBuilder::from_socket_addr(healthy_backend.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(unhealthy_backend.addr()))
        .http_health_check("/health", Duration::from_millis(100), Duration::from_millis(50))
        .build();

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
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    healthy_backend.await_path_request_count("/health", 2, Duration::from_secs(5)).await.unwrap();
    unhealthy_backend.await_path_request_count("/health", 2, Duration::from_secs(5)).await.unwrap();

    let client = TestClient::new(listener_addr);

    // Require several consecutive "healthy" responses to confirm the unhealthy backend is
    // stably excluded — guards against the race where Orion received the health-check result
    // but hasn't yet propagated the "unhealthy" decision to the load-balancer.
    let mut consecutive_healthy = 0usize;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client.get("/test").await {
                Ok(r) if r.status == StatusCode::OK && r.body_str() == Some("healthy") => {
                    consecutive_healthy += 1;
                    if consecutive_healthy >= 5 {
                        break;
                    }
                },
                _ => consecutive_healthy = 0,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for 5 consecutive healthy HTTP responses with unhealthy backend excluded");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_health_check_recovery() {
    let mut backend1 = TestBackend::start().await.unwrap();
    let mut backend2 = TestBackend::start().await.unwrap();

    backend1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_status(StatusCode::SERVICE_UNAVAILABLE)).await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend")
        .round_robin()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .health_check(
            HttpHealthCheckBuilder::new("/health", Duration::from_millis(100), Duration::from_millis(50))
                .unhealthy_threshold(2)
                .healthy_threshold(1)
                .accept_2xx(),
        )
        .build();

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
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    backend1.await_path_request_count("/health", 1, Duration::from_secs(5)).await.unwrap();
    backend2.await_path_request_count("/health", 2, Duration::from_secs(5)).await.unwrap();

    let client = TestClient::new(listener_addr);

    // Require several consecutive b1 responses to confirm b2 is stably excluded before
    // flipping b2 to healthy — guards against the health-check propagation race.
    let mut consecutive_b1 = 0usize;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client.get("/test").await {
                Ok(r) if r.status == StatusCode::OK && r.body_str() == Some("b1") => {
                    consecutive_b1 += 1;
                    if consecutive_b1 >= 5 {
                        break;
                    }
                },
                _ => consecutive_b1 = 0,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for 5 consecutive b1 HTTP responses while b2 is unhealthy");

    backend2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    backend2.drain_requests_for_path("/health");

    backend2.await_path_request_count("/health", 3, Duration::from_secs(5)).await.unwrap();

    // Verify both backends receive traffic (round-robin should distribute)
    let mut saw_b1 = false;
    let mut saw_b2 = false;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get("/test").await {
                if response.status == StatusCode::OK {
                    let body = response.body_str().unwrap_or("");
                    if body == "b1" {
                        saw_b1 = true;
                    } else if body == "b2" {
                        saw_b2 = true;
                    }
                    if saw_b1 && saw_b2 {
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for backend2 to recover and receive traffic");

    assert!(saw_b1, "Expected backend1 to continue receiving traffic");
    assert!(saw_b2, "Expected backend2 to recover and receive traffic after health check passes");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_tcp_health_check_excludes_unreachable() {
    let mut healthy_backend = TcpTestBackend::start().await.unwrap();
    healthy_backend.set_send_on_connect(b"healthy").await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let unreachable_port = harness.allocate_listener_port().unwrap();

    let cluster = ClusterBuilder::new("backend")
        .round_robin()
        .endpoint(EndpointBuilder::from_socket_addr(healthy_backend.addr()))
        .endpoint(EndpointBuilder::new("127.0.0.1", unreachable_port))
        .tcp_health_check(Duration::from_millis(100), Duration::from_millis(50))
        .build();

    let listener = ListenerBuilder::new("tcp")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    healthy_backend.await_connection_count(2, Duration::from_secs(5)).await.unwrap();

    let client = TcpTestClient::new(listener_addr);

    // Require several consecutive "healthy" responses to confirm the unreachable backend is
    // stably excluded — guards against the health-check propagation race.
    let mut consecutive_healthy = 0usize;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client.receive_on_connect_with_timeout(Duration::from_millis(500)).await {
                Ok(r) if r.starts_with(b"healthy") => {
                    consecutive_healthy += 1;
                    if consecutive_healthy >= 5 {
                        break;
                    }
                },
                _ => consecutive_healthy = 0,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for 5 consecutive healthy TCP responses with unreachable backend excluded");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_tcp_health_check_recovery() {
    let mut backend1 = TcpTestBackend::start().await.unwrap();
    backend1.set_send_on_connect(b"b1").await;

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let backend2_port = harness.allocate_listener_port().unwrap();

    let cluster = ClusterBuilder::new("backend")
        .round_robin()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::new("127.0.0.1", backend2_port))
        .health_check(
            TcpHealthCheckBuilder::new(Duration::from_millis(100), Duration::from_millis(50))
                .unhealthy_threshold(2)
                .healthy_threshold(1),
        )
        .build();

    let listener = ListenerBuilder::new("tcp")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    backend1.await_connection_count(2, Duration::from_secs(5)).await.unwrap();

    let client = TcpTestClient::new(listener_addr);

    // Require several consecutive b1 responses to confirm the unreachable backend2 is
    // stably excluded before bringing it back — guards against the health-check propagation race.
    let mut consecutive_b1 = 0usize;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client.receive_on_connect_with_timeout(Duration::from_millis(500)).await {
                Ok(r) if r.starts_with(b"b1") => {
                    consecutive_b1 += 1;
                    if consecutive_b1 >= 5 {
                        break;
                    }
                },
                _ => consecutive_b1 = 0,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for 5 consecutive b1 TCP responses while backend2 is unreachable");

    let mut backend2 = TcpTestBackend::start_on_port(backend2_port).await.unwrap();
    backend2.set_send_on_connect(b"b2").await;

    backend2.await_connection_count(10, Duration::from_secs(10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut saw_b1 = false;
    let mut saw_b2 = false;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.receive_on_connect_with_timeout(Duration::from_millis(500)).await {
                if response.starts_with(b"b1") {
                    saw_b1 = true;
                } else if response.starts_with(b"b2") {
                    saw_b2 = true;
                }
                if saw_b1 && saw_b2 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for backend2 to recover and receive traffic");

    assert!(saw_b1, "Expected backend1 to continue receiving traffic");
    assert!(saw_b2, "Expected backend2 to recover and receive traffic after becoming reachable");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_grpc_health_check_excludes_not_serving() {
    let healthy =
        GrpcTestBackend::builder().with_health().with_test_service().backend_id("healthy").start().await.unwrap();
    healthy.set_serving("").await.unwrap();

    let unhealthy =
        GrpcTestBackend::builder().with_health().with_test_service().backend_id("unhealthy").start().await.unwrap();
    unhealthy.set_not_serving("").await.unwrap();

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend")
        .round_robin()
        .http2()
        .endpoint(EndpointBuilder::from_socket_addr(healthy.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(unhealthy.addr()))
        .grpc_health_check(Duration::from_millis(100), Duration::from_millis(50))
        .build();

    let listener = ListenerBuilder::new("grpc")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().http2().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    healthy.await_health_check_count(2, Duration::from_secs(5)).await.unwrap();
    unhealthy.await_health_check_count(2, Duration::from_secs(5)).await.unwrap();

    let mut client = GrpcTestClient::connect(listener_addr).await.unwrap();

    // Require several consecutive "healthy" responses to confirm the NOT_SERVING backend is
    // stably excluded — guards against the health-check propagation race.
    let mut consecutive_healthy = 0usize;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client.echo_backend_id("test").await {
                Ok(id) if id == "healthy" => {
                    consecutive_healthy += 1;
                    if consecutive_healthy >= 5 {
                        break;
                    }
                },
                _ => consecutive_healthy = 0,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for 5 consecutive responses from healthy gRPC backend only");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_grpc_health_check_recovery() {
    let backend1 = GrpcTestBackend::builder().with_health().with_test_service().backend_id("b1").start().await.unwrap();
    backend1.set_serving("").await.unwrap();

    let backend2 = GrpcTestBackend::builder().with_health().with_test_service().backend_id("b2").start().await.unwrap();
    backend2.set_not_serving("").await.unwrap();

    let mut harness = XdsEnabledHarness::start().await.unwrap();
    let listener_port = harness.allocate_listener_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend")
        .round_robin()
        .http2()
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .health_check(
            GrpcHealthCheckBuilder::new(Duration::from_millis(100), Duration::from_millis(50))
                .unhealthy_threshold(2)
                .healthy_threshold(2),
        )
        .build();

    let listener = ListenerBuilder::new("grpc")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().http2().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    backend1.await_health_check_count(2, Duration::from_secs(10)).await.unwrap();
    backend2.await_health_check_count(2, Duration::from_secs(10)).await.unwrap();

    let mut client = GrpcTestClient::connect(listener_addr).await.unwrap();

    // Wait until b2 is consistently excluded: require several consecutive b1 responses
    // to guard against the race where Orion has received the health-check but hasn't yet
    // propagated the "unhealthy" decision to the load-balancer.
    let mut consecutive_b1 = 0usize;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client.echo_backend_id("test").await {
                Ok(id) if id == "b1" => {
                    consecutive_b1 += 1;
                    if consecutive_b1 >= 5 {
                        break;
                    }
                },
                _ => {
                    consecutive_b1 = 0;
                },
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for 5 consecutive b1 responses while b2 is NOT_SERVING");

    backend2.set_serving("").await.unwrap();
    backend2.reset_health_check_count();
    backend2.await_health_check_count(2, Duration::from_secs(10)).await.unwrap();

    let mut saw_b1 = false;
    let mut saw_b2 = false;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(backend_id) = client.echo_backend_id("test").await {
                if backend_id == "b1" {
                    saw_b1 = true;
                } else if backend_id == "b2" {
                    saw_b2 = true;
                }
                if saw_b1 && saw_b2 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for backend2 to recover and receive traffic");

    assert!(saw_b1, "Expected backend1 to continue receiving traffic");
    assert!(saw_b2, "Expected backend2 to recover and receive traffic after becoming SERVING");

    harness.shutdown();
}
