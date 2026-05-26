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
    presets, BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder, FilterChainBuilder, HcmBuilder,
    ListenerBuilder, RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    cleanup_config_file, parse_metric_value, OrionInstance, PortBlock, PreConfiguredResponse, SpawnOptions,
    TestBackend, TestClient, TestCerts, TlsTestClientBuilder,
};

#[tokio::test]
#[ignore]
async fn test_tls_handshake_metric() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let http_port = port_block.allocate().expect("Failed to allocate http port");

    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));
    let http_addr = SocketAddr::from(([127, 0, 0, 1], http_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend!")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    // Build the HTTP listener manually with a valid route config
    let http_listener = ListenerBuilder::new("http")
        .port(http_port)
        .filter_chain(
            FilterChainBuilder::new("main").hcm(
                HcmBuilder::new().http1().route_config(
                    RouteConfigBuilder::new("routes")
                        .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route("backend"))),
                )
            )
        );

    // Configure both an HTTPS listener and a cleartext HTTP listener
    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
        .listener(http_listener)
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr))
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    // Spawn Orion tracking the "https" listener address
    let mut orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    // Wait for the cleartext HTTP listener to be ready
    orion.wait_for_listener_at(http_addr, Duration::from_secs(10)).await.expect("HTTP listener not ready");

    let admin_client = TestClient::new(admin_addr);

    // 1. Initial check: tls_handshake should be 0 or not present
    let initial_metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    initial_metrics_resp.assert_status(StatusCode::OK);
    let initial_metrics = initial_metrics_resp.body_str().unwrap();

    let initial_handshakes = parse_metric_value(initial_metrics, "tls_handshake").unwrap_or(0.0);
    assert_eq!(initial_handshakes, 0.0_f64, "Initial TLS handshakes should be 0");

    // 2. Send a cleartext HTTP request to the HTTP listener
    let cleartext_client = TestClient::new(http_addr).with_header("connection", "close");
    let response = cleartext_client.get("/hello").await.expect("Failed to send cleartext request");
    response.assert_status(StatusCode::OK);
    response.assert_body("Hello from backend!");

    // 3. Verify that cleartext HTTP request did NOT increment the tls_handshake metric
    let mid_metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    mid_metrics_resp.assert_status(StatusCode::OK);
    let mid_metrics = mid_metrics_resp.body_str().unwrap();

    let mid_handshakes = parse_metric_value(mid_metrics, "tls_handshake").unwrap_or(0.0);
    assert_eq!(mid_handshakes, 0.0, "Cleartext HTTP request should not increment TLS handshakes");

    // 4. Perform TLS handshake by sending an HTTPS request
    let tls_client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let response = tls_client.get("/hello").await.expect("Failed to send HTTPS request");
    response.assert_status(StatusCode::OK);
    response.assert_body("Hello from backend!");

    // 5. Check metrics after handshake: should be exactly 1.0
    let final_metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    final_metrics_resp.assert_status(StatusCode::OK);
    let final_metrics = final_metrics_resp.body_str().unwrap();

    let handshakes = parse_metric_value(final_metrics, "tls_handshake").expect("Missing tls_handshake metric");
    assert_eq!(handshakes, 1.0, "Expected exactly 1 TLS handshake after HTTPS request");

    orion.shutdown();
    cleanup_config_file(&config_path);
}
