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

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder,
    RouteConfigBuilder, UpstreamTlsBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, SpawnOptions, TestCerts, TestClient, TlsTestBackend};

fn http_listener(name: &str, cluster_name: &str) -> ListenerBuilder {
    ListenerBuilder::new(name).port(0).filter_chain(
        FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().route_config(
                RouteConfigBuilder::new("routes")
                    .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route(cluster_name))),
            ),
        ),
    )
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_origination_skip_verify() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from TLS backend!")).await;

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().skip_server_verification("dublin.beefcake.example.com", &ca_path));

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/hello").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Hello from TLS backend!");

    let captured_request = backend.await_request().await.expect("No request received by backend");
    assert_eq!(captured_request.path(), "/hello");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_with_ca_validation() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Validated TLS OK")).await;

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().sni("dublin.beefcake.example.com").validation_context(&ca_path));

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Validated TLS OK");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_wrong_ca_fails() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let wrong_ca_path = TestCerts::path_to_string(&certs.deadbeef_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().sni("dublin.beefcake.example.com").validation_context(&wrong_ca_path));

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/").await.expect("Request should complete");

    assert!(
        response.status == StatusCode::SERVICE_UNAVAILABLE || response.status == StatusCode::BAD_GATEWAY,
        "Expected error status due to TLS failure, got {:?}",
        response.status
    );

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_sni_sent() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("SNI OK")).await;

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().skip_server_verification("custom.sni.example.com", &ca_path));

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("SNI OK");

    let _ = backend.await_request().await.expect("No request received");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_multiple_requests() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("response")).await;

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().skip_server_verification("dublin.beefcake.example.com", &ca_path));

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());

    for i in 0..5 {
        let response = client.get(&format!("/request/{i}")).await.expect("Failed to send request");
        response.assert_status(StatusCode::OK);

        let req = backend.await_request().await.expect("No request received");
        assert_eq!(req.path(), format!("/request/{i}"));
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_post_with_body() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_status(StatusCode::CREATED)).await;

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().skip_server_verification("dublin.beefcake.example.com", &ca_path));

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let body = r#"{"name": "upstream-tls-test", "value": 123}"#;
    let response = client.post("/api/data", body).await.expect("Failed to send request");

    response.assert_status(StatusCode::CREATED);

    let req = backend.await_request().await.expect("No request received");
    assert_eq!(req.path(), "/api/data");
    assert_eq!(req.body_str(), Some(body));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_multiple_backends() {
    let certs = TestCerts::new();

    let backend1 = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend 1");
    backend1.set_default_response(PreConfiguredResponse::with_body("backend1")).await;

    let backend2 = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend 2");
    backend2.set_default_response(PreConfiguredResponse::with_body("backend2")).await;

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
        .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().skip_server_verification("dublin.beefcake.example.com", &ca_path));

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());

    let mut responses = Vec::new();
    for _ in 0..4 {
        let response = client.get("/").await.expect("Failed to send request");
        response.assert_status(StatusCode::OK);
        responses.push(response.body_str().unwrap_or("").to_owned());
    }

    let has_backend1 = responses.iter().any(|r| r == "backend1");
    let has_backend2 = responses.iter().any(|r| r == "backend2");
    assert!(has_backend1 && has_backend2, "Expected requests to be distributed to both backends, got: {responses:?}");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_1_2_only() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("TLS 1.2 OK")).await;

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster =
        ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).upstream_tls(
            UpstreamTlsBuilder::new().sni("dublin.beefcake.example.com").validation_context(&ca_path).tls_1_2_only(),
        );

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("TLS 1.2 OK");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_1_3_only() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("TLS 1.3 OK")).await;

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster =
        ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).upstream_tls(
            UpstreamTlsBuilder::new().sni("dublin.beefcake.example.com").validation_context(&ca_path).tls_1_3_only(),
        );

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("TLS 1.3 OK");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_upstream_tls_sni_mismatch_fails() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().sni("wrong.example.com").validation_context(&ca_path));

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/").await.expect("Request should complete");

    assert!(
        response.status == StatusCode::SERVICE_UNAVAILABLE || response.status == StatusCode::BAD_GATEWAY,
        "Expected error status due to SNI/hostname mismatch, got {:?}",
        response.status
    );

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
