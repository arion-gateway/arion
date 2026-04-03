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

use ctor::ctor;
use http::StatusCode;
use orion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder,
    RouteConfigBuilder, UpstreamTlsBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    OrionInstance, PreConfiguredResponse, SpawnOptions, TestCerts, TestClient, TlsBackendConfig, TlsTestBackend,
};

#[ctor]
fn init() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Could not install crypto provider (aws-lc-rs)");
}

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
async fn test_mtls_upstream_proxy_presents_cert() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_mtls(
        certs.beefcake_dublin_cert(),
        certs.beefcake_dublin_key(),
        certs.beefcake_ca_chain(),
    )
    .await
    .expect("Failed to start mTLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Upstream mTLS OK!")).await;

    let client_cert_path = TestCerts::path_to_string(&certs.beefcake_athlone_cert());
    let client_key_path = TestCerts::path_to_string(&certs.beefcake_athlone_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let cluster =
        ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).upstream_tls(
            UpstreamTlsBuilder::new()
                .client_cert_files(&client_cert_path, &client_key_path)
                .skip_server_verification("dublin.beefcake.example.com", &ca_path),
        );

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/mtls-upstream").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Upstream mTLS OK!");

    let captured_request = backend.await_request().await.expect("No request received by backend");
    assert_eq!(captured_request.path(), "/mtls-upstream");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mtls_upstream_no_client_cert_rejected() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_mtls(
        certs.beefcake_dublin_cert(),
        certs.beefcake_dublin_key(),
        certs.beefcake_ca_chain(),
    )
    .await
    .expect("Failed to start mTLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

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
    let response = client.get("/").await.expect("Request should complete");

    assert!(
        response.status == StatusCode::SERVICE_UNAVAILABLE || response.status == StatusCode::BAD_GATEWAY,
        "Expected error status due to missing client cert, got {:?}",
        response.status
    );

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mtls_upstream_wrong_client_ca_rejected() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_mtls(
        certs.beefcake_dublin_cert(),
        certs.beefcake_dublin_key(),
        certs.beefcake_ca_chain(),
    )
    .await
    .expect("Failed to start mTLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let wrong_cert_path = TestCerts::path_to_string(&certs.deadbeef_dublin_cert());
    let wrong_key_path = TestCerts::path_to_string(&certs.deadbeef_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let cluster =
        ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).upstream_tls(
            UpstreamTlsBuilder::new()
                .client_cert_files(&wrong_cert_path, &wrong_key_path)
                .skip_server_verification("dublin.beefcake.example.com", &ca_path),
        );

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/").await.expect("Request should complete");

    assert!(
        response.status == StatusCode::SERVICE_UNAVAILABLE || response.status == StatusCode::BAD_GATEWAY,
        "Expected error status due to wrong client CA, got {:?}",
        response.status
    );

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mtls_upstream_full_validation() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_mtls(
        certs.beefcake_dublin_cert(),
        certs.beefcake_dublin_key(),
        certs.beefcake_ca_chain(),
    )
    .await
    .expect("Failed to start mTLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Full mTLS validation OK!")).await;

    let client_cert_path = TestCerts::path_to_string(&certs.beefcake_athlone_cert());
    let client_key_path = TestCerts::path_to_string(&certs.beefcake_athlone_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let cluster =
        ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).upstream_tls(
            UpstreamTlsBuilder::new()
                .sni("dublin.beefcake.example.com")
                .client_cert_files(&client_cert_path, &client_key_path)
                .validation_context(&ca_path),
        );

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/fully-validated").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Full mTLS validation OK!");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mtls_upstream_multiple_requests() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_mtls(
        certs.beefcake_dublin_cert(),
        certs.beefcake_dublin_key(),
        certs.beefcake_ca_chain(),
    )
    .await
    .expect("Failed to start mTLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("response")).await;

    let client_cert_path = TestCerts::path_to_string(&certs.beefcake_athlone_cert());
    let client_key_path = TestCerts::path_to_string(&certs.beefcake_athlone_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let cluster =
        ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).upstream_tls(
            UpstreamTlsBuilder::new()
                .client_cert_files(&client_cert_path, &client_key_path)
                .skip_server_verification("dublin.beefcake.example.com", &ca_path),
        );

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
async fn test_mtls_upstream_post_with_body() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_mtls(
        certs.beefcake_dublin_cert(),
        certs.beefcake_dublin_key(),
        certs.beefcake_ca_chain(),
    )
    .await
    .expect("Failed to start mTLS backend");
    backend.set_default_response(PreConfiguredResponse::with_status(StatusCode::CREATED)).await;

    let client_cert_path = TestCerts::path_to_string(&certs.beefcake_athlone_cert());
    let client_key_path = TestCerts::path_to_string(&certs.beefcake_athlone_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let cluster =
        ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).upstream_tls(
            UpstreamTlsBuilder::new()
                .client_cert_files(&client_cert_path, &client_key_path)
                .skip_server_verification("dublin.beefcake.example.com", &ca_path),
        );

    let bootstrap = BootstrapBuilder::new().listener(http_listener("http", "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let body = r#"{"upstream_mtls": true, "proxy_authenticated": true}"#;
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
async fn test_mtls_upstream_optional_client_cert() {
    let certs = TestCerts::new();

    let config = TlsBackendConfig::from_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .expect("Failed to create backend config")
        .with_client_ca(certs.beefcake_ca_chain())
        .expect("Failed to add client CA");

    let backend = TlsTestBackend::start(config).await.expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("optional mTLS OK")).await;

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
    let response = client.get("/").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("optional mTLS OK");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
