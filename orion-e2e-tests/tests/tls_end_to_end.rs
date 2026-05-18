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
    presets, BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder, EndpointBuilder, UpstreamTlsBuilder,
};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, SpawnOptions, TestCerts, TlsTestBackend,
    TlsTestClientBuilder,
};

#[tokio::test]
#[ignore]
async fn test_full_tls_chain_skip_verify() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Full TLS chain OK!")).await;

    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let downstream_tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().skip_server_verification("dublin.beefcake.example.com", &ca_path));

    let bootstrap =
        BootstrapBuilder::new().listener(presets::https_listener("https", downstream_tls, "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let response = client.get("/full-chain").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Full TLS chain OK!");

    let captured_request = backend.await_request().await.expect("No request received by backend");
    assert_eq!(captured_request.path(), "/full-chain");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_full_tls_chain_with_ca_validation() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Validated chain OK!")).await;

    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let downstream_tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().sni("dublin.beefcake.example.com").validation_context(&ca_path));

    let bootstrap =
        BootstrapBuilder::new().listener(presets::https_listener("https", downstream_tls, "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let response = client.get("/validated").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Validated chain OK!");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_full_tls_chain_different_certs_each_side() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_with_files(certs.beefcake_athlone_cert(), certs.beefcake_athlone_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Different certs OK!")).await;

    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let downstream_tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().sni("athlone.beefcake.example.com").validation_context(&ca_path));

    let bootstrap =
        BootstrapBuilder::new().listener(presets::https_listener("https", downstream_tls, "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let response = client.get("/different-certs").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Different certs OK!");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_full_tls_chain_post_with_body() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_status(StatusCode::CREATED)).await;

    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let downstream_tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().skip_server_verification("dublin.beefcake.example.com", &ca_path));

    let bootstrap =
        BootstrapBuilder::new().listener(presets::https_listener("https", downstream_tls, "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let body = r#"{"full_chain_test": true, "encrypted": "both_hops"}"#;
    let response = client.post("/api/data", body).await.expect("Failed to send request");

    response.assert_status(StatusCode::CREATED);

    let req = backend.await_request().await.expect("No request received");
    assert_eq!(req.path(), "/api/data");
    assert_eq!(req.body_str(), Some(body));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_full_tls_chain_multiple_requests() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("chain response")).await;

    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());
    let downstream_tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().skip_server_verification("dublin.beefcake.example.com", &ca_path));

    let bootstrap =
        BootstrapBuilder::new().listener(presets::https_listener("https", downstream_tls, "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    for i in 0..5 {
        let response = client.get(&format!("/request/{i}")).await.expect("Failed to send request");
        response.assert_status(StatusCode::OK);

        let req = backend.await_request().await.expect("No request received");
        assert_eq!(req.path(), format!("/request/{i}"));
    }

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_full_tls_chain_upstream_validation_failure() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let downstream_tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let wrong_ca_path = TestCerts::path_to_string(&certs.deadbeef_ca_chain());
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(UpstreamTlsBuilder::new().sni("dublin.beefcake.example.com").validation_context(&wrong_ca_path));

    let bootstrap =
        BootstrapBuilder::new().listener(presets::https_listener("https", downstream_tls, "backend")).cluster(cluster);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let response = client.get("/").await.expect("Request should complete");

    assert!(
        response.status == StatusCode::SERVICE_UNAVAILABLE || response.status == StatusCode::BAD_GATEWAY,
        "Expected error status due to upstream TLS failure, got {:?}",
        response.status
    );

    orion.shutdown();
    cleanup_config_file(&config_path);
}
