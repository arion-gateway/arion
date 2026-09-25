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

use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestCerts,
    TlsTestClientBuilder,
};

#[tokio::test]
#[ignore]
async fn test_mtls_downstream_valid_client_cert() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("mTLS OK!")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let downstream_tls = DownstreamTlsBuilder::new()
        .cert_files(&cert_path, &key_path)
        .require_client_cert()
        .validation_context(&ca_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", downstream_tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .client_cert(certs.beefcake_athlone_cert(), certs.beefcake_athlone_key())
        .build()
        .expect("Failed to build TLS client");

    let response = client.get("/mtls").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("mTLS OK!");

    let captured_request = backend.await_request().await.expect("No request received by backend");
    assert_eq!(captured_request.path(), "/mtls");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mtls_downstream_no_client_cert_rejected() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let downstream_tls = DownstreamTlsBuilder::new()
        .cert_files(&cert_path, &key_path)
        .require_client_cert()
        .validation_context(&ca_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", downstream_tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let result = client.get("/").await;
    assert!(result.is_err(), "Expected TLS handshake to fail without client cert");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mtls_downstream_wrong_ca_rejected() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let downstream_tls = DownstreamTlsBuilder::new()
        .cert_files(&cert_path, &key_path)
        .require_client_cert()
        .validation_context(&ca_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", downstream_tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .client_cert(certs.deadbeef_dublin_cert(), certs.deadbeef_dublin_key())
        .build()
        .expect("Failed to build TLS client");

    let result = client.get("/").await;
    assert!(result.is_err(), "Expected TLS handshake to fail with wrong client CA");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mtls_downstream_multiple_requests() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("response")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let downstream_tls = DownstreamTlsBuilder::new()
        .cert_files(&cert_path, &key_path)
        .require_client_cert()
        .validation_context(&ca_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", downstream_tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .client_cert(certs.beefcake_athlone_cert(), certs.beefcake_athlone_key())
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
async fn test_mtls_downstream_post_with_body() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_status(StatusCode::CREATED)).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let downstream_tls = DownstreamTlsBuilder::new()
        .cert_files(&cert_path, &key_path)
        .require_client_cert()
        .validation_context(&ca_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", downstream_tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .client_cert(certs.beefcake_athlone_cert(), certs.beefcake_athlone_key())
        .build()
        .expect("Failed to build TLS client");

    let body = r#"{"mtls": true, "client_authenticated": true}"#;
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
async fn test_mtls_downstream_different_valid_certs() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("multi-cert OK")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let ca_path = TestCerts::path_to_string(&certs.beefcake_ca_chain());

    let downstream_tls = DownstreamTlsBuilder::new()
        .cert_files(&cert_path, &key_path)
        .require_client_cert()
        .validation_context(&ca_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", downstream_tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client1 = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .client_cert(certs.beefcake_athlone_cert(), certs.beefcake_athlone_key())
        .build()
        .expect("Failed to build TLS client 1");

    let response = client1.get("/from-athlone").await.expect("Failed to send request");
    response.assert_status(StatusCode::OK);
    let _ = backend.await_request().await.expect("No request received");

    let client2 = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .client_cert(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .build()
        .expect("Failed to build TLS client 2");

    let response = client2.get("/from-dublin").await.expect("Failed to send request");
    response.assert_status(StatusCode::OK);
    let _ = backend.await_request().await.expect("No request received");

    orion.shutdown();
    cleanup_config_file(&config_path);
}
