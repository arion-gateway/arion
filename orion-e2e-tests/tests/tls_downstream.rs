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
use orion_e2e_tests::config_builder::{presets, BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder, TlsVersion};
use orion_e2e_tests::{
    OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestCerts, TlsTestClientBuilder,
};

#[tokio::test]
#[ignore]
async fn test_downstream_tls_termination_file_cert() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend!")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let response = client.get("/hello").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Hello from backend!");

    let captured_request = backend.await_request().await.expect("No request received by backend");
    assert_eq!(captured_request.path(), "/hello");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_downstream_tls_1_2_only() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("TLS 1.2 OK")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path).tls_1_2_only();

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
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

    let response = client.get("/").await.expect("Failed to send request");
    response.assert_status(StatusCode::OK);
    response.assert_body("TLS 1.2 OK");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_downstream_tls_1_3_only() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("TLS 1.3 OK")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path).tls_1_3_only();

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
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

    let response = client.get("/").await.expect("Failed to send request");
    response.assert_status(StatusCode::OK);
    response.assert_body("TLS 1.3 OK");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_downstream_tls_wrong_ca_fails() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.deadbeef_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let result = client.get("/").await;
    assert!(result.is_err(), "Expected TLS handshake to fail with wrong CA");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_downstream_tls_multiple_requests() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("response")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
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
async fn test_downstream_tls_post_with_body() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_status(StatusCode::CREATED)).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
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

    let body = r#"{"name": "test", "value": 42}"#;
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
async fn test_downstream_tls_version_range() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("version range OK")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new()
        .cert_files(&cert_path, &key_path)
        .tls_minimum_version(TlsVersion::Tls1_2)
        .tls_maximum_version(TlsVersion::Tls1_3);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
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

    let response = client.get("/").await.expect("Failed to send request");
    response.assert_status(StatusCode::OK);
    response.assert_body("version range OK");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_downstream_tls_skip_verification_client() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("insecure OK")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .skip_verification()
        .build()
        .expect("Failed to build TLS client");

    let response = client.get("/").await.expect("Failed to send request");
    response.assert_status(StatusCode::OK);
    response.assert_body("insecure OK");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_downstream_tls_1_2_only_rejects_1_3_client() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path).tls_1_2_only();

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .tls_1_3_only()
        .build()
        .expect("Failed to build TLS client");

    let result = client.get("/").await;
    assert!(result.is_err(), "Expected TLS handshake to fail due to version mismatch");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_downstream_tls_1_3_only_rejects_1_2_client() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path).tls_1_3_only();

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .tls_1_2_only()
        .build()
        .expect("Failed to build TLS client");

    let result = client.get("/").await;
    assert!(result.is_err(), "Expected TLS handshake to fail due to version mismatch");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_downstream_tls_sni_mismatch() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Should not reach")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("wrong.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let result = client.get("/").await;
    assert!(result.is_err(), "Expected TLS handshake to fail due to SNI/hostname mismatch");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
