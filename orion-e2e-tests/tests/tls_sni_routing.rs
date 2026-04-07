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
    presets, BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestCerts, TlsTestClientBuilder,
};

fn sni_filter_chain(
    name: &str,
    server_names: &[&str],
    downstream_tls: DownstreamTlsBuilder,
    cluster_name: &str,
) -> FilterChainBuilder {
    FilterChainBuilder::new(name).server_names(server_names).downstream_tls(downstream_tls).hcm(
        HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes")
                .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route(cluster_name))),
        ),
    )
}

#[tokio::test]
#[ignore]
async fn test_sni_routing_two_domains() {
    let mut backend1 = TestBackend::start().await.expect("Failed to start backend 1");
    let mut backend2 = TestBackend::start().await.expect("Failed to start backend 2");
    backend1.set_default_response(PreConfiguredResponse::with_body("backend1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("backend2")).await;

    let certs = TestCerts::new();
    let dublin_cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let dublin_key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let athlone_cert_path = TestCerts::path_to_string(&certs.beefcake_athlone_cert());
    let athlone_key_path = TestCerts::path_to_string(&certs.beefcake_athlone_key());

    let tls_dublin = DownstreamTlsBuilder::new().cert_files(&dublin_cert_path, &dublin_key_path);
    let tls_athlone = DownstreamTlsBuilder::new().cert_files(&athlone_cert_path, &athlone_key_path);

    let listener = ListenerBuilder::new("https")
        .port(0)
        .with_tls_inspector()
        .filter_chain(sni_filter_chain("dublin", &["dublin.beefcake.example.com"], tls_dublin, "backend1"))
        .filter_chain(sni_filter_chain("athlone", &["athlone.beefcake.example.com"], tls_athlone, "backend2"));

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::with_endpoint("backend1", backend1.addr()))
        .cluster(ClusterBuilder::with_endpoint("backend2", backend2.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let addr = orion.listener_addr().unwrap();

    let client_dublin = TlsTestClientBuilder::new(addr)
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build client for dublin");

    let response = client_dublin.get("/dublin").await.expect("Failed to send dublin request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend1");

    let req = backend1.await_request().await.expect("No request to backend1");
    assert_eq!(req.path(), "/dublin");

    let client_athlone = TlsTestClientBuilder::new(addr)
        .server_name("athlone.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build client for athlone");

    let response = client_athlone.get("/athlone").await.expect("Failed to send athlone request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend2");

    let req = backend2.await_request().await.expect("No request to backend2");
    assert_eq!(req.path(), "/athlone");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_sni_routing_different_certs() {
    let mut backend = TestBackend::start().await.expect("Failed to start backend");
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let certs = TestCerts::new();

    let dublin_cert = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let dublin_key = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let tls_dublin = DownstreamTlsBuilder::new().cert_files(&dublin_cert, &dublin_key);

    let athlone_cert = TestCerts::path_to_string(&certs.beefcake_athlone_cert());
    let athlone_key = TestCerts::path_to_string(&certs.beefcake_athlone_key());
    let tls_athlone = DownstreamTlsBuilder::new().cert_files(&athlone_cert, &athlone_key);

    let listener = ListenerBuilder::new("https")
        .port(0)
        .with_tls_inspector()
        .filter_chain(sni_filter_chain("dublin", &["dublin.beefcake.example.com"], tls_dublin, "backend"))
        .filter_chain(sni_filter_chain("athlone", &["athlone.beefcake.example.com"], tls_athlone, "backend"));

    let bootstrap =
        BootstrapBuilder::new().listener(listener).cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let addr = orion.listener_addr().unwrap();

    let client_dublin = TlsTestClientBuilder::new(addr)
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build client for dublin");

    let response = client_dublin.get("/dublin").await.expect("Failed to send dublin request");
    response.assert_status(StatusCode::OK);
    let _ = backend.await_request().await;

    let client_athlone = TlsTestClientBuilder::new(addr)
        .server_name("athlone.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build client for athlone");

    let response = client_athlone.get("/athlone").await.expect("Failed to send athlone request");
    response.assert_status(StatusCode::OK);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_sni_routing_multiple_names_per_chain() {
    let mut backend = TestBackend::start().await.expect("Failed to start backend");
    backend.set_default_response(PreConfiguredResponse::with_body("multi-sni OK")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let listener = ListenerBuilder::new("https").port(0).with_tls_inspector().filter_chain(sni_filter_chain(
        "multi",
        &["www.beefcake.example.com", "api.beefcake.example.com", "app.beefcake.example.com"],
        tls,
        "backend",
    ));

    let bootstrap =
        BootstrapBuilder::new().listener(listener).cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let addr = orion.listener_addr().unwrap();

    for sni in &["www.beefcake.example.com", "api.beefcake.example.com", "app.beefcake.example.com"] {
        let client = TlsTestClientBuilder::new(addr)
            .server_name(*sni)
            .root_ca(certs.beefcake_ca_chain())
            .build()
            .expect("Failed to build client");

        let response = client.get("/test").await.expect("Failed to send request");
        response.assert_status(StatusCode::OK);
        response.assert_body("multi-sni OK");

        let _ = backend.await_request().await;
    }

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_sni_routing_default_chain() {
    let mut backend_specific = TestBackend::start().await.expect("Failed to start specific backend");
    let mut backend_default = TestBackend::start().await.expect("Failed to start default backend");
    backend_specific.set_default_response(PreConfiguredResponse::with_body("specific")).await;
    backend_default.set_default_response(PreConfiguredResponse::with_body("default")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls_specific = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);
    let tls_default = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let listener = ListenerBuilder::new("https")
        .port(0)
        .with_tls_inspector()
        .filter_chain(sni_filter_chain("specific", &["known.beefcake.example.com"], tls_specific, "backend_specific"))
        .filter_chain(
            FilterChainBuilder::new("default").downstream_tls(tls_default).hcm(
                HcmBuilder::new().route_config(
                    RouteConfigBuilder::new("routes").virtual_host(
                        VirtualHostBuilder::new("default").route(presets::default_route("backend_default")),
                    ),
                ),
            ),
        );

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::with_endpoint("backend_specific", backend_specific.addr()))
        .cluster(ClusterBuilder::with_endpoint("backend_default", backend_default.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let addr = orion.listener_addr().unwrap();

    let client_known = TlsTestClientBuilder::new(addr)
        .server_name("known.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build client for known");

    let response = client_known.get("/known").await.expect("Failed to send known request");
    response.assert_status(StatusCode::OK);
    response.assert_body("specific");
    let _ = backend_specific.await_request().await;

    let client_unknown = TlsTestClientBuilder::new(addr)
        .server_name("unknown.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build client for unknown");

    let response = client_unknown.get("/unknown").await.expect("Failed to send unknown request");
    response.assert_status(StatusCode::OK);
    response.assert_body("default");
    let _ = backend_default.await_request().await;

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_sni_routing_post_with_body() {
    let mut backend1 = TestBackend::start().await.expect("Failed to start backend 1");
    let mut backend2 = TestBackend::start().await.expect("Failed to start backend 2");
    backend1.set_default_response(PreConfiguredResponse::with_status(StatusCode::CREATED)).await;
    backend2.set_default_response(PreConfiguredResponse::with_status(StatusCode::ACCEPTED)).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls1 = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);
    let tls2 = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let listener = ListenerBuilder::new("https")
        .port(0)
        .with_tls_inspector()
        .filter_chain(sni_filter_chain("service1", &["service1.beefcake.example.com"], tls1, "backend1"))
        .filter_chain(sni_filter_chain("service2", &["service2.beefcake.example.com"], tls2, "backend2"));

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::with_endpoint("backend1", backend1.addr()))
        .cluster(ClusterBuilder::with_endpoint("backend2", backend2.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let addr = orion.listener_addr().unwrap();

    let client1 = TlsTestClientBuilder::new(addr)
        .server_name("service1.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build client 1");

    let body1 = r#"{"service": 1}"#;
    let response = client1.post("/api", body1).await.expect("Failed to send to service1");
    response.assert_status(StatusCode::CREATED);

    let req1 = backend1.await_request().await.expect("No request to backend1");
    assert_eq!(req1.body_str(), Some(body1));

    let client2 = TlsTestClientBuilder::new(addr)
        .server_name("service2.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build client 2");

    let body2 = r#"{"service": 2}"#;
    let response = client2.post("/api", body2).await.expect("Failed to send to service2");
    response.assert_status(StatusCode::ACCEPTED);

    let req2 = backend2.await_request().await.expect("No request to backend2");
    assert_eq!(req2.body_str(), Some(body2));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_sni_routing_mismatched_sni() {
    let mut backend_specific = TestBackend::start().await.expect("Failed to start specific backend");
    let mut backend_default = TestBackend::start().await.expect("Failed to start default backend");
    backend_specific.set_default_response(PreConfiguredResponse::with_body("specific")).await;
    backend_default.set_default_response(PreConfiguredResponse::with_body("default")).await;

    let certs = TestCerts::new();
    let specific_cert = TestCerts::path_to_string(&certs.sni_test_specific_cert());
    let specific_key = TestCerts::path_to_string(&certs.sni_test_specific_key());
    let default_cert = TestCerts::path_to_string(&certs.sni_test_default_cert());
    let default_key = TestCerts::path_to_string(&certs.sni_test_default_key());

    let tls_specific = DownstreamTlsBuilder::new().cert_files(&specific_cert, &specific_key);
    let tls_default = DownstreamTlsBuilder::new().cert_files(&default_cert, &default_key);

    let listener =
        ListenerBuilder::new("https")
            .port(0)
            .with_tls_inspector()
            .filter_chain(sni_filter_chain("specific", &["specific.example.com"], tls_specific, "backend_specific"))
            .filter_chain(FilterChainBuilder::new("default").downstream_tls(tls_default).hcm(
                HcmBuilder::new().route_config(
                    RouteConfigBuilder::new("routes").virtual_host(
                        VirtualHostBuilder::new("default").route(presets::default_route("backend_default")),
                    ),
                ),
            ));

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::with_endpoint("backend_specific", backend_specific.addr()))
        .cluster(ClusterBuilder::with_endpoint("backend_default", backend_default.addr()));

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let addr = orion.listener_addr().unwrap();

    // Test 1: Matching SNI should work and route to specific backend
    let client_matching = TlsTestClientBuilder::new(addr)
        .server_name("specific.example.com")
        .root_ca(certs.sni_test_ca())
        .build()
        .expect("Failed to build client for matching SNI");

    let response = client_matching.get("/test").await.expect("Failed to send matching SNI request");
    response.assert_status(StatusCode::OK);
    response.assert_body("specific");
    let _ = backend_specific.await_request().await;

    // Test 2: Non-matching SNI should fall back to default chain
    // Client sends SNI "unknown.example.com" which doesn't match any filter chain
    // Filter chain selection falls back to the default chain
    // Default chain presents its certificate (for "default.example.com")
    // Connection succeeds (client skips verification since cert won't match SNI)
    let client_mismatched = TlsTestClientBuilder::new(addr)
        .server_name("unknown.example.com")
        .skip_verification() // Server presents cert for default.example.com, but we sent SNI for unknown
        .build()
        .expect("Failed to build client for mismatched SNI");

    let response = client_mismatched.get("/test").await.expect("Failed to send mismatched SNI request");
    response.assert_status(StatusCode::OK);
    response.assert_body("default");
    let _ = backend_default.await_request().await;

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
