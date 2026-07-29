#![allow(clippy::expect_used, reason = "test infrastructure — panicking on setup failure is intentional")]

use std::path::PathBuf;

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder,
    RouteBuilder, RouteConfigBuilder, VirtualHostBuilder, WasmBuilder,
};
use orion_e2e_tests::{
    OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestCerts, TestClient,
    TlsTestClientBuilder,
};

async fn setup_wasm(wasm_builder: WasmBuilder) -> (TestBackend, OrionInstance, TestClient, PathBuf) {
    let backend = TestBackend::start().await.expect("backend start");
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().http1().wasm(wasm_builder).route_config(RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            )),
        )))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("build config");
    let orion =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn orion");
    #[allow(clippy::unwrap_used)]
    let client = TestClient::new(orion.listener_addr().unwrap());

    (backend, orion, client, config_path)
}

fn get_wasm_path(filter_name: &str) -> String {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set");
    let mut path = PathBuf::from(manifest_dir);
    path.pop(); // Go to workspace root
    path.push("orion-wasm-sdk");
    path.push("target");
    path.push("wasm32-unknown-unknown");
    path.push("debug");
    path.push(format!("{filter_name}.wasm"));
    path.to_string_lossy().into_owned()
}

fn dummy_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("dummy_filter")
        .root_id("dummy_root_id")
        .vm_id("dummy_vm_id")
        .code_filename(get_wasm_path("dummy_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_dummy_filter_missing_authorization() {
    let (_backend, _orion, client, _cfg) = setup_wasm(dummy_filter_builder()).await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::UNAUTHORIZED);
    response.assert_body("401 Unauthorized: missing Authorization header");
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_dummy_filter_invalid_authorization() {
    let (_backend, _orion, client, _cfg) = setup_wasm(dummy_filter_builder()).await;

    let response =
        client.send(RequestBuilder::get("/test").header("Authorization", "Bearer invalid")).await.expect("request");
    response.assert_status(StatusCode::UNAUTHORIZED);
    response.assert_body("401 Unauthorized: invalid credentials");
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_dummy_filter_authorized() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(dummy_filter_builder()).await;

    let response = client
        .send(RequestBuilder::get("/test").header("Authorization", "Bearer secret-token"))
        .await
        .expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.path(), "/test");
}

fn header_mutations_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("header_mutations_filter")
        .root_id("header_mutations_root_id")
        .vm_id("header_mutations_vm_id")
        .code_filename(get_wasm_path("header_mutations_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_header_mutations_filter() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(header_mutations_filter_builder()).await;

    backend
        .set_default_response(
            PreConfiguredResponse::default()
                .header("x-response-add", "initial-res-value")
                .header("x-response-remove", "to-be-removed")
                .header("x-response-set", "will-be-replaced"),
        )
        .await;

    let response = client
        .send(RequestBuilder::get("/test").header("user-agent", "my-agent").header("x-custom-add", "initial-value"))
        .await
        .expect("request");

    response.assert_status(StatusCode::OK);
    response.assert_header("x-response-set", "res-replaced-value");

    let added_headers: Vec<_> = response.header_all("x-response-add");
    assert_eq!(added_headers, vec!["initial-res-value", "res-add-value"]);

    assert_eq!(response.header("x-response-remove"), None);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.header("x-custom-set"), Some("replaced-value"));
    assert_eq!(captured.header_all("x-custom-add"), vec!["initial-value", "add-value"]);
    assert_eq!(captured.header("user-agent"), None);
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_dummy_filter_buffer_body_valid() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(dummy_filter_builder()).await;

    let response = client
        .send(RequestBuilder::post("/test").header("Authorization", "Bearer buffer-me").body("valid"))
        .await
        .expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("valid"));
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_dummy_filter_buffer_body_invalid() {
    let (_backend, _orion, client, _cfg) = setup_wasm(dummy_filter_builder()).await;

    let response = client
        .send(RequestBuilder::post("/test").header("Authorization", "Bearer buffer-me").body("invalid-body"))
        .await
        .expect("request");
    response.assert_status(StatusCode::FORBIDDEN);
    response.assert_body("403 Forbidden: body did not contain the magic word 'valid'");
}

fn header_api_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("header_api_filter")
        .root_id("header_api_root_id")
        .vm_id("header_api_vm_id")
        .code_filename(get_wasm_path("header_api_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_header_api_filter() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(header_api_filter_builder()).await;

    backend
        .set_default_response(
            PreConfiguredResponse::default()
                .header("x-response-add", "initial-res-value")
                .header("x-response-remove", "to-be-removed")
                .header("x-response-set", "will-be-replaced"),
        )
        .await;

    let response = client
        .send(RequestBuilder::get("/test").header("user-agent", "my-agent").header("x-custom-add", "initial-value"))
        .await
        .expect("request");

    response.assert_status(StatusCode::OK);
    // 1. set_header
    // wait, replace_header in our plugin uses x-response-set.
    // The plugin does:
    // ctx.set_header("x-response-set", "res-set-value")
    // ctx.replace_header("x-response-set", "res-replaced-value")
    response.assert_header("x-response-set", "res-replaced-value");

    // 2. add_header
    assert_eq!(response.header_all("x-response-add"), vec!["initial-res-value", "res-add-value"]);

    // 3. remove_header
    assert_eq!(response.header("x-response-remove"), None);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.header("x-custom-set"), Some("replaced-value"));
    assert_eq!(captured.header_all("x-custom-add"), vec!["initial-value", "add-value"]);
    assert_eq!(captured.header("user-agent"), None);
}

fn headers_map_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("headers_map_filter")
        .root_id("headers_map_root_id")
        .vm_id("headers_map_vm_id")
        .code_filename(get_wasm_path("headers_map_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_headers_map_filter() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(headers_map_filter_builder()).await;

    backend
        .set_default_response(
            PreConfiguredResponse::default()
                .header("x-response-add", "initial-res-value")
                .header("x-response-remove", "to-be-removed")
                .header("x-response-set", "will-be-replaced"),
        )
        .await;

    let response = client
        .send(RequestBuilder::get("/test").header("user-agent", "my-agent").header("x-custom-add", "initial-value"))
        .await
        .expect("request");

    response.assert_status(StatusCode::OK);
    response.assert_header("x-response-set", "res-replaced-value");

    let added_headers: Vec<_> = response.header_all("x-response-add");
    assert_eq!(added_headers, vec!["initial-res-value", "res-add-value"]);

    assert_eq!(response.header("x-response-remove"), None);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.header("x-custom-set"), Some("replaced-value"));
    assert_eq!(captured.header_all("x-custom-add"), vec!["initial-value", "add-value"]);
    assert_eq!(captured.header("user-agent"), None);
}

fn body_mutation_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("body_mutation_filter")
        .root_id("body_mutation_root_id")
        .vm_id("body_mutation_vm_id")
        .code_filename(get_wasm_path("body_mutation_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_body_mutation_filter() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(body_mutation_filter_builder()).await;
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    // Test Append on both
    let response = client
        .send(
            RequestBuilder::post("/test")
                .header("x-req-mutation", "append")
                .header("x-res-mutation", "append")
                .body("client request"),
        )
        .await
        .expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend response [appended]");
    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("client request [appended]"));

    // Test Prepend on both
    let response = client
        .send(
            RequestBuilder::post("/test")
                .header("x-req-mutation", "prepend")
                .header("x-res-mutation", "prepend")
                .body("client request"),
        )
        .await
        .expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("[prepended] backend response");
    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("[prepended] client request"));

    // Test Replace on both
    let response = client
        .send(
            RequestBuilder::post("/test")
                .header("x-req-mutation", "replace")
                .header("x-res-mutation", "replace")
                .body("client request"),
        )
        .await
        .expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("[replaced]");
    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("[replaced]"));
}

fn callout_body_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("callout_body_filter")
        .root_id("callout_body_root_id")
        .vm_id("callout_body_vm_id")
        .code_filename(get_wasm_path("callout_body_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_callout_body_filter() {
    let mut backend = TestBackend::start().await.expect("backend start");
    let mut callout_backend = TestBackend::start().await.expect("callout start");

    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;
    callout_backend.set_default_response(PreConfiguredResponse::with_body("callout response")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().http1().wasm(callout_body_filter_builder()).route_config(
                RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                ),
            ),
        )))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()))
        .cluster(ClusterBuilder::with_endpoint("service", callout_backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("build config");
    let orion_instance =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn orion");
    #[allow(clippy::unwrap_used)]
    let client = TestClient::new(orion_instance.listener_addr().unwrap());

    let response = client.send(RequestBuilder::post("/test").body("client request")).await.expect("request");

    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured_callout = callout_backend.await_request().await.expect("callout request");
    assert_eq!(captured_callout.body_str(), Some("client request"));
    assert_eq!(captured_callout.header("x-callout-id"), Some("wasm-plugin-123"));

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("callout response"));

    // ── Test Failure Scenario (Callout returns 500) ──
    // In this case, the filter should keep the original body
    callout_backend.set_default_response(PreConfiguredResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR)).await;

    let response = client.send(RequestBuilder::post("/test").body("original client request")).await.expect("request");

    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured_callout = callout_backend.await_request().await.expect("callout request");
    assert_eq!(captured_callout.body_str(), Some("original client request"));

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("original client request"));
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_metadata_filter() {
    let builder = WasmBuilder::new()
        .name("metadata_filter")
        .root_id("metadata_root_id")
        .vm_id("metadata_vm_id")
        .code_filename(get_wasm_path("metadata_filter"));

    let (mut backend, _orion, client, _cfg) = setup_wasm(builder).await;
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    let response = client.send(RequestBuilder::get("/test")).await.expect("request");

    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured = backend.await_request().await.expect("backend request");

    // Test that the wasm filter successfully injected the metadata into headers
    assert_eq!(captured.header("x-listener-name"), Some("http"));
    // Connection metadata was split into peer and local
    assert!(captured.header("x-connection-peer").is_some());
    assert!(captured.header("x-connection-local").is_some());
    // SNI is None in our plaintext HTTP E2E tests, so x-sni should not be set
    assert_eq!(captured.header("x-sni"), None);
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_metadata_filter_sni() {
    let mut backend = TestBackend::start().await.expect("Failed to start backend");
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let wasm_builder = WasmBuilder::new()
        .name("metadata_filter")
        .root_id("metadata_root_id")
        .vm_id("metadata_vm_id")
        .code_filename(get_wasm_path("metadata_filter"));

    let listener = ListenerBuilder::new("https").port(0).with_tls_inspector().filter_chain(
        FilterChainBuilder::new("main").downstream_tls(tls).hcm(
            HcmBuilder::new().http1().wasm(wasm_builder).route_config(RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            )),
        ),
    );

    let bootstrap =
        BootstrapBuilder::new().listener(listener).cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TlsTestClientBuilder::new(orion.listener_addr().unwrap())
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build client");

    let response = client.get("/test").await.expect("Failed to send request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured = backend.await_request().await.expect("backend request");

    assert_eq!(captured.header("x-listener-name"), Some("https"));
    assert_eq!(captured.header("x-sni"), Some("dublin.beefcake.example.com"));
    assert!(captured.header("x-connection-peer").is_some());
    assert!(captured.header("x-connection-local").is_some());
}
fn config_logger_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("config_logger_filter")
        .root_id("config_logger_root_id")
        .vm_id("config_logger_vm_id")
        .configuration("my-custom-plugin-config-12345")
        .code_filename(get_wasm_path("config_logger_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_config_logger_filter() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(config_logger_filter_builder()).await;
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    let response = client.send(RequestBuilder::get("/test")).await.expect("request");

    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.header("x-wasm-config"), Some("my-custom-plugin-config-12345"));
}
#[tokio::test]
#[test_log::test]
async fn test_wasm_sleep_timeout_filter() {
    let builder = WasmBuilder::new()
        .name("sleep_timeout_filter")
        .root_id("sleep_timeout_root_id")
        .vm_id("sleep_timeout_vm_id")
        .code_filename(get_wasm_path("sleep_timeout_filter"));

    let (mut backend, _orion, client, _cfg) = setup_wasm(builder).await;
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    let start = std::time::Instant::now();
    let response = client.send(RequestBuilder::get("/test")).await.expect("request");
    let elapsed = start.elapsed();

    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured = backend.await_request().await.expect("backend request");

    assert_eq!(captured.header("x-timeout-test"), Some("passed"));

    // The filter sleeps for 1 second, sets 1 second IO timeout, then tries to sleep 10 seconds.
    // It should timeout after 1 second, so the total time should be roughly 2 seconds.
    // Allow some buffer for execution overhead (between 1.5s and 4.0s)
    assert!(elapsed.as_secs_f64() > 1.5, "Elapsed time too short: {elapsed:?}");
    assert!(elapsed.as_secs_f64() < 4.0, "Elapsed time too long: {elapsed:?}");
}
#[tokio::test]
#[test_log::test]
async fn test_wasm_access_log_operator_filter() {
    let backend = TestBackend::start().await.expect("backend start");
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;
    let backend_addr = backend.addr();

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-wasm-{}.txt", std::process::id()));

    // We want to test that our Wasm filter injects these custom operators
    let log_format = "op1=%op_1%||op2=%op_2%||op3=%op_3%||op4=%op_4%\n";

    let builder = WasmBuilder::new()
        .name("access_log_operator_filter")
        .root_id("access_log_root")
        .vm_id("access_log_vm")
        .code_filename(get_wasm_path("access_log_operator_filter"));

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new()
                        .http1()
                        .access_log_file(log_path.to_str().unwrap(), log_format)
                        .wasm(builder)
                        .route_config(
                            RouteConfigBuilder::new("routes").virtual_host(
                                VirtualHostBuilder::new("default")
                                    .route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                            ),
                        ),
                ),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr))
        .access_log(orion_configuration::config::log::AccessLogConfig {
            blocking: true,
            custom_operators: vec![
                smol_str::SmolStr::new("op_1"),
                smol_str::SmolStr::new("op_2"),
                smol_str::SmolStr::new("op_3"),
                smol_str::SmolStr::new("op_4"),
            ],
            ..orion_configuration::config::log::AccessLogConfig::default()
        });

    let config_path = bootstrap.build_to_temp().expect("build config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default().with_verbose())
        .await
        .expect("spawn orion");
    let listener_addr = orion.listener_addr().unwrap();
    let raw_req = orion_e2e_tests::RawHttpRequestBuilder::new()
        .method("GET")
        .uri("/")
        .host("localhost")
        .header("Connection", "close")
        .build();

    let mut stream = tokio::net::TcpStream::connect(listener_addr).await.expect("Failed to connect");
    tokio::io::AsyncWriteExt::write_all(&mut stream, &raw_req).await.expect("Failed to write");
    let mut resp = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut resp).await.expect("Failed to read");

    // Give it a moment to flush the access log
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    orion.shutdown();

    let mut content = String::new();
    for _ in 0..20 {
        if let Ok(c) = std::fs::read_to_string(&log_path) {
            if !c.trim().is_empty() {
                content = c;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(!content.is_empty(), "Failed to read log file or it was empty");

    // The filter sets:
    // op_1: "\"value_from_request_phase\""
    // op_2: "42"
    // op_3: "{\"nested\": true}"
    // op_4: "\"value_from_response_phase\""
    assert!(content.contains("op1=\"value_from_request_phase\""));
    assert!(content.contains("op2=42"));
    assert!(content.contains("op3={\"nested\": true}"));
    assert!(content.contains("op4=\"value_from_response_phase\""));

    _ = std::fs::remove_file(log_path);
}

// Helper for metrics custom filter tests
use http::HeaderName;
use orion_configuration::config::metrics::{CustomMetric, CustomMetrics, MetricsConfig};
use orion_e2e_tests::PortBlock;

fn custom_metric_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("custom_metric_filter")
        .root_id("custom_metric_root_id")
        .vm_id("custom_metric_vm_id")
        .code_filename(get_wasm_path("custom_metric_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_custom_metric_filter() {
    let backend = TestBackend::start().await.expect("backend start");
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;
    let backend_addr = backend.addr();

    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = std::net::SocketAddr::from(([127, 0, 0, 1], admin_port));

    // Wasm custom metrics config uses `header_name` to match the keys from Wasm SDK
    let custom_metrics = CustomMetrics {
        wasm: vec![
            CustomMetric::Counter {
                name: "custom_wasm_counter".into(),
                description: "A custom metric from Wasm".into(),
                header_name: HeaderName::from_static("custom"),
                attribute_name: Some("attr_custom".into()),
            },
            CustomMetric::Counter {
                name: "custom_wasm_user_tier".into(),
                description: "A custom metric from Wasm for user tier".into(),
                header_name: HeaderName::from_static("user_tier"),
                attribute_name: Some("tier".into()),
            },
            CustomMetric::Counter {
                name: "custom_wasm_datacenter".into(),
                description: "A custom metric from Wasm for datacenter".into(),
                header_name: HeaderName::from_static("datacenter"),
                attribute_name: Some("dc".into()),
            },
        ],
        ..CustomMetrics::default()
    };

    let metrics_config = MetricsConfig {
        user_key: None,
        custom_keys: smallvec::smallvec![],
        rename: std::collections::HashMap::new(),
        custom_metrics,
    };

    let bootstrap = BootstrapBuilder::new()
        .admin("127.0.0.1", admin_port)
        .metrics(metrics_config)
        .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().http1().wasm(custom_metric_filter_builder()).route_config(
                RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                ),
            ),
        )))
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr));

    let config_path = bootstrap.build_to_temp().expect("build config");
    let orion_instance =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn orion");
    let client = TestClient::new(orion_instance.listener_addr().unwrap());
    let admin_client = TestClient::new(admin_addr);

    let response = client.send(RequestBuilder::get("/test")).await.expect("request");
    response.assert_status(StatusCode::OK);

    // Give it a moment to flush metrics
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    assert!(metrics.contains("custom_custom_wasm_counter{attr_custom=\"metric\"} 1"));
    assert!(metrics.contains("custom_custom_wasm_user_tier{tier=\"premium\"} 1"));
    assert!(metrics.contains("custom_custom_wasm_datacenter{dc=\"eu-west-1\"} 1"));
}

fn shared_atomic_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("shared_atomic")
        .root_id("shared_atomic_root_id")
        .vm_id("shared_atomic_vm_id")
        .code_filename(get_wasm_path("shared_atomic"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_shared_atomic_filter() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(shared_atomic_filter_builder()).await;

    // First request
    let response1 = client.send(RequestBuilder::get("/test1")).await.expect("request 1");
    response1.assert_status(StatusCode::OK);
    let captured1 = backend.await_request().await.expect("backend request 1");
    assert_eq!(captured1.header("x-request-counter"), Some("1"));

    // Second request
    let response2 = client.send(RequestBuilder::get("/test2")).await.expect("request 2");
    response2.assert_status(StatusCode::OK);
    let captured2 = backend.await_request().await.expect("backend request 2");
    assert_eq!(captured2.header("x-request-counter"), Some("2"));
}

fn shared_blob_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("shared_blob")
        .root_id("shared_blob_root_id")
        .vm_id("shared_blob_vm_id")
        .code_filename(get_wasm_path("shared_blob"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_shared_blob_filter() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(shared_blob_filter_builder()).await;

    // Send first request with Bob
    let response1 = client.send(RequestBuilder::get("/test1").header("x-client-id", "Bob")).await.expect("request 1");
    response1.assert_status(StatusCode::OK);
    let captured1 = backend.await_request().await.expect("backend request 1");
    assert_eq!(captured1.header("x-seen-clients"), Some("Bob"));

    // Send second request with Alice
    let response2 = client.send(RequestBuilder::get("/test2").header("x-client-id", "Alice")).await.expect("request 2");
    response2.assert_status(StatusCode::OK);
    let captured2 = backend.await_request().await.expect("backend request 2");
    assert_eq!(captured2.header("x-seen-clients"), Some("Bob, Alice"));

    // Send third request with Bob again
    let response3 = client.send(RequestBuilder::get("/test3").header("x-client-id", "Bob")).await.expect("request 3");
    response3.assert_status(StatusCode::OK);
    let captured3 = backend.await_request().await.expect("backend request 3");
    assert_eq!(captured3.header("x-seen-clients"), Some("Bob, Alice"));
}

fn grpc_callout_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("grpc_callout_filter")
        .root_id("grpc_callout_root_id")
        .vm_id("grpc_callout_vm_id")
        .code_filename(get_wasm_path("grpc_callout_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_grpc_callout_filter() {
    let backend = TestBackend::start().await.expect("backend start");
    let callout_backend = TestBackend::start_h2().await.expect("callout start");

    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    // The Wasm module sends an EchoRequest with message="Hello from Wasm!"
    // The callout_backend needs to return a valid gRPC response.
    // 1 byte compressed flag (0), 4 bytes length, then protobuf encoded EchoResponse.
    // Let's craft it manually since TestBackend is just an HTTP server:
    // EchoResponse { message: "Hello from Host!", backend_id: "test" }
    // Wire format for string: Tag 1 = 10, length 16, "Hello from Host!"
    // Tag 2 = 18, length 4, "test"
    let mut proto_payload = Vec::new();
    proto_payload.extend_from_slice(&[10, 16]);
    proto_payload.extend_from_slice(b"Hello from Host!");
    proto_payload.extend_from_slice(&[18, 4]);
    proto_payload.extend_from_slice(b"test");

    let mut grpc_frame = Vec::new();
    grpc_frame.push(0); // uncompressed

    #[allow(clippy::cast_possible_truncation)]
    grpc_frame.extend_from_slice(&(proto_payload.len() as u32).to_be_bytes());
    grpc_frame.extend_from_slice(&proto_payload);

    callout_backend
        .set_default_response(
            PreConfiguredResponse::with_status(StatusCode::OK)
                .header("content-type", "application/grpc")
                .header("grpc-status", "0")
                .body(grpc_frame),
        )
        .await;

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().http1().wasm(grpc_callout_filter_builder()).route_config(
                RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                ),
            ),
        )))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()))
        // The Wasm plugin sends gRPC to "service" cluster
        .cluster(orion_e2e_tests::config_builder::presets::ext_proc_cluster("service", callout_backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("build config");
    let orion_instance =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn orion");
    let client = TestClient::new(orion_instance.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test")).await.expect("request");

    // The plugin replaces the response body with "grpc callout success: Hello from Host!"
    response.assert_status(StatusCode::OK);
    response.assert_body("grpc callout success: Hello from Host!");
}

fn direct_response_filter_builder() -> WasmBuilder {
    WasmBuilder::new()
        .name("direct_response_filter")
        .root_id("direct_response_root_id")
        .vm_id("direct_response_vm_id")
        .code_filename(get_wasm_path("direct_response_filter"))
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_direct_response_filter() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(direct_response_filter_builder()).await;
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    // Normal request should pass through
    let response = client.send(RequestBuilder::get("/test")).await.expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    // Request with x-trigger-direct should be intercepted and return the DirectResponse
    let response = client.send(RequestBuilder::get("/test").header("x-trigger-direct", "1")).await.expect("request");

    response.assert_status(StatusCode::FORBIDDEN);
    response.assert_header("x-custom-response-header", "was-intercepted");
    response.assert_body("Intercepted by Wasm Direct Response!");
}
