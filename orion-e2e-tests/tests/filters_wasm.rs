#![allow(clippy::expect_used, reason = "test infrastructure — panicking on setup failure is intentional")]

use std::path::PathBuf;

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder, WasmBuilder, DownstreamTlsBuilder
};
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient, TestCerts, TlsTestClientBuilder};

async fn setup_wasm(wasm_builder: WasmBuilder) -> (TestBackend, OrionInstance, TestClient, PathBuf) {
    let backend = TestBackend::start().await.expect("backend start");
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().http1().wasm(wasm_builder).route_config(
                RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                ),
            ),
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
    path.push(format!("{}.wasm", filter_name));
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

    let response = client.send(RequestBuilder::get("/test").header("Authorization", "Bearer invalid")).await.expect("request");
    response.assert_status(StatusCode::UNAUTHORIZED);
    response.assert_body("401 Unauthorized: invalid credentials");
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_dummy_filter_authorized() {
    let (mut backend, _orion, client, _cfg) = setup_wasm(dummy_filter_builder()).await;

    let response = client.send(RequestBuilder::get("/test").header("Authorization", "Bearer secret-token")).await.expect("request");
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

    backend.set_default_response(
        PreConfiguredResponse::default()
            .header("x-response-add", "initial-res-value")
            .header("x-response-remove", "to-be-removed")
            .header("x-response-set", "will-be-replaced")
    ).await;

    let response = client
        .send(
            RequestBuilder::get("/test")
                .header("user-agent", "my-agent")
                .header("x-custom-add", "initial-value")
        )
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

    let response = client.send(RequestBuilder::post("/test").header("Authorization", "Bearer buffer-me").body("valid")).await.expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("valid"));
}

#[tokio::test]
#[test_log::test]
async fn test_wasm_dummy_filter_buffer_body_invalid() {
    let (_backend, _orion, client, _cfg) = setup_wasm(dummy_filter_builder()).await;

    let response = client.send(RequestBuilder::post("/test").header("Authorization", "Bearer buffer-me").body("invalid-body")).await.expect("request");
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

    backend.set_default_response(
        PreConfiguredResponse::default()
            .header("x-response-add", "initial-res-value")
            .header("x-response-remove", "to-be-removed")
            .header("x-response-set", "will-be-replaced")
    ).await;

    let response = client
        .send(
            RequestBuilder::get("/test")
                .header("user-agent", "my-agent")
                .header("x-custom-add", "initial-value")
        )
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

    backend.set_default_response(
        PreConfiguredResponse::default()
            .header("x-response-add", "initial-res-value")
            .header("x-response-remove", "to-be-removed")
            .header("x-response-set", "will-be-replaced")
    ).await;

    let response = client
        .send(
            RequestBuilder::get("/test")
                .header("user-agent", "my-agent")
                .header("x-custom-add", "initial-value")
        )
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
                .body("client request")
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
                .body("client request")
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
                .body("client request")
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
    let _orion =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn orion");
    #[allow(clippy::unwrap_used)]
    let client = TestClient::new(_orion.listener_addr().unwrap());

    let response = client
        .send(
            RequestBuilder::post("/test").body("client request")
        )
        .await
        .expect("request");

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

    let response = client
        .send(
            RequestBuilder::post("/test").body("original client request")
        )
        .await
        .expect("request");

    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured_callout = callout_backend.await_request().await.expect("callout request");
    assert_eq!(captured_callout.body_str(), Some("original client request"));

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("original client request"));
}

macro_rules! define_simple_wasm_test {
    ($test_name:ident, $filter_name:expr) => {
        #[tokio::test]
        #[test_log::test]
        async fn $test_name() {
            let builder = WasmBuilder::new()
                .name($filter_name)
                .root_id(format!("{}_root_id", $filter_name))
                .vm_id(format!("{}_vm_id", $filter_name))
                .code_filename(get_wasm_path($filter_name));

            let (backend, _orion, client, _cfg) = setup_wasm(builder).await;
            backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

            let response = client
                .send(RequestBuilder::get("/test"))
                .await
                .expect("request");

            response.assert_status(StatusCode::OK);
            response.assert_body("backend response");
        }
    };
}

define_simple_wasm_test!(test_wasm_custom_metric_filter, "custom_metric_filter");

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

    let response = client
        .send(RequestBuilder::get("/test"))
        .await
        .expect("request");

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

    let listener = ListenerBuilder::new("https")
        .port(0)
        .with_tls_inspector()
        .filter_chain(
            FilterChainBuilder::new("main")
                .downstream_tls(tls)
                .hcm(
                    HcmBuilder::new().http1().wasm(wasm_builder).route_config(
                        RouteConfigBuilder::new("routes").virtual_host(
                            VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                        ),
                    ),
                ),
        );

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

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

    let response = client
        .send(
            RequestBuilder::get("/test")
        )
        .await
        .expect("request");

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
    let response = client
        .send(RequestBuilder::get("/test"))
        .await
        .expect("request");
    let elapsed = start.elapsed();

    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");

    let captured = backend.await_request().await.expect("backend request");
    
    assert_eq!(captured.header("x-timeout-test"), Some("passed"));

    // The filter sleeps for 1 second, sets 1 second IO timeout, then tries to sleep 10 seconds.
    // It should timeout after 1 second, so the total time should be roughly 2 seconds.
    // Allow some buffer for execution overhead (between 1.5s and 4.0s)
    assert!(elapsed.as_secs_f64() > 1.5, "Elapsed time too short: {:?}", elapsed);
    assert!(elapsed.as_secs_f64() < 4.0, "Elapsed time too long: {:?}", elapsed);
}
#[tokio::test]
#[test_log::test]
async fn test_wasm_access_log_operator_filter() {
    let backend = TestBackend::start().await.expect("backend start");
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
        .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), log_format).wasm(builder).route_config(
                RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                ),
            ),
        )))
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
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default().with_verbose()).await.expect("spawn orion");
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
    
    let _ = std::fs::remove_file(log_path);
}

