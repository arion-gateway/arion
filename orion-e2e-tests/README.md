# Orion E2E Test Harness

End-to-end testing utilities for Orion Proxy.

## Quick Start

```bash
# Build the proxy binary first
cargo build -p orion-proxy

# Run e2e tests (ignored by default)
cargo test -p orion-e2e-tests -- --ignored
```

## Architecture

The test harness supports both static and dynamic configuration:

```
                                                    ┌──────────────┐
TestClient  ──────►  OrionInstance  ──────────────► │ TestBackend  │
                     (proxy subprocess)             │ (mock server)│
                           ▲                        └──────────────┘
                           │
           ┌───────────────┴───────────────┐
           │                               │
    Static Config                    Dynamic Config
           │                               │
           ▼                               ▼
   BootstrapBuilder               XdsEnabledHarness
   (generates YAML)               (manages xDS server + Orion)
```

**Static mode**: Generate YAML config with `BootstrapBuilder`, write to file, spawn Orion.

**Dynamic mode**: Use `XdsEnabledHarness` which handles xDS server setup and Orion lifecycle.

## Examples

### Basic Proxy Test

The simplest way to test proxy functionality using the preset helpers:

```rust
use std::net::SocketAddr;
use http::StatusCode;
use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, TestBackend, TestClient};

#[tokio::test]
#[ignore]
async fn test_basic_proxy() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello!")).await;

    let (bootstrap, port) = presets::simple_proxy("backend", backend.addr()).unwrap();
    let config_path = bootstrap.build_to_temp().unwrap();

    let listener_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let orion = OrionInstance::spawn(&config_path, listener_addr).await.unwrap();

    let client = TestClient::new(orion.listener_addr());
    let response = client.get("/test").await.unwrap();

    response.assert_status(StatusCode::OK);
    response.assert_body("Hello!");

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.path(), "/test");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
```

### Composable Builder API

For complex configurations, use the full composable builder API:

```rust
use std::net::SocketAddr;
use std::time::Duration;
use http::StatusCode;
use orion_e2e_tests::config_builder::*;
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, TestBackend, TestClient};

#[tokio::test]
#[ignore]
async fn test_with_custom_config() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("API response")).await;

    let cluster = ClusterBuilder::new("api-backend")
        .endpoint(backend.addr())
        .connect_timeout(Duration::from_secs(10))
        .round_robin()
        .build();

    let route = RouteBuilder::new()
        .match_prefix("/api")
        .cluster("api-backend")
        .timeout(Duration::from_secs(30))
        .build();

    let vhost = VirtualHostBuilder::new("default")
        .route(route)
        .build();

    let route_config = RouteConfigBuilder::new("routes")
        .virtual_host(vhost)
        .build();

    let hcm = HcmBuilder::new()
        .http1()
        .route_config(route_config)
        .build();

    let filter_chain = FilterChainBuilder::new("main")
        .hcm(hcm)
        .build();

    let (listener, port) = ListenerBuilder::new("http")
        .auto_port()
        .unwrap();
    let listener = listener.filter_chain(filter_chain).build();

    let config_path = BootstrapBuilder::new()
        .listener(listener)
        .cluster(cluster)
        .build_to_temp()
        .unwrap();

    let listener_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let orion = OrionInstance::spawn(&config_path, listener_addr).await.unwrap();

    let client = TestClient::new(orion.listener_addr());
    let response = client.get("/api/users").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
```

### Multiple Routes

```rust
use orion_e2e_tests::config_builder::presets;

let api_backend: SocketAddr = "127.0.0.1:8080".parse().unwrap();
let static_backend: SocketAddr = "127.0.0.1:8081".parse().unwrap();

let routes = vec![
    presets::prefix_route("/api", "api-cluster"),
    presets::prefix_route("/static", "static-cluster"),
    presets::default_route("api-cluster"),
];

let clusters = vec![
    presets::static_cluster("api-cluster", api_backend),
    presets::static_cluster("static-cluster", static_backend),
];

let (bootstrap, port) = presets::routed_proxy(routes, clusters).unwrap();
```

### Direct Response (Health Check)

```rust
use orion_e2e_tests::config_builder::presets;

let (bootstrap, port) = presets::routed_proxy(
    [
        presets::direct_response_route("/health", 200, "OK"),
        presets::default_route("backend"),
    ],
    [presets::static_cluster("backend", backend_addr)],
).unwrap();
```

### With Retry Policy

```rust
use orion_e2e_tests::config_builder::*;

let route = RouteBuilder::new()
    .match_prefix("/api")
    .cluster("backend")
    .retry_policy(
        RetryPolicyBuilder::new()
            .on_5xx()
            .on_connect_failure()
            .num_retries(3)
            .per_try_timeout(Duration::from_secs(5))
    )
    .build();
```

### Header Matching

```rust
use orion_e2e_tests::config_builder::*;

let route = RouteBuilder::new()
    .match_prefix("/api")
    .match_header(HeaderMatch::exact("x-version", "2"))
    .match_header(HeaderMatch::present("authorization"))
    .cluster("v2-backend")
    .build();
```

### Dynamic xDS Configuration

Use `XdsEnabledHarness` for dynamic configuration via xDS. It handles xDS server setup, Orion lifecycle, and ACK/NACK tracking automatically:

```rust
use std::net::SocketAddr;
use std::time::Duration;
use http::StatusCode;
use orion_e2e_tests::allocate_port;
use orion_e2e_tests::config_builder::{
    ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder,
    ListenerBuilder, RouteBuilder, RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{PreConfiguredResponse, TestBackend, TestClient, XdsEnabledHarness};

#[tokio::test]
#[ignore]
async fn test_dynamic_xds_config() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend (all configured by xDS)!")).await;

    // Start harness (spawns xDS server and Orion, waits for connection)
    let mut harness = XdsEnabledHarness::start().await.unwrap();

    let listener_port = allocate_port().unwrap();
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    // Build and push configuration
    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .build();

    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default")
                    .route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.unwrap();
    harness.push_listener(&listener).await.unwrap();

    // Wait for listener to be ready
    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.unwrap();

    // Test the proxy
    let client = TestClient::new(listener_addr);
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("Hello from backend (all configured by xDS)!");

    harness.shutdown();
}
```

### Dynamic Configuration Updates

Update configuration at runtime by pushing new resources:

```rust
// Initial setup with backend1
let cluster1 = ClusterBuilder::new("backend1")
    .endpoint(EndpointBuilder::from_socket_addr(backend1.addr()))
    .build();
harness.push_cluster(&cluster1).await.unwrap();

// Later, add a second cluster
let cluster2 = ClusterBuilder::new("backend2")
    .endpoint(EndpointBuilder::from_socket_addr(backend2.addr()))
    .build();
harness.push_cluster(&cluster2).await.unwrap();

// Routes can reference both clusters
let listener = ListenerBuilder::new("http")
    .port(listener_port)
    .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
        RouteConfigBuilder::new("routes").virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/api").cluster("backend1"))
                .route(RouteBuilder::new().match_prefix("/service").cluster("backend2")),
        ),
    )))
    .build();
harness.push_listener(&listener).await.unwrap();
```

### Advanced: Low-Level xDS API

For advanced use cases requiring more control, access the underlying `ConfigPusher`:

```rust
use std::time::Duration;
use orion_e2e_tests::config_builder::xds::PushResult;

let harness = XdsEnabledHarness::start().await.unwrap();
let pusher = harness.pusher();

// Push with explicit timeout and result handling
let result = pusher.push_cluster(&cluster, Duration::from_secs(10)).await.unwrap();
match result {
    PushResult::Ack => println!("Config accepted"),
    PushResult::Nack { error_code, error_message } => {
        println!("Config rejected: {} (code {})", error_message, error_code);
    }
}

// Remove resources
pusher.remove_cluster("old-cluster", Duration::from_secs(5)).await.unwrap();
pusher.remove_listener("old-listener", Duration::from_secs(5)).await.unwrap();
```

## Component Reference

### TestBackend

Programmable HTTP mock server.

```rust
let mut backend = TestBackend::start().await?;

backend.set_default_response(
    PreConfiguredResponse::with_status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(r#"{"status": "ok"}"#)
        .delay(Duration::from_millis(100))
).await;

backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::CREATED)).await;

let request = backend.await_request().await?;
assert_eq!(request.path(), "/expected");
assert_eq!(request.header("x-custom"), Some("value"));
```

### TestClient

HTTP client for sending requests to the proxy.

```rust
let client = TestClient::new(addr)
    .with_timeout(Duration::from_secs(10))
    .with_header("authorization", "Bearer token");

let response = client.get("/path").await?;
let response = client.post("/path", "body").await?;

let request = RequestBuilder::post("/api/users")
    .header("content-type", "application/json")
    .body(r#"{"name": "test"}"#)
    .host("example.com");
let response = client.send(request).await?;

response.assert_status(StatusCode::OK);
response.assert_body("expected");
response.assert_header("x-custom", "value");
```

### PortAllocator

```rust
use orion_e2e_tests::{allocate_port, PortAllocator};

let port = allocate_port()?;

let allocator = PortAllocator::new();
let port = allocator.allocate()?;
let addr = allocator.allocate_addr()?;
```

### OrionInstance

```rust
use orion_e2e_tests::orion_instance::{OrionInstance, SpawnOptions};

let orion = OrionInstance::spawn(&config_path, listener_addr).await?;

let options = SpawnOptions::default()
    .with_ready_timeout(Duration::from_secs(30))
    .with_num_cpus(4)
    .with_cleanup();
let orion = OrionInstance::spawn_with_options(&config_path, listener_addr, options).await?;

orion.shutdown();
```

### XdsEnabledHarness

All-in-one harness for xDS-based testing. Manages xDS server, Orion instance, and configuration pushing.

```rust
use std::time::Duration;
use orion_e2e_tests::{XdsEnabledHarness, XdsHarnessOptions, HarnessTimeouts};

// Simple start with defaults
let mut harness = XdsEnabledHarness::start().await?;

// Or with custom options
let options = XdsHarnessOptions {
    timeouts: HarnessTimeouts {
        connection: Duration::from_secs(10),
        push: Duration::from_secs(10),
    },
    log_level: "info".into(),
    ..Default::default()
};
let mut harness = XdsEnabledHarness::start_with_options(options).await?;

// Push resources (automatically waits for ACK, returns error on NACK)
harness.push_cluster(&cluster).await?;
harness.push_listener(&listener).await?;
harness.push_route_config(&route_config).await?;
harness.push_endpoints("cluster-name", &endpoints).await?;
harness.push_secret(&secret).await?;

// Access Orion instance for listener readiness checks
harness.orion_mut().wait_for_listener_at(addr, Duration::from_secs(10)).await?;

// Access underlying ConfigPusher for advanced operations
let pusher = harness.pusher();

harness.shutdown();
```

### ConfigPusher (Low-Level xDS)

Low-level xDS resource pushing with explicit timeout and result handling. Access via `XdsEnabledHarness::pusher()` or create manually for advanced setups.

```rust
use std::time::Duration;
use orion_e2e_tests::config_builder::xds::PushResult;

let pusher = harness.pusher();

// Push with explicit result handling
let result = pusher.push_cluster(&cluster, Duration::from_secs(5)).await?;
assert!(matches!(result, PushResult::Ack));

// Push endpoints (EDS) - update backend endpoints without changing cluster config
let endpoints = vec![
    EndpointBuilder::from_socket_addr(backend.addr()).build(),
];
let result = pusher.push_endpoints("cluster-name", &endpoints, Duration::from_secs(5)).await?;

// Push secrets (SDS) - deliver TLS certificates dynamically
let secret = SecretBuilder::new("my-tls-secret")
    .tls_certificate(cert_chain_pem, private_key_pem)
    .build();
let result = pusher.push_secret(&secret, Duration::from_secs(5)).await?;

// Remove resources
pusher.remove_cluster("cluster-name", Duration::from_secs(5)).await?;
pusher.remove_listener("listener-name", Duration::from_secs(5)).await?;
pusher.remove_route_config("route-config-name", Duration::from_secs(5)).await?;
pusher.remove_endpoints("cluster-name", Duration::from_secs(5)).await?;
pusher.remove_secret("secret-name", Duration::from_secs(5)).await?;
```

## Builder Hierarchy

```
Static Configuration:
BootstrapBuilder
├── ListenerBuilder
│   └── FilterChainBuilder
│       └── HcmBuilder
│           └── RouteConfigBuilder
│               └── VirtualHostBuilder
│                   └── RouteBuilder
│                       └── RetryPolicyBuilder
├── ClusterBuilder
│   └── EndpointBuilder
└── SecretBuilder

Dynamic Configuration:
XdsEnabledHarness
├── push_listener(Listener)
├── push_cluster(Cluster)
├── push_route_config(RouteConfig)
├── push_endpoints(cluster_name, [Endpoint])
├── push_secret(Secret)
└── pusher() → ConfigPusher (for advanced operations)
```

## Preset Functions

| Function | Description |
|----------|-------------|
| `presets::simple_proxy(name, addr)` | Single cluster, catch-all route |
| `presets::routed_proxy(routes, clusters)` | Multiple routes and clusters |
| `presets::http_listener(name, port)` | HTTP listener with HCM |
| `presets::http_listener_auto(name)` | Auto-allocate port |
| `presets::static_cluster(name, addr)` | Single-endpoint cluster |
| `presets::static_cluster_multi(name, addrs)` | Multi-endpoint cluster |
| `presets::default_route(cluster)` | Catch-all `/` route |
| `presets::prefix_route(prefix, cluster)` | Prefix-matched route |
| `presets::direct_response_route(prefix, status, body)` | Direct response |

## Error Types

| Error | Description |
|-------|-------------|
| `Io` | File system or network errors |
| `ProcessStartFailed` | Binary not found or failed to spawn |
| `ProcessExitedUnexpectedly` | Proxy crashed |
| `ReadyTimeout` | Listener not ready in time |
| `Config` | Invalid configuration |
| `Yaml` | YAML serialization error |
| `Http` | HTTP error |
| `RequestTimeout` | Request timed out |
| `NoRequestReceived` | Backend received no request |
| `PortAllocationFailed` | No ports available |
| `HarnessError::Nack` | xDS configuration rejected by Orion |
| `HarnessError::Xds` | xDS communication error |

## Best Practices

1. Use `presets::simple_proxy()` for basic static config tests
2. Use `XdsEnabledHarness` for dynamic xDS-based tests
3. Use auto port allocation to avoid conflicts in parallel tests
4. Always call `shutdown()` on OrionInstance or XdsEnabledHarness
5. Verify both the response and what the backend received
6. Build the proxy binary first: `cargo build -p orion-proxy`
