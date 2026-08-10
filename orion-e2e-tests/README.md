# Orion E2E Test Harness

End-to-end testing utilities for Orion Proxy.

## Quick Start

```bash
# Build the proxy binary first
cargo build --all-features -p orion-proxy

# Run e2e tests (ignored by default)
# We recommend using a single thread for e2e tests to avoid race conditions
cargo test -p orion-e2e-tests -- --ignored --test-threads=1
```

## Runnable Examples

- [MCP Gateway local semantic search demo](examples/README.md)

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
use http::StatusCode;
use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient};

#[tokio::test]
#[ignore]
async fn test_basic_proxy() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello!")).await;

    let bootstrap = presets::simple_proxy("backend", backend.addr());
    let config_path = bootstrap.build_to_temp().unwrap();

    // spawn_auto_port discovers the actual port from Orion's logs
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
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
use std::time::Duration;
use http::StatusCode;
use orion_e2e_tests::config_builder::*;
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient};

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

    let listener = ListenerBuilder::new("http")
        .port(0)
        .filter_chain(filter_chain)
        .build();

    let config_path = BootstrapBuilder::new()
        .listener(listener)
        .cluster(cluster)
        .build_to_temp()
        .unwrap();

    // spawn_auto_port discovers the actual port from Orion's logs
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/api/users").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
```

### Multiple Routes

```rust
use std::net::SocketAddr;
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

let bootstrap = presets::routed_proxy(routes, clusters);
```

### Direct Response (Health Check)

```rust
use std::net::SocketAddr;
use orion_e2e_tests::config_builder::presets;

let backend_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

let bootstrap = presets::routed_proxy(
    [
        presets::direct_response_route("/health", 200, "OK"),
        presets::default_route("backend"),
    ],
    [presets::static_cluster("backend", backend_addr)],
);
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

    let listener_port = harness.allocate_listener_port().unwrap();
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

### PortBlock (for xDS tests)

For xDS tests that need explicit ports before configuration is pushed, use `PortBlock` to reserve a block of ports:

```rust
use orion_e2e_tests::PortBlock;

// Reserve a block of 50 ports (uses file-based locking for parallel safety)
let block = PortBlock::reserve()?;
let port1 = block.allocate()?;
let port2 = block.allocate()?;
// Block is released when dropped

// Or use XdsEnabledHarness which manages PortBlock automatically:
let mut harness = XdsEnabledHarness::start().await?;
let listener_port = harness.allocate_listener_port()?;
```

### OrionInstance

```rust
use std::time::Duration;
use orion_e2e_tests::{OrionInstance, SpawnOptions};

// Recommended: Use port 0 in config and discover port from logs
let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await?;
let listener_addr = orion.listener_addr().unwrap();

// With custom options
let options = SpawnOptions::default()
    .with_ready_timeout(Duration::from_secs(30))
    .with_num_cpus(4)
    .with_cleanup();
let orion = OrionInstance::spawn_auto_port(&config_path, "http", options).await?;

// For xDS tests where no listener is configured initially
let orion = OrionInstance::spawn_no_listener(&config_path, SpawnOptions::default()).await?;

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
| `presets::simple_proxy(name, addr)` | Single cluster, catch-all route (port 0) |
| `presets::routed_proxy(routes, clusters)` | Multiple routes and clusters (port 0) |
| `presets::http_listener(name, port)` | HTTP listener with HCM |
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

## MCP Gateway Tests

The MCP (Model Context Protocol) gateway tests are located in `tests/mcp_gateway_tests.rs` and provide comprehensive coverage of the MCP gateway functionality.

### Running MCP Tests

```bash
# Build Orion first
cargo build -p orion-proxy

# Run all MCP gateway tests
cargo test --test mcp_gateway_tests -- --ignored

# Run a specific test
cargo test --test mcp_gateway_tests test_mcp_gateway_rest_path_templating -- --ignored
```

### Test Categories

| Category | Description |
|----------|-------------|
| **Basic Protocol** | MCP handshake, ping, tools/list |
| **REST Transcoding** | Path templating, query params, body templating |
| **MCP Backend** | Proxying to upstream MCP servers |
| **RBAC** | JWT claim/header-based access control |
| **Discovery** | Dynamic tool discovery API |

### Key Test Utilities

- `McpTestClient` - HTTP client for MCP protocol requests
- `MockMcpServer` - Simulated MCP backend server
- `JwtKeyPair` - RSA key generation for JWT testing
- `mcp_gateway_config()` - Builder for MCP gateway configuration
- `rest_tool_config()` - Create REST backend tool definitions
- `mcp_server_tool_config()` - Create MCP backend tool definitions
- `rbac_config()` - Create RBAC permission configurations

### Example MCP Test

```rust
#[tokio::test]
#[ignore]
async fn test_mcp_gateway_tools_list() {
    // Start mock backend
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(
        PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)
    ).await;

    // Create tool configuration
    let tool = rest_tool_config(
        "get_weather",
        "Get weather forecast",
        "weather_cluster",
        "GET",
        "/weather",
        json!({"type": "object", "properties": {}}),
        vec![],
        None,
        None,
    );

    // Build configuration
    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("weather_cluster")
            .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
    );

    let config_path = bootstrap.build_to_temp().unwrap();

    // Spawn Orion
    let orion = OrionInstance::spawn_auto_port(
        &config_path,
        "http",
        SpawnOptions::default()
    ).await.unwrap();

    // Create MCP client and test
    let mut client = McpTestClient::new(
        format!("http://{}", orion.listener_addr().unwrap())
    );
    client.initialize().await.unwrap();
    
    let tools = client.list_tools().await.unwrap();
    assert_eq!(tools.tools.len(), 1);
    assert_eq!(tools.tools[0].name, "get_weather");

    orion.shutdown();
}
```
