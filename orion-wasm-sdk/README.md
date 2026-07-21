# Orion WebAssembly SDK

The Orion WebAssembly SDK provides a high-level, idiomatic Rust API for writing WebAssembly plugins that run inside the Orion proxy via the Wasmtime runtime.

This SDK hides the complexity of raw FFI (Foreign Function Interface) hostcalls behind a safe, strongly-typed Rust interface. This allows you to focus purely on your business logic—like traffic filtering, authentication, request routing, or analytics—without worrying about the underlying Wasm memory management or ABI constraints.

---

## 🚀 Getting Started

Writing a plugin for Orion requires two main steps:
1. Implementing the `Plugin` trait for your stateful or stateless struct.
2. Exporting your struct using the `#[orion_plugin]` macro so the Orion proxy host can discover and invoke it.

### Example: A Simple Authentication Filter

```rust
use orion_wasm_sdk::prelude::*;
use orion_wasm_types::FilterAction;

#[derive(Default)]
struct AuthFilter;

#[orion_plugin]
impl Plugin for AuthFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        // Read a header from the incoming HTTP request
        match ctx.get_header("Authorization") {
            Ok(Some(v)) if v == "Bearer secret-token" => {
                // The token is valid, allow the request to proceed downstream
                FilterAction::Continue
            }
            _ => {
                // Missing or invalid token, short-circuit the request with a 401
                ctx.direct_response(401, b"Unauthorized access")
            }
        }
    }
}
```

---

## The Prelude

To minimize boilerplate, the SDK provides a prelude module that brings all the essential traits, typestates, and macros into scope:

```rust
use orion_wasm_sdk::prelude::*;
```

This imports:
- **`Plugin`**: The core trait representing the plugin lifecycle and filter hooks.
- **`RequestHandle` & `ResponseHandle`**: The primary context objects to inspect and manipulate HTTP traffic.
- **`HttpHeaders` & `HttpBody`**: The typestate markers that guarantee compile-time safety.
- **`#[orion_plugin]`**: The macro for FFI generation.

---

## The `Plugin` Trait Lifecycle

The `Plugin` trait exposes several hooks for the lifecycle of the Wasm module and for handling individual HTTP transactions. You only need to implement the methods you require; all methods have default, no-op implementations (usually returning `FilterAction::Continue`).

### Module Lifecycle Hooks
- `fn on_plugin_start(&mut self)`: Invoked exactly once when the Wasm module is instantiated. Useful for one-time initialization, such as parsing plugin configurations or setting up tracing/logging.
- `fn on_plugin_destroy(&mut self)`: Invoked when the host destroys the Wasm module instance. 

### Transaction Lifecycle Hooks
- `fn on_transaction_start(&mut self)`: Invoked when a new HTTP request is received.
- `fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction`: Invoked when the request headers arrive, but before the body is processed.
- `fn on_request_body(&mut self, ctx: &RequestHandle<HttpBody>) -> FilterAction`: Invoked only if the plugin previously paused the request to buffer the body.
- `fn on_response_headers(&mut self, ctx: &ResponseHandle<HttpHeaders>) -> FilterAction`: Invoked when the upstream service returns response headers, before the response body is processed.
- `fn on_response_body(&mut self, ctx: &ResponseHandle<HttpBody>) -> FilterAction`: Invoked only if the plugin previously paused the response to buffer the body.
- `fn on_transaction_complete(&mut self)`: Invoked when the entire request/response cycle is fully processed. Ideal for transaction-specific cleanup or logging metrics.

---

## Typestate Pattern

The SDK strictly enforces a **typestate pattern** to prevent invalid operations at compile-time. This guarantees you can only perform actions that make sense for the current phase of the HTTP transaction.

For example:
- `RequestHandle<HttpHeaders>` allows you to manipulate headers. However, if you try to call `.get_body()` on it, your code will fail to compile because the body hasn't been read or buffered yet.
- To access the body, your `on_request_headers` method must return `FilterAction::PauseAndBufferBody`. The Orion proxy will then collect the entire payload and subsequently invoke `on_request_body`, passing you a `RequestHandle<HttpBody>`. At this stage, calling `.get_body()` is perfectly safe and valid.

---

## Request and Response Context API

The `RequestHandle<S>` and `ResponseHandle<S>` objects are your gateway to manipulating the HTTP traffic.

### Headers API
*(Available on `RequestHandle<HttpHeaders>`, `RequestHandle<HttpBody>`, `ResponseHandle<HttpHeaders>`, and `ResponseHandle<HttpBody>`)*

- **`get_header(name: &str) -> Result<Option<HeaderValue>, OrionWasmError>`**
  Reads a specific header by name. Returns `Ok(None)` if the header is missing.
- **`set_header(name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError>`**
  Sets a header, completely replacing any existing header with the same name.
- **`add_header(name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError>`**
  Appends a header. If the header already exists, both values will be sent (useful for multi-value headers like `Set-Cookie`).
- **`remove_header(name: &HeaderName) -> Result<(), OrionWasmError>`**
  Removes all instances of a specific header.
- **`replace_header(name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError>`**
  Replaces a header if it exists. If it does not exist, it may act similarly to `set_header` depending on the host implementation.
- **`get_headers_map() -> Result<HeaderMap, OrionWasmError>`**
  Retrieves all current headers as a standard `http::HeaderMap` for bulk inspection.
- **`set_headers_map(headers: &HeaderMap) -> Result<(), OrionWasmError>`**
  Replaces the entire set of headers with the provided `HeaderMap`.
- **`apply_header_mutations(mutations: &[HeaderMutation]) -> Result<(), OrionWasmError>`**
  Efficiently applies a batch of header mutations (additions, removals, replacements) in a single FFI call. Highly recommended for performance if you need to perform multiple header modifications.

### Trailers API
*(Available only on `RequestHandle<HttpBody>` and `ResponseHandle<HttpBody>`)*

Trailers behave exactly like headers but appear at the end of a chunked HTTP message. The API perfectly mirrors the Headers API:
- `get_trailer`, `set_trailer`, `add_trailer`, `remove_trailer`, `replace_trailer`
- `get_trailers_map`, `set_trailers_map`, `apply_trailer_mutations`

### Body API
*(Available only on `RequestHandle<HttpBody>` and `ResponseHandle<HttpBody>`)*

- **`get_body() -> Result<Vec<u8>, OrionWasmError>`**
  Retrieves the buffered HTTP body as raw bytes.
- **`set_body(body: &[u8]) -> Result<(), OrionWasmError>`**
  Replaces the existing HTTP body payload entirely. If the new body size differs, the host will automatically adjust the `Content-Length` header for you.

### Request-Specific Actions
*(Available only on `RequestHandle`)*

- **`get_downstream_metadata() -> Result<Option<DownstreamMetadata>, OrionWasmError>`**
  Retrieves contextual metadata about the client connection that initiated the request, such as TLS cipher info or remote IP addresses.
- **`send_direct_response(status_code: u16, body: &[u8]) -> Result<(), OrionWasmError>`**
  Short-circuits the proxy pipeline immediately and generates a local response to the client with the given HTTP status code and body. Upstream servers are completely bypassed.
- **`direct_response(status_code: u16, body: &[u8]) -> FilterAction`**
  A developer-friendly convenience wrapper around `send_direct_response` that automatically translates the result into `FilterAction::DirectResponse` or `FilterAction::Continue` on failure, ideal for return statements.

---

## Host API (Free Functions)

The `host` module (whose contents are re-exported at the root of the crate) provides "free functions" that communicate directly with the Orion proxy host. These allow your plugin to interact with the outside world, handle asynchronous IO, or configure its own environment.

- **`get_plugin_config() -> Result<Option<String>, OrionWasmError>`**
  Retrieves the configuration string assigned to this specific plugin instance by the host control plane. Call this inside `on_plugin_start` to parse your JSON/YAML config.
- **`dispatch_http_call(request: &CalloutRequest) -> Result<CalloutResponse, OrionWasmError>`**
  Dispatches an asynchronous HTTP request to an external service (e.g., an auth server or rate-limiting redis) using the proxy's internal cluster manager. Because Orion utilizes an async Wasm engine, this function is seamlessly paused and resumed; it **does not block** the proxy server thread.
- **`sleep(duration: std::time::Duration) -> Result<(), OrionWasmError>`**
  Suspends the current WebAssembly execution for a specified duration. Again, thanks to the async engine, this is a non-blocking operation on the host.
- **`set_io_timeout(duration: std::time::Duration) -> Result<(), OrionWasmError>`**
  Sets an absolute deadline for subsequent blocking IO operations (such as `dispatch_http_call`). If the IO operation fails to complete before the deadline, it will return `OrionWasmError::Timeout`.
- **`clear_io_timeout() -> Result<std::time::Duration, OrionWasmError>`**
  Disarms any currently active IO timeout and returns the remaining duration.
- **`set_custom_metrics(metrics: I) -> Result<(), OrionWasmError>`**
  Exports custom metric key-value tags to the proxy’s telemetry system for monitoring and alerting.
- **`set_access_log_operators(operators: I) -> Result<(), OrionWasmError>`**
  Injects custom variables and data into the proxy's access log for the current transaction. Useful for appending plugin-specific debug info to your SIEM logs.

---

## Tracing and Logging

The SDK integrates gracefully with the standard Rust `tracing` ecosystem. Instead of relying on `println!` (which wouldn't appear in the proxy's log files), you can wire `tracing` directly to the host's logging system.

To enable it, simply call `init_tracing()` exactly once, typically in your `on_plugin_start` method.

```rust
use orion_wasm_sdk::prelude::*;

#[derive(Default)]
struct MyPlugin;

#[orion_plugin]
impl Plugin for MyPlugin {
    fn on_plugin_start(&mut self) {
        // Wire up the tracing macros to the Orion proxy logger
        let _ = orion_wasm_sdk::init_tracing();
        
        tracing::info!("My custom plugin initialized successfully!");
        tracing::debug!("This debug log will only show if the host is configured for debug logs.");
    }
}
```

Once initialized, all `tracing::error!`, `tracing::warn!`, `tracing::info!`, `tracing::debug!`, and `tracing::trace!` calls are seamlessly transmitted over FFI to the Orion proxy and emitted alongside the proxy's own native logs.
