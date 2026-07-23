# Orion WebAssembly SDK

The Orion WebAssembly SDK provides a high-level, idiomatic Rust API for writing WebAssembly plugins that run inside the Orion proxy via the Wasmtime runtime.

This SDK hides the complexity of raw FFI (Foreign Function Interface) hostcalls behind a safe, strongly-typed Rust interface. This allows you to focus purely on your business logic—like traffic filtering, authentication, request routing, distributed metrics, or shared state synchronization—without worrying about underlying Wasm memory management or ABI constraints.

---

## 📋 Table of Contents

- [🚀 Getting Started](#-getting-started)
- [The Prelude & Core Types](#the-prelude--core-types)
- [The `Plugin` Trait Lifecycle](#the-plugin-trait-lifecycle)
- [Typestate Pattern](#typestate-pattern)
- [Request and Response Context API](#request-and-response-context-api)
- [Host API (Free Functions)](#host-api-free-functions)
- [🧠 Shared Memory Primitives](#-shared-memory-primitives)
- [💡 Shared Memory Use Cases & Code Examples](#-shared-memory-use-cases--code-examples)
- [Tracing and Logging](#tracing-and-logging)

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

## The Prelude & Core Types

To minimize boilerplate, the SDK provides a prelude module that brings all essential traits, typestates, and macros into scope:

```rust
use orion_wasm_sdk::prelude::*;
```

This imports:
- **`Plugin`**: The core trait representing the plugin lifecycle and filter hooks.
- **`RequestHandle` & `ResponseHandle`**: The primary context objects to inspect and manipulate HTTP traffic.
- **`HttpHeaders` & `HttpBody`**: The typestate markers that guarantee compile-time safety.
- **`#[orion_plugin]`**: The macro for FFI generation.

Additionally, core types such as **`FilterAction`**, **`HeaderMutation`**, **`OrionWasmError`**, and shared memory primitives (**`AtomicU64`**, **`AtomicI64`**, **`SharedBlob`**) are re-exported at the root of `orion_wasm_sdk` for convenient importing:

```rust
use orion_wasm_sdk::{FilterAction, HeaderMutation, AtomicU64, SharedBlob};
```

---

## The `Plugin` Trait Lifecycle

The `Plugin` trait exposes several hooks for the lifecycle of the Wasm module and for handling individual HTTP transactions. You only need to implement the methods you require; all methods have default, no-op implementations (usually returning `FilterAction::Continue`).

### Module Lifecycle Hooks
- `fn on_plugin_start(&mut self)`: Invoked exactly once when the Wasm module is instantiated. Useful for one-time initialization, such as parsing plugin configurations, initializing shared memory variables, or setting up tracing/logging.
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

The `RequestHandle<S>` and `ResponseHandle<S>` objects are your gateway to manipulating HTTP traffic.

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
  Retrieves the configuration string assigned to this specific plugin instance by the control plane. Call this inside `on_plugin_start` to parse your JSON/YAML config.
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

## 🧠 Shared Memory Primitives

WebAssembly instances in Orion are isolated per worker thread or connection context; therefore, local struct variables are instance-private. To enable state synchronization across concurrent requests and across worker threads, the SDK provides host-backed **Shared Memory Primitives** under `orion_wasm_sdk::shared` (also re-exported at the crate root).

Shared memory variables are identified by string names. When a plugin calls `try_new("variable_name")`, the host resolves or allocates the requested shared variable in host memory and returns a handle bound to that variable.

### 1. Atomic Variables (`AtomicU64` and `AtomicI64`)

`AtomicU64` and `AtomicI64` provide thread-safe, atomic integer operations backed by host memory. They mirror Rust's standard `std::sync::atomic::AtomicU64` / `AtomicI64` types and accept standard `std::sync::atomic::Ordering` parameters (`Relaxed`, `Release`, `Acquire`, `AcqRel`, `SeqCst`).

#### API Reference

* **`AtomicU64::try_new(name: &str) -> Result<AtomicU64, SharedVarError>`**  
  **`AtomicI64::try_new(name: &str) -> Result<AtomicI64, SharedVarError>`**  
  Look up or allocate a named atomic variable. Returns `Err(SharedVarError::InitFailed)` if initialization fails.

* **`load(&self, order: Ordering) -> u64` / `(i64)`**  
  Loads the current value atomically with the specified memory ordering.

* **`store(&self, val: u64, order: Ordering)` / `(i64)`**  
  Stores a value into the atomic variable atomically.

* **`swap(&self, val: u64, order: Ordering) -> u64` / `(i64)`**  
  Atomically stores `val` and returns the previous value.

* **`compare_exchange(&self, current: u64, new: u64, success: Ordering, failure: Ordering) -> Result<u64, u64>`**  
  Atomically compares the current value with `current`. If equal, sets it to `new` and returns `Ok(previous)`. Otherwise, returns `Err(actual_current)`.

* **`compare_exchange_weak(&self, current: u64, new: u64, success: Ordering, failure: Ordering) -> Result<u64, u64>`**  
  Weak variant of `compare_exchange` (equivalent in this host implementation).

* **`fetch_add(&self, val: u64, order: Ordering) -> u64` / `fetch_sub(...)`**  
  Atomically adds (or subtracts) `val` and returns the previous value.

* **`fetch_and`, `fetch_nand`, `fetch_or`, `fetch_xor`, `fetch_max`, `fetch_min`**  
  Standard bitwise and arithmetic atomic operations.

* **`fetch_update<F>(&self, set_order: Ordering, fetch_order: Ordering, mut f: F) -> Result<u64, u64>`**  
  Fetches the value, applies the closure `f`, and attempts a `compare_exchange` in a retry loop until success or until the closure returns `None`.

---

### 2. Shared Blobs (`SharedBlob`)

`SharedBlob` provides host-backed storage for dynamic, arbitrary binary data (`Vec<u8>`) shared across Wasm instances. It utilizes **optimistic concurrency control (OCC)** via monotonic version numbers (`u64`), allowing concurrent readers and writers to synchronize safely without global lock contention.

#### API Reference & Data Structures

* **`BlobData`**:
  ```rust
  pub struct BlobData {
      pub data: Vec<u8>,
      pub version: u64,
  }
  ```

* **`SharedBlob::try_new(name: &str) -> Result<SharedBlob, SharedVarError>`**  
  Looks up or creates a named shared blob.

* **`read(&self) -> BlobData`**  
  Reads the current byte payload and its associated version counter. The SDK automatically resizes its internal buffers as needed to fetch the complete blob data.

* **`write(&self, data: &[u8]) -> u64`**  
  Unconditionally overwrites the blob content with `data` and increments the version, returning the new version.

* **`compare_and_swap(&self, data: &[u8], expected_version: u64) -> Result<u64, ()>`**  
  Performs an optimistic Compare-And-Swap (CAS) write. If the current host version matches `expected_version`, the host replaces the content with `data`, increments the version, and returns `Ok(new_version)`. If another worker has updated the blob in the meantime, it returns `Err(())`.

---

## Shared Memory Use Cases & Code Examples

### Use Case 1: Inter-Instance Global Request Counter & Metrics

**Scenario**: You need to maintain a global request counter across all threads and Wasm worker instances, appending the request sequence number to upstream headers.

```rust
use orion_wasm_sdk::prelude::*;
use orion_wasm_sdk::shared::AtomicU64;
use orion_wasm_sdk::HeaderMutation;
use std::sync::atomic::Ordering;
use tracing::{info, error};

#[derive(Default)]
struct RequestCounterFilter {
    counter: Option<AtomicU64>,
}

#[orion_plugin]
impl Plugin for RequestCounterFilter {
    fn on_plugin_start(&mut self) {
        let _ = orion_wasm_sdk::init_tracing();

        // Resolve or create global shared atomic counter
        match AtomicU64::try_new("global_request_counter") {
            Ok(atomic) => self.counter = Some(atomic),
            Err(e) => error!("Failed to open shared atomic counter: {:?}", e),
        }
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Some(counter) = &self.counter {
            // Atomically increment counter across all worker threads
            let prev = counter.fetch_add(1, Ordering::SeqCst);
            let current = prev + 1;

            info!("Request #{} processed by worker", current);

            // Inject the counter into an HTTP header passed downstream/upstream
            let header_val = http::HeaderValue::from_str(&current.to_string()).unwrap();
            let _ = ctx.apply_header_mutations(&[
                HeaderMutation::Set(
                    http::HeaderName::from_static("x-request-counter"),
                    header_val,
                )
            ]);
        }

        FilterAction::Continue
    }
}
```

---

### Use Case 2: Synchronized Shared State & Dynamic Whitelists (SharedBlob CAS)

**Scenario**: You want to maintain a shared list of unique client IDs seen across all worker instances. Multiple worker threads process requests concurrently, so updates to the shared list must be synchronized using optimistic Compare-And-Swap (CAS).

```rust
use orion_wasm_sdk::prelude::*;
use orion_wasm_sdk::shared::SharedBlob;
use orion_wasm_sdk::HeaderMutation;
use tracing::{info, error};

#[derive(Default)]
struct ClientTrackerFilter {
    blob: Option<SharedBlob>,
}

#[orion_plugin]
impl Plugin for ClientTrackerFilter {
    fn on_plugin_start(&mut self) {
        let _ = orion_wasm_sdk::init_tracing();

        match SharedBlob::try_new("active_clients_blob") {
            Ok(blob) => {
                // Initialize blob with empty data if uninitialized
                let current = blob.read();
                if current.version == 0 && current.data.is_empty() {
                    blob.write(b"");
                }
                self.blob = Some(blob);
            }
            Err(e) => error!("Failed to open active_clients_blob: {:?}", e),
        }
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Some(blob) = &self.blob {
            let client_id = ctx.get_header("x-client-id")
                .ok()
                .flatten()
                .and_then(|v| v.to_str().ok().map(String::from))
                .unwrap_or_else(|| "anonymous".to_string());

            let mut resolved_list = String::new();

            // Retry loop for optimistic concurrency control (CAS)
            loop {
                let current = blob.read();
                let current_str = String::from_utf8_lossy(&current.data);

                // If client_id is already present, no mutation required
                if current_str.split(',').any(|s| s.trim() == client_id) {
                    resolved_list = current_str.to_string();
                    break;
                }

                // Append new client_id
                let new_str = if current_str.is_empty() {
                    client_id.clone()
                } else {
                    format!("{}, {}", current_str, client_id)
                };

                // Attempt optimistic compare-and-swap write using read version
                if blob.compare_and_swap(new_str.as_bytes(), current.version).is_ok() {
                    info!("Client list updated via CAS to version {}", current.version + 1);
                    resolved_list = new_str;
                    break;
                }

                // If CAS failed, another worker updated the blob concurrently — retry loop
            }

            // Expose updated seen clients list upstream
            if let Ok(header_val) = http::HeaderValue::try_from(resolved_list) {
                let _ = ctx.apply_header_mutations(&[
                    HeaderMutation::Set(
                        http::HeaderName::from_static("x-seen-clients"),
                        header_val,
                    )
                ]);
            }
        }

        FilterAction::Continue
    }
}
```

---

### Use Case 3: Distributed Max Concurrent Requests Limiter (Bulkhead)

**Scenario**: You want to enforce a global limit on the number of concurrent requests processed across all worker instances (a bulkhead pattern) to protect upstream services from being overwhelmed.

```rust
use orion_wasm_sdk::prelude::*;
use orion_wasm_sdk::shared::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Default)]
struct MaxInFlightRequestsFilter {
    active_requests: Option<AtomicU64>,
    accepted: bool,
}

const MAX_CONCURRENT_REQUESTS: u64 = 1000;

#[orion_plugin]
impl Plugin for MaxInFlightRequestsFilter {
    fn on_plugin_start(&mut self) {
        if let Ok(atomic) = AtomicU64::try_new("active_request_counter") {
            self.active_requests = Some(atomic);
        }
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Some(counter) = &self.active_requests {
            // Use fetch_update to atomically check and increment the limit
            let res = counter.fetch_update(Ordering::SeqCst, Ordering::Relaxed, |current| {
                if current < MAX_CONCURRENT_REQUESTS {
                    Some(current + 1)
                } else {
                    None // Reached maximum concurrent capacity!
                }
            });

            if res.is_err() {
                // Return 429 Too Many Requests immediately
                return ctx.direct_response(429, b"Server at capacity. Try again later.");
            }
            
            // Mark as accepted so we know to decrement it later
            self.accepted = true;
        }

        FilterAction::Continue
    }

    fn on_transaction_complete(&mut self) {
        // Only decrement if we successfully incremented it
        if self.accepted {
            if let Some(counter) = &self.active_requests {
                counter.fetch_sub(1, Ordering::SeqCst);
            }
            // Reset state for the next transaction handled by this instance
            self.accepted = false;
        }
    }
}
```

---

## Tracing and Logging

The SDK integrates gracefully with the standard Rust `tracing` ecosystem. Instead of relying on `println!` (which wouldn't appear in proxy log files), you can wire `tracing` directly to the host logging system.

To enable it, call `init_tracing()` exactly once in your `on_plugin_start` method.

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
