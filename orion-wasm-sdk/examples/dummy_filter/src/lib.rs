//! Dummy example using the Orion Wasm SDK.
//!
//! This plugin demonstrates the high-level `Plugin` trait API: the user
//! implements `Plugin` with idiomatic Rust methods returning `FilterAction`,
//! and the `#[orion_plugin]` procedural macro generates the `extern "C"` entry points the
//! Orion host imports.

use orion_wasm_sdk::{init_tracing, orion_plugin, FilterAction, HttpBody, HttpHeaders, Plugin, RequestHandle};
use tracing::{debug, error, warn};

#[derive(Default)]
struct DummyFilter;

#[orion_plugin]
impl Plugin for DummyFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!(version = "1.0", "DummyFilter Wasm: on_plugin_start - Instance initialized!");
    }

    fn on_plugin_destroy(&mut self) {
        debug!(version = "1.0", "DummyFilter Wasm: on_plugin_destroy - Instance destroyed!");
    }

    fn on_transaction_start(&mut self) {
        debug!(version = "1.0", "DummyFilter Wasm: on_transaction_start - New HTTP Request!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        let auth = match ctx.get_header("Authorization") {
            Ok(Some(value)) => {
                debug!("Header Authorization received: {:?}", value);
                value
            },
            Ok(None) => {
                warn!("No header Authorization provided!");
                return ctx.direct_response(401, b"401 Unauthorized: missing Authorization header");
            },
            Err(_) => {
                error!("Could not read from HTTP headers");
                return ctx.direct_response(500, b"500 Internal Server Error");
            },
        };

        if auth == "Bearer secret-token" {
            // Authorized — let the request continue through the filter chain.
            debug!(version = "1.0", "DummyFilter: continue....");
            FilterAction::Continue
        } else if auth == "Bearer buffer-me" {
            // Ask the host to buffer the body and invoke `on_request_body`.
            debug!(version = "1.0", "DummyFilter: pause and buffer body....");
            FilterAction::PauseAndBufferBody
        } else {
            ctx.direct_response(401, b"401 Unauthorized: invalid credentials")
        }
    }

    fn on_request_body(&mut self, ctx: &RequestHandle<HttpBody>) -> FilterAction {
        let body = match ctx.get_body() {
            Ok(bytes) => bytes,
            Err(_) => return ctx.direct_response(500, b"500 Internal Server Error"),
        };

        if body.as_slice() == b"valid" {
            FilterAction::Continue
        } else {
            ctx.direct_response(403, b"403 Forbidden: body did not contain the magic word 'valid'")
        }
    }

    fn on_transaction_complete(&mut self) {
        debug!(version = "1.0", "DummyFilter Wasm: on_transaction_complete - Request finished.");
    }
}
