//! Dummy example using the Orion Wasm SDK.
//!
//! This plugin demonstrates the high-level `Plugin` trait API: the user
//! implements `Plugin` with idiomatic Rust methods returning `FilterAction`,
//! and the `orion_plugin!` macro generates the `extern "C"` entry points the
//! Orion host imports.
//!
//! Behaviour:
//!
//! - `on_request_headers` checks the `Authorization` header:
//!     * `Bearer secret-token`  → [`FilterAction::Continue`] (let the request
//!       through).
//!     * `Bearer buffer-me`     → [`FilterAction::PauseAndBufferBody`]; the
//!       host will buffer the body and call `on_request_body`.
//!     * anything else / missing → `401` direct response via
//!       [`direct_response`].
//!
//! - `on_request_body` reads the body via [`get_request_body`] and, if it
//!   equals `b"unblock"`, lets the request continue; otherwise it sends a
//!   `403` direct response.

use orion_wasm_sdk::{
    direct_response, get_http_request_body, get_http_request_header, orion_plugin, FilterAction,
    Plugin,
};

#[derive(Default)]
struct DummyFilter;

impl Plugin for DummyFilter {
    fn on_request_headers(&mut self, request_handle: u64) -> FilterAction {
        let auth = match get_http_request_header(request_handle, "Authorization") {
            Ok(Some(value)) => value,
            Ok(None) => return direct_response(request_handle, 401, b"401 Unauthorized: missing Authorization header"),
            Err(_) => return direct_response(request_handle, 500, b"500 Internal Server Error"),
        };

        if auth == "Bearer secret-token" {
            // Authorized — let the request continue through the filter chain.
            FilterAction::Continue
        } else if auth == "Bearer buffer-me" {
            // Ask the host to buffer the body and invoke `on_request_body`.
            FilterAction::PauseAndBufferBody
        } else {
            direct_response(request_handle, 401, b"401 Unauthorized: invalid credentials")
        }
    }

    fn on_request_body(&mut self, request_handle: u64) -> FilterAction {
        let body = match get_http_request_body(request_handle) {
            Ok(bytes) => bytes,
            Err(_) => return direct_response(request_handle, 500, b"500 Internal Server Error"),
        };

        if body.as_slice() == b"unblock" {
            FilterAction::Continue
        } else {
            direct_response(request_handle, 403, b"403 Forbidden: body did not contain the magic word")
        }
    }
}

orion_plugin!(DummyFilter);
