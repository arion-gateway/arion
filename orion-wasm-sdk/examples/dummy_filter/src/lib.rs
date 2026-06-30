//! Dummy example using the Orion Wasm SDK.
//!
//! This plugin demonstrates the high-level `Plugin` trait API: the user
//! implements `Plugin` with idiomatic Rust methods returning `FilterAction`,
//! and the `orion_plugin!` macro generates the `extern "C"` entry points the
//! Orion host imports.

use orion_wasm_sdk::{
    orion_plugin, FilterAction, Plugin, RequestBody, RequestHandle, RequestHeaders,
};

#[derive(Default)]
struct DummyFilter;

impl Plugin for DummyFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<RequestHeaders>) -> FilterAction {
        let auth = match ctx.get_header("Authorization") {
            Ok(Some(value)) => value,
            Ok(None) => return ctx.direct_response(401, b"401 Unauthorized: missing Authorization header"),
            Err(_) => return ctx.direct_response(500, b"500 Internal Server Error"),
        };

        if auth == "Bearer secret-token" {
            // Authorized — let the request continue through the filter chain.
            FilterAction::Continue
        } else if auth == "Bearer buffer-me" {
            // Ask the host to buffer the body and invoke `on_request_body`.
            FilterAction::PauseAndBufferBody
        } else {
            ctx.direct_response(401, b"401 Unauthorized: invalid credentials")
        }
    }

    fn on_request_body(&mut self, ctx: &RequestHandle<RequestBody>) -> FilterAction {
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
}

orion_plugin!(DummyFilter);
