// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Dummy example using the Arion Wasm SDK.
//!
//! This plugin demonstrates the high-level `Plugin` trait API: the user
//! implements `Plugin` with idiomatic Rust methods returning `FilterAction`,
//! and the `#[arion_plugin]` procedural macro generates the `extern "C"` entry points the
//! Arion host imports.

use arion_wasm_sdk::{
    bytes, http, init_tracing, arion_plugin, FilterAction, HttpBody, HttpHeaders, Plugin, RequestHandle,
};
use tracing::{debug, error, warn};

#[derive(Default)]
struct DummyFilter;

#[arion_plugin]
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
                return ctx.direct_response(
                    http::Response::builder()
                        .status(401)
                        .body(bytes::Bytes::from_static(b"401 Unauthorized: missing Authorization header"))
                        .unwrap(),
                );
            },
            Err(_) => {
                error!("Could not read from HTTP headers");
                return ctx.direct_response(
                    http::Response::builder()
                        .status(500)
                        .body(bytes::Bytes::from_static(b"500 Internal Server Error"))
                        .unwrap(),
                );
            },
        };

        if auth.to_str().unwrap_or_default() == "Bearer secret-token" {
            // Authorized — let the request continue through the filter chain.
            debug!(version = "1.0", "DummyFilter: continue....");
            FilterAction::Continue
        } else if auth.to_str().unwrap_or_default() == "Bearer buffer-me" {
            // Ask the host to buffer the body and invoke `on_request_body`.
            debug!(version = "1.0", "DummyFilter: pause and buffer body....");
            FilterAction::PauseAndBufferBody
        } else {
            ctx.direct_response(
                http::Response::builder()
                    .status(401)
                    .body(bytes::Bytes::from_static(b"401 Unauthorized: invalid credentials"))
                    .unwrap(),
            )
        }
    }

    fn on_request_body(&mut self, ctx: &RequestHandle<HttpBody>) -> FilterAction {
        let body = match ctx.get_body() {
            Ok(bytes) => bytes,
            Err(_) => {
                return ctx.direct_response(
                    http::Response::builder()
                        .status(500)
                        .body(bytes::Bytes::from_static(b"500 Internal Server Error"))
                        .unwrap(),
                )
            },
        };

        if body.as_ref() == b"valid" {
            FilterAction::Continue
        } else {
            ctx.direct_response(
                http::Response::builder()
                    .status(403)
                    .body(bytes::Bytes::from_static(b"403 Forbidden: body did not contain the magic word 'valid'"))
                    .unwrap(),
            )
        }
    }

    fn on_transaction_complete(&mut self) {
        debug!(version = "1.0", "DummyFilter Wasm: on_transaction_complete - Request finished.");
    }
}
