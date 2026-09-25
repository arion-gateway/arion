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

//! Example using the Arion Wasm SDK to mutate HTTP headers natively.
use arion_wasm_sdk::{init_tracing, arion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle, ResponseHandle};
use tracing::{error, debug};

#[derive(Default)]
struct HeaderApiFilter;

#[arion_plugin]
impl Plugin for HeaderApiFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!(version = "1.0", "HeaderApiFilter Wasm: Instance initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        debug!("--- Processing Request Headers ---");

        // 1. Set a new header (or replace if it exists)
        if let Err(e) = ctx.set_header(
            http::header::HeaderName::from_static("x-custom-set"),
            http::header::HeaderValue::from_static("set-value"),
        ) {
            error!("Failed to set header: {:?}", e);
        }

        // 2. Add a header (appends to existing)
        if let Err(e) = ctx.add_header(
            http::header::HeaderName::from_static("x-custom-add"),
            http::header::HeaderValue::from_static("add-value"),
        ) {
            error!("Failed to add header: {:?}", e);
        }

        // 3. Remove a header
        if let Err(e) = ctx.remove_header("user-agent") {
            error!("Failed to remove header: {:?}", e);
        }

        // 4. Replace an existing header (only if it exists)
        if let Err(e) = ctx.replace_header(
            http::header::HeaderName::from_static("x-custom-set"),
            http::header::HeaderValue::from_static("replaced-value"),
        ) {
            error!("Failed to replace header: {:?}", e);
        }

        FilterAction::Continue
    }

    fn on_response_headers(&mut self, ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        debug!("--- Processing Response Headers ---");

        // 1. Set a response header
        if let Err(e) = ctx.set_header(
            http::header::HeaderName::from_static("x-response-set"),
            http::header::HeaderValue::from_static("res-set-value"),
        ) {
            error!("Failed to set response header: {:?}", e);
        }

        // 2. Add a response header (appends)
        if let Err(e) = ctx.add_header(
            http::header::HeaderName::from_static("x-response-add"),
            http::header::HeaderValue::from_static("res-add-value"),
        ) {
            error!("Failed to add response header: {:?}", e);
        }

        // 3. Remove a response header
        if let Err(e) = ctx.remove_header("x-response-remove") {
            error!("Failed to remove response header: {:?}", e);
        }

        // 4. Replace an existing response header
        if let Err(e) = ctx.replace_header(
            http::header::HeaderName::from_static("x-response-set"),
            http::header::HeaderValue::from_static("res-replaced-value"),
        ) {
            error!("Failed to replace response header: {:?}", e);
        }

        FilterAction::Continue
    }
}
