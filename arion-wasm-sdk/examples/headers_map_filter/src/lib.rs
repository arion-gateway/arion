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

//! Example using the Arion Wasm SDK to mutate HTTP headers.
use arion_wasm_sdk::{bytes, http, init_tracing, arion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle};
use tracing::{error, debug};

#[derive(Default)]
struct HeadersMapFilter;

#[arion_plugin]
impl Plugin for HeadersMapFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!(version = "1.0", "HeadersMapFilter Wasm: Instance initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        debug!("on_request_headers called");
        let mut headers = match ctx.get_headers_map() {
            Ok(h) => h,
            Err(e) => {
                error!("get_headers_map failed: {:?}", e);
                return ctx.direct_response(
                    http::Response::builder()
                        .status(500)
                        .body(bytes::Bytes::from_static(b"Internal Server Error"))
                        .unwrap(),
                );
            },
        };

        // 1. Remove user-agent
        headers.remove(http::header::HeaderName::from_static("user-agent"));

        // 2. Insert (Set/Replace) x-custom-set
        headers.insert(
            http::header::HeaderName::from_static("x-custom-set"),
            http::header::HeaderValue::from_static("replaced-value"),
        );

        // 3. Append (Add) x-custom-add
        headers.append(
            http::header::HeaderName::from_static("x-custom-add"),
            http::header::HeaderValue::from_static("add-value"),
        );

        if let Err(e) = ctx.set_headers_map(&headers) {
            error!("Failed to set headers map: {:?}", e);
            return ctx.direct_response(
                http::Response::builder()
                    .status(500)
                    .body(bytes::Bytes::from_static(b"Internal Server Error"))
                    .unwrap(),
            );
        }

        FilterAction::Continue
    }

    fn on_response_headers(&mut self, ctx: &arion_wasm_sdk::ResponseHandle<HttpHeaders>) -> FilterAction {
        debug!("on_response_headers called");
        let mut headers = match ctx.get_headers_map() {
            Ok(h) => h,
            Err(e) => {
                error!("get_headers_map failed: {:?}", e);
                return FilterAction::Continue;
            },
        };

        // 1. Remove x-response-remove
        headers.remove(http::header::HeaderName::from_static("x-response-remove"));

        // 2. Insert (Set/Replace) x-response-set
        headers.insert(
            http::header::HeaderName::from_static("x-response-set"),
            http::header::HeaderValue::from_static("res-replaced-value"),
        );

        // 3. Append (Add) x-response-add
        headers.append(
            http::header::HeaderName::from_static("x-response-add"),
            http::header::HeaderValue::from_static("res-add-value"),
        );

        if let Err(e) = ctx.set_headers_map(&headers) {
            error!("Failed to set response headers map: {:?}", e);
        }

        FilterAction::Continue
    }
}
