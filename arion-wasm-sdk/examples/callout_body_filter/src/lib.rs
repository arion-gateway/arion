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

//! Example using the Arion Wasm SDK to perform an HTTP Callout and replace the request body.
use arion_wasm_sdk::{
    dispatch_http_call, http::HeaderMap, http::Method, init_tracing, arion_plugin, FilterAction, HttpBody, Plugin,
    RequestHandle,
};
use tracing::{error, debug};

#[derive(Default)]
struct CalloutBodyFilter;

#[arion_plugin]
impl Plugin for CalloutBodyFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!(version = "1.0", "CalloutBodyFilter Wasm: Instance initialized!");
    }

    fn on_request_body(&mut self, ctx: &RequestHandle<HttpBody>) -> FilterAction {
        debug!("--- Processing Request Body with Callout ---");

        // 1. Get the original body to send to the external service
        let original_body = match ctx.get_body() {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to get request body: {:?}", e);
                return FilterAction::Continue;
            },
        };

        // 2. Prepare the callout request
        let mut callout_headers = HeaderMap::new();
        callout_headers.insert("x-callout-id", "wasm-plugin-123".parse().unwrap());
        callout_headers.insert("accept", "application/json".parse().unwrap());

        let req = http::Request::builder()
            .method(Method::POST)
            .uri("/")
            .header("x-callout-id", "wasm-plugin-123")
            .header("accept", "application/json")
            .header("host", "service")
            .body(bytes::Bytes::from(original_body))
            .unwrap();

        // 3. Perform the synchronous-looking asynchronous callout
        debug!("Dispatching HTTP POST call to cluster 'service'...");
        match dispatch_http_call("service", req) {
            Ok(response) => {
                debug!("Callout completed with status: {}", response.status());

                if response.status().is_success() {
                    // 4. Extract the body from the callout response and replace the original request body
                    let new_body = response.into_body().to_vec();

                    if let Err(e) = ctx.set_body(&new_body) {
                        error!("Failed to replace the request body: {:?}", e);
                    } else {
                        debug!("Successfully replaced request body with callout response! ({} bytes)", new_body.len());
                    }
                } else {
                    error!("Callout returned non-success status, keeping original body");
                }
            },
            Err(e) => {
                error!("Failed to execute callout: {:?}", e);
            },
        }

        FilterAction::Continue
    }
}
