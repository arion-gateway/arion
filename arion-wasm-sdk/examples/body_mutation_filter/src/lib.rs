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

//! Example using the Arion Wasm SDK to mutate HTTP bodies natively.
use arion_wasm_sdk::{init_tracing, arion_plugin, FilterAction, HttpBody, Plugin, RequestHandle, ResponseHandle};
use tracing::debug;

#[derive(Default)]
struct BodyMutationFilter {
    req_action: String,
    res_action: String,
}

#[arion_plugin]
impl Plugin for BodyMutationFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!(version = "1.0", "BodyMutationFilter Wasm: Instance initialized!");
    }

    fn on_request_headers(&mut self, ctx: &arion_wasm_sdk::RequestHandle<arion_wasm_sdk::HttpHeaders>) -> FilterAction {
        if let Ok(Some(action)) = ctx.get_header("x-req-mutation") {
            self.req_action = action.to_str().unwrap_or("").to_string();
        }
        if let Ok(Some(action)) = ctx.get_header("x-res-mutation") {
            self.res_action = action.to_str().unwrap_or("").to_string();
        }
        FilterAction::PauseAndBufferBody
    }

    fn on_request_body(&mut self, ctx: &RequestHandle<HttpBody>) -> FilterAction {
        if let Ok(body_bytes) = ctx.get_body() {
            let mut new_body = body_bytes.to_vec();
            match self.req_action.as_str() {
                "append" => new_body.extend_from_slice(b" [appended]"),
                "prepend" => {
                    new_body = b"[prepended] ".to_vec();
                    new_body.extend_from_slice(&body_bytes);
                },
                "replace" => new_body = b"[replaced]".to_vec(),
                _ => {}, // no mutation
            }
            if self.req_action != "" {
                let _ = ctx.set_body(&new_body);
            }
        }
        FilterAction::Continue
    }

    fn on_response_body(&mut self, ctx: &ResponseHandle<HttpBody>) -> FilterAction {
        if let Ok(body_bytes) = ctx.get_body() {
            let mut new_body = body_bytes.to_vec();
            match self.res_action.as_str() {
                "append" => new_body.extend_from_slice(b" [appended]"),
                "prepend" => {
                    new_body = b"[prepended] ".to_vec();
                    new_body.extend_from_slice(&body_bytes);
                },
                "replace" => new_body = b"[replaced]".to_vec(),
                _ => {}, // no mutation
            }
            if self.res_action != "" {
                let _ = ctx.set_body(&new_body);
            }
        }
        FilterAction::Continue
    }
}
