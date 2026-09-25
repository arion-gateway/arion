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

/// Example using the Arion Wasm SDK to mutate HTTP headers natively using the batch API.
use arion_wasm_sdk::{
    init_tracing, arion_plugin, FilterAction, HeaderMutation, HttpHeaders, Plugin, RequestHandle, ResponseHandle,
};
use arion_wasm_sdk::{WasmHeaderName, WasmHeaderValue};
use tracing::{error, debug};

#[derive(Default)]
struct HeaderMutationsFilter;

#[arion_plugin]
impl Plugin for HeaderMutationsFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!(version = "1.0", "HeaderMutationsFilter Wasm: Instance initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        debug!("--- Processing Request Headers with Batch API ---");

        let mutations = [
            HeaderMutation::Set(WasmHeaderName::from("x-custom-set"), WasmHeaderValue::from("set-value")),
            HeaderMutation::Add(WasmHeaderName::from("x-custom-add"), WasmHeaderValue::from("add-value")),
            HeaderMutation::Remove(WasmHeaderName::from("user-agent")),
            HeaderMutation::Replace(WasmHeaderName::from("x-custom-set"), WasmHeaderValue::from("replaced-value")),
        ];

        if let Err(e) = ctx.apply_header_mutations(&mutations) {
            error!("Failed to apply request header mutations: {:?}", e);
        }

        FilterAction::Continue
    }

    fn on_response_headers(&mut self, ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        debug!("--- Processing Response Headers with Batch API ---");

        let mutations = [
            HeaderMutation::Set(WasmHeaderName::from("x-response-set"), WasmHeaderValue::from("res-set-value")),
            HeaderMutation::Add(WasmHeaderName::from("x-response-add"), WasmHeaderValue::from("res-add-value")),
            HeaderMutation::Remove(WasmHeaderName::from("x-response-remove")),
            HeaderMutation::Replace(
                WasmHeaderName::from("x-response-set"),
                WasmHeaderValue::from("res-replaced-value"),
            ),
        ];

        if let Err(e) = ctx.apply_header_mutations(&mutations) {
            error!("Failed to apply response header mutations: {:?}", e);
        }

        FilterAction::Continue
    }
}
