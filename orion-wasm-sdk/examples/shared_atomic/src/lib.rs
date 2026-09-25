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

use orion_wasm_sdk::shared::SharedAtomicU64;
use orion_wasm_sdk::{init_tracing, orion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle};
use orion_wasm_sdk::{WasmHeaderName, WasmHeaderValue};
use std::sync::atomic::Ordering;
use tracing::{debug, error};

#[derive(Default)]
struct SharedAtomicFilter {
    counter: Option<SharedAtomicU64>,
}

#[orion_plugin]
impl Plugin for SharedAtomicFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!("SharedAtomicFilter: on_plugin_start");

        match SharedAtomicU64::try_new("request_counter") {
            Ok(atomic) => {
                debug!("Successfully created/opened shared atomic variable 'request_counter'");
                self.counter = Some(atomic);
            },
            Err(e) => {
                error!("Failed to open shared atomic variable: {:?}", e);
            },
        }
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Some(counter) = &self.counter {
            // Increment the shared counter by 1
            let prev = counter.fetch_add(1, Ordering::SeqCst);
            let current = prev + 1;
            debug!("Shared counter incremented! Previous value: {}, New value: {}", prev, current);

            // Set the result as an HTTP header sent to the upstream
            if let Err(e) = ctx.set_header(
                WasmHeaderName::from("x-request-counter"),
                WasmHeaderValue::from(current.to_string().as_str()),
            ) {
                error!("Failed to set x-request-counter header: {:?}", e);
            }
        } else {
            error!("Shared counter is not initialized!");
        }

        FilterAction::Continue
    }
}
