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

use bytes::Bytes;
use http::Response;
use orion_wasm_sdk::prelude::*;
use orion_wasm_sdk::FilterAction;

#[derive(Default)]
struct DirectResponseFilter;

#[orion_plugin]
impl Plugin for DirectResponseFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Ok(Some(_)) = ctx.get_header("x-trigger-direct") {
            let builder = Response::builder().status(403).header("x-custom-response-header", "was-intercepted");

            let response = builder.body(Bytes::from("Intercepted by Wasm Direct Response!")).unwrap();

            return ctx.direct_response(response);
        }
        FilterAction::Continue
    }
}
