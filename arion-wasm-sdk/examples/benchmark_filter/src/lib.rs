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

use arion_wasm_sdk::prelude::*;
use arion_wasm_sdk::{bytes, http, FilterAction};

#[derive(Default)]
struct BenchmarkFilter;

#[arion_plugin]
impl Plugin for BenchmarkFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        match ctx.get_header("Authorization") {
            Ok(Some(_)) => FilterAction::Continue,
            _ => ctx.direct_response(
                http::Response::builder().status(401).body(bytes::Bytes::from_static(b"Unauthorized")).unwrap(),
            ),
        }
    }
}
