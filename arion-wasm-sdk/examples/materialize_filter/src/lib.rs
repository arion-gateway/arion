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
use arion_wasm_sdk::{bytes::Bytes, http, FilterAction};

#[derive(Default)]
struct MaterializeFilter;

#[arion_plugin]
impl Plugin for MaterializeFilter {
    fn on_request_headers(&mut self, _ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        FilterAction::PauseAndBufferBody
    }

    fn on_request_body(&mut self, ctx: &RequestHandle<HttpBody>) -> FilterAction {
        if let Ok(mut req) = ctx.take_request() {
            if req.uri().path() == "/transform-me" {
                // Manipulate the fully materialized request
                let mut new_body = req.body().to_vec();
                new_body.extend_from_slice(b" [materialized req]");
                *req.body_mut() = Bytes::from(new_body);

                // Add a header
                req.headers_mut().insert(
                    http::header::HeaderName::from_static("x-materialized-req"),
                    http::header::HeaderValue::from_static("true"),
                );

                // Save it back
                let _ = ctx.replace_request(&req);
            }
        }
        FilterAction::Continue
    }

    fn on_response_headers(&mut self, _ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        FilterAction::PauseAndBufferBody
    }

    fn on_response_body(&mut self, ctx: &ResponseHandle<HttpBody>) -> FilterAction {
        if let Ok(mut res) = ctx.take_response() {
            if res.status() == http::StatusCode::OK {
                // Manipulate the fully materialized response
                let mut new_body = res.body().to_vec();
                new_body.extend_from_slice(b" [materialized res]");
                *res.body_mut() = Bytes::from(new_body);

                // Add a header
                res.headers_mut().insert(
                    http::header::HeaderName::from_static("x-materialized-res"),
                    http::header::HeaderValue::from_static("true"),
                );

                // Save it back
                let _ = ctx.replace_response(&res);
            }
        }
        FilterAction::Continue
    }
}
