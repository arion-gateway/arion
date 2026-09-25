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

use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Info);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(HeaderMutationsRoot) });
}}

struct HeaderMutationsRoot;
impl Context for HeaderMutationsRoot {}
impl RootContext for HeaderMutationsRoot {
    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::HttpContext)
    }

    fn create_http_context(&self, _context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(HeaderMutationsFilter))
    }
}

struct HeaderMutationsFilter;
impl Context for HeaderMutationsFilter {}
impl HttpContext for HeaderMutationsFilter {
    fn on_http_request_headers(&mut self, _num_headers: usize, _end_of_stream: bool) -> Action {
        self.set_http_request_header("x-custom-set", Some("set-value"));
        self.add_http_request_header("x-custom-add", "add-value");
        self.set_http_request_header("user-agent", None);
        self.set_http_request_header("x-custom-set", Some("replaced-value"));
        Action::Continue
    }
    fn on_http_response_headers(&mut self, _num_headers: usize, _end_of_stream: bool) -> Action {
        self.set_http_response_header("x-response-set", Some("res-set-value"));
        self.add_http_response_header("x-response-add", "res-add-value");
        self.set_http_response_header("x-response-remove", None);
        self.set_http_response_header("x-response-set", Some("res-replaced-value"));
        Action::Continue
    }
}
