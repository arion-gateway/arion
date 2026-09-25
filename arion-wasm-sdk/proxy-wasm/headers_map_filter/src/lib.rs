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
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(HeadersMapRoot) });
}}

struct HeadersMapRoot;
impl Context for HeadersMapRoot {}
impl RootContext for HeadersMapRoot {
    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::HttpContext)
    }

    fn create_http_context(&self, _context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(HeadersMapFilter))
    }
}

struct HeadersMapFilter;
impl Context for HeadersMapFilter {}
impl HttpContext for HeadersMapFilter {
    fn on_http_request_headers(&mut self, _num_headers: usize, _end_of_stream: bool) -> Action {
        let mut headers: Vec<(String, String)> = self.get_http_request_headers()
            .into_iter()
            .filter(|(k, _)| k != "user-agent")
            .collect();
        headers.push(("x-custom-set".to_string(), "replaced-value".to_string()));
        headers.push(("x-custom-add".to_string(), "add-value".to_string()));
        self.set_http_request_headers(headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect::<Vec<_>>());
        Action::Continue
    }
    fn on_http_response_headers(&mut self, _num_headers: usize, _end_of_stream: bool) -> Action {
        let mut headers: Vec<(String, String)> = self.get_http_response_headers()
            .into_iter()
            .filter(|(k, _)| k != "x-response-remove")
            .collect();
        headers.push(("x-response-set".to_string(), "res-replaced-value".to_string()));
        headers.push(("x-response-add".to_string(), "res-add-value".to_string()));
        self.set_http_response_headers(headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect::<Vec<_>>());
        Action::Continue
    }
}
