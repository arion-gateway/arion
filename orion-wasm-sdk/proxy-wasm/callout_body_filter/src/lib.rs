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
    proxy_wasm::set_log_level(LogLevel::Warn);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(CalloutBodyRoot) });
}}

struct CalloutBodyRoot;
impl Context for CalloutBodyRoot {}
impl RootContext for CalloutBodyRoot {
    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::HttpContext)
    }

    fn on_configure(&mut self, _plugin_configuration_size: usize) -> bool {
        true
    }

    fn create_http_context(&self, _context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(CalloutBodyFilter))
    }
}

struct CalloutBodyFilter;

// ngx_wasm_module does not re-invoke on_http_request_body after resume_http_request(),
// so body replacement from the callout response is not possible here. The callout is
// still performed to preserve the latency and throughput characteristics of the filter.
impl Context for CalloutBodyFilter {
    fn on_http_call_response(&mut self, _token_id: u32, _num_headers: usize, body_size: usize, num_trailers: usize) {
        // Consume headers to prevent memory leaks in the host (ngx_wasm_module)
        let _ = self.get_http_call_response_headers();
        if let Some(status) = self.get_http_call_response_header(":status") {
            if !status.starts_with('2') {
                log::error!("Callout returned non-success status");
            }
        }

        // Always consume the body to prevent memory leaks
        if body_size > 0 {
            let _ = self.get_http_call_response_body(0, body_size);
        }

        // Consume trailers if any
        if num_trailers > 0 {
            let _ = self.get_http_call_response_trailers();
        }

        self.resume_http_request();
    }
}

impl HttpContext for CalloutBodyFilter {
    fn on_http_request_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        if !end_of_stream {
            return Action::Pause;
        }
        let body = self.get_http_request_body(0, body_size).unwrap_or_default();
        match self.dispatch_http_call(
            "service",
            vec![
                (":method", "POST"),
                (":path", "/"),
                (":authority", "service"),
                ("x-callout-id", "wasm-plugin-123"),
                ("accept", "application/json"),
            ],
            Some(&body),
            vec![],
            std::time::Duration::from_secs(5),
        ) {
            Ok(_) => Action::Pause,
            Err(e) => {
                log::error!("dispatch_http_call failed: {:?}", e);
                Action::Continue
            }
        }
    }
}
