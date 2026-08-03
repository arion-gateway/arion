use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Info);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(CalloutBodyRoot) });
}}

struct CalloutBodyRoot;
impl Context for CalloutBodyRoot {}
impl RootContext for CalloutBodyRoot {
    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::HttpContext)
    }

    fn on_configure(&mut self, _plugin_configuration_size: usize) -> bool {
        log::info!("CalloutBodyFilter Wasm: Instance initialized!");
        true
    }

    fn create_http_context(&self, _context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(CalloutBodyFilter))
    }
}

struct CalloutBodyFilter;

impl Context for CalloutBodyFilter {
    fn on_http_call_response(&mut self, _token_id: u32, _num_headers: usize, body_size: usize, _num_trailers: usize) {
        if let Some(status) = self.get_http_call_response_header(":status") {
            if status.starts_with('2') {
                let new_body = self.get_http_call_response_body(0, body_size).unwrap_or_default();
                self.set_http_request_body(0, new_body.len(), &new_body);
            } else {
                log::error!("Callout returned non-success status, keeping original body");
            }
        }
        self.resume_http_request();
    }
}

impl HttpContext for CalloutBodyFilter {
    fn on_http_request_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        if !end_of_stream {
            return Action::Pause;
        }
        log::info!("--- Processing Request Body with Callout ---");
        let body = self.get_http_request_body(0, body_size).unwrap_or_default();
        log::info!("Dispatching HTTP POST call to cluster 'service'...");
        self.dispatch_http_call(
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
        ).unwrap();
        Action::Pause
    }
}
