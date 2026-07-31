use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Info);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(CalloutBodyRoot) });
}}

struct CalloutBodyRoot;
impl Context for CalloutBodyRoot {}
impl RootContext for CalloutBodyRoot {
    fn create_http_context(&self, _context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(CalloutBodyFilter))
    }
}

struct CalloutBodyFilter;
impl Context for CalloutBodyFilter {}
impl HttpContext for CalloutBodyFilter {
    fn on_http_request_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        if !end_of_stream {
            return Action::Pause;
        }
        let body = self.get_http_request_body(0, body_size).unwrap_or_default();
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
