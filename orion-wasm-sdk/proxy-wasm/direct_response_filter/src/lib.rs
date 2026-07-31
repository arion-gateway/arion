use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Trace);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(DirectResponseRoot) });
}}

struct DirectResponseRoot;
impl Context for DirectResponseRoot {}
impl RootContext for DirectResponseRoot {
    fn create_http_context(&self, _context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(DirectResponseFilter))
    }
}

struct DirectResponseFilter;
impl Context for DirectResponseFilter {}
impl HttpContext for DirectResponseFilter {
    fn on_http_request_headers(&mut self, _num_headers: usize, _end_of_stream: bool) -> Action {
        if self.get_http_request_header("x-trigger-direct").is_some() {
            self.send_http_response(
                403,
                vec![("x-custom-response-header", "was-intercepted")],
                Some(b"Intercepted by Wasm Direct Response!"),
            );
            return Action::Pause;
        }
        Action::Continue
    }
}
