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
