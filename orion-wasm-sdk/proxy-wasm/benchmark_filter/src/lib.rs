use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Trace);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(BenchmarkRoot) });
}}

struct BenchmarkRoot;

impl Context for BenchmarkRoot {}

impl RootContext for BenchmarkRoot {
    fn create_http_context(&self, _context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(BenchmarkFilter))
    }
}

struct BenchmarkFilter;

impl Context for BenchmarkFilter {}

impl HttpContext for BenchmarkFilter {
    fn on_http_request_headers(&mut self, _num_headers: usize, _end_of_stream: bool) -> Action {
        match self.get_http_request_header("Authorization") {
            Some(_) => Action::Continue,
            None => {
                self.send_http_response(
                    401,
                    vec![],
                    Some(b"Unauthorized"),
                );
                Action::Pause
            }
        }
    }
}
