use proxy_wasm::traits::*;
use proxy_wasm::types::*;
use std::sync::atomic::{AtomicUsize, Ordering};

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Info);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(SharedAtomicRoot) });
}}

static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct SharedAtomicRoot;
impl Context for SharedAtomicRoot {}
impl RootContext for SharedAtomicRoot {
    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::HttpContext)
    }

    fn create_http_context(&self, _context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(SharedAtomicFilter))
    }
}

struct SharedAtomicFilter;
impl Context for SharedAtomicFilter {}
impl HttpContext for SharedAtomicFilter {
    fn on_http_request_headers(&mut self, _num_headers: usize, _end_of_stream: bool) -> Action {
        let prev = COUNTER.fetch_add(1, Ordering::SeqCst);
        let current = prev + 1;
        self.set_http_request_header("x-request-counter", Some(&current.to_string()));
        Action::Continue
    }
}
