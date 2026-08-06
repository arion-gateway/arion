use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Info);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(SharedAtomicRoot) });
}}

const COUNTER_KEY: &str = "request_counter";

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
        // Retry loop for CAS: get current value, increment, write back with the CAS token.
        // set_shared_data returns Err(Status::CasMismatch) if another VM raced us.
        let current = loop {
            let (data, cas) = self.get_shared_data(COUNTER_KEY);
            let prev = data
                .as_deref()
                .and_then(|b| b.try_into().ok())
                .map(u64::from_be_bytes)
                .unwrap_or(0);
            let next = prev + 1;
            match self.set_shared_data(COUNTER_KEY, Some(&next.to_be_bytes()), cas) {
                Ok(()) => break next,
                Err(Status::CasMismatch) => continue,
                Err(_) => break prev,
            }
        };
        self.set_http_request_header("x-request-counter", Some(&current.to_string()));
        Action::Continue
    }
}
