use proxy_wasm::traits::*;
use proxy_wasm::types::*;
use std::sync::atomic::{AtomicU32, Ordering};

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Warn);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(CalloutBodyRoot) });
}}

static DISPATCHED: AtomicU32 = AtomicU32::new(0);
static RESPONDED: AtomicU32 = AtomicU32::new(0);

struct CalloutBodyRoot;
impl Context for CalloutBodyRoot {}
impl RootContext for CalloutBodyRoot {
    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::HttpContext)
    }

    fn on_configure(&mut self, _plugin_configuration_size: usize) -> bool {
        true
    }

    fn on_tick(&mut self) {
        let d = DISPATCHED.load(Ordering::Relaxed);
        let r = RESPONDED.load(Ordering::Relaxed);
        log::warn!("callout stats: dispatched={} responded={} pending={}", d, r, d.wrapping_sub(r));
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
    fn on_http_call_response(&mut self, _token_id: u32, _num_headers: usize, body_size: usize, _num_trailers: usize) {
        RESPONDED.fetch_add(1, Ordering::Relaxed);
        if let Some(status) = self.get_http_call_response_header(":status") {
            if !status.starts_with('2') {
                log::error!("Callout returned non-success status");
            }
            let _ = self.get_http_call_response_body(0, body_size);
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
            Ok(_) => {
                DISPATCHED.fetch_add(1, Ordering::Relaxed);
                Action::Pause
            }
            Err(e) => {
                log::error!("dispatch_http_call failed: {:?}", e);
                Action::Continue
            }
        }
    }
}
