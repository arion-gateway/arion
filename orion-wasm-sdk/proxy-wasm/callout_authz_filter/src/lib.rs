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

impl Context for CalloutBodyFilter {
    fn on_http_call_response(&mut self, _token_id: u32, _num_headers: usize, body_size: usize, num_trailers: usize) {
        let headers = self.get_http_call_response_headers();

        let authorized = headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("x-authorized"))
            .map(|(_, v)| v == "true")
            .unwrap_or(false);

        if body_size > 0 {
            let _ = self.get_http_call_response_body(0, body_size);
        }
        if num_trailers > 0 {
            let _ = self.get_http_call_response_trailers();
        }

        if authorized {
            self.resume_http_request();
        } else {
            self.send_http_response(403, vec![], Some(b"Forbidden"));
        }
    }
}

impl HttpContext for CalloutBodyFilter {
    fn on_http_request_headers(&mut self, _num_headers: usize, _end_of_stream: bool) -> Action {
        let request_headers = self.get_http_request_headers();
        let mut callout_headers: Vec<(&str, &str)> = vec![
            (":method", "GET"),
            (":path", "/auth"),
            (":authority", "service"),
        ];
        for (k, v) in &request_headers {
            if !k.starts_with(':') {
                callout_headers.push((k.as_str(), v.as_str()));
            }
        }

        match self.dispatch_http_call(
            "service",
            callout_headers,
            None,
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
