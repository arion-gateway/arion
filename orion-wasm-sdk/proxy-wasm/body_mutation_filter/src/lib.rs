use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Info);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(BodyMutationRoot) });
}}

struct BodyMutationRoot;
impl Context for BodyMutationRoot {}
impl RootContext for BodyMutationRoot {
    fn on_configure(&mut self, _plugin_configuration_size: usize) -> bool {
        log::info!("BodyMutationFilter Wasm: Instance initialized!");
        true
    }
    fn create_http_context(&self, _context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(BodyMutationFilter::default()))
    }
}

#[derive(Default)]
struct BodyMutationFilter {
    req_action: String,
    res_action: String,
}

impl Context for BodyMutationFilter {}
impl HttpContext for BodyMutationFilter {
    fn on_http_request_headers(&mut self, _num_headers: usize, _end_of_stream: bool) -> Action {
        if let Some(action) = self.get_http_request_header("x-req-mutation") {
            self.req_action = action;
        }
        if let Some(action) = self.get_http_request_header("x-res-mutation") {
            self.res_action = action;
        }
        Action::Continue
    }

    fn on_http_request_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        if !end_of_stream {
            return Action::Pause;
        }
        if let Some(body_bytes) = self.get_http_request_body(0, body_size) {
            let mut new_body = body_bytes.clone();
            match self.req_action.as_str() {
                "append" => new_body.extend_from_slice(b" [appended]"),
                "prepend" => {
                    new_body = b"[prepended] ".to_vec();
                    new_body.extend_from_slice(&body_bytes);
                },
                "replace" => new_body = b"[replaced]".to_vec(),
                _ => {},
            }
            if !self.req_action.is_empty() {
                self.set_http_request_body(0, body_bytes.len(), &new_body);
            }
        }
        Action::Continue
    }

    fn on_http_response_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        if !end_of_stream {
            return Action::Pause;
        }
        if let Some(body_bytes) = self.get_http_response_body(0, body_size) {
            let mut new_body = body_bytes.clone();
            match self.res_action.as_str() {
                "append" => new_body.extend_from_slice(b" [appended]"),
                "prepend" => {
                    new_body = b"[prepended] ".to_vec();
                    new_body.extend_from_slice(&body_bytes);
                },
                "replace" => new_body = b"[replaced]".to_vec(),
                _ => {},
            }
            if !self.res_action.is_empty() {
                self.set_http_response_body(0, body_bytes.len(), &new_body);
            }
        }
        Action::Continue
    }
}
