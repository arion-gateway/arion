use crate::{
    listeners::http_filters::{FilterDecision, FilterFactory},
    OrionRequestBody, OrionResponseBody,
};
use http::{Request, Response};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::wasm::WasmConfig;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct WasmFilterInner {
    pub config: WasmConfig,
}

#[derive(Debug, Clone)]
pub struct WasmFilter {
    inner: Arc<WasmFilterInner>,
}

impl WasmFilter {
    pub fn new(config: WasmConfig) -> Self {
        Self { inner: Arc::new(WasmFilterInner { config }) }
    }

    pub fn apply_request(&mut self, _req: &mut Request<OrionRequestBody>) -> FilterDecision {
        FilterDecision::Continue
    }

    pub fn apply_response(&mut self, _res: &mut Response<OrionResponseBody>) -> FilterDecision {
        FilterDecision::Continue
    }
}

impl FilterFactory for WasmFilter {
    fn new_from(&self) -> Self {
        self.clone()
    }
}
