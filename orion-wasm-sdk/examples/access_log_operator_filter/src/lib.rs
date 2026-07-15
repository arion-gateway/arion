use orion_wasm_sdk::{
    init_tracing, orion_plugin, set_access_log_operators, FilterAction, HttpHeaders, Plugin, RequestHandle,
    ResponseHandle,
};
use tracing::{error, info};

#[derive(Default)]
struct AccessLogOperatorFilter;

#[orion_plugin]
impl Plugin for AccessLogOperatorFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!("AccessLogOperatorFilter: Wasm module initialized.");
    }

    fn on_request_headers(&mut self, _ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        // Set multiple access log operators during request processing
        let operators = [("op_1", "\"value_from_request_phase\""), ("op_2", "42"), ("op_3", "{\"nested\": true}")];

        if let Err(e) = set_access_log_operators(operators) {
            error!("Failed to set access log operators: {:?}", e);
        } else {
            info!("Successfully set access log operators during request phase");
        }

        FilterAction::Continue
    }

    fn on_response_headers(&mut self, _ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        // We can also set or overwrite them during the response phase
        let operators = [("op_4", "\"value_from_response_phase\"")];

        if let Err(e) = set_access_log_operators(operators) {
            error!("Failed to set access log operators on response: {:?}", e);
        } else {
            info!("Successfully set access log operators during response phase");
        }

        FilterAction::Continue
    }
}
