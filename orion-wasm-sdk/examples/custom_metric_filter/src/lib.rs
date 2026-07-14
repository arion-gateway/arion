//! Example using the Orion Wasm SDK to evaluate a custom metric.
//!
//! This plugin demonstrates how to set a custom metric dynamically
//! from within a Wasm plugin using `set_custom_metric`.

use orion_wasm_sdk::{
    orion_plugin, set_custom_metric, FilterAction, Plugin, RequestHandle, RequestHeaders, init_tracing
};
use tracing::{info, debug, error};

#[derive(Default)]
struct CustomMetricFilter;

#[orion_plugin]
impl Plugin for CustomMetricFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!("CustomMetricFilter: Wasm module initialized.");
    }

    fn on_request_headers(&mut self, _ctx: &RequestHandle<RequestHeaders>) -> FilterAction {
        // Evaluate a custom metric with key "custom" and value "metric".
        // This will be processed by the host if a metric with header_name "custom"
        // is registered under the `wasm` hook in the metrics configuration.
        match set_custom_metric("custom", "metric") {
            Ok(_) => debug!("Custom metric successfully evaluated!"),
            Err(_) => error!("Failed to evaluate custom metric!"),
        }

        FilterAction::Continue
    }
}
