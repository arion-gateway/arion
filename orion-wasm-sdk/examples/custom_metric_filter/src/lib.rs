//! Example using the Orion Wasm SDK to evaluate a custom metric.
//!
//! This plugin demonstrates how to set a custom metric dynamically
//! from within a Wasm plugin using `set_custom_metric`.

use orion_wasm_sdk::{
    init_tracing, orion_plugin, set_custom_metrics, FilterAction, HttpHeaders, Plugin, RequestHandle,
};
use tracing::{debug, error};

#[derive(Default)]
struct CustomMetricFilter;

#[orion_plugin]
impl Plugin for CustomMetricFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!("CustomMetricFilter: Wasm module initialized.");
    }

    fn on_request_headers(&mut self, _ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        // Evaluate a single custom metric with key "custom" and value "metric".
        match set_custom_metrics([("custom", "metric")]) {
            Ok(_) => debug!("Single custom metric successfully evaluated!"),
            Err(_) => error!("Failed to evaluate single custom metric!"),
        }

        // Evaluate multiple custom metrics at once without allocating a HashMap!
        let multi_metrics = [("user_tier", "premium"), ("datacenter", "eu-west-1")];

        match orion_wasm_sdk::set_custom_metrics(multi_metrics) {
            Ok(_) => debug!("Multiple custom metrics successfully evaluated!"),
            Err(_) => error!("Failed to evaluate multiple custom metrics!"),
        }

        FilterAction::Continue
    }
}
