use orion_wasm_sdk::{WasmHeaderName, WasmHeaderValue};
/// Example using the Orion Wasm SDK to mutate HTTP headers natively using the batch API.
use orion_wasm_sdk::{
    init_tracing, orion_plugin, FilterAction, HeaderMutation, HttpHeaders, Plugin, RequestHandle, ResponseHandle,
};
use tracing::{error, info};

#[derive(Default)]
struct HeaderMutationsFilter;

#[orion_plugin]
impl Plugin for HeaderMutationsFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!(version = "1.0", "HeaderMutationsFilter Wasm: Instance initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        info!("--- Processing Request Headers with Batch API ---");

        let mutations = vec![
            HeaderMutation::Set(
                WasmHeaderName::from("x-custom-set"),
                WasmHeaderValue::from("set-value"),
            ),
            HeaderMutation::Add(
                WasmHeaderName::from("x-custom-add"),
                WasmHeaderValue::from("add-value"),
            ),
            HeaderMutation::Remove(WasmHeaderName::from("user-agent")),
            HeaderMutation::Replace(
                WasmHeaderName::from("x-custom-set"),
                WasmHeaderValue::from("replaced-value"),
            ),
        ];

        if let Err(e) = ctx.apply_header_mutations(&mutations) {
            error!("Failed to apply request header mutations: {:?}", e);
        }

        FilterAction::Continue
    }

    fn on_response_headers(&mut self, ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        info!("--- Processing Response Headers with Batch API ---");

        let mutations = vec![
            HeaderMutation::Set(
                WasmHeaderName::from("x-response-set"),
                WasmHeaderValue::from("res-set-value"),
            ),
            HeaderMutation::Add(
                WasmHeaderName::from("x-response-add"),
                WasmHeaderValue::from("res-add-value"),
            ),
            HeaderMutation::Remove(WasmHeaderName::from("x-response-remove")),
            HeaderMutation::Replace(
                WasmHeaderName::from("x-response-set"),
                WasmHeaderValue::from("res-replaced-value"),
            ),
        ];

        if let Err(e) = ctx.apply_header_mutations(&mutations) {
            error!("Failed to apply response header mutations: {:?}", e);
        }

        FilterAction::Continue
    }
}
