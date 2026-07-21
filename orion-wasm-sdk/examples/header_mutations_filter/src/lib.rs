//! Example using the Orion Wasm SDK to mutate HTTP headers natively using the batch API.
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
                http::header::HeaderName::from_static("x-custom-set"),
                http::header::HeaderValue::from_static("set-value"),
            ),
            HeaderMutation::Add(
                http::header::HeaderName::from_static("x-custom-add"),
                http::header::HeaderValue::from_static("add-value"),
            ),
            HeaderMutation::Remove(http::header::HeaderName::from_static("user-agent")),
            HeaderMutation::Replace(
                http::header::HeaderName::from_static("x-custom-set"),
                http::header::HeaderValue::from_static("replaced-value"),
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
                http::header::HeaderName::from_static("x-response-set"),
                http::header::HeaderValue::from_static("res-set-value"),
            ),
            HeaderMutation::Add(
                http::header::HeaderName::from_static("x-response-add"),
                http::header::HeaderValue::from_static("res-add-value"),
            ),
            HeaderMutation::Remove(http::header::HeaderName::from_static("x-response-remove")),
            HeaderMutation::Replace(
                http::header::HeaderName::from_static("x-response-set"),
                http::header::HeaderValue::from_static("res-replaced-value"),
            ),
        ];

        if let Err(e) = ctx.apply_header_mutations(&mutations) {
            error!("Failed to apply response header mutations: {:?}", e);
        }

        FilterAction::Continue
    }
}
