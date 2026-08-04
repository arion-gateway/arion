//! Example using the Orion Wasm SDK to mutate HTTP headers natively.
use orion_wasm_sdk::{init_tracing, orion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle, ResponseHandle};
use tracing::{error, info};

#[derive(Default)]
struct HeaderApiFilter;

#[orion_plugin]
impl Plugin for HeaderApiFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!(version = "1.0", "HeaderApiFilter Wasm: Instance initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        info!("--- Processing Request Headers ---");

        // 1. Set a new header (or replace if it exists)
        if let Err(e) = ctx.set_header(
            http::header::HeaderName::from_static("x-custom-set"),
            http::header::HeaderValue::from_static("set-value"),
        ) {
            error!("Failed to set header: {:?}", e);
        }

        // 2. Add a header (appends to existing)
        if let Err(e) = ctx.add_header(
            http::header::HeaderName::from_static("x-custom-add"),
            http::header::HeaderValue::from_static("add-value"),
        ) {
            error!("Failed to add header: {:?}", e);
        }

        // 3. Remove a header
        if let Err(e) = ctx.remove_header("user-agent") {
            error!("Failed to remove header: {:?}", e);
        }

        // 4. Replace an existing header (only if it exists)
        if let Err(e) = ctx.replace_header(
            http::header::HeaderName::from_static("x-custom-set"),
            http::header::HeaderValue::from_static("replaced-value"),
        ) {
            error!("Failed to replace header: {:?}", e);
        }

        FilterAction::Continue
    }

    fn on_response_headers(&mut self, ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        info!("--- Processing Response Headers ---");

        // 1. Set a response header
        if let Err(e) = ctx.set_header(
            http::header::HeaderName::from_static("x-response-set"),
            http::header::HeaderValue::from_static("res-set-value"),
        ) {
            error!("Failed to set response header: {:?}", e);
        }

        // 2. Add a response header (appends)
        if let Err(e) = ctx.add_header(
            http::header::HeaderName::from_static("x-response-add"),
            http::header::HeaderValue::from_static("res-add-value"),
        ) {
            error!("Failed to add response header: {:?}", e);
        }

        // 3. Remove a response header
        if let Err(e) = ctx.remove_header("x-response-remove") {
            error!("Failed to remove response header: {:?}", e);
        }

        // 4. Replace an existing response header
        if let Err(e) = ctx.replace_header(
            http::header::HeaderName::from_static("x-response-set"),
            http::header::HeaderValue::from_static("res-replaced-value"),
        ) {
            error!("Failed to replace response header: {:?}", e);
        }

        FilterAction::Continue
    }
}
