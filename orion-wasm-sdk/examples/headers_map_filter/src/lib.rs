//! Example using the Orion Wasm SDK to mutate HTTP headers.
use orion_wasm_sdk::{init_tracing, orion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle};
use tracing::{debug, error, info};

#[derive(Default)]
struct HeadersMapFilter;

#[orion_plugin]
impl Plugin for HeadersMapFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!(version = "1.0", "HeadersMapFilter Wasm: Instance initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        // 1. Get the current headers map
        let mut headers = match ctx.get_headers_map() {
            Ok(h) => h,
            Err(_) => {
                error!("Failed to get headers map from host");
                return ctx.direct_response(500, b"Internal Server Error");
            },
        };

        // 2. Log all headers
        info!("--- Incoming Request Headers ---");
        for (key, val) in headers.iter() {
            info!("{}: {:?}", key, val);
        }
        info!("--------------------------------");

        // 3. Add a new header
        info!("Injecting new header 'X-Wasm-Mutated: true'");
        headers.insert(
            http::header::HeaderName::from_static("x-wasm-mutated"),
            http::header::HeaderValue::from_static("true"),
        );

        // 4. Set the headers map back to the host
        if let Err(e) = ctx.set_headers_map(&headers) {
            error!("Failed to set headers map: {:?}", e);
            return ctx.direct_response(500, b"Internal Server Error");
        }

        FilterAction::Continue
    }
}
