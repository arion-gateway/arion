//! Example using the Orion Wasm SDK to mutate HTTP headers.
use orion_wasm_sdk::{init_tracing, orion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle, http, bytes};
use tracing::{error, info};

#[derive(Default)]
struct HeadersMapFilter;

#[orion_plugin]
impl Plugin for HeadersMapFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!(version = "1.0", "HeadersMapFilter Wasm: Instance initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        info!("on_request_headers called");
        let mut headers = match ctx.get_headers_map() {
            Ok(h) => h,
            Err(e) => {
                error!("get_headers_map failed: {:?}", e);
                return ctx.direct_response(http::Response::builder().status(500).body(bytes::Bytes::from_static(b"Internal Server Error")).unwrap());
            }
        };

        // 1. Remove user-agent
        headers.remove(http::header::HeaderName::from_static("user-agent"));

        // 2. Insert (Set/Replace) x-custom-set
        headers.insert(
            http::header::HeaderName::from_static("x-custom-set"),
            http::header::HeaderValue::from_static("replaced-value")
        );

        // 3. Append (Add) x-custom-add
        headers.append(
            http::header::HeaderName::from_static("x-custom-add"),
            http::header::HeaderValue::from_static("add-value")
        );

        if let Err(e) = ctx.set_headers_map(&headers) {
            error!("Failed to set headers map: {:?}", e);
            return ctx.direct_response(http::Response::builder().status(500).body(bytes::Bytes::from_static(b"Internal Server Error")).unwrap());
        }

        FilterAction::Continue
    }

    fn on_response_headers(&mut self, ctx: &orion_wasm_sdk::ResponseHandle<HttpHeaders>) -> FilterAction {
        info!("on_response_headers called");
        let mut headers = match ctx.get_headers_map() {
            Ok(h) => h,
            Err(e) => {
                error!("get_headers_map failed: {:?}", e);
                return FilterAction::Continue;
            }
        };

        // 1. Remove x-response-remove
        headers.remove(http::header::HeaderName::from_static("x-response-remove"));

        // 2. Insert (Set/Replace) x-response-set
        headers.insert(
            http::header::HeaderName::from_static("x-response-set"),
            http::header::HeaderValue::from_static("res-replaced-value")
        );

        // 3. Append (Add) x-response-add
        headers.append(
            http::header::HeaderName::from_static("x-response-add"),
            http::header::HeaderValue::from_static("res-add-value")
        );

        if let Err(e) = ctx.set_headers_map(&headers) {
            error!("Failed to set response headers map: {:?}", e);
        }

        FilterAction::Continue
    }
}
