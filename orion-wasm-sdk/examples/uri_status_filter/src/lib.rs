use orion_wasm_sdk::prelude::*;
use orion_wasm_sdk::{FilterAction, http};

#[derive(Default)]
struct UriStatusFilter;

#[orion_plugin]
impl Plugin for UriStatusFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Ok(uri) = ctx.get_uri() {
            if uri.path() == "/old-path" {
                // Change URI path
                let mut parts = uri.into_parts();
                parts.path_and_query = Some("/new-path".parse().unwrap());
                if let Ok(new_uri) = http::Uri::from_parts(parts) {
                    let _ = ctx.set_uri(&new_uri);
                }
            }
        }
        FilterAction::Continue
    }

    fn on_response_headers(&mut self, ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        if let Ok(Some(status)) = ctx.get_status_code() {
            if status == http::StatusCode::NOT_FOUND {
                // Change 404 to 418 I'm a teapot
                let _ = ctx.set_status_code(http::StatusCode::IM_A_TEAPOT);
            }
        }
        FilterAction::Continue
    }
}
