use orion_wasm_sdk::prelude::*;
use orion_wasm_sdk::{http, FilterAction};

#[derive(Default)]
struct UriStatusFilter;

#[orion_plugin]
impl Plugin for UriStatusFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Ok(uri) = ctx.get_uri() {
            if let Ok(parsed_uri) = http::Uri::try_from(uri.as_str()) {
                if parsed_uri.path() == "/old-path" {
                    let mut parts = parsed_uri.into_parts();
                    parts.path_and_query = Some("/new-path".parse().unwrap());
                    if let Ok(new_uri) = http::Uri::from_parts(parts) {
                        let _ = ctx.set_uri(new_uri.to_string());
                    }
                }
            }
        }
        FilterAction::Continue
    }

    fn on_response_headers(&mut self, ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        if let Ok(Some(status)) = ctx.get_status_code() {
            if status == 404 {
                let _ = ctx.set_status_code(418);
            }
        }
        FilterAction::Continue
    }
}
