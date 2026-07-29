use orion_wasm_sdk::prelude::*;
use orion_wasm_sdk::{FilterAction, http, bytes};

#[derive(Default)]
struct BenchmarkFilter;

#[orion_plugin]
impl Plugin for BenchmarkFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        match ctx.get_header("Authorization") {
            Ok(Some(_)) => {
                FilterAction::Continue
            }
            _ => {
                ctx.direct_response(http::Response::builder().status(401).body(bytes::Bytes::from_static(b"Unauthorized")).unwrap())
            }
        }
    }
}
