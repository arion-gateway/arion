use orion_wasm_sdk::prelude::*;
use orion_wasm_sdk::FilterAction;

#[derive(Default)]
struct BenchmarkFilter;

#[orion_plugin]
impl Plugin for BenchmarkFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        match ctx.get_header("Authorization") {
            Ok(Some(_)) => {
                // L'header è definito, lasciamo passare la richiesta
                FilterAction::Continue
            }
            _ => {
                // L'header non c'è, droppiamo la richiesta (ritorniamo 401)
                ctx.direct_response(401, b"Unauthorized")
            }
        }
    }
}
