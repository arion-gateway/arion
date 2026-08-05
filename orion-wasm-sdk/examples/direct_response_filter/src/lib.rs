use bytes::Bytes;
use http::Response;
use orion_wasm_sdk::prelude::*;
use orion_wasm_sdk::FilterAction;

#[derive(Default)]
struct DirectResponseFilter;

#[orion_plugin]
impl Plugin for DirectResponseFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Ok(Some(_)) = ctx.get_header("x-trigger-direct") {
            let builder = Response::builder().status(403).header("x-custom-response-header", "was-intercepted");

            let response = builder.body(Bytes::from("Intercepted by Wasm Direct Response!")).unwrap();

            return ctx.direct_response(response);
        }
        FilterAction::Continue
    }
}
