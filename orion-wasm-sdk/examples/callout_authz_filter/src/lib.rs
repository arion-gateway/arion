use orion_wasm_sdk::{dispatch_http_call, orion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle, http, bytes};

#[derive(Default)]
struct CalloutAuthzFilter;

#[orion_plugin]
impl Plugin for CalloutAuthzFilter {
    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        let mut req = http::Request::builder()
            .method(http::Method::GET)
            .uri("/auth")
            .header("host", "service");

        if let Ok(headers) = ctx.get_headers_map() {
            for (name, value) in &headers {
                if !name.as_str().starts_with(':') {
                    req = req.header(name, value);
                }
            }
        }

        let req = req.body(bytes::Bytes::new()).unwrap();

        match dispatch_http_call("service", req) {
            Ok(response) => {
                let authorized = response
                    .headers()
                    .get("x-authorized")
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);

                if authorized {
                    FilterAction::Continue
                } else {
                    ctx.direct_response(
                        http::Response::builder()
                            .status(403)
                            .body(bytes::Bytes::from_static(b"Forbidden"))
                            .unwrap(),
                    )
                }
            }
            Err(_) => FilterAction::Continue,
        }
    }
}
