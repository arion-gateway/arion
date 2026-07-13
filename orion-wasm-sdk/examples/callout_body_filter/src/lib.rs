//! Example using the Orion Wasm SDK to perform an HTTP Callout and replace the request body.
use orion_wasm_sdk::{
    dispatch_http_call, init_tracing, orion_plugin, CalloutRequest, FilterAction, Plugin,
    RequestBody, RequestHandle,
};
use tracing::{error, info};
use http::{Method, HeaderMap};
use smol_str::SmolStr;

#[derive(Default)]
struct CalloutBodyFilter;

#[orion_plugin]
impl Plugin for CalloutBodyFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!(version = "1.0", "CalloutBodyFilter Wasm: Instance initialized!");
    }

    fn on_request_body(&mut self, ctx: &RequestHandle<RequestBody>) -> FilterAction {
        info!("--- Processing Request Body with Callout ---");

        // 1. Get the original body to send to the external service
        let original_body = match ctx.get_body() {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to get request body: {:?}", e);
                return FilterAction::Continue;
            }
        };

        // 2. Prepare the callout request
        let mut callout_headers = HeaderMap::new();
        callout_headers.insert("x-callout-id", "wasm-plugin-123".parse().unwrap());
        callout_headers.insert("accept", "application/json".parse().unwrap());

        let req = CalloutRequest {
            cluster_name: SmolStr::new("service"),
            path: SmolStr::new("/"),
            method: Method::POST,
            headers: callout_headers,
            body: Some(original_body),
        };

        // 3. Perform the synchronous-looking asynchronous callout
        info!("Dispatching HTTP POST call to cluster 'service'...");
        match dispatch_http_call(&req) {
            Ok(response) => {
                info!("Callout completed with status: {}", response.status);

                if response.status.is_success() {
                    // 4. Extract the body from the callout response and replace the original request body
                    let new_body = response.body.unwrap_or_else(|| b"Fallback Body".to_vec());
                    
                    if let Err(e) = ctx.set_body(&new_body) {
                        error!("Failed to replace the request body: {:?}", e);
                    } else {
                        info!("Successfully replaced request body with callout response! ({} bytes)", new_body.len());
                    }
                } else {
                    error!("Callout returned non-success status, keeping original body");
                }
            }
            Err(e) => {
                error!("Failed to execute callout: {:?}", e);
            }
        }

        FilterAction::Continue
    }
}
