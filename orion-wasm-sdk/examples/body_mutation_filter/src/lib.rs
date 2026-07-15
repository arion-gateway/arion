//! Example using the Orion Wasm SDK to mutate HTTP bodies natively.
use orion_wasm_sdk::{init_tracing, orion_plugin, FilterAction, HttpBody, Plugin, RequestHandle, ResponseHandle};
use tracing::{error, info};

#[derive(Default)]
struct BodyMutationFilter;

#[orion_plugin]
impl Plugin for BodyMutationFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!(version = "1.0", "BodyMutationFilter Wasm: Instance initialized!");
    }

    fn on_request_body(&mut self, ctx: &RequestHandle<HttpBody>) -> FilterAction {
        info!("--- Processing Request Body ---");

        match ctx.get_body() {
            Ok(body_bytes) => {
                info!("Original request body ({} bytes)", body_bytes.len());

                let mut new_body = body_bytes.clone();
                new_body.extend_from_slice(b" [appended by wasm on request]");

                if let Err(e) = ctx.set_body(&new_body) {
                    error!("Failed to set new request body: {:?}", e);
                } else {
                    info!("Successfully mutated request body!");
                }
            },
            Err(e) => {
                error!("Failed to read request body: {:?}", e);
            },
        }

        FilterAction::Continue
    }

    fn on_response_body(&mut self, ctx: &ResponseHandle<HttpBody>) -> FilterAction {
        info!("--- Processing Response Body ---");

        match ctx.get_body() {
            Ok(body_bytes) => {
                info!("Original response body ({} bytes)", body_bytes.len());

                let mut new_body = b"[prepended by wasm on response] ".to_vec();
                new_body.extend_from_slice(&body_bytes);

                if let Err(e) = ctx.set_body(&new_body) {
                    error!("Failed to set new response body: {:?}", e);
                } else {
                    info!("Successfully mutated response body!");
                }
            },
            Err(e) => {
                error!("Failed to read response body: {:?}", e);
            },
        }

        FilterAction::Continue
    }
}
