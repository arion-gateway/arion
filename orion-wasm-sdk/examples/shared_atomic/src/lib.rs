use orion_wasm_sdk::{WasmHeaderName, WasmHeaderValue};
use orion_wasm_sdk::{init_tracing, orion_plugin, FilterAction, HeaderMutation, HttpHeaders, Plugin, RequestHandle};
use orion_wasm_sdk::shared::SharedAtomicU64;
use tracing::{debug, error, info};
use std::sync::atomic::Ordering;

#[derive(Default)]
struct SharedAtomicFilter {
    counter: Option<SharedAtomicU64>,
}

#[orion_plugin]
impl Plugin for SharedAtomicFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!("SharedAtomicFilter: on_plugin_start");

        match SharedAtomicU64::try_new("request_counter") {
            Ok(atomic) => {
                info!("Successfully created/opened shared atomic variable 'request_counter'");
                self.counter = Some(atomic);
            }
            Err(e) => {
                error!("Failed to open shared atomic variable: {:?}", e);
            }
        }
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Some(counter) = &self.counter {
            // Increment the shared counter by 1
            let prev = counter.fetch_add(1, Ordering::SeqCst);
            let current = prev + 1;
            info!("Shared counter incremented! Previous value: {}, New value: {}", prev, current);

            // Set the result as an HTTP header sent to the upstream
            let header_value_str = format!("{}", current);
            let header_value = bytes::Bytes::from(header_value_str);
                if let Err(e) = ctx.apply_header_mutations(&[
                    HeaderMutation::Set(
                        WasmHeaderName::from("x-request-counter"),
                        WasmHeaderValue::from(header_value.to_vec()),
                    )
                ]) {
                    error!("Failed to set x-request-counter header: {:?}", e);
                }
            
        } else {
            error!("Shared counter is not initialized!");
        }

        FilterAction::Continue
    }
}
