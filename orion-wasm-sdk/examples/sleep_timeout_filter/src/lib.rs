//! Sleep Timeout Filter example using the Orion Wasm SDK.
//!
//! This plugin demonstrates the sleep and io timeout APIs.
//! It sleeps for 1 second, sets an io timeout of 1 second, and then attempts to sleep for 10 seconds.
//! The second sleep should be interrupted by the timeout.

use orion_wasm_sdk::{
    init_tracing, orion_plugin, set_io_timeout, sleep, FilterAction, HttpHeaders, Plugin, RequestHandle, OrionWasmError
};
use tracing::{info, error};
use std::time::Duration;

#[derive(Default)]
struct SleepTimeoutFilter;

#[orion_plugin]
impl Plugin for SleepTimeoutFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!("SleepTimeoutFilter initialized!");
    }

    fn on_request_headers(&mut self, _ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        info!("Step 1: Sleeping for 1 second...");
        match sleep(Duration::from_secs(1)) {
            Ok(_) => info!("Step 1: Sleep completed successfully."),
            Err(e) => error!("Step 1: Sleep failed: {:?}", e),
        }

        info!("Step 2: Setting IO timeout to 1 second...");
        match set_io_timeout(Duration::from_secs(1)) {
            Ok(_) => info!("Step 2: Timeout set successfully."),
            Err(e) => error!("Step 2: Failed to set timeout: {:?}", e),
        }

        info!("Step 3: Attempting to sleep for 10 seconds (should timeout)...");
        match sleep(Duration::from_secs(10)) {
            Ok(_) => {
                error!("Step 3: Sleep completed completely, but it should have timed out!");
                let _ = _ctx.set_header(
                    http::header::HeaderName::from_static("x-timeout-test"),
                    http::header::HeaderValue::from_static("failed-did-not-timeout"),
                );
            }
            Err(OrionWasmError::Timeout) => {
                info!("Step 3: Sleep timed out exactly as expected!");
                let _ = _ctx.set_header(
                    http::header::HeaderName::from_static("x-timeout-test"),
                    http::header::HeaderValue::from_static("passed"),
                );
            }
            Err(e) => {
                error!("Step 3: Sleep failed with unexpected error: {:?}", e);
                let _ = _ctx.set_header(
                    http::header::HeaderName::from_static("x-timeout-test"),
                    http::header::HeaderValue::from_static("failed-unexpected-error"),
                );
            }
        }

        // We continue the filter chain
        info!("SleepTimeoutFilter: Resuming upstream request...");
        FilterAction::Continue
    }
}
