//! Example using the Orion Wasm SDK to fetch plugin configuration on startup
//! and log it on every request.

use orion_wasm_sdk::{get_plugin_config, init_tracing, orion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle};
use std::sync::OnceLock;
use tracing::{info, warn};

// This global static variable will hold our plugin configuration.
// Since WebAssembly memory is per-VM-instance, this is safe and will persist
// across HTTP requests handled by the same Wasm instance.
static GLOBAL_CONFIG: OnceLock<String> = OnceLock::new();

#[derive(Default)]
struct ConfigLoggerFilter;

#[orion_plugin]
impl Plugin for ConfigLoggerFilter {
    fn on_plugin_start(&mut self) {
        // Initialize tracing so that the info! and warn! macros work.
        let _ = init_tracing();

        info!("ConfigLoggerFilter started. Attempting to load configuration...");

        // Fetch the configuration passed from the control plane (or local config)
        match get_plugin_config() {
            Ok(Some(config_str)) => {
                info!("Successfully loaded configuration! Storing it in global state.");
                let _ = GLOBAL_CONFIG.set(config_str);
            },
            Ok(None) => {
                warn!("No configuration was provided to this plugin instance.");
            },
            Err(e) => {
                warn!("An error occurred while fetching the plugin configuration: {:?}", e);
            },
        }
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        // Retrieve the configuration string from our global static state.
        let current_config = GLOBAL_CONFIG.get().map(|s| s.as_str()).unwrap_or("<None>");

        info!("--- New Request Received ---");
        info!("The active global configuration is: {}", current_config);

        // Inject into request header for testing verification
        let _ = ctx.set_header("x-wasm-config", current_config);

        FilterAction::Continue
    }
}
