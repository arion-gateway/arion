//! Metadata filter example using the Orion Wasm SDK.
//!
//! This plugin demonstrates how to extract downstream connection metadata.

use orion_wasm_sdk::{init_tracing, orion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle};
use tracing::{info, error};

#[derive(Default)]
struct MetadataFilter;

#[orion_plugin]
impl Plugin for MetadataFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        info!("MetadataFilter initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        info!("Extracting downstream metadata...");
        match ctx.get_downstream_metadata() {
            Ok(Some(metadata)) => {
                info!("Successfully extracted downstream metadata:");
                info!("  Listener Name: {}", metadata.listener_name);
                if let Some(sni) = &metadata.sni {
                    info!("  SNI: {}", sni);
                } else {
                    info!("  SNI: None");
                }
                info!("  Connection: {:?}", metadata.connection);
            }
            Ok(None) => {
                info!("No downstream metadata found for this request.");
            }
            Err(e) => {
                error!("Error while extracting downstream metadata: {:?}", e);
            }
        }

        FilterAction::Continue
    }
}
