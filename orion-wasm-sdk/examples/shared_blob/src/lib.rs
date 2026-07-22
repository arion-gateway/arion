use orion_wasm_sdk::{init_tracing, orion_plugin, FilterAction, HeaderMutation, HttpHeaders, Plugin, RequestHandle};
use orion_wasm_sdk::shared::SharedBlob;
use tracing::{debug, error, info};

#[derive(Default)]
struct SharedBlobFilter {
    blob: Option<SharedBlob>,
}

#[orion_plugin]
impl Plugin for SharedBlobFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!("SharedBlobFilter: on_plugin_start");
        
        match SharedBlob::try_new("my_shared_blob") {
            Ok(blob) => {
                info!("Successfully created/opened shared blob 'my_shared_blob'");
                
                // Initialize with an empty list if it's empty
                let current = blob.read();
                if current.version == 0 && current.data.is_empty() {
                    blob.write(b"");
                }
                
                self.blob = Some(blob);
            }
            Err(e) => {
                error!("Failed to open shared blob: {:?}", e);
            }
        }
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        if let Some(blob) = &self.blob {
            let client_id = match ctx.get_header("x-client-id") {
                Ok(Some(val)) => val.to_str().unwrap_or("unknown").to_string(),
                _ => "unknown".to_string(),
            };

            let list_for_upstream_header;
            
            // We want to ensure the blob contains this client_id in a comma-separated list.
            // We MUST use compare_and_swap because we are appending to the existing state.
            loop {
                let current = blob.read();
                let current_str = String::from_utf8_lossy(&current.data);
                
                // If it already contains our client_id, we have nothing to do!
                if current_str.split(',').any(|s| s.trim() == client_id) {
                    list_for_upstream_header = current_str.to_string();
                    break;
                }
                
                // Otherwise, append our ID
                let new_str = if current_str.is_empty() {
                    client_id.clone()
                } else {
                    format!("{}, {}", current_str, client_id)
                };
                
                // Try to write the new string safely
                if blob.compare_and_swap(new_str.as_bytes(), current.version).is_ok() {
                    info!("Blob successfully updated via CAS to version {}", current.version + 1);
                    // Save the value we just successfully wrote to avoid race conditions!
                    // If we did blob.read() here, we might read a newer value modified by another worker.
                    list_for_upstream_header = new_str;
                    break;
                }
                
                info!("CAS failed (version mismatch), retrying...");
            }

            // Set the result as an HTTP header sent to the upstream using the exact string we resolved
            if let Ok(header_value) = http::header::HeaderValue::try_from(list_for_upstream_header) {
                let mutations = vec![
                    HeaderMutation::Set(
                        http::header::HeaderName::from_static("x-seen-clients"),
                        header_value,
                    )
                ];
                if let Err(e) = ctx.apply_header_mutations(&mutations) {
                    error!("Failed to set x-seen-clients header: {:?}", e);
                }
            }
        } else {
            error!("Shared blob is not initialized!");
        }

        FilterAction::Continue
    }
}
