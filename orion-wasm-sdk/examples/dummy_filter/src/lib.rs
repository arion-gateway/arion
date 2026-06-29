// Dummy Example using the SDK
use orion_wasm_sdk::RequestContext;

#[no_mangle]
pub extern "C" fn on_request_headers(request_handle: u64) -> i32 {
    let ctx = RequestContext::new(request_handle);

    // Request header through the SDK
    if let Some(auth_val) = ctx.get_header("Authorization") {
        if auth_val == "Bearer secret-token" {
            return 200; // Success
        }
    }

    401 // Unauthorized
}
