#[link(wasm_import_module = "env")]
extern "C" {
    /// Read the plugin configuration.
    pub fn orion_get_plugin_config(config_ptr: *mut u8, max_len: u32, written_len_ptr: *mut u32) -> i32;

    /// Set multiple custom metrics at once.
    pub fn orion_set_custom_metrics(buffer_ptr: *const u8, buffer_len: u32) -> i32;

    /// Set multiple access log operators at once.
    pub fn orion_set_access_log_operators(buffer_ptr: *const u8, buffer_len: u32) -> i32;

    /// Read an HTTP header by name.
    pub fn orion_get_header(
        handle: u64,
        handle_type: u32,
        name_ptr: *const u8,
        name_len: u32,
        value_ptr: *mut u8,
        value_max_len: u32,
        written_len_ptr: *mut u32,
    ) -> i32;

    /// Read the buffered body.
    pub fn orion_get_body(
        handle: u64,
        handle_type: u32,
        body_ptr: *mut u8,
        max_len: u32,
        written_len_ptr: *mut u32,
    ) -> i32;

    /// Replace the buffered body.
    pub fn orion_set_body(handle: u64, handle_type: u32, body_ptr: *const u8, body_len: u32) -> i32;

    /// Dispatch an async HTTP call via the host cluster manager.
    pub fn orion_dispatch_http_call(
        req_ptr: *const u8,
        req_len: u32,
        resp_ptr_ptr: *mut *mut u8,
        resp_len_ptr: *mut u32,
    ) -> i32;

    /// Send a direct (local) HTTP response, short-circuiting the filter chain.
    pub fn orion_send_direct_response(request_handle: u64, status_code: u32, body_ptr: *const u8, body_len: u32)
        -> i32;

    /// Log a message via the host's tracing framework.
    pub fn orion_log(level: u32, msg_ptr: *const u8, msg_len: u32) -> i32;

    pub fn orion_get_downstream_metadata(
        handle: u64,
        out_ptr_ptr: *mut *mut u8,
        out_len_ptr: *mut u32,
    ) -> i32;

    pub fn orion_get_headers_map(
        handle: u64,
        handle_type: u32,
        buf_ptr: *mut u8,
        max_len: u32,
        written_len_ptr: *mut u32,
    ) -> i32;
    pub fn orion_set_headers_map(handle: u64, handle_type: u32, buf_ptr: *const u8, buf_len: u32) -> i32;

    pub fn orion_set_header(
        handle: u64,
        handle_type: u32,
        name_ptr: *const u8,
        name_len: u32,
        value_ptr: *const u8,
        value_len: u32,
    ) -> i32;
    pub fn orion_add_header(
        handle: u64,
        handle_type: u32,
        name_ptr: *const u8,
        name_len: u32,
        value_ptr: *const u8,
        value_len: u32,
    ) -> i32;
    pub fn orion_remove_header(handle: u64, handle_type: u32, name_ptr: *const u8, name_len: u32) -> i32;
    pub fn orion_replace_header(
        handle: u64,
        handle_type: u32,
        name_ptr: *const u8,
        name_len: u32,
        value_ptr: *const u8,
        value_len: u32,
    ) -> i32;
    pub fn orion_apply_header_mutations(handle: u64, handle_type: u32, buf_ptr: *const u8, buf_len: u32) -> i32;

    pub fn orion_set_io_timeout(microseconds: u64) -> i32;
    pub fn orion_clear_io_timeout(remaining_us_ptr: *mut u64) -> i32;
    pub fn orion_sleep(microseconds: u64) -> i32;

    pub fn ext_shared_resolve(name_ptr: *const u8, name_len: u32, var_type: u32) -> u32;

    pub fn ext_shared_u64_load(id: u32, order: u32) -> u64;
    pub fn ext_shared_u64_store(id: u32, val: u64, order: u32);
    pub fn ext_shared_u64_swap(id: u32, val: u64, order: u32) -> u64;
    pub fn ext_shared_u64_compare_exchange(id: u32, current: u64, new: u64, succ: u32, fail: u32) -> u64;
    pub fn ext_shared_u64_fetch_add(id: u32, val: u64, order: u32) -> u64;
    pub fn ext_shared_u64_fetch_sub(id: u32, val: u64, order: u32) -> u64;
    pub fn ext_shared_u64_fetch_and(id: u32, val: u64, order: u32) -> u64;
    pub fn ext_shared_u64_fetch_nand(id: u32, val: u64, order: u32) -> u64;
    pub fn ext_shared_u64_fetch_or(id: u32, val: u64, order: u32) -> u64;
    pub fn ext_shared_u64_fetch_xor(id: u32, val: u64, order: u32) -> u64;
    pub fn ext_shared_u64_fetch_max(id: u32, val: u64, order: u32) -> u64;
    pub fn ext_shared_u64_fetch_min(id: u32, val: u64, order: u32) -> u64;

    pub fn ext_shared_i64_load(id: u32, order: u32) -> i64;
    pub fn ext_shared_i64_store(id: u32, val: i64, order: u32);
    pub fn ext_shared_i64_swap(id: u32, val: i64, order: u32) -> i64;
    pub fn ext_shared_i64_compare_exchange(id: u32, current: i64, new: i64, succ: u32, fail: u32) -> i64;
    pub fn ext_shared_i64_fetch_add(id: u32, val: i64, order: u32) -> i64;
    pub fn ext_shared_i64_fetch_sub(id: u32, val: i64, order: u32) -> i64;
    pub fn ext_shared_i64_fetch_and(id: u32, val: i64, order: u32) -> i64;
    pub fn ext_shared_i64_fetch_nand(id: u32, val: i64, order: u32) -> i64;
    pub fn ext_shared_i64_fetch_or(id: u32, val: i64, order: u32) -> i64;
    pub fn ext_shared_i64_fetch_xor(id: u32, val: i64, order: u32) -> i64;
    pub fn ext_shared_i64_fetch_max(id: u32, val: i64, order: u32) -> i64;
    pub fn ext_shared_i64_fetch_min(id: u32, val: i64, order: u32) -> i64;

    pub fn ext_shared_blob_read(id: u32, buf_ptr: *mut u8, buf_len: u32, out_version_ptr: *mut u64) -> u32;
    pub fn ext_shared_blob_write(id: u32, buf_ptr: *const u8, buf_len: u32) -> u64;
    pub fn ext_shared_blob_cas(id: u32, buf_ptr: *const u8, buf_len: u32, expected_version: u64, out_success_ptr: *mut u32) -> u64;
}

pub use orion_wasm_types::HeaderTarget;
