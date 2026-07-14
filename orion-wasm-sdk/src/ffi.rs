#[link(wasm_import_module = "env")]
extern "C" {
    /// Read the plugin configuration.
    pub fn orion_get_plugin_config(
        config_ptr: *mut u8,
        max_len: u32,
        written_len_ptr: *mut u32,
    ) -> i32;

    /// Set a custom metric.
    pub fn orion_set_custom_metric(
        key_ptr: *const u8,
        key_len: u32,
        value_ptr: *const u8,
        value_len: u32,
    ) -> i32;

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
    pub fn orion_set_body(
        handle: u64,
        handle_type: u32,
        body_ptr: *const u8,
        body_len: u32,
    ) -> i32;

    /// Dispatch an async HTTP call via the host cluster manager.
    pub fn orion_dispatch_http_call(
        req_ptr: *const u8,
        req_len: u32,
        resp_ptr_ptr: *mut *mut u8,
        resp_len_ptr: *mut u32,
    ) -> i32;

    /// Send a direct (local) HTTP response, short-circuiting the filter chain.
    pub fn orion_send_direct_response(
        request_handle: u64,
        status_code: u32,
        body_ptr: *const u8,
        body_len: u32,
    ) -> i32;

    /// Log a message via the host's tracing framework.
    pub fn orion_log(level: u32, msg_ptr: *const u8, msg_len: u32) -> i32;

    pub fn orion_get_headers_map(handle: u64, handle_type: u32, buf_ptr: *mut u8, max_len: u32, written_len_ptr: *mut u32) -> i32;
    pub fn orion_set_headers_map(handle: u64, handle_type: u32, buf_ptr: *const u8, buf_len: u32) -> i32;

    pub fn orion_set_header(handle: u64, handle_type: u32, name_ptr: *const u8, name_len: u32, value_ptr: *const u8, value_len: u32) -> i32;
    pub fn orion_add_header(handle: u64, handle_type: u32, name_ptr: *const u8, name_len: u32, value_ptr: *const u8, value_len: u32) -> i32;
    pub fn orion_remove_header(handle: u64, handle_type: u32, name_ptr: *const u8, name_len: u32) -> i32;
    pub fn orion_replace_header(handle: u64, handle_type: u32, name_ptr: *const u8, name_len: u32, value_ptr: *const u8, value_len: u32) -> i32;
    pub fn orion_apply_header_mutations(handle: u64, handle_type: u32, buf_ptr: *const u8, buf_len: u32) -> i32;
}

#[derive(Copy, Clone)]
#[repr(u32)]
pub enum HeaderTarget {
    Request = 0,
    Response = 1,
}

