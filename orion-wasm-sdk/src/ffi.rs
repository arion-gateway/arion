#[link(wasm_import_module = "env")]
extern "C" {
    /// Read an HTTP request header by name.
    pub fn orion_get_request_header(
        request_handle: u64,
        name_ptr: *const u8,
        name_len: u32,
        value_ptr: *mut u8,
        value_max_len: u32,
        written_len_ptr: *mut u32,
    ) -> i32;

    /// Read the buffered request body.
    pub fn orion_get_request_body(
        request_handle: u64,
        body_ptr: *mut u8,
        max_len: u32,
        written_len_ptr: *mut u32,
    ) -> i32;

    /// Send a direct (local) HTTP response, short-circuiting the filter chain.
    pub fn orion_send_direct_response(
        request_handle: u64,
        status_code: u32,
        body_ptr: *const u8,
        body_len: u32,
    ) -> i32;

    /// Read an HTTP response header by name.
    pub fn orion_get_response_header(
        response_handle: u64,
        name_ptr: *const u8,
        name_len: u32,
        value_ptr: *mut u8,
        value_max_len: u32,
        written_len_ptr: *mut u32,
    ) -> i32;

    /// Read the buffered response body.
    pub fn orion_get_response_body(
        response_handle: u64,
        body_ptr: *mut u8,
        max_len: u32,
        written_len_ptr: *mut u32,
    ) -> i32;

    /// Log a message via the host's tracing framework.
    pub fn orion_log(level: u32, msg_ptr: *const u8, msg_len: u32) -> i32;

    pub fn orion_get_request_headers_map(request_handle: u64, buf_ptr: *mut u8, max_len: u32, written_len_ptr: *mut u32) -> i32;
    pub fn orion_set_request_headers_map(request_handle: u64, buf_ptr: *const u8, buf_len: u32) -> i32;
    pub fn orion_get_response_headers_map(response_handle: u64, buf_ptr: *mut u8, max_len: u32, written_len_ptr: *mut u32) -> i32;
    pub fn orion_set_response_headers_map(response_handle: u64, buf_ptr: *const u8, buf_len: u32) -> i32;

    pub fn orion_set_header(handle: u64, handle_type: u32, name_ptr: *const u8, name_len: u32, value_ptr: *const u8, value_len: u32) -> i32;
    pub fn orion_add_header(handle: u64, handle_type: u32, name_ptr: *const u8, name_len: u32, value_ptr: *const u8, value_len: u32) -> i32;
    pub fn orion_remove_header(handle: u64, handle_type: u32, name_ptr: *const u8, name_len: u32) -> i32;
    pub fn orion_replace_header(handle: u64, handle_type: u32, name_ptr: *const u8, name_len: u32, value_ptr: *const u8, value_len: u32) -> i32;
}

#[repr(u32)]
pub enum HeaderTarget {
    Request = 0,
    Response = 1,
}

