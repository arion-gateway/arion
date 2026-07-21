use crate::ffi;
use crate::internal::*;
use crate::typestate::{HttpBody, State};
use http::{header::{HeaderName, HeaderValue}, HeaderMap};
use orion_wasm_types::{HeaderMutation, OrionWasmError};

/// Typestate wrapper around a response handle.
pub struct ResponseHandle<S: State> {
    handle: u64,
    _marker: core::marker::PhantomData<S>,
}

impl<S: State> ResponseHandle<S> {
    /// Create a new response handle.
    #[doc(hidden)]
    pub unsafe fn new(handle: u64) -> Self {
        Self { handle, _marker: core::marker::PhantomData }
    }

    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmError> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::Response)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmError> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::Response, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmError> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::Response, mutations)
    }

    /// Read an HTTP response header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmError> {
        get_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        set_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        add_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmError> {
        remove_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        replace_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }
}

impl ResponseHandle<HttpBody> {
    /// Read the buffered response body.
    pub fn get_body(&self) -> Result<Vec<u8>, OrionWasmError> {
        get_http_body(self.handle, ffi::HeaderTarget::Response)
    }

    /// Replace the buffered response body.
    pub fn set_body(&self, body: &[u8]) -> Result<(), OrionWasmError> {
        set_http_body(self.handle, ffi::HeaderTarget::Response, body)
    }

    pub fn get_trailers_map(&self) -> Result<HeaderMap, OrionWasmError> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::ResponseTrailers)
    }

    pub fn set_trailers_map(&self, trailers: &HeaderMap) -> Result<(), OrionWasmError> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::ResponseTrailers, trailers)
    }

    pub fn get_trailer(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmError> {
        get_http_header(self.handle, ffi::HeaderTarget::ResponseTrailers, name)
    }

    pub fn set_trailer(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        set_http_header(self.handle, ffi::HeaderTarget::ResponseTrailers, &name, &value)
    }

    pub fn add_trailer(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        add_http_header(self.handle, ffi::HeaderTarget::ResponseTrailers, &name, &value)
    }

    pub fn remove_trailer(&self, name: &HeaderName) -> Result<(), OrionWasmError> {
        remove_http_header(self.handle, ffi::HeaderTarget::ResponseTrailers, name)
    }

    pub fn replace_trailer(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        replace_http_header(self.handle, ffi::HeaderTarget::ResponseTrailers, &name, &value)
    }

    pub fn apply_trailer_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmError> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::ResponseTrailers, mutations)
    }
}
