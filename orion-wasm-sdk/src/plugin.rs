use crate::request::RequestHandle;
use crate::response::ResponseHandle;
use crate::typestate::{HttpBody, HttpHeaders};
use orion_wasm_types::FilterAction;

/// Idiomatic interface implemented by Orion Wasm plugins.
pub trait Plugin {
    /// Invoked once when the Wasm module is instantiated.
    #[inline]
    fn on_plugin_start(&mut self) {}

    /// Invoked when the Wasm module instance is destroyed by the host.
    #[inline]
    fn on_plugin_destroy(&mut self) {}

    /// Invoked at the beginning of a new HTTP request/transaction.
    #[inline]
    fn on_transaction_start(&mut self) {}

    /// Invoked on the request path before the body has been buffered.
    #[inline]
    fn on_request_headers(&mut self, _ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked after the plugin returned [`FilterAction::PauseAndBufferBody`]
    /// from [`Plugin::on_request_headers`] and the host has buffered the full
    /// request body.
    #[inline]
    fn on_request_body(&mut self, _ctx: &RequestHandle<HttpBody>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked on the response path before the body has been buffered.
    #[inline]
    fn on_response_headers(&mut self, _ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked after the plugin returned [`FilterAction::PauseAndBufferBody`]
    /// from [`Plugin::on_response_headers`] and the host has buffered the full
    /// response body.
    #[inline]
    fn on_response_body(&mut self, _ctx: &ResponseHandle<HttpBody>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked when the request/response has been fully processed and the transaction is complete.
    /// This is a good place to clean up any transaction-specific resources before the plugin is reused.
    #[inline]
    fn on_transaction_complete(&mut self) {}
}

/// # Example
///
/// ```no_run
/// use orion_wasm_sdk::{Plugin, FilterAction, RequestHandle, HttpHeaders};
///
/// #[derive(Default)]
/// struct AuthFilter;
///
/// #[orion_plugin]
/// impl Plugin for AuthFilter {
///     fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
///         match ctx.get_header("Authorization") {
///             Ok(Some(v)) if v == "Bearer secret-token" => FilterAction::Continue,
///             _ => ctx.direct_response(401, b"unauthorized"),
///         }
///     }
/// }
///
/// ```
pub use orion_wasm_sdk_macros::orion_plugin;
