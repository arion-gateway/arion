// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    /// Invoked after the host has buffered the full request body.
    ///
    /// Runs when headers returned [`FilterAction::PauseAndBufferBody`], or when
    /// the plugin exports a body hook without a headers hook (host buffers implicitly).
    #[inline]
    fn on_request_body(&mut self, _ctx: &RequestHandle<HttpBody>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked on the response path before the body has been buffered.
    #[inline]
    fn on_response_headers(&mut self, _ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked after the host has buffered the full response body
    /// (same rules as [`Plugin::on_request_body`]).
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
/// use orion_wasm_sdk::prelude::*;
/// use orion_wasm_types::FilterAction;
///
/// #[derive(Default)]
/// struct AuthFilter;
///
/// #[orion_plugin]
/// impl Plugin for AuthFilter {
///     fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
///         match ctx.get_header("Authorization") {
///             Ok(Some(v)) if v.as_bytes() == b"Bearer secret-token" => FilterAction::Continue,
///             _ => {
///                 let response = http::Response::builder()
///                     .status(401)
///                     .body(bytes::Bytes::from_static(b"unauthorized"))
///                     .unwrap();
///                 ctx.direct_response(response)
///             }
///         }
///     }
/// }
/// ```
pub use orion_wasm_sdk_macros::orion_plugin;
