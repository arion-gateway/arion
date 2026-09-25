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

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ImplItem, ItemImpl};

#[proc_macro_attribute]
pub fn arion_plugin(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let self_ty = &input.self_ty;

    let mut has_req_headers = false;
    let mut has_req_body = false;
    let mut has_resp_headers = false;
    let mut has_resp_body = false;
    let mut has_plugin_start = false;
    let mut has_plugin_destroy = false;
    let mut has_transaction_start = false;
    let mut has_transaction_complete = false;

    for item in &input.items {
        if let ImplItem::Fn(method) = item {
            match method.sig.ident.to_string().as_str() {
                "on_request_headers" => has_req_headers = true,
                "on_request_body" => has_req_body = true,
                "on_response_headers" => has_resp_headers = true,
                "on_response_body" => has_resp_body = true,
                "on_plugin_start" => has_plugin_start = true,
                "on_plugin_destroy" => has_plugin_destroy = true,
                "on_transaction_start" => has_transaction_start = true,
                "on_transaction_complete" => has_transaction_complete = true,
                _ => {}
            }
        }
    }

    let req_headers_export = if has_req_headers {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_request_headers() -> i32 {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                let ctx = ::arion_wasm_sdk::RequestHandle::<::arion_wasm_sdk::HttpHeaders>::new();
                ::arion_wasm_sdk::Plugin::on_request_headers(plugin, &ctx).into()
            }
        }
    } else {
        quote! {}
    };

    let req_body_export = if has_req_body {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_request_body(_body_len: u32) -> i32 {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                let ctx = ::arion_wasm_sdk::RequestHandle::<::arion_wasm_sdk::HttpBody>::new();
                ::arion_wasm_sdk::Plugin::on_request_body(plugin, &ctx).into()
            }
        }
    } else {
        quote! {}
    };

    let resp_headers_export = if has_resp_headers {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_response_headers() -> i32 {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                let ctx = ::arion_wasm_sdk::ResponseHandle::<::arion_wasm_sdk::HttpHeaders>::new();
                ::arion_wasm_sdk::Plugin::on_response_headers(plugin, &ctx).into()
            }
        }
    } else {
        quote! {}
    };

    let resp_body_export = if has_resp_body {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_response_body(_body_len: u32) -> i32 {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                let ctx = ::arion_wasm_sdk::ResponseHandle::<::arion_wasm_sdk::HttpBody>::new();
                ::arion_wasm_sdk::Plugin::on_response_body(plugin, &ctx).into()
            }
        }
    } else {
        quote! {}
    };

    let plugin_start_export = if has_plugin_start {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_plugin_start() {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                ::arion_wasm_sdk::Plugin::on_plugin_start(plugin);
            }
        }
    } else {
        quote! {}
    };

    let plugin_destroy_export = if has_plugin_destroy {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_plugin_destroy() {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                ::arion_wasm_sdk::Plugin::on_plugin_destroy(plugin);
            }
        }
    } else {
        quote! {}
    };

    let transaction_start_export = if has_transaction_start {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_transaction_start() {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                ::arion_wasm_sdk::Plugin::on_transaction_start(plugin);
            }
        }
    } else {
        quote! {}
    };

    let transaction_complete_export = if has_transaction_complete {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_transaction_complete() {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                ::arion_wasm_sdk::Plugin::on_transaction_complete(plugin);
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #input

        static mut PLUGIN: ::std::option::Option<#self_ty> = ::std::option::Option::None;

        #req_headers_export
        #req_body_export
        #resp_headers_export
        #resp_body_export
        #plugin_start_export
        #plugin_destroy_export
        #transaction_start_export
        #transaction_complete_export
    };

    TokenStream::from(expanded)
}
