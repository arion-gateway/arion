use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ImplItem, ItemImpl};

#[proc_macro_attribute]
pub fn orion_plugin(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let self_ty = &input.self_ty;

    let mut has_req_headers = false;
    let mut has_req_body = false;
    let mut has_resp_headers = false;
    let mut has_resp_body = false;

    for item in &input.items {
        if let ImplItem::Fn(method) = item {
            match method.sig.ident.to_string().as_str() {
                "on_request_headers" => has_req_headers = true,
                "on_request_body" => has_req_body = true,
                "on_response_headers" => has_resp_headers = true,
                "on_response_body" => has_resp_body = true,
                _ => {}
            }
        }
    }

    let req_headers_export = if has_req_headers {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_request_headers(request_handle: u64) -> i32 {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                let ctx = unsafe { ::orion_wasm_sdk::RequestHandle::<::orion_wasm_sdk::RequestHeaders>::new(request_handle) };
                ::orion_wasm_sdk::Plugin::on_request_headers(plugin, &ctx).into()
            }
        }
    } else {
        quote! {}
    };

    let req_body_export = if has_req_body {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_request_body(request_handle: u64, _body_len: u32) -> i32 {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                let ctx = unsafe { ::orion_wasm_sdk::RequestHandle::<::orion_wasm_sdk::RequestBody>::new(request_handle) };
                ::orion_wasm_sdk::Plugin::on_request_body(plugin, &ctx).into()
            }
        }
    } else {
        quote! {}
    };

    let resp_headers_export = if has_resp_headers {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_response_headers(response_handle: u64) -> i32 {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                let ctx = unsafe { ::orion_wasm_sdk::ResponseHandle::<::orion_wasm_sdk::ResponseHeaders>::new(response_handle) };
                ::orion_wasm_sdk::Plugin::on_response_headers(plugin, &ctx).into()
            }
        }
    } else {
        quote! {}
    };

    let resp_body_export = if has_resp_body {
        quote! {
            #[no_mangle]
            pub extern "C" fn on_response_body(response_handle: u64, _body_len: u32) -> i32 {
                let plugin = unsafe {
                    if PLUGIN.is_none() {
                        PLUGIN = ::std::option::Option::Some(<#self_ty as ::std::default::Default>::default());
                    }
                    PLUGIN.as_mut().unwrap()
                };
                let ctx = unsafe { ::orion_wasm_sdk::ResponseHandle::<::orion_wasm_sdk::ResponseBody>::new(response_handle) };
                ::orion_wasm_sdk::Plugin::on_response_body(plugin, &ctx).into()
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
    };

    TokenStream::from(expanded)
}
