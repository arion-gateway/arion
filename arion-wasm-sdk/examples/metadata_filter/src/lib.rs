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

//! Metadata filter example using the Arion Wasm SDK.
//!
//! This plugin demonstrates how to extract downstream connection metadata.

use arion_wasm_sdk::{init_tracing, arion_plugin, FilterAction, HttpHeaders, Plugin, RequestHandle};
use tracing::{error, debug};

#[derive(Default)]
struct MetadataFilter;

#[arion_plugin]
impl Plugin for MetadataFilter {
    fn on_plugin_start(&mut self) {
        let _ = init_tracing();
        debug!("MetadataFilter initialized!");
    }

    fn on_request_headers(&mut self, ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        debug!("Extracting downstream metadata...");
        match ctx.get_downstream_metadata() {
            Ok(Some(metadata)) => {
                debug!("Successfully extracted downstream metadata:");
                debug!("  Listener Name: {}", metadata.listener_name);

                let _ = ctx.set_header(
                    http::header::HeaderName::from_static("x-listener-name"),
                    http::header::HeaderValue::from_str(&metadata.listener_name).unwrap(),
                );

                if let Some(sni) = &metadata.sni {
                    debug!("  SNI: {}", sni);
                    let _ = ctx.set_header(
                        http::header::HeaderName::from_static("x-sni"),
                        http::header::HeaderValue::from_str(sni).unwrap(),
                    );
                } else {
                    debug!("  SNI: None");
                }
                debug!("  Connection: {:?}", metadata.connection);

                match &metadata.connection {
                    arion_wasm_types::DownstreamConnectionMetadata::FromSocket { peer_address, local_address } => {
                        let _ = ctx.set_header(
                            http::header::HeaderName::from_static("x-connection-peer"),
                            http::header::HeaderValue::from_str(&peer_address.to_string()).unwrap(),
                        );
                        let _ = ctx.set_header(
                            http::header::HeaderName::from_static("x-connection-local"),
                            http::header::HeaderValue::from_str(&local_address.to_string()).unwrap(),
                        );
                    },
                    arion_wasm_types::DownstreamConnectionMetadata::FromProxyProtocol {
                        proxy_peer_address,
                        proxy_local_address,
                        ..
                    } => {
                        let _ = ctx.set_header(
                            http::header::HeaderName::from_static("x-connection-peer"),
                            http::header::HeaderValue::from_str(&proxy_peer_address.to_string()).unwrap(),
                        );
                        let _ = ctx.set_header(
                            http::header::HeaderName::from_static("x-connection-local"),
                            http::header::HeaderValue::from_str(&proxy_local_address.to_string()).unwrap(),
                        );
                    },
                }
            },
            Ok(None) => {
                debug!("No downstream metadata found for this request.");
            },
            Err(e) => {
                error!("Error while extracting downstream metadata: {:?}", e);
            },
        }

        FilterAction::Continue
    }
}
