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

use arion_configuration::config::listener::ListenerType;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
};
use std::fmt::Write;

use crate::admin::{query_listener_configuration, AdminState};

async fn build_listeners_output(admin_state: AdminState) -> String {
    let mut out = String::new();
    if let Some(config_dump) = query_listener_configuration(&admin_state.configuration_senders).await {
        if let Some(listeners) = config_dump.listeners {
            for listener in &listeners {
                let address = match &listener.listener_type {
                    ListenerType::Socket { address, .. } => address.to_string(),
                    ListenerType::Internal { .. } => String::new(),
                };
                _ = writeln!(out, "{}::{address}", listener.name);
            }
        }
    }
    out
}

pub async fn listeners_handler(
    State(admin_state): State<AdminState>,
) -> Result<(HeaderMap, String), (StatusCode, String)> {
    let out = build_listeners_output(admin_state).await;

    let mut headers = HeaderMap::new();
    #[allow(clippy::unwrap_used)]
    headers.insert(::http::header::CONTENT_TYPE, "text/plain".parse().unwrap());
    Ok((headers, out))
}
