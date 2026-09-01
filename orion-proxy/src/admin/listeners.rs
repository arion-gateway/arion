use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
};
use orion_configuration::config::listener::ListenerType;
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
