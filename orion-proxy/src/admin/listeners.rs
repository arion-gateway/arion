use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
};
use orion_configuration::config::listener::ListenerType;
use orion_lib::{ConfigDump, ConfigurationSenders, ListenerConfigurationChange};
use std::fmt::Write;
use tokio::sync::mpsc;

use crate::{admin::AdminState, xds_configurator::send_change_to_runtimes};

async fn build_listeners_output(admin_state: AdminState) -> String {
    let mut listeners_senders = Vec::with_capacity(admin_state.configuration_senders.len());
    for ConfigurationSenders { listener_configuration_sender, .. } in admin_state.configuration_senders {
        listeners_senders.push(listener_configuration_sender);
    }

    let (config_dump_sender, mut config_dump_receiver) = mpsc::channel::<ConfigDump>(100);
    let change = ListenerConfigurationChange::GetConfiguration(config_dump_sender);
    let _ = send_change_to_runtimes(&listeners_senders, change).await.ok();

    let mut out = String::new();
    if let Some(config_dump) = config_dump_receiver.recv().await {
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
