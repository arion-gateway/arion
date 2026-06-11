use std::collections::HashSet;

use axum::{extract::State, Json};
use orion_configuration::config::{
    cluster::TlsSecret,
    listener::TlsConfig as ListenerTlsConfig,
    transport::{CommonTlsValidationContext, Secrets, UpstreamTransportSocketConfig},
};
use orion_lib::{
    clusters::clusters_manager::get_all_clusters, ConfigDump, ConfigurationSenders, ListenerConfigurationChange,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::admin::AdminState;
use crate::xds_configurator::send_change_to_runtimes;

pub async fn get_certs(State(admin_state): State<AdminState>) -> Json<Value> {
    let mut cert_names: HashSet<String> = HashSet::new();
    let mut ca_names: HashSet<String> = HashSet::new();

    collect_listener_sds_names(&admin_state.configuration_senders, &mut cert_names, &mut ca_names).await;

    for cluster in get_all_clusters() {
        collect_cluster_sds_names(&cluster.transport_socket, &mut cert_names, &mut ca_names);
    }

    let certificates = admin_state.secret_manager.read().get_certs_info(&cert_names, &ca_names);
    Json(json!({ "certificates": certificates }))
}

async fn collect_listener_sds_names(
    configuration_senders: &[ConfigurationSenders],
    cert_names: &mut HashSet<String>,
    ca_names: &mut HashSet<String>,
) {
    let listeners_senders: Vec<_> = configuration_senders
        .iter()
        .map(|ConfigurationSenders { listener_configuration_sender, .. }| listener_configuration_sender.clone())
        .collect();
    let (dump_tx, mut dump_rx) = mpsc::channel::<ConfigDump>(1);
    let _ =
        send_change_to_runtimes(&listeners_senders, ListenerConfigurationChange::GetConfiguration(dump_tx)).await.ok();
    if let Some(dump) = dump_rx.recv().await {
        for listener in dump.listeners.iter().flatten() {
            for filter_chain in listener.filter_chains.values() {
                collect_filter_chain_tls_sds_names(filter_chain.tls_config.as_ref(), cert_names, ca_names);
            }
        }
    }
}

fn collect_filter_chain_tls_sds_names(
    tls_config: Option<&ListenerTlsConfig>,
    cert_names: &mut HashSet<String>,
    ca_names: &mut HashSet<String>,
) {
    let Some(tls) = tls_config else { return };
    if let Secrets::SdsConfig(names) = &tls.common_tls_context.secrets {
        cert_names.extend(names.iter().map(|n| n.to_string()));
    }
    if let Some(CommonTlsValidationContext::SdsConfig(name)) = &tls.common_tls_context.validation_context {
        ca_names.insert(name.to_string());
    }
}

fn collect_cluster_sds_names(
    transport_socket: &Option<UpstreamTransportSocketConfig>,
    cert_names: &mut HashSet<String>,
    ca_names: &mut HashSet<String>,
) {
    let tls = match transport_socket {
        Some(UpstreamTransportSocketConfig::Tls(tls)) => Some(tls),
        Some(UpstreamTransportSocketConfig::ProxyProtocol(pp)) => pp.inner_tls_config.as_ref(),
        _ => None,
    };
    let Some(tls) = tls else { return };
    if let Some(TlsSecret::SdsConfig(name)) = &tls.secret {
        cert_names.insert(name.to_string());
    }
    if let Some(CommonTlsValidationContext::SdsConfig(name)) = &tls.validation_context {
        ca_names.insert(name.to_string());
    }
}
