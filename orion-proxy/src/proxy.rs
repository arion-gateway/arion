// Copyright 2025 The kmesh Authors
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

use crate::{
    admin::start_admin_server,
    core_affinity,
    runtime::{self, RuntimeId},
    xds_configurator::XdsConfigurationHandler,
};
use futures::future::join_all;
use orion_configuration::config::{
    bootstrap::Node, log::AccessLogConfig, metrics::MetricsConfig, runtime::Affinity, Bootstrap,
};

#[cfg(feature = "tracing")]
use {
    orion_configuration::config::network_filters::tracing::{TracingConfig, TracingKey},
    std::collections::HashMap,
};

use tokio::{sync::mpsc::Sender, task::JoinSet};

#[cfg(feature = "access-log")]
use orion_lib::access_log::{start_access_loggers, update_configuration};

use orion_error::Context;
use orion_lib::{
    clusters::cluster::ClusterType, get_listeners_and_clusters, new_configuration_channel, runtime_config,
    ConfigurationReceivers, ConfigurationSenders, ListenerConfigurationChange, PartialClusterType, Result,
    SecretManager,
};
#[cfg(feature = "metrics")]
use orion_metrics::{metrics::init_global_metrics, wait_for_metrics_setup, OtelExporterConfig};

use parking_lot::RwLock;
use std::{
    sync::Arc,
    thread::{self, JoinHandle},
};

use tracing::{debug, error, info, warn};

pub fn run_orion(bootstrap: Bootstrap, metrics: Option<MetricsConfig>, access_log_config: Option<AccessLogConfig>) {
    debug!("Starting on thread {:?}", std::thread::current().name());

    // launch the runtimes...
    if let Err(e) = launch_runtimes(bootstrap, metrics, access_log_config) {
        error!("Failed to launch runtimes: {e:?}");
        std::process::exit(1);
    }
}

fn calculate_num_threads_per_runtime(num_cpus: usize, num_runtimes: usize) -> Result<usize> {
    let avail_cpus = core_affinity::get_avail_core_num()?;
    if num_cpus > avail_cpus {
        return Err(
            format!("The number of CPUs ({num_cpus}) exceeds those available for this process ({avail_cpus})").into()
        );
    }

    let threads = num_cpus / num_runtimes;
    if threads == 0 {
        return Err(
            format!("The number of runtimes greater than the number of cpus ({num_cpus} < {num_runtimes})").into()
        );
    }

    if !num_cpus.is_multiple_of(num_runtimes) {
        return Err(format!(
            "The number of CPUs ({num_cpus}) is not a multiple of the number of runtimes ({num_runtimes})",
        )
        .into());
    }

    Ok(threads)
}

#[derive(Debug, Clone)]
struct ServiceInfo {
    bootstrap: Bootstrap,
    node: Node,
    configuration_senders: Vec<ConfigurationSenders>,
    secret_manager: Arc<RwLock<SecretManager>>,
    listener_factories: Vec<orion_lib::ListenerFactory>,
    clusters: Vec<orion_lib::PartialClusterType>,
    ads_cluster_names: Vec<String>,
    #[cfg(feature = "access-log")]
    access_log_config: Option<AccessLogConfig>,
    #[cfg(feature = "tracing")]
    tracing: HashMap<TracingKey, TracingConfig>,
    #[cfg(feature = "metrics")]
    otel_exporters: Vec<OtelExporterConfig>,
}

type SenderGuards = Vec<ConfigurationSenders>;

fn launch_runtimes(
    bootstrap: Bootstrap,
    #[allow(unused_variables)] metrics_config: Option<MetricsConfig>,
    _access_log_config: Option<AccessLogConfig>,
) -> Result<SenderGuards> {
    let rt_config = runtime_config();
    let num_runtimes = rt_config.num_runtimes();
    let num_cpus = rt_config.num_cpus();

    // build the XDS configuration channels...
    //

    let (config_senders, config_receivers): (Vec<ConfigurationSenders>, Vec<ConfigurationReceivers>) =
        (0..num_runtimes).map(|_| new_configuration_channel(100)).collect::<Vec<_>>().into_iter().unzip();

    // keep a copy of the senders to avoid them being dropped if no services are configured...
    //

    let sender_guards = config_senders.clone();

    // launch services runtime...
    //

    #[cfg(feature = "metrics")]
    let otel_exporters = OtelExporterConfig::extract_from_bootstrap(&bootstrap);

    #[cfg(feature = "tracing")]
    let tracing = bootstrap
        .static_resources
        .listeners
        .iter()
        .flat_map(orion_configuration::config::Listener::get_tracing_configurations)
        .collect::<HashMap<_, _>>();

    // The xDS runtime always runs - this is necessary for initialization even if we do not
    // use dynamic updates from remote xDS servers. The decision on whether dynamic updates
    // are used is based on:
    // - The bootstrap loader from orion-data-plane-api gets the list of cluster names used
    //   in dynamic_resources/ads_config (for grpc_services)
    // - resolve ads clusters into endpoints, to be used as xDS address
    // TODO: the xDS client could receive updates for endpoints too i.e. dynamic clusters. We
    // should replace this with passing a configuration receiver. For now endpoints from
    // static clusters.

    let ads_cluster_names: Vec<String> = bootstrap.get_ads_configs().iter().map(ToString::to_string).collect();
    let node = bootstrap.node.clone().unwrap_or_else(|| Node { id: "".into(), cluster_id: "".into() });

    let (secret_manager, listener_factories, clusters) =
        get_listeners_and_clusters(bootstrap.clone()).with_context_msg("Failed to get listeners and clusters")?;
    let secret_manager = Arc::new(RwLock::new(secret_manager));

    if listener_factories.is_empty() && ads_cluster_names.is_empty() {
        return Err("No listeners and no ads clusters configured".into());
    }

    #[allow(clippy::used_underscore_binding)]
    let service_info = ServiceInfo {
        node,
        configuration_senders: config_senders,
        secret_manager,
        listener_factories,
        bootstrap,
        clusters,
        ads_cluster_names,
        #[cfg(feature = "access-log")]
        access_log_config: _access_log_config,
        #[cfg(feature = "tracing")]
        tracing,
        #[cfg(feature = "metrics")]
        otel_exporters: otel_exporters.clone(),
    };

    info!("Launching Service runtime with {} threads", rt_config.num_service_threads.get());

    let services_handle = spawn_services_runtime_from_thread(
        "services",
        rt_config.num_service_threads.get() as usize,
        None,
        service_info,
    )?;

    #[cfg(feature = "metrics")]
    if !otel_exporters.is_empty() {
        info!("Waiting for metrics setup to complete...");
        wait_for_metrics_setup();
    }

    //////////////////////////////////////////////////////////////////////////////////////////
    // run the proxy runtimes...
    //

    let num_threads_per_runtime = calculate_num_threads_per_runtime(num_cpus, num_runtimes)
        .with_context_msg("failed to calculate number of threads to use per runtime")?;

    #[cfg(feature = "metrics")]
    init_global_metrics(
        &otel_exporters,
        metrics_config.as_ref().unwrap_or(&MetricsConfig::default()),
        num_threads_per_runtime * num_runtimes,
    );

    info!("Launching {num_runtimes} worker runtime(s) with {num_threads_per_runtime} thread(s) each");

    let proxy_handles = {
        (0..num_runtimes)
            .zip(config_receivers.into_iter())
            .map(|(id, config_receivers)| {
                spawn_proxy_runtime_from_thread(
                    "proxy",
                    num_threads_per_runtime,
                    rt_config.affinity_strategy.clone().map(|affinity| (RuntimeId(id), affinity)),
                    config_receivers,
                    #[cfg(feature = "metrics")]
                    otel_exporters.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?
    };

    let handles = proxy_handles.into_iter().chain(std::iter::once(services_handle)).collect::<Vec<_>>();

    for h in handles {
        if let Err(err) = h.join() {
            warn!("Closing handler with error {err:?}");
        }
    }

    #[cfg(feature = "instrumentation")]
    {
        orion_lib::instrumentation::dump_instrumentation_counters();
    }

    Ok(sender_guards)
}

type RuntimeHandle = JoinHandle<Result<()>>;

fn spawn_proxy_runtime_from_thread(
    thread_name: &'static str,
    num_threads: usize,
    affinity_info: Option<(RuntimeId, Affinity)>,
    configuration_receivers: ConfigurationReceivers,
    #[cfg(feature = "metrics")] otel_exporters: Vec<OtelExporterConfig>,
) -> Result<RuntimeHandle> {
    let thread_name = build_thread_name(thread_name, affinity_info.as_ref());

    let handle: JoinHandle<Result<()>> = thread::Builder::new().name(thread_name.clone()).spawn(move || {
        #[cfg(feature = "metrics")]
        let rt = runtime::build_tokio_runtime(&thread_name, num_threads, affinity_info, otel_exporters);
        #[cfg(not(feature = "metrics"))]
        let rt = runtime::build_tokio_runtime(&thread_name, num_threads, affinity_info);

        rt.block_on(async {
            tokio::select! {
                _ = start_proxy(configuration_receivers) => {
                    info!("Proxy Runtime terminated!");
                    Ok(())
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("CTRL+C (Proxy runtime)!");
                    Ok(())
                }
            }
        })
    })?;
    Ok(handle)
}

fn spawn_services_runtime_from_thread(
    thread_name: &'static str,
    num_threads: usize,
    affinity_info: Option<(RuntimeId, Affinity)>,
    service_info: ServiceInfo,
) -> Result<RuntimeHandle> {
    let thread_name = build_thread_name(thread_name, affinity_info.as_ref());

    let rt_handle = thread::Builder::new().name(thread_name.clone()).spawn(move || {
        #[cfg(feature = "metrics")]
        let rt = runtime::build_tokio_runtime(&thread_name, num_threads, affinity_info, vec![]);
        #[cfg(not(feature = "metrics"))]
        let rt = runtime::build_tokio_runtime(&thread_name, num_threads, affinity_info);

        rt.block_on(async {
            tokio::select! {
                result = spawn_services(service_info) => {
                    if let Err(err) = result {
                        warn!("Error in services runtime: {err:?}");
                    }
                    info!("Service Runtime terminated!");
                    Ok(())
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("CTRL+C (service runtime)!");
                    Ok(())
                }
            }
        })
    })?;

    Ok(rt_handle)
}

#[inline]
fn build_thread_name(thread_name: &'static str, affinity_info: Option<&(RuntimeId, Affinity)>) -> String {
    match affinity_info {
        Some((runtime_id, _)) => format!("{thread_name}_RT{runtime_id}"),
        None => format!("{thread_name}_RT"),
    }
}

async fn spawn_services(info: ServiceInfo) -> Result<()> {
    let ServiceInfo {
        bootstrap,
        node,
        configuration_senders,
        secret_manager,
        listener_factories,
        clusters,
        ads_cluster_names,
        #[cfg(feature = "access-log")]
        access_log_config,
        #[cfg(feature = "tracing")]
        tracing,
        #[cfg(feature = "metrics")]
            otel_exporters: exporters,
    } = info;
    let mut set: JoinSet<Result<()>> = JoinSet::new();

    // spawn XSD configuration service...
    let configuration_senders_clone = configuration_senders.clone();
    let bootstrap_clone = bootstrap.clone();
    let secret_manager_clone = secret_manager.clone();
    set.spawn(async move {
        let initial_clusters = configure_initial_resources(
            bootstrap_clone,
            listener_factories,
            clusters,
            configuration_senders_clone.clone(),
        )
        .await?;
        if !ads_cluster_names.is_empty() {
            let mut xds_handler = XdsConfigurationHandler::new(secret_manager_clone, configuration_senders_clone);
            _ = xds_handler.run_loop(node, initial_clusters, ads_cluster_names).await;
        }
        Ok(())
    });

    // spawn access loggers service...
    #[cfg(feature = "access-log")]
    if let Some(conf) = access_log_config {
        let listeners = bootstrap.static_resources.listeners.clone();
        set.spawn(async move {
            let handles = start_access_loggers(
                conf.num_instances.get(),
                conf.queue_length.get(),
                conf.log_rotation.map(|x| x.0).clone(),
                conf.log_max_size.clone(),
                conf.max_log_files.get(),
                conf.blocking,
            );

            info!("Access loggers started with {} instances", conf.num_instances);

            for (target, access_log_config) in
                listeners.iter().map(|l| l.all_access_log_configs()).flatten().collect::<Vec<_>>()
            {
                _ = update_configuration(target.into(), access_log_config).await;
            }

            handles.join_all().await;
            Ok(())
        });
    }

    // spawn admin interface task
    if bootstrap.admin.is_some() {
        set.spawn(async move {
            _ = start_admin_server(bootstrap, configuration_senders, secret_manager).await;
            Ok(())
        });
    }

    // spawn metrics exporter...
    #[cfg(feature = "metrics")]
    if exporters.is_empty() {
        info!("OTEL metrics: stats_sink not configured (skipped)");
    } else {
        orion_metrics::otel_launch_exporter(&exporters).await?;
    }

    // spawn tracing exporters...
    #[cfg(feature = "tracing")]
    if tracing.is_empty() {
        info!("OTEL tracing: no tracers configured (skipped)");
    } else {
        orion_tracing::otel_update_tracers(tracing)?;
    }

    set.join_all().await;
    Ok(())
}

async fn configure_initial_resources(
    bootstrap: Bootstrap,
    listeners: Vec<orion_lib::ListenerFactory>,
    clusters: Vec<PartialClusterType>,
    configuration_senders: Vec<ConfigurationSenders>,
) -> Result<Vec<ClusterType>> {
    let initial_clusters: Vec<ClusterType> =
        clusters.into_iter().map(orion_lib::clusters::add_cluster).collect::<Result<_>>()?;

    let listeners_tx: Vec<_> = configuration_senders
        .into_iter()
        .map(|ConfigurationSenders { listener_configuration_sender, route_configuration_sender: _ }| {
            listener_configuration_sender
        })
        .collect();

    for (listener, listener_conf) in listeners.iter().zip(bootstrap.static_resources.listeners) {
        let _ = join_all(listeners_tx.iter().map(|listener_tx: &Sender<ListenerConfigurationChange>| {
            listener_tx.send(ListenerConfigurationChange::Added(Box::new((listener.clone(), listener_conf.clone()))))
        }))
        .await
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::<orion_error::Error>::into)?;
    }

    Ok(initial_clusters)
}

async fn start_proxy(configuration_receivers: ConfigurationReceivers) -> Result<()> {
    orion_lib::start_listener_manager(configuration_receivers).await?;
    Ok(())
}
