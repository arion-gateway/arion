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

use std::time::Instant;
use std::time::{Duration, Instant};
use triomphe::Arc;

use axum::{routing::get, Router};
use orion_configuration::config::Bootstrap;
use orion_error::{Error, Result};
use orion_lib::{ConfigDump, ConfigurationSenders, ListenerConfigurationChange, SecretManager};
use parking_lot::RwLock;
use pingora_timeout::fast_timeout::fast_timeout;
use tokio::sync::mpsc;
use tracing::warn;

use crate::admin::{
    certs::certs_handler, clusters::clusters_handler, help::help_handler, home::home_handler,
    listeners::listeners_handler, memory::memory_handler, ready::ready_handler, server_info::server_info_handler,
};

mod certs;
mod clusters;
#[cfg(feature = "config-dump")]
mod config_dump;
mod help;
mod home;
mod listeners;
mod memory;
mod ready;
#[cfg(feature = "metrics")]
mod reset_counters;
mod server_info;
#[cfg(feature = "metrics")]
mod stats;

const CONFIG_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
/// Ask a proxy runtime for its listener configuration.
/// Runtimes are tried in order until one replies, rather than queried all at once.
async fn query_listener_configuration(configuration_senders: &[ConfigurationSenders]) -> Option<ConfigDump> {
    query_listener_configuration_within(configuration_senders, CONFIG_QUERY_TIMEOUT).await
}

async fn query_listener_configuration_within(
    configuration_senders: &[ConfigurationSenders],
    budget: Duration,
) -> Option<ConfigDump> {
    let deadline = Instant::now() + budget;

    for (id, ConfigurationSenders { listener_configuration_sender, .. }) in configuration_senders.iter().enumerate() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        let (dump_tx, mut dump_rx) = mpsc::channel::<ConfigDump>(1);
        let attempt = async {
            listener_configuration_sender.send(ListenerConfigurationChange::GetConfiguration(dump_tx)).await.ok()?;
            dump_rx.recv().await
        };
        if let Ok(Some(dump)) = fast_timeout(remaining, attempt).await {
            return Some(dump);
        }
        warn!("Proxy runtime {id} did not answer the listener configuration query");
    }
    None
}

#[allow(dead_code)]
#[derive(Clone)]
struct AdminState {
    bootstrap: Bootstrap,
    configuration_senders: Vec<ConfigurationSenders>,
    secret_manager: Arc<RwLock<SecretManager>>,
    server_startup: Instant,
}

fn build_admin_router(admin_state: AdminState) -> Router {
    let mut router = Router::new();
    router = router.route("/", get(home_handler));
    router = router.route("/certs", get(certs_handler));
    router = router.route("/clusters", get(clusters_handler));

    #[cfg(feature = "config-dump")]
    {
        router = router.route("/config_dump", get(config_dump::config_dump_handler))
    }

    router = router.route("/help", get(help_handler));
    router = router.route("/listeners", get(listeners_handler));
    router = router.route("/memory", get(memory_handler));
    #[cfg(feature = "metrics")]
    {
        use crate::admin::reset_counters::reset_counters_handler;
        use axum::routing::post;
        router = router.route("/reset_counters", post(reset_counters_handler))
    };

    #[cfg(feature = "metrics")]
    {
        use crate::admin::stats::canonical::stats_handler;
        router = router.route("/stats", get(stats_handler))
    };

    #[cfg(feature = "prometheus")]
    {
        use crate::admin::stats::prometheus::prometheus_handler;
        router = router.route("/stats/prometheus", get(prometheus_handler))
    }

    router = router.route("/ready", get(ready_handler));
    router = router.route("/server_info", get(server_info_handler));

    router.with_state(admin_state)
}

pub async fn start_admin_server(
    bootstrap: Bootstrap,
    configuration_senders: Vec<ConfigurationSenders>,
    secret_manager: Arc<RwLock<SecretManager>>,
) -> Result<()> {
    let admin_state = AdminState {
        bootstrap: bootstrap.clone(),
        configuration_senders,
        secret_manager,
        server_startup: Instant::now(),
    };
    let app = build_admin_router(admin_state);
    let address =
        bootstrap.admin.ok_or(Error::from("Missing admin configuration in bootstrap"))?.address.into_socket_addr()?;
    let listener = tokio::net::TcpListener::bind(address).await?;

    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum_test::TestServer;
    use orion_stats::{set_proxy_state, ProxyState};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn ready_endpoint_response() {
        let admin_state = AdminState {
            bootstrap: Bootstrap::default(),
            configuration_senders: vec![],
            secret_manager: Arc::new(RwLock::new(orion_lib::SecretManager::default())),
            server_startup: Instant::now(),
        };
        let app = build_admin_router(admin_state);
        let server = TestServer::new(app).unwrap();

        // Before the proxy reports itself live, /ready must fail.
        let response = server.get("/ready").await;
        response.assert_status_service_unavailable();

        set_proxy_state(ProxyState::Live);

        let response = server.get("/ready").await;
        response.assert_status_ok();
        assert_eq!(response.text(), "LIVE");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_busy_runtime_survives_a_config_query() {
        use std::time::Duration as StdDuration;

        // runtime 1: idle, answers immediately
        let (listener_tx_1, listener_rx_1) = mpsc::channel(100);
        let (route_tx_1, route_rx_1) = mpsc::channel(100);
        let idle_manager = tokio::spawn(orion_lib::ListenersManager::new(listener_rx_1, route_rx_1).start());

        // runtime 2: on its own runtime thread, occupied with data-plane work when the query lands
        let (listener_tx_2, listener_rx_2) = mpsc::channel(100);
        let (route_tx_2, route_rx_2) = mpsc::channel(100);
        let (exited_tx, exited_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            #[allow(clippy::unwrap_used)]
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async move {
                std::thread::sleep(StdDuration::from_millis(300));
                let result = orion_lib::ListenersManager::new(listener_rx_2, route_rx_2).start().await;
                let _ = exited_tx.send(result);
            });
        });

        let admin_state = AdminState {
            bootstrap: Bootstrap::default(),
            configuration_senders: vec![
                ConfigurationSenders {
                    listener_configuration_sender: listener_tx_1,
                    route_configuration_sender: route_tx_1,
                },
                ConfigurationSenders {
                    listener_configuration_sender: listener_tx_2,
                    route_configuration_sender: route_tx_2,
                },
            ],
            secret_manager: Arc::new(RwLock::new(orion_lib::SecretManager::default())),
            server_startup: Instant::now(),
        };
        #[allow(clippy::unwrap_used)]
        let server = TestServer::new(build_admin_router(admin_state)).unwrap();

        let mut endpoints = vec!["/certs", "/listeners"];
        #[cfg(feature = "config-dump")]
        endpoints.push("/config_dump");
        for endpoint in endpoints {
            server.get(endpoint).await.assert_status_ok();
        }

        // the busy runtime's manager must still be serving once it works through the query
        match exited_rx.recv_timeout(StdDuration::from_secs(1)) {
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {},
            Ok(result) => panic!("listeners manager on the busy runtime exited: {result:?}"),
            Err(err) => panic!("busy runtime thread went away: {err}"),
        }
        assert!(!idle_manager.is_finished(), "listeners manager on the idle runtime exited");
        idle_manager.abort();
    }

    /// Spawns a stand-in for a runtime's listeners manager.
    fn spawn_stub_runtime(
        replies: bool,
        seen: Arc<AtomicUsize>,
        undeliverable: Arc<AtomicUsize>,
    ) -> (ConfigurationSenders, tokio::task::JoinHandle<()>) {
        let (listener_configuration_sender, mut listener_rx) = mpsc::channel(100);
        let (route_configuration_sender, route_rx) = mpsc::channel(100);
        let handle = tokio::spawn(async move {
            let _keep_route_rx_open = route_rx;
            while let Some(change) = listener_rx.recv().await {
                if let ListenerConfigurationChange::GetConfiguration(reply_tx) = change {
                    seen.fetch_add(1, Ordering::SeqCst);
                    if !replies {
                        continue;
                    }
                    let dump = ConfigDump { listeners: Some(vec![]), ..Default::default() };
                    if reply_tx.send(dump).await.is_err() {
                        undeliverable.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });
        (ConfigurationSenders { listener_configuration_sender, route_configuration_sender }, handle)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_config_query_reaches_only_one_runtime() {
        let (seen_a, seen_b) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let undeliverable = Arc::new(AtomicUsize::new(0));
        let (senders_a, task_a) = spawn_stub_runtime(true, Arc::clone(&seen_a), Arc::clone(&undeliverable));
        let (senders_b, task_b) = spawn_stub_runtime(true, Arc::clone(&seen_b), Arc::clone(&undeliverable));

        let dump = query_listener_configuration(&[senders_a, senders_b]).await;

        assert!(dump.is_some(), "no runtime answered the configuration query");
        assert_eq!(seen_a.load(Ordering::SeqCst), 1, "the first runtime should have been queried once");
        assert_eq!(seen_b.load(Ordering::SeqCst), 0, "the second runtime should not have been queried");
        assert_eq!(undeliverable.load(Ordering::SeqCst), 0, "a reply was left undeliverable");
        task_a.abort();
        task_b.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_silent_runtime_falls_through_to_the_next() {
        let (seen_a, seen_b) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let undeliverable = Arc::new(AtomicUsize::new(0));
        let (senders_a, task_a) = spawn_stub_runtime(false, Arc::clone(&seen_a), Arc::clone(&undeliverable));
        let (senders_b, task_b) = spawn_stub_runtime(true, Arc::clone(&seen_b), Arc::clone(&undeliverable));

        let dump = query_listener_configuration_within(&[senders_a, senders_b], Duration::from_millis(500)).await;

        assert!(dump.is_some(), "the healthy runtime should have answered");
        assert_eq!(seen_a.load(Ordering::SeqCst), 1, "the silent runtime should have been tried first");
        assert_eq!(seen_b.load(Ordering::SeqCst), 1, "the healthy runtime should have been tried next");
        task_a.abort();
        task_b.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_runtimes_silent_gives_up_within_the_budget() {
        let seen = Arc::new(AtomicUsize::new(0));
        let undeliverable = Arc::new(AtomicUsize::new(0));
        let (senders_a, task_a) = spawn_stub_runtime(false, Arc::clone(&seen), Arc::clone(&undeliverable));
        let (senders_b, task_b) = spawn_stub_runtime(false, Arc::clone(&seen), Arc::clone(&undeliverable));

        let budget = Duration::from_millis(500);
        let start = Instant::now();
        let dump = query_listener_configuration_within(&[senders_a, senders_b], budget).await;

        assert!(dump.is_none());
        assert!(start.elapsed() < budget * 3, "gave up after {:?}, well past the budget", start.elapsed());
        task_a.abort();
        task_b.abort();
    }
}
