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

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{routing::get, Router};
use orion_configuration::config::Bootstrap;
use orion_error::{Error, Result};
use orion_lib::{ConfigurationSenders, SecretManager};
use parking_lot::RwLock;
use serde::Serialize;

use crate::admin::{
    certs::get_certs, clusters::get_clusters, help::get_help, home::get_home, listeners::get_listeners,
    memory::get_memory, ready::get_ready, server_info::get_server_info,
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

#[allow(dead_code)]
#[derive(Clone)]
struct AdminState {
    bootstrap: Bootstrap,
    configuration_senders: Vec<ConfigurationSenders>,
    secret_manager: Arc<RwLock<SecretManager>>,
    server_info: ServerInfo,
    server_startup: Instant,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default, Serialize)]
enum ProxyState {
    #[default]
    Live,
    Draining,
    PreInitializing,
    Initializing,
}

#[derive(Debug, Default, Serialize, Clone)]
struct ServerInfo {
    #[serde(default = "Default::default")]
    state: ProxyState,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    uptime_all_epochs: Option<Duration>,
}

fn build_admin_router(admin_state: AdminState) -> Router {
    let mut router = Router::new();
    router = router.route("/", get(get_home));
    router = router.route("/certs", get(get_certs));
    router = router.route("/clusters", get(get_clusters));

    #[cfg(feature = "config-dump")]
    {
        router = router.route("/config_dump", get(config_dump::get_config_dump))
    }

    router = router.route("/help", get(get_help));
    router = router.route("/listeners", get(get_listeners));
    router = router.route("/memory", get(get_memory));
    #[cfg(feature = "metrics")]
    {
        use crate::admin::reset_counters::post_reset_counters;
        use axum::routing::post;
        router = router.route("/reset_counters", post(post_reset_counters));
    }

    #[cfg(feature = "metrics")]
    {
        use crate::admin::stats::get_stats;

        router = router.route("/stats", get(get_stats));
    }

    #[cfg(feature = "prometheus")]
    {
        use crate::admin::stats::prometheus::prometheus_handler;
        router = router.route("/stats/prometheus", get(prometheus_handler))
    }

    router = router.route("/ready", get(get_ready));
    router = router.route("/server_info", get(get_server_info));

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
        server_info: ServerInfo::default(),
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

    #[tokio::test]
    #[allow(clippy::indexing_slicing)]
    async fn ready_endpoint_response() {
        let server_startup = Instant::now();
        let admin_state = AdminState {
            bootstrap: Bootstrap::default(),
            configuration_senders: vec![],
            secret_manager: Arc::new(RwLock::new(orion_lib::SecretManager::default())),
            server_info: ServerInfo::default(),
            server_startup,
        };
        let app = build_admin_router(admin_state);
        let server = TestServer::new(app).unwrap();

        // Add a small delay to ensure some uptime has elapsed
        tokio::time::sleep(Duration::from_millis(10)).await;

        let response = server.get("/ready").await;
        response.assert_status_ok();

        let value: serde_json::Value = response.json();

        // Validate the response structure
        assert_eq!(value["state"], "Live");
        assert!(value["uptime_all_epochs"].is_object());

        // Parse the protobuf Duration format and validate it's reasonable
        let uptime_obj = &value["uptime_all_epochs"];
        assert!(uptime_obj["secs"].is_number());
        assert!(uptime_obj["nanos"].is_number());

        let seconds = uptime_obj["secs"].as_u64().unwrap();
        let nanos = uptime_obj["nanos"].as_u64().unwrap();
        let uptime_duration = Duration::new(seconds, u32::try_from(nanos).unwrap());

        // The uptime should be at least 10ms (our sleep)
        assert!(uptime_duration >= Duration::from_millis(10));
    }
}
