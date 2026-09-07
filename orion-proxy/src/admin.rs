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
use triomphe::Arc;

use axum::{routing::get, Router};
use orion_configuration::config::Bootstrap;
use orion_error::{Error, Result};
use orion_lib::{ConfigurationSenders, SecretManager};
use parking_lot::RwLock;

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
}
