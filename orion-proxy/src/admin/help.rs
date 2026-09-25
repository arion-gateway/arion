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

use crate::admin::AdminState;
use axum::extract::State;

#[cfg(feature = "config-dump")]
const CONFIG_DUMP_HELP: &str = "  /config_dump: dump current Orion configs\n";
#[cfg(not(feature = "config-dump"))]
const CONFIG_DUMP_HELP: &str = "";

#[cfg(feature = "metrics")]
const STATS_HELP: &str = "  /stats: print server stats\n";
#[cfg(not(feature = "metrics"))]
const STATS_HELP: &str = "";

#[cfg(feature = "prometheus")]
const PROMETHEUS_HELP: &str = "  /stats/prometheus: print server stats in prometheus format\n";
#[cfg(not(feature = "prometheus"))]
const PROMETHEUS_HELP: &str = "";

#[cfg(feature = "metrics")]
const RESET_COUNTERS_HELP: &str = "  /reset_counters (POST): reset all counters to zero\n";
#[cfg(not(feature = "metrics"))]
const RESET_COUNTERS_HELP: &str = "";

pub async fn help_handler(State(mut _admin_state): State<AdminState>) -> String {
    const_format::concatcp!(
        "admin commands are:\n",
        "  /: admin home page\n",
        "  /certs: print certs on machine\n",
        "  /clusters: upstream cluster status\n",
        CONFIG_DUMP_HELP,
        "  /help: print out list of admin commands\n",
        "  /listeners: print listener info\n",
        "  /memory: print current allocation/heap usage\n",
        "  /ready: print server state, return 200 if LIVE, otherwise return 503\n",
        RESET_COUNTERS_HELP,
        "  /server_info: print server version/status information\n",
        STATS_HELP,
        PROMETHEUS_HELP,
    )
    .into()
}
