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

use axum::{
    extract::State,
    response::{IntoResponse, Json},
};
use serde::Serialize;

use crate::admin::AdminState;

#[derive(Serialize)]
struct ServerInfoResponse {
    version: String,
    state: String,
    uptime_current_epoch: String,
    uptime_all_epochs: String,
    command_line_options: CommandLineOptions,
    node: NodeInfo,
}

#[derive(Serialize)]
struct CommandLineOptions {
    concurrency: usize,
}

#[derive(Serialize)]
struct NodeInfo {
    id: String,
    cluster: String,
}

pub async fn server_info_handler(State(admin_state): State<AdminState>) -> impl IntoResponse {
    let uptime_str = format!("{}s", admin_state.server_startup.elapsed().as_secs());

    let state = arion_stats::get_proxy_state()
        .map(|s| format!("{s:?}").to_uppercase())
        .unwrap_or_else(|| "INITIALIZING".to_owned());

    let concurrency = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);

    let node = admin_state.bootstrap.node.as_ref().map_or_else(
        || NodeInfo { id: String::new(), cluster: String::new() },
        |n| NodeInfo { id: n.id.to_string(), cluster: n.cluster_id.to_string() },
    );

    Json(ServerInfoResponse {
        version: format!("arion/{}", env!("CARGO_PKG_VERSION")),
        state,
        uptime_current_epoch: uptime_str.clone(),
        uptime_all_epochs: uptime_str,
        command_line_options: CommandLineOptions { concurrency },
        node,
    })
}
