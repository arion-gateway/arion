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

use orion_configuration::config::metrics::MetricsConfig;
use tracing::info;

use crate::OtelExporterConfig;
pub mod clusters;
pub mod custom;
pub mod filters;
pub mod http;
pub mod listeners;
pub mod mcp;
pub mod server;
pub mod tcp;
pub mod tls;
pub mod user;

pub const PREFIX_TCP: &str = "tcp";
pub const PREFIX_HTTP: &str = "http";
pub const PREFIX_CUSTOM: &str = "custom";
pub const PREFIX_SERVER: &str = "server";
pub const PREFIX_TLS: &str = "tls";
pub const PREFIX_CLUSTER: &str = "cluster";
pub const PREFIX_FILTER: &str = "filter";
pub const PREFIX_USER: &str = "user";
pub const PREFIX_LISTENERS: &str = "listeners";

pub struct Metric<T> {
    pub prefix: &'static str,
    pub name: &'static str,
    pub descr: &'static str,
    pub value: T,
}

impl<T> Metric<T> {
    #[allow(dead_code)]
    fn new(prefix: &'static str, name: &'static str, descr: &'static str, value: T) -> Self {
        Metric { prefix, name, descr, value }
    }
}

// This function initializes per-thread metrics based on the provided configuration.
// It must be called in the context of each thread that needs to collect metrics (e.g. Tokio threads)
pub fn init_per_thread_metrics(_metrics: &[OtelExporterConfig]) {
    info!("Initializing per-thread metrics...");
}

pub fn resolve_metric_name(
    rename: &std::collections::HashMap<String, String>,
    default_name: &'static str,
) -> &'static str {
    if let Some(new_name) = rename.get(default_name) {
        orion_interner::StringInterner::to_static_str(new_name)
    } else {
        default_name
    }
}

// This function initializes global metrics based on the provided configuration. Must be called once at application startup.
//
pub fn init_global_metrics(_exporters_config: &[OtelExporterConfig], config: &MetricsConfig, number_of_threads: usize) {
    info!("Initializing global metrics...");
    tcp::init_metrics(&config.rename);
    tls::init_metrics(&config.rename);
    http::init_metrics(&config.rename);
    listeners::init_metrics(&config.rename);
    clusters::init_metrics(&config.rename);
    filters::init_metrics(&config.rename);
    server::init_metrics(number_of_threads, &config.rename);
    user::init_metrics(&config.rename);
    mcp::init_metrics();
    custom::init_metrics(&config.custom_metrics);
}

use crate::sharded::Clearable;

impl<T: Clearable> Clearable for Metric<T> {
    fn clear(&self) {
        self.value.clear();
    }
}

impl<T: Clearable> Clearable for std::sync::OnceLock<T> {
    fn clear(&self) {
        if let Some(val) = self.get() {
            val.clear();
        }
    }
}

pub fn reset_global_metrics() {
    info!("Resetting global metrics...");
    tcp::reset_metrics();
    tls::reset_metrics();
    http::reset_metrics();
    listeners::reset_metrics();
    clusters::reset_metrics();
    filters::reset_metrics();
    server::reset_metrics();
    user::reset_metrics();
    custom::reset_metrics();
}
