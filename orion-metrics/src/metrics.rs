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

use tracing::info;

use crate::Metrics;
pub mod clusters;
pub mod filters;
pub mod http;
pub mod listeners;
pub mod server;
pub mod tcp;
pub mod tls;
pub mod user;

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
pub fn init_per_thread_metrics(_metrics: &[Metrics]) {
    info!("Initializing per-thread metrics...");
}

// This function initializes global metrics based on the provided configuration. Must be called once at application startup.
//
pub fn init_global_metrics(_metrics: &[Metrics], number_of_threads: usize) {
    info!("Initializing global metrics...");
    tcp::init_metrics();
    tls::init_metrics();
    http::init_metrics();
    listeners::init_metrics();
    clusters::init_metrics();
    server::init_metrics(number_of_threads);
    user::init_metrics();
    filters::init_metrics();
}
