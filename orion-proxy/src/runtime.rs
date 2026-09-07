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

use crate::core_affinity::{self, AffinityStrategy};
use orion_configuration::config::runtime::Affinity;
use orion_lib::runtime_config;
use orion_lib::runtime_context::set_runtime_id;

#[cfg(feature = "metrics")]
use orion_metrics::{metrics::init_per_thread_metrics, OtelExporterConfig};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt::Display, ops::Deref};
use tokio::runtime::{Builder, Runtime};
use tracing::{info, warn};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RuntimeId(pub usize);

impl Display for RuntimeId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for RuntimeId {
    type Target = usize;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn build_tokio_runtime(
    thread_name: &str,
    num_threads: usize,
    runtime_id: Option<RuntimeId>,
    affinity_info: Option<Affinity>,
    #[cfg(feature = "metrics")] otel_exporters: Vec<OtelExporterConfig>,
) -> Runtime {
    let config = runtime_config();

    match runtime_id {
        Some(runtime_id) => info!("{thread_name}: building runtime[{runtime_id}]..."),
        None => info!("{thread_name}: building runtime..."),
    }

    // Incoming name is already `proxy_RTn` / `services`. Only append a suffix
    // when this runtime has multiple Tokio workers, and then it is the worker id.
    let thread_name = thread_name.to_owned();

    if let Some(affinity) = affinity_info {
        if let Some(runtime_id) = runtime_id {
            match affinity.run_strategy(runtime_id, num_threads) {
                Ok(aff) => {
                    if let Err(err) = core_affinity::set_cores_for_current(&aff) {
                        warn!("{thread_name}: Couldn't pin thread to core {aff:?}: {err}");
                    } else {
                        info!("{thread_name}: runtime[{runtime_id}] pinned to core {aff:?}");
                    }
                },
                Err(e) => {
                    warn!("{thread_name}: Strategy: {e}");
                },
            }
        }
    }

    let num_threads = num_threads.max(1);

    // Important note: although using `current_thread` when `num_threads == 1` may seem attractive,
    // We can now use a single-threaded runtime because all `block_in_place` calls have been removed!

    let mut builder = Builder::new_multi_thread();
    builder.worker_threads(num_threads).max_blocking_threads(num_threads).enable_all();

    config.global_queue_interval.map(|val| builder.global_queue_interval(val.into()));
    config.event_interval.map(|val| builder.event_interval(val));
    config.max_io_events_per_tick.map(|val| builder.max_io_events_per_tick(val.into()));

    // initialize per-thread state: runtime ID and metrics
    #[cfg(feature = "metrics")]
    builder.on_thread_start(move || {
        if let Some(runtime_id) = runtime_id {
            set_runtime_id(runtime_id.0);
        }
        init_per_thread_metrics(&otel_exporters);
    });
    #[cfg(not(feature = "metrics"))]
    builder.on_thread_start(move || {
        if let Some(runtime_id) = runtime_id {
            set_runtime_id(runtime_id.0);
        }
    });

    if num_threads == 1 {
        builder.thread_name(thread_name);
    } else {
        let worker_id = AtomicUsize::new(0);
        builder.thread_name_fn(move || {
            let id = worker_id.fetch_add(1, Ordering::Relaxed);
            format!("{thread_name}_{id}")
        });
    }

    #[allow(clippy::expect_used)]
    builder.build().expect("failed to build basic runtime")
}
