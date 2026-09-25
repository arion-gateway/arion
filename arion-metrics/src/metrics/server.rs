// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

use crate::{metrics::Metric, sharded::Gauge};
use std::sync::OnceLock;

use arion_stats::init_startup_time;
use opentelemetry::global;

pub static UPTIME: OnceLock<Metric<Gauge>> = OnceLock::new();
pub static CONCURRENCY: OnceLock<Metric<Gauge>> = OnceLock::new();
pub static MEMORY_HEAP_SIZE: OnceLock<Metric<Gauge>> = OnceLock::new();
pub static MEMORY_PHYSICAL_SIZE: OnceLock<Metric<Gauge>> = OnceLock::new();
pub static MEMORY_ALLOCATED: OnceLock<Metric<Gauge>> = OnceLock::new();

pub fn update_server_metrics(rename: &std::collections::HashMap<String, String>) {
    let uptime = util::server_uptime();
    let physical_memory = util::get_memory_physical_size().unwrap_or(0);
    let memory_heap_size = util::get_memory_heap_size().unwrap_or(physical_memory) as u64;
    let memory_allocated = util::get_memory_allocated().unwrap_or(physical_memory) as u64;

    UPTIME
        .get_or_init(|| {
            Metric::new(
                crate::metrics::PREFIX_SERVER,
                crate::metrics::resolve_metric_name(rename, "uptime"),
                "Current server uptime in seconds",
                Gauge::new(),
            )
        })
        .value
        .record(uptime, &[]);

    MEMORY_HEAP_SIZE
        .get_or_init(|| {
            Metric::new(
                crate::metrics::PREFIX_SERVER,
                crate::metrics::resolve_metric_name(rename, "memory_heap_size"),
                "Current memory heap size in bytes",
                Gauge::new(),
            )
        })
        .value
        .record(memory_heap_size, &[]);

    MEMORY_PHYSICAL_SIZE
        .get_or_init(|| {
            Metric::new(
                crate::metrics::PREFIX_SERVER,
                crate::metrics::resolve_metric_name(rename, "memory_physical_size"),
                "Current memory physical size",
                Gauge::new(),
            )
        })
        .value
        .record(physical_memory as u64, &[]);

    MEMORY_ALLOCATED
        .get_or_init(|| {
            Metric::new(
                crate::metrics::PREFIX_SERVER,
                crate::metrics::resolve_metric_name(rename, "memory_allocated"),
                "Current memory allocated in bytes",
                Gauge::new(),
            )
        })
        .value
        .record(memory_allocated, &[]);
}

pub(crate) fn init_metrics(number_of_threads: usize, rename: &std::collections::HashMap<String, String>) {
    init_startup_time();
    CONCURRENCY
        .get_or_init(|| {
            Metric::new(
                crate::metrics::PREFIX_SERVER,
                crate::metrics::resolve_metric_name(rename, "concurrency"),
                "Number of worker threads",
                Gauge::new(),
            )
        })
        .value
        .record(number_of_threads as u64, &[]);

    update_server_metrics(rename);

    global::meter(const_format::concatcp!("arion.", crate::metrics::PREFIX_SERVER))
        .u64_observable_gauge(UPTIME.wait().name)
        .with_description(UPTIME.wait().descr)
        .with_callback(move |observer| observer.observe(util::server_uptime(), &[]))
        .build();

    global::meter(const_format::concatcp!("arion.", crate::metrics::PREFIX_SERVER))
        .u64_observable_gauge(CONCURRENCY.wait().name)
        .with_description(CONCURRENCY.wait().descr)
        .with_callback(move |observer| observer.observe(number_of_threads as u64, &[]))
        .build();

    let physical_memory = util::get_memory_physical_size().unwrap_or(0);

    global::meter(const_format::concatcp!("arion.", crate::metrics::PREFIX_SERVER))
        .u64_observable_gauge(MEMORY_HEAP_SIZE.wait().name)
        .with_description(MEMORY_HEAP_SIZE.wait().descr)
        .with_callback(move |observer| {
            observer.observe(util::get_memory_heap_size().unwrap_or(physical_memory) as u64, &[])
        })
        .build();

    global::meter(const_format::concatcp!("arion.", crate::metrics::PREFIX_SERVER))
        .u64_observable_gauge(MEMORY_PHYSICAL_SIZE.wait().name)
        .with_description(MEMORY_PHYSICAL_SIZE.wait().descr)
        .with_callback(move |observer| observer.observe(physical_memory as u64, &[]))
        .build();

    global::meter(const_format::concatcp!("arion.", crate::metrics::PREFIX_SERVER))
        .u64_observable_gauge(MEMORY_ALLOCATED.wait().name)
        .with_description(MEMORY_ALLOCATED.wait().descr)
        .with_callback(move |observer| {
            observer.observe(util::get_memory_allocated().unwrap_or(physical_memory) as u64, &[])
        })
        .build();
}

pub fn reset_metrics() {
    use crate::sharded::Clearable;
    let metrics: &[&dyn Clearable] =
        &[&UPTIME, &CONCURRENCY, &MEMORY_HEAP_SIZE, &MEMORY_PHYSICAL_SIZE, &MEMORY_ALLOCATED];
    for metric in metrics {
        metric.clear();
    }
}

mod util {
    use arion_stats::get_startup_time;

    /// Return the physical memory allocated by the process.
    ///
    pub(crate) fn get_memory_physical_size() -> Option<usize> {
        memory_stats::memory_stats().map(|s| s.physical_mem)
    }

    /// Return the precise memory allocated on the heap.
    ///
    pub(crate) fn get_memory_allocated() -> Option<usize> {
        #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
        {
            use tikv_jemalloc_ctl::{epoch, stats};
            epoch::advance().ok()?;
            let allocated_bytes = stats::allocated::mib().ok()?;
            allocated_bytes.read().ok()
        }
        #[cfg(not(all(feature = "jemalloc", not(target_env = "msvc"))))]
        {
            None
        }
    }

    /// Return the total size of the heap (active pages) managed by the allocator.
    ///
    pub(crate) fn get_memory_heap_size() -> Option<usize> {
        #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
        {
            use tikv_jemalloc_ctl::{epoch, stats};
            epoch::advance().ok()?;
            let active_bytes = stats::active::mib().ok()?;
            active_bytes.read().ok()
        }
        #[cfg(not(all(feature = "jemalloc", not(target_env = "msvc"))))]
        {
            None
        }
    }

    pub(crate) fn server_uptime() -> u64 {
        use std::time::Instant;
        let start_up_time = get_startup_time().copied().unwrap_or_else(Instant::now);
        start_up_time.elapsed().as_secs()
    }
}
