// Copyright 2026 The arion-gateway Authors
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

//! Benchmark 2 — "miss" path.
//!
//! `THREADS` threads intern always-new (globally unique) strings for a fixed
//! duration. Every call is an insertion, so this measures insert throughput,
//! latency spikes (hash table growth) and retained memory per string.
//!
//! Not a criterion benchmark on purpose: the interner state grows monotonically,
//! so repeated criterion samples would not be comparable.
//!
//! Run:  `cargo bench -p arion-interner --bench intern_miss [-- <impl-filter>]`
//! Env:  `INTERN_BENCH_SECS` (default 5), `INTERN_BENCH_MEM_CAP_MIB` (default 4096)
//!
//! The `control` row runs the same loop with a no-op interner: it is the cost of
//! generating the strings, to be subtracted mentally from the other rows.
//! For clean memory numbers, run one implementation per process (use the filter).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::indexing_slicing,
    reason = "benchmark code"
)]

mod common;

use std::{
    alloc::{GlobalAlloc, Layout, System},
    fmt::Write as _,
    hint::black_box,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    },
    thread,
    time::{Duration, Instant},
};

use common::{IMPLS, InternFn, THREADS};

// ---------------------------------------------------------------------------
// Live-bytes counting allocator
// ---------------------------------------------------------------------------

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

struct CountingAlloc;

// SAFETY: every method forwards verbatim to `System`, which upholds the
// `GlobalAlloc` contract; we only update a counter on the side.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: same contract as the caller's, forwarded to `System`.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: same contract as the caller's, forwarded to `System`.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr`/`layout` come from a previous `alloc` on this allocator.
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: same contract as the caller's, forwarded to `System`.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(new_size, Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Relaxed);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

/// Interns per timed batch. Timing every single call would cost as much as the call.
const BATCH: u64 = 1024;

/// Upper bound of batches/s per thread used to pre-size the sample buffer, so that
/// the measurement itself does not allocate (and pollute the memory figures).
const MAX_BATCHES_PER_SEC: usize = 200_000;

struct Config {
    duration: Duration,
    mem_cap: usize,
    filter: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        let env_u64 = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        Self {
            duration: Duration::from_secs(env_u64("INTERN_BENCH_SECS", 5)),
            mem_cap: usize::try_from(env_u64("INTERN_BENCH_MEM_CAP_MIB", 4096)).unwrap() << 20,
            // `cargo bench` passes `--bench`; take the first non-flag argument as filter.
            filter: std::env::args().skip(1).find(|a| !a.starts_with('-')),
        }
    }
}

struct ThreadReport {
    ops: u64,
    batch_ns: Vec<u64>,
}

fn noop_intern(s: &str) -> &'static str {
    black_box(s);
    ""
}

fn worker(t: usize, intern: InternFn, stop: &AtomicBool, barrier: &Barrier, capacity: usize) -> ThreadReport {
    let mut buf = String::with_capacity(64);
    let mut batch_ns = Vec::with_capacity(capacity);
    let mut ops = 0u64;

    barrier.wait();
    while !stop.load(Relaxed) {
        let t0 = Instant::now();
        for _ in 0..BATCH {
            buf.clear();
            // Unique across threads (thread id prefix) and within a thread (counter).
            write!(buf, "tenant-{t}/user-{ops:016x}.svc.cluster.local").unwrap();
            black_box(intern(black_box(buf.as_str())));
            ops += 1;
        }
        batch_ns.push(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    ThreadReport { ops, batch_ns }
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

fn run(name: &str, intern: InternFn, cfg: &Config) {
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(THREADS + 1));
    let capacity = MAX_BATCHES_PER_SEC * cfg.duration.as_secs().max(1) as usize;

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            thread::Builder::new()
                .name(format!("miss-{name}-{t}"))
                .spawn(move || worker(t, intern, &stop, &barrier, capacity))
                .expect("spawn worker")
        })
        .collect();

    barrier.wait(); // all workers have allocated their buffers
    let mem_before = LIVE_BYTES.load(Relaxed);
    let start = Instant::now();
    let mut capped = false;
    while start.elapsed() < cfg.duration {
        if LIVE_BYTES.load(Relaxed).saturating_sub(mem_before) > cfg.mem_cap {
            capped = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Relaxed);
    let reports: Vec<ThreadReport> = handles.into_iter().map(|h| h.join().expect("worker panicked")).collect();
    let elapsed = start.elapsed();
    // Retained = still-live bytes after the worker threads have exited (sample
    // buffers freed): i.e. what the interner keeps for the rest of the process.
    let retained = LIVE_BYTES.load(Relaxed).saturating_sub(mem_before);

    let total_ops: u64 = reports.iter().map(|r| r.ops).sum();
    let per_thread: Vec<String> = reports.iter().map(|r| format!("{:.1}", r.ops as f64 / 1e6)).collect();
    let mut batches: Vec<u64> = reports.into_iter().flat_map(|r| r.batch_ns).collect();
    batches.sort_unstable();

    let secs = elapsed.as_secs_f64();
    let ns_per_op = |ns: u64| ns as f64 / BATCH as f64;
    println!(
        "{name:<10} {secs:>6.2}s {:>9.2}M ops {:>8.2} Mops/s  [{} M/thread]  ns/op p50 {:>7.1}  p99 {:>7.1}  \
         p99.9 {:>8.1}  max-batch {:>8.2} ms  retained {:>8.1} MiB ({:.1} B/string){}",
        total_ops as f64 / 1e6,
        total_ops as f64 / secs / 1e6,
        per_thread.join("/"),
        ns_per_op(percentile(&batches, 0.50)),
        ns_per_op(percentile(&batches, 0.99)),
        ns_per_op(percentile(&batches, 0.999)),
        batches.last().copied().unwrap_or(0) as f64 / 1e6,
        retained as f64 / f64::from(1u32 << 20),
        if total_ops == 0 { 0.0 } else { retained as f64 / total_ops as f64 },
        if capped { "  ** stopped early: memory cap reached **" } else { "" },
    );
}

fn main() {
    let cfg = Config::from_env();
    println!(
        "intern_miss: {THREADS} threads, {:?} per implementation, batch {BATCH}, mem cap {} MiB",
        cfg.duration,
        cfg.mem_cap >> 20
    );

    let control: (&str, InternFn) = ("control", noop_intern);
    for &(name, intern) in std::iter::once(&control).chain(IMPLS) {
        if cfg.filter.as_deref().is_some_and(|f| !name.contains(f)) {
            continue;
        }
        run(name, intern, &cfg);
    }
}
