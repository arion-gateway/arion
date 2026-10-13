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

//! Benchmark 1 — "hit" path.
//!
//! `THREADS` threads intern a fixed set of strings once (warm-up), then keep
//! re-interning the very same strings. Since every lookup is a hit, the
//! measured loop performs no allocation and leaks nothing.
//!
//! Worker threads are persistent for the whole benchmark of one implementation:
//! spawning threads per sample would (for thread-local interners) create and
//! leak a fresh interner every time and measure a cold cache.
//!
//! One criterion iteration = every thread interns the whole set once.
//! Reported throughput is the aggregate over all threads.
//!
//! Run: `cargo bench -p arion-interner --bench intern_hit`

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "benchmark code")]

mod common;

use std::{
    hint::black_box,
    sync::{Arc, Barrier, mpsc},
    thread,
    time::{Duration, Instant},
};

use common::{IMPLS, InternFn, THREADS};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Set sizes: small (fits in L1), medium (stresses cache) and large (16k).
const SET_SIZES: &[usize] = &[64, 4096, 16384];

/// Realistic config-like names (cluster/listener ids), ~35-40 bytes each.
fn make_set(n: usize) -> Arc<[String]> {
    (0..n).map(|i| format!("cluster-{i:05}.ns-{:02}.svc.cluster.local", i % 17)).collect()
}

struct Pool {
    jobs: Vec<mpsc::Sender<u64>>,
    results: mpsc::Receiver<Duration>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl Pool {
    fn new(name: &str, intern: InternFn, set: &Arc<[String]>) -> Self {
        let barrier = Arc::new(Barrier::new(THREADS));
        let (res_tx, results) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let mut jobs = Vec::with_capacity(THREADS);
        let mut handles = Vec::with_capacity(THREADS);

        for t in 0..THREADS {
            let (job_tx, job_rx) = mpsc::channel::<u64>();
            let set = Arc::clone(set);
            let barrier = Arc::clone(&barrier);
            let res_tx = res_tx.clone();
            let ready_tx = ready_tx.clone();

            let handle = thread::Builder::new()
                .name(format!("hit-{name}-{t}"))
                .spawn(move || {
                    // Warm-up: populate the interner as seen by this thread...
                    let first: Vec<&'static str> = set.iter().map(|s| intern(s)).collect();
                    // ...and verify a second pass returns the same slices, i.e. the
                    // measured loop below is pure lookup (no new interning, no leak).
                    for (s, &p) in set.iter().zip(&first) {
                        assert!(std::ptr::eq(intern(s), p), "string re-interned on hit path");
                    }
                    ready_tx.send(()).unwrap();

                    for iters in job_rx {
                        barrier.wait();
                        let start = Instant::now();
                        for _ in 0..iters {
                            for s in set.iter() {
                                black_box(intern(black_box(s.as_str())));
                            }
                        }
                        res_tx.send(start.elapsed()).unwrap();
                    }
                })
                .expect("spawn worker");

            jobs.push(job_tx);
            handles.push(handle);
        }

        for _ in 0..THREADS {
            ready_rx.recv().expect("worker warm-up failed");
        }
        Self { jobs, results, handles }
    }

    /// Runs `iters` iterations on all threads concurrently; returns the wall-clock
    /// time of the slowest thread (threads start together on a barrier).
    fn run(&self, iters: u64) -> Duration {
        for job in &self.jobs {
            job.send(iters).unwrap();
        }
        (0..THREADS).map(|_| self.results.recv().unwrap()).max().unwrap_or_default()
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.jobs.clear(); // closes the job channels -> workers exit their loop
        for handle in self.handles.drain(..) {
            if handle.join().is_err() {
                eprintln!("intern_hit: worker panicked");
            }
        }
    }
}

fn bench_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("intern_hit/{THREADS}threads"));
    for &size in SET_SIZES {
        let set = make_set(size);
        group.throughput(Throughput::Elements(u64::try_from(size * THREADS).unwrap()));
        for &(name, intern) in IMPLS {
            let pool = Pool::new(name, intern, &set);
            group.bench_with_input(BenchmarkId::new(name, size), &pool, |b, pool| {
                b.iter_custom(|iters| pool.run(iters));
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_hit);
criterion_main!(benches);
