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
    collections::HashMap,
    hash::Hash,
    sync::atomic::{AtomicU64, Ordering},
};

use ahash::RandomState;
use opentelemetry::KeyValue;
use papaya::HashMap as ConcurrentHashMap;
use smallvec::SmallVec;
use std::{collections::hash_map, fmt};

pub struct ShardedU64<S> {
    data: ConcurrentHashMap<S, ConcurrentHashMap<SmallVec<[KeyValue; 4]>, AtomicU64, RandomState>, RandomState>,
}

impl<S: Eq + Hash> ShardedU64<S> {
    pub fn new() -> Self {
        ShardedU64 { data: ConcurrentHashMap::with_hasher(RandomState::new()) }
    }

    pub fn add(&self, value: u64, shard_id: S, key: &[KeyValue]) {
        let map = self.data.pin();
        let shard = map.get_or_insert_with(shard_id, || ConcurrentHashMap::with_hasher(RandomState::new()));
        let shard_pin = shard.pin();
        if let Some(counter) = shard_pin.get(key) {
            counter.fetch_add(value, Ordering::Relaxed);
        } else {
            let counter = shard_pin.get_or_insert_with(SmallVec::from(key), || AtomicU64::new(0));
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }

    pub fn sub(&self, value: u64, shard_id: S, key: &[KeyValue]) {
        let map = self.data.pin();
        if let Some(shard) = map.get(&shard_id) {
            let shard_pin = shard.pin();
            if let Some(counter) = shard_pin.get(key) {
                let mut current = counter.load(Ordering::Relaxed);
                loop {
                    let new = current.saturating_sub(value);
                    match counter.compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed) {
                        Ok(_) => break,
                        Err(x) => current = x,
                    }
                }
            }
        }
    }

    pub fn load_all(&self) -> HashMap<SmallVec<[KeyValue; 4]>, u64, RandomState> {
        let map = self.data.pin();
        let mut result = HashMap::with_capacity_and_hasher(map.len(), RandomState::new());
        for (_, shard) in map.iter() {
            let shard_pin = shard.pin();
            for (key, counter) in shard_pin.iter() {
                let value = counter.load(Ordering::Relaxed);
                *result.entry(key.clone()).or_insert(0) += value;
            }
        }
        result
    }

    pub fn load(&self, key: &[KeyValue]) -> Option<u64> {
        let mut total = None;
        let map = self.data.pin();
        for (_, shard) in map.iter() {
            let shard_pin = shard.pin();
            if let Some(counter) = shard_pin.get(key) {
                let value = counter.load(Ordering::Relaxed);
                total = Some(total.unwrap_or(0) + value);
            }
        }
        total
    }

    pub fn shard_count(&self) -> usize {
        self.data.pin().len()
    }

    pub fn clear(&self) {
        self.data.pin().clear();
    }

    pub fn remove(&self, shard_id: S, key: &[KeyValue]) -> Option<u64> {
        let map = self.data.pin();
        let shard = map.get(&shard_id)?;
        let shard_pin = shard.pin();
        shard_pin.remove(key).map(|counter| counter.load(Ordering::Relaxed))
    }
}

pub struct ShardedU64IntoIter {
    inner: hash_map::IntoIter<SmallVec<[KeyValue; 4]>, u64>,
}

impl Iterator for ShardedU64IntoIter {
    type Item = (SmallVec<[KeyValue; 4]>, u64);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<S: Eq + Hash> IntoIterator for &'_ ShardedU64<S> {
    type Item = (SmallVec<[KeyValue; 4]>, u64);
    type IntoIter = ShardedU64IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let snapshot = self.load_all();
        ShardedU64IntoIter { inner: snapshot.into_iter() }
    }
}

impl<S: Eq + Hash> Default for ShardedU64<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Eq + Hash> fmt::Debug for ShardedU64<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.load_all()).finish()
    }
}

pub struct Gauge {
    data: ConcurrentHashMap<SmallVec<[KeyValue; 4]>, AtomicU64, RandomState>,
}

impl Gauge {
    pub fn new() -> Self {
        Gauge { data: ConcurrentHashMap::with_hasher(RandomState::new()) }
    }

    pub fn record(&self, value: u64, key: &[KeyValue]) {
        let map = self.data.pin();
        if let Some(gauge) = map.get(key) {
            gauge.store(value, Ordering::Relaxed);
        } else {
            map.insert(SmallVec::from(key), AtomicU64::new(value));
        }
    }

    pub fn load_all(&self) -> HashMap<SmallVec<[KeyValue; 4]>, u64, RandomState> {
        let map = self.data.pin();
        let mut result = HashMap::with_capacity_and_hasher(map.len(), RandomState::new());
        for (key, counter) in map.iter() {
            result.insert(key.clone(), counter.load(Ordering::Relaxed));
        }
        result
    }
}

pub struct GaugeIntoIter {
    inner: hash_map::IntoIter<SmallVec<[KeyValue; 4]>, u64>,
}

impl Iterator for GaugeIntoIter {
    type Item = (SmallVec<[KeyValue; 4]>, u64);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl IntoIterator for &'_ Gauge {
    type Item = (SmallVec<[KeyValue; 4]>, u64);
    type IntoIter = GaugeIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let snapshot = self.load_all();
        GaugeIntoIter { inner: snapshot.into_iter() }
    }
}

impl Default for Gauge {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Gauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.load_all()).finish()
    }
}

pub struct ShardedHistogram<S> {
    buckets: Vec<u64>,
    counts: Vec<ShardedU64<S>>,
    sum: ShardedU64<S>,
    count: ShardedU64<S>,
    otel_histogram: Option<opentelemetry::metrics::Histogram<u64>>,
}

impl<S: Eq + Hash + Clone + Copy> ShardedHistogram<S> {
    pub fn new(mut buckets: Vec<u64>, otel_histogram: Option<opentelemetry::metrics::Histogram<u64>>) -> Self {
        buckets.sort_unstable();
        let mut counts = Vec::with_capacity(buckets.len());
        for _ in 0..buckets.len() {
            counts.push(ShardedU64::new());
        }
        Self { buckets, counts, sum: ShardedU64::new(), count: ShardedU64::new(), otel_histogram }
    }

    pub fn record(&self, value: u64, shard_id: S, key: &[KeyValue]) {
        if let Some(otel) = &self.otel_histogram {
            otel.record(value, key);
        }

        self.sum.add(value, shard_id, key);
        self.count.add(1, shard_id, key);

        for (i, &bound) in self.buckets.iter().enumerate() {
            if value <= bound {
                self.counts[i].add(1, shard_id, key);
            }
        }
    }

    pub fn buckets(&self) -> &[u64] {
        &self.buckets
    }

    pub fn counts(&self) -> &[ShardedU64<S>] {
        &self.counts
    }

    pub fn sum(&self) -> &ShardedU64<S> {
        &self.sum
    }

    pub fn count(&self) -> &ShardedU64<S> {
        &self.count
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc, thread::ThreadId};

    use super::*;

    fn build_thread_id(n: u64) -> ThreadId {
        let nz = NonZeroU64::new(n).expect("NonZeroU64 should not be zero");
        // SAFETY: the only way to build arbitrary ThreadId for testing
        unsafe { std::mem::transmute(nz) }
    }

    #[test]
    fn test_basic_add_and_load() {
        let s = ShardedU64::new();
        let shard_1 = build_thread_id(1);
        let shard_2 = build_thread_id(2);
        let key = [KeyValue::new("metric", "requests")];

        assert_eq!(s.load(&key), None);

        s.add(10, shard_1, &key);
        assert_eq!(s.load(&key), Some(10));

        s.add(5, shard_2, &key);
        assert_eq!(s.load(&key), Some(15));
    }

    #[test]
    fn test_saturating_sub() {
        let s = ShardedU64::new();
        let shard_1 = build_thread_id(1);
        let key = vec![KeyValue::new("metric", "inventory")];

        s.sub(10, shard_1, &key);
        assert_eq!(s.load(&key), None);

        s.add(20, shard_1, &key);
        assert_eq!(s.load(&key), Some(20));

        s.sub(5, shard_1, &key);
        assert_eq!(s.load(&key), Some(15));

        s.sub(25, shard_1, &key);
        assert_eq!(s.load(&key), Some(0));

        s.sub(100, shard_1, &key);
        assert_eq!(s.load(&key), Some(0));
    }

    #[test]
    fn test_debug() {
        let s = ShardedU64::new();
        let shard_1 = build_thread_id(1);
        let key = vec![KeyValue::new("metric", "value")];

        s.add(100, shard_1, &key);
        assert_eq!(s.load(&key), Some(100));

        s.add(42, shard_1, &key);
        assert_eq!(s.load(&key), Some(142));

        s.add(0, shard_1, &[]);
        println!("{s:?}");
    }

    #[test]
    fn test_multi_shard_aggregation() {
        let s = ShardedU64::new();
        let (tid1, tid2, tid3) = (build_thread_id(1), build_thread_id(2), build_thread_id(3));

        let key_a = vec![KeyValue::new("metric", "a")];
        let key_b = vec![KeyValue::new("metric", "b")];

        s.add(10, tid1, &key_a); // a = 10
        s.add(20, tid2, &key_a); // a = 10 (tid1) + 20 (tid2) = 30
        s.sub(5, tid1, &key_a); // a = 5 (tid1) + 20 (tid2) = 25

        s.add(100, tid3, &key_b);

        assert_eq!(s.load(&key_a), Some(25));
        assert_eq!(s.load(&key_b), Some(100));

        let all_data = s.load_all();
        assert_eq!(all_data.get(key_a.as_slice()), Some(&25));
        assert_eq!(all_data.get(key_b.as_slice()), Some(&100));
        assert_eq!(all_data.len(), 2);
    }

    #[test]
    fn test_clear_and_shard_count() {
        let s = ShardedU64::new();
        let (tid1, tid2) = (build_thread_id(1), build_thread_id(2));
        let key = vec![KeyValue::new("metric", "requests")];

        assert_eq!(s.shard_count(), 0);

        s.add(10, tid1, &key);
        assert_eq!(s.shard_count(), 1);

        s.add(5, tid2, &key);
        assert_eq!(s.shard_count(), 2);

        s.add(1, tid1, &key);
        assert_eq!(s.shard_count(), 2);

        s.clear();
        assert_eq!(s.shard_count(), 0);
        assert_eq!(s.load(&key), None);
    }

    #[test]
    fn test_actual_concurrency() {
        let s = Arc::new(ShardedU64::<ThreadId>::new());
        let mut handles = vec![];
        let num_threads = 10;
        let increments_per_thread = 1000;
        let key = vec![KeyValue::new("metric", "concurrent_counter")];

        for _ in 0..num_threads {
            let s_clone = Arc::clone(&s);
            let key_clone = key.clone();
            handles.push(std::thread::spawn(move || {
                let tid = std::thread::current().id();
                for _ in 0..increments_per_thread {
                    s_clone.add(1, tid, &key_clone);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let expected_value = (num_threads * increments_per_thread) as u64;
        assert_eq!(s.load(&key), Some(expected_value));
    }

    #[test]
    fn test_gauge_record_and_load() {
        let g = Gauge::new();
        let key = vec![KeyValue::new("metric", "gauge_test")];

        g.record(100, &key);
        let all = g.load_all();
        assert_eq!(all.get(key.as_slice()), Some(&100));

        g.record(50, &key);
        let all2 = g.load_all();
        assert_eq!(all2.get(key.as_slice()), Some(&50));
    }
}
