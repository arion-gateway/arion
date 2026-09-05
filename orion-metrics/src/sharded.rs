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
    any::Any,
    cell::RefCell,
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, HashMap},
    hash::Hash,
    ptr,
    sync::atomic::{AtomicU64, Ordering},
    thread::ThreadId,
};

use ahash::RandomState;
use opentelemetry::{KeyValue, Value};
use papaya::HashMap as ConcurrentHashMap;
use smallvec::SmallVec;
use std::{collections::hash_map, fmt};

pub trait Clearable {
    fn clear(&self);
}

struct CacheKey(SmallVec<[KeyValue; 4]>);

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_slice() == other.0.as_slice()
    }
}

impl Eq for CacheKey {}

impl PartialOrd for CacheKey {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for CacheKey {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        cmp_labels(self.0.as_slice(), other.0.as_slice())
    }
}

struct CachedCell {
    shard_id: ThreadId,
    cell: *const AtomicU64,
}

fn cmp_labels(left: &[KeyValue], right: &[KeyValue]) -> CmpOrdering {
    match left.len().cmp(&right.len()) {
        CmpOrdering::Equal => {
            for (a, b) in left.iter().zip(right.iter()) {
                match cmp_key_value(a, b) {
                    CmpOrdering::Equal => {},
                    ordering => return ordering,
                }
            }
            CmpOrdering::Equal
        },
        ordering => ordering,
    }
}

fn cmp_key_value(left: &KeyValue, right: &KeyValue) -> CmpOrdering {
    match left.key.as_str().cmp(right.key.as_str()) {
        CmpOrdering::Equal => match (&left.value, &right.value) {
            (Value::String(a), Value::String(b)) => a.as_str().cmp(b.as_str()),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::I64(a), Value::I64(b)) => a.cmp(b),
            _ => left.value.as_str().cmp(&right.value.as_str()),
        },
        ordering => ordering,
    }
}

struct PerMetric {
    epoch: u64,
    cells: BTreeMap<CacheKey, CachedCell>,
}

thread_local! {
    static LAST: RefCell<HashMap<usize, PerMetric, RandomState>> =
        RefCell::new(HashMap::with_hasher(RandomState::new()));
}

fn cache_lookup(
    tls: &mut HashMap<usize, PerMetric, RandomState>,
    owner: usize,
    epoch: u64,
    shard_id: ThreadId,
    cache_key: &CacheKey,
) -> Option<*const AtomicU64> {
    let cached = tls.get_mut(&owner)?;
    if cached.epoch != epoch {
        cached.epoch = epoch;
        cached.cells.clear();
        return None;
    }
    let hit = cached.cells.get(cache_key)?;
    (hit.shard_id == shard_id).then_some(hit.cell)
}

fn cache_store(
    tls: &mut HashMap<usize, PerMetric, RandomState>,
    owner: usize,
    epoch: u64,
    shard_id: ThreadId,
    cache_key: CacheKey,
    cell: *const AtomicU64,
) {
    let cached = tls.entry(owner).or_insert_with(|| PerMetric { epoch, cells: BTreeMap::new() });
    if cached.epoch != epoch {
        cached.epoch = epoch;
        cached.cells.clear();
    }
    cached.cells.insert(cache_key, CachedCell { shard_id, cell });
}

fn as_thread_id<S: Copy + 'static>(shard_id: &S) -> Option<ThreadId> {
    (shard_id as &dyn Any).downcast_ref::<ThreadId>().copied()
}

fn saturating_sub(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let new = current.saturating_sub(value);
        match counter.compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(x) => current = x,
        }
    }
}

pub struct ShardedU64<S> {
    data: ConcurrentHashMap<S, ConcurrentHashMap<SmallVec<[KeyValue; 4]>, AtomicU64, RandomState>, RandomState>,
    epoch: AtomicU64,
}

impl<S: Eq + Hash> ShardedU64<S> {
    pub fn new() -> Self {
        ShardedU64 { data: ConcurrentHashMap::with_hasher(RandomState::new()), epoch: AtomicU64::new(0) }
    }

    fn owner_key(&self) -> usize {
        ptr::from_ref(self) as usize
    }

    fn lookup_or_insert_ptr(&self, shard_id: S, key: &[KeyValue]) -> *const AtomicU64 {
        let map = self.data.pin();
        let shard = map.get_or_insert_with(shard_id, || ConcurrentHashMap::with_hasher(RandomState::new()));
        let shard_pin = shard.pin();
        let counter = if let Some(counter) = shard_pin.get(key) {
            counter
        } else {
            shard_pin.get_or_insert_with(SmallVec::from(key), || AtomicU64::new(0))
        };
        ptr::from_ref(counter)
    }

    fn lookup_ptr(&self, shard_id: S, key: &[KeyValue]) -> Option<*const AtomicU64> {
        let map = self.data.pin();
        let shard = map.get(&shard_id)?;
        let shard_pin = shard.pin();
        shard_pin.get(key).map(ptr::from_ref)
    }
}

impl<S: Eq + Hash + Copy + 'static> ShardedU64<S> {
    pub fn add(&self, value: u64, shard_id: S, key: &[KeyValue]) {
        let owner = self.owner_key();
        let epoch = self.epoch.load(Ordering::Relaxed);
        LAST.with(|tls| {
            let mut tls = tls.borrow_mut();
            if let Some(tid) = as_thread_id(&shard_id) {
                let cache_key = CacheKey(SmallVec::from(key));
                if let Some(cell) = cache_lookup(&mut tls, owner, epoch, tid, &cache_key) {
                    // SAFETY: `epoch` matches, so this entry has not been cleared/removed.
                    // The `AtomicU64` address is stable in papaya until then.
                    unsafe { (*cell).fetch_add(value, Ordering::Relaxed) };
                    return;
                }
                let cell = self.lookup_or_insert_ptr(shard_id, key);
                // SAFETY: `cell` was obtained from the live map in `lookup_or_insert_ptr`.
                unsafe { (*cell).fetch_add(value, Ordering::Relaxed) };
                cache_store(&mut tls, owner, epoch, tid, cache_key, cell);
                return;
            }
            let cell = self.lookup_or_insert_ptr(shard_id, key);
            // SAFETY: `cell` was obtained from the live map in `lookup_or_insert_ptr`.
            unsafe { (*cell).fetch_add(value, Ordering::Relaxed) };
        });
    }

    pub fn sub(&self, value: u64, shard_id: S, key: &[KeyValue]) {
        let owner = self.owner_key();
        let epoch = self.epoch.load(Ordering::Relaxed);
        LAST.with(|tls| {
            let mut tls = tls.borrow_mut();
            if let Some(tid) = as_thread_id(&shard_id) {
                let cache_key = CacheKey(SmallVec::from(key));
                if let Some(cell) = cache_lookup(&mut tls, owner, epoch, tid, &cache_key) {
                    // SAFETY: same as `add`: epoch still matches, cell has not been reclaimed.
                    unsafe { saturating_sub(&*cell, value) };
                    return;
                }
                let Some(cell) = self.lookup_ptr(shard_id, key) else {
                    return;
                };
                // SAFETY: `cell` was obtained from the live map in `lookup_ptr`.
                unsafe { saturating_sub(&*cell, value) };
                cache_store(&mut tls, owner, epoch, tid, cache_key, cell);
                return;
            }
            if let Some(cell) = self.lookup_ptr(shard_id, key) {
                // SAFETY: `cell` was obtained from the live map in `lookup_ptr`.
                unsafe { saturating_sub(&*cell, value) };
            }
        });
    }
}

impl<S: Eq + Hash> ShardedU64<S> {
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

    pub fn remove(&self, shard_id: S, key: &[KeyValue]) -> Option<u64> {
        let map = self.data.pin();
        let shard = map.get(&shard_id)?;
        let shard_pin = shard.pin();
        let removed = shard_pin.remove(key).map(|counter| counter.load(Ordering::Relaxed));
        if removed.is_some() {
            self.epoch.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }
}

impl<S: Eq + Hash> Clearable for ShardedU64<S> {
    fn clear(&self) {
        self.data.pin().clear();
        self.epoch.fetch_add(1, Ordering::Relaxed);
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

impl Clearable for Gauge {
    fn clear(&self) {
        self.data.pin().clear();
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

impl<S> ShardedHistogram<S> {
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

impl<S: Eq + Hash + Clone + Copy + 'static> ShardedHistogram<S> {
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
}

impl<S: Eq + Hash> Clearable for ShardedHistogram<S> {
    fn clear(&self) {
        for count in &self.counts {
            count.clear();
        }
        self.sum.clear();
        self.count.clear();
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
