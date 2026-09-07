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

use crate::clusters::cached_watch::{CachedWatch, CachedWatcher};
use crate::listeners::metadata::DownstreamConnectionMetadata;
use crate::runtime_context::get_runtime_id;
use crate::transport::AsyncInstrumentedStream;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::Instant;
use triomphe::Arc;
use tokio::sync::mpsc;
use tracing::{debug, warn};

pub struct InternalConnection {
    pub stream: AsyncInstrumentedStream,
    pub downstream_metadata: Arc<DownstreamConnectionMetadata>,
    pub start_instant: Instant,
}

type ListenersMap = BTreeMap<usize, BTreeMap<&'static str, mpsc::Sender<InternalConnection>>>;

static INTERNAL_LISTENERS_MAP: CachedWatch<ListenersMap> = CachedWatch::new(ListenersMap::new());

thread_local! {
    static LISTENERS_MAP_CACHE: RefCell<CachedWatcher<'static, ListenersMap>> =
        RefCell::new(INTERNAL_LISTENERS_MAP.watcher());
}

pub fn register(name: &'static str, sender: mpsc::Sender<InternalConnection>) {
    let runtime_id = get_runtime_id();
    INTERNAL_LISTENERS_MAP.update(|listeners| {
        let runtime_listeners = listeners.entry(runtime_id).or_default();
        if runtime_listeners.insert(name, sender).is_some() {
            warn!("Internal listener '{name}' was already registered for runtime {runtime_id}, replacing it");
        }
        debug!("Registered internal listener '{name}' for runtime {runtime_id}");
    });
}

pub fn unregister(name: &str) {
    let runtime_id = get_runtime_id();
    INTERNAL_LISTENERS_MAP.update(|listeners| {
        if let Some(runtime_listeners) = listeners.get_mut(&runtime_id) {
            if runtime_listeners.remove(name).is_some() {
                debug!("Unregistered internal listener '{name}' for runtime {runtime_id}");
                return;
            }
        }
        warn!("Attempted to unregister non-existent internal listener '{name}' for runtime {runtime_id}");
    });
}

pub fn get_connection_sender_for_listener(name: &str) -> Option<mpsc::Sender<InternalConnection>> {
    let runtime_id = get_runtime_id();
    LISTENERS_MAP_CACHE.with_borrow_mut(|watcher| {
        watcher.cached_or_latest().get(&runtime_id).and_then(|runtime_listeners| runtime_listeners.get(name).cloned())
    })
}
