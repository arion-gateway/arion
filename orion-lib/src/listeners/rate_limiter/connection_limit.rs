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
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock,
    },
    time::Duration,
};

use orion_configuration::config::network_filters::ConnectionLimit as ConnectionLimitConfig;
use papaya::HashMap as PapayaMap;

static GLOBAL_CONNECTION_COUNTS: LazyLock<PapayaMap<(&'static str, u64), Arc<AtomicU64>, ahash::RandomState>> =
    LazyLock::new(|| PapayaMap::with_hasher(ahash::RandomState::new()));

#[derive(Debug, Clone)]
pub struct NetworkConnectionLimit {
    active: Arc<AtomicU64>,
    max_connections: u64,
    delay: Option<Duration>,
}

pub struct ConnectionGuard(Arc<AtomicU64>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl From<(&'static str, u64, ConnectionLimitConfig)> for NetworkConnectionLimit {
    fn from((listener_name, filterchain_id, config): (&'static str, u64, ConnectionLimitConfig)) -> Self {
        let active = {
            let map = GLOBAL_CONNECTION_COUNTS.pin();
            map.get_or_insert_with((listener_name, filterchain_id), || Arc::new(AtomicU64::new(0))).clone()
        };
        Self { active, max_connections: config.max_connections, delay: config.delay }
    }
}

impl NetworkConnectionLimit {
    pub async fn check(&self) -> crate::Result<ConnectionGuard> {
        if self.active.load(Ordering::Acquire) >= self.max_connections {
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            return Err("connection limit exceeded".into());
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        Ok(ConnectionGuard(Arc::clone(&self.active)))
    }
}
