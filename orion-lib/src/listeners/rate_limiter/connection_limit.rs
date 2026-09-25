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

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        LazyLock,
    },
    time::Duration,
};
use triomphe::Arc;

use orion_configuration::config::network_filters::ConnectionLimit as ConnectionLimitConfig;
use papaya::HashMap as PapayaMap;
use tracing::debug;

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
        let active = self.0.fetch_sub(1, Ordering::Relaxed) - 1;
        debug!(target: "connection_limit", active, "connection closed");
    }
}

impl From<(&'static str, u64, ConnectionLimitConfig)> for NetworkConnectionLimit {
    fn from((listener_name, filterchain_id, config): (&'static str, u64, ConnectionLimitConfig)) -> Self {
        let active = {
            let map = GLOBAL_CONNECTION_COUNTS.pin();
            Arc::clone(map.get_or_insert_with((listener_name, filterchain_id), || Arc::new(AtomicU64::new(0))))
        };
        Self { active, max_connections: config.max_connections, delay: config.delay }
    }
}

impl NetworkConnectionLimit {
    pub async fn check(&self) -> crate::Result<ConnectionGuard> {
        let current = self.active.load(Ordering::Acquire);
        if current >= self.max_connections {
            debug!(
                target: "connection_limit",
                active = current,
                max = self.max_connections,
                delay = ?self.delay,
                "connection rejected: limit reached"
            );
            if let Some(delay) = self.delay {
                pingora_timeout::sleep(delay).await;
            }
            return Err("connection limit exceeded".into());
        }
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        debug!(target: "connection_limit", active, max = self.max_connections, "connection accepted");
        Ok(ConnectionGuard(Arc::clone(&self.active)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_limiter(listener: &'static str, max: u64) -> NetworkConnectionLimit {
        NetworkConnectionLimit::from((
            listener,
            0u64,
            ConnectionLimitConfig { stat_prefix: "test".into(), max_connections: max, delay: None },
        ))
    }

    #[tokio::test]
    async fn guard_decrement() {
        let limiter = make_limiter("test_guard_decrement", 3);

        assert_eq!(limiter.active.load(Ordering::Acquire), 0);

        let guard = limiter.check().await.unwrap();
        assert_eq!(limiter.active.load(Ordering::Acquire), 1);

        drop(guard);
        assert_eq!(limiter.active.load(Ordering::Acquire), 0);

        let guard2 = limiter.check().await.unwrap();
        assert_eq!(limiter.active.load(Ordering::Acquire), 1);
        drop(guard2);
        assert_eq!(limiter.active.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn limit_enforcement() {
        let limiter = make_limiter("test_limit_enforcement", 2);

        let g1 = limiter.check().await.unwrap();
        let g2 = limiter.check().await.unwrap();
        assert_eq!(limiter.active.load(Ordering::Acquire), 2);

        assert!(limiter.check().await.is_err());

        drop(g1);
        assert_eq!(limiter.active.load(Ordering::Acquire), 1);

        let g3 = limiter.check().await.unwrap();
        assert_eq!(limiter.active.load(Ordering::Acquire), 2);

        drop(g2);
        drop(g3);
        assert_eq!(limiter.active.load(Ordering::Acquire), 0);
    }

    // 20 tasks race against a limit of 5 across 4 threads; the key assertion is
    // `active == 0` after all tasks finish, which proves no leaked increments
    // or double decrements regardless of the accept overshoot
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_accepts() {
        let max = 5u64;
        let limiter = Arc::new(make_limiter("test_concurrent_accepts", max));
        let mut handles = Vec::new();

        for _ in 0..20 {
            let limiter = Arc::clone(&limiter);
            handles.push(tokio::spawn(async move {
                if let Ok(_guard) = limiter.check().await {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    // guard drops here
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(limiter.active.load(Ordering::Acquire), 0);
    }
}
