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

use std::sync::atomic::{AtomicU32, Ordering};

pub use orion_configuration::config::cluster::RoutingPriority;
use orion_configuration::config::cluster::{CircuitBreakerThresholds, CircuitBreakers};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerDenial {
    MaxConnections,
    MaxRequests,
    MaxRetries,
}

#[derive(Debug)]
pub struct PriorityCircuitBreakerState {
    pub thresholds: CircuitBreakerThresholds,
    pub active_requests: AtomicU32,
    pub active_retries: AtomicU32,
    pub active_connections: AtomicU32,
}

impl PriorityCircuitBreakerState {
    pub fn new(thresholds: CircuitBreakerThresholds) -> Self {
        Self {
            thresholds,
            active_requests: AtomicU32::new(0),
            active_retries: AtomicU32::new(0),
            active_connections: AtomicU32::new(0),
        }
    }
}

impl Default for PriorityCircuitBreakerState {
    fn default() -> Self {
        Self::new(CircuitBreakerThresholds::default())
    }
}

impl Clone for PriorityCircuitBreakerState {
    fn clone(&self) -> Self {
        Self::new(self.thresholds.clone())
    }
}

#[derive(Debug)]
pub struct ClusterCircuitBreaker {
    default_priority: PriorityCircuitBreakerState,
    high_priority: PriorityCircuitBreakerState,
}

impl ClusterCircuitBreaker {
    pub fn new(default_priority: PriorityCircuitBreakerState, high_priority: PriorityCircuitBreakerState) -> Self {
        Self { default_priority, high_priority }
    }

    pub fn get_state(&self, priority: RoutingPriority) -> &PriorityCircuitBreakerState {
        match priority {
            RoutingPriority::Default => &self.default_priority,
            RoutingPriority::High => &self.high_priority,
        }
    }

    pub fn increment_connections(&self, priority: RoutingPriority) {
        self.get_state(priority).active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_connections(&self, priority: RoutingPriority) {
        self.get_state(priority).active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn try_increment_requests(&self, priority: RoutingPriority) -> Result<(), CircuitBreakerDenial> {
        let state = self.get_state(priority);

        let active_cx = state.active_connections.load(Ordering::Relaxed);
        if active_cx >= state.thresholds.max_connections {
            return Err(CircuitBreakerDenial::MaxConnections);
        }

        let prev = state.active_requests.fetch_add(1, Ordering::Relaxed);
        if prev >= state.thresholds.max_requests {
            state.active_requests.fetch_sub(1, Ordering::Relaxed);
            return Err(CircuitBreakerDenial::MaxRequests);
        }

        Ok(())
    }

    pub fn decrement_requests(&self, priority: RoutingPriority) {
        self.get_state(priority).active_requests.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn try_increment_retries(&self, priority: RoutingPriority) -> Result<(), CircuitBreakerDenial> {
        let state = self.get_state(priority);

        let prev = state.active_retries.fetch_add(1, Ordering::Relaxed);
        if prev >= state.thresholds.max_retries {
            state.active_retries.fetch_sub(1, Ordering::Relaxed);
            return Err(CircuitBreakerDenial::MaxRetries);
        }

        Ok(())
    }

    pub fn decrement_retries(&self, priority: RoutingPriority) {
        self.get_state(priority).active_retries.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Default for ClusterCircuitBreaker {
    fn default() -> Self {
        Self::new(PriorityCircuitBreakerState::default(), PriorityCircuitBreakerState::default())
    }
}

impl Clone for ClusterCircuitBreaker {
    fn clone(&self) -> Self {
        Self::new(self.default_priority.clone(), self.high_priority.clone())
    }
}

impl From<&CircuitBreakers> for ClusterCircuitBreaker {
    fn from(config: &CircuitBreakers) -> Self {
        let find = |priority| config.thresholds.iter().rfind(|t| t.priority == priority).cloned().unwrap_or_default();

        Self::new(
            PriorityCircuitBreakerState::new(find(RoutingPriority::Default)),
            PriorityCircuitBreakerState::new(find(RoutingPriority::High)),
        )
    }
}
