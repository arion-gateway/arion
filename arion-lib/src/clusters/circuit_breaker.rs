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

use std::sync::atomic::{AtomicU32, Ordering};
use triomphe::Arc;

pub use arion_configuration::config::cluster::RoutingPriority;
use arion_configuration::config::cluster::{CircuitBreakerThresholds, CircuitBreakers};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerDenial {
    MaxConnections,
    MaxRequests,
    MaxRetries,
}

#[derive(Debug, Default)]
pub struct CircuitBreakerCounters {
    pub active_requests: AtomicU32,
    pub active_retries: AtomicU32,
    pub active_connections: AtomicU32,
}

impl CircuitBreakerCounters {
    pub fn new() -> Self {
        Self {
            active_requests: AtomicU32::new(0),
            active_retries: AtomicU32::new(0),
            active_connections: AtomicU32::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PriorityCircuitBreakerState {
    pub thresholds: CircuitBreakerThresholds,
    pub counters: Arc<CircuitBreakerCounters>, // Arc here is required to share counters when CircuitBreaker is updated
}

impl PriorityCircuitBreakerState {
    pub fn new(thresholds: CircuitBreakerThresholds, counters: Arc<CircuitBreakerCounters>) -> Self {
        Self { thresholds, counters }
    }
}

impl Default for PriorityCircuitBreakerState {
    fn default() -> Self {
        Self::new(CircuitBreakerThresholds::default(), Arc::new(CircuitBreakerCounters::new()))
    }
}

#[derive(Debug)]
pub struct ClusterCircuitBreaker {
    pub default_priority: PriorityCircuitBreakerState,
    pub high_priority: PriorityCircuitBreakerState,
}

impl ClusterCircuitBreaker {
    pub fn new(default_priority: PriorityCircuitBreakerState, high_priority: PriorityCircuitBreakerState) -> Self {
        Self { default_priority, high_priority }
    }

    #[must_use]
    pub fn with_counters(
        self,
        def_counters: Option<Arc<CircuitBreakerCounters>>,
        high_counters: Option<Arc<CircuitBreakerCounters>>,
    ) -> Self {
        Self {
            default_priority: PriorityCircuitBreakerState::new(
                self.default_priority.thresholds,
                def_counters.unwrap_or_else(|| Arc::new(CircuitBreakerCounters::new())),
            ),
            high_priority: PriorityCircuitBreakerState::new(
                self.high_priority.thresholds,
                high_counters.unwrap_or_else(|| Arc::new(CircuitBreakerCounters::new())),
            ),
        }
    }

    pub fn get_state(&self, priority: RoutingPriority) -> &PriorityCircuitBreakerState {
        match priority {
            RoutingPriority::Default => &self.default_priority,
            RoutingPriority::High => &self.high_priority,
        }
    }

    pub fn try_increment_connections(&self, priority: RoutingPriority) -> Result<(), CircuitBreakerDenial> {
        let state = self.get_state(priority);

        let prev = state.counters.active_connections.fetch_add(1, Ordering::Relaxed);
        if prev >= state.thresholds.max_connections {
            state.counters.active_connections.fetch_sub(1, Ordering::Relaxed);
            return Err(CircuitBreakerDenial::MaxConnections);
        }

        Ok(())
    }

    pub fn increment_connections(&self, priority: RoutingPriority) {
        self.get_state(priority).counters.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_connections(&self, priority: RoutingPriority) {
        self.get_state(priority).counters.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn try_increment_requests(&self, priority: RoutingPriority) -> Result<(), CircuitBreakerDenial> {
        let state = self.get_state(priority);

        let prev = state.counters.active_requests.fetch_add(1, Ordering::Relaxed);
        if prev >= state.thresholds.max_requests {
            state.counters.active_requests.fetch_sub(1, Ordering::Relaxed);
            return Err(CircuitBreakerDenial::MaxRequests);
        }

        Ok(())
    }

    pub fn decrement_requests(&self, priority: RoutingPriority) {
        self.get_state(priority).counters.active_requests.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn try_increment_retries(&self, priority: RoutingPriority) -> Result<(), CircuitBreakerDenial> {
        let state = self.get_state(priority);

        let prev = state.counters.active_retries.fetch_add(1, Ordering::Relaxed);
        if prev >= state.thresholds.max_retries {
            state.counters.active_retries.fetch_sub(1, Ordering::Relaxed);
            return Err(CircuitBreakerDenial::MaxRetries);
        }

        Ok(())
    }

    pub fn decrement_retries(&self, priority: RoutingPriority) {
        self.get_state(priority).counters.active_retries.fetch_sub(1, Ordering::Relaxed);
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
            PriorityCircuitBreakerState::new(find(RoutingPriority::Default), Arc::new(CircuitBreakerCounters::new())),
            PriorityCircuitBreakerState::new(find(RoutingPriority::High), Arc::new(CircuitBreakerCounters::new())),
        )
    }
}
