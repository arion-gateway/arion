// Copyright 2025 The kmesh Authors
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

use crate::OrionRequestBody;
use hyper::client::conn::http1::SendRequest;
use parking_lot::Mutex;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

struct IdleConn {
    tx: SendRequest<OrionRequestBody>,
    idle_at: Instant,
}

pub struct Http1Idle {
    idle: Mutex<Vec<IdleConn>>,
    idle_timeout: Duration,
}

impl Http1Idle {
    pub fn new(idle_timeout: Duration) -> Arc<Self> {
        Arc::new(Self { idle: Mutex::new(Vec::new()), idle_timeout })
    }

    pub fn pop(&self) -> Option<SendRequest<OrionRequestBody>> {
        let now = Instant::now();
        let mut idle = self.idle.lock();
        while let Some(conn) = idle.last() {
            if now.saturating_duration_since(conn.idle_at) > self.idle_timeout {
                idle.pop();
            } else {
                break;
            }
        }
        idle.pop().map(|conn| conn.tx)
    }

    pub fn release(&self, tx: SendRequest<OrionRequestBody>) {
        if tx.is_closed() {
            return;
        }
        self.idle.lock().push(IdleConn { tx, idle_at: Instant::now() });
    }

    pub fn len(&self) -> usize {
        self.idle.lock().len()
    }
}

/// Concrete handle that returns an HTTP/1 sender to its pool when the body ends.
pub struct Http1Permit {
    idle: Arc<Http1Idle>,
    tx: SendRequest<OrionRequestBody>,
}

impl Http1Permit {
    pub fn new(idle: Arc<Http1Idle>, tx: SendRequest<OrionRequestBody>) -> Self {
        Self { idle, tx }
    }

    pub fn on_body_end(self, completed: bool) {
        if self.tx.is_closed() {
            return;
        }
        if completed || self.tx.is_ready() {
            self.idle.release(self.tx);
        }
    }
}
