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

use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU16, Ordering};

use crate::{Error, Result};

const BASE_PORT: u16 = 40000;
const MAX_PORT: u16 = 50000;

static NEXT_PORT: AtomicU16 = AtomicU16::new(BASE_PORT);

#[derive(Debug, Default)]
pub struct PortAllocator;

impl PortAllocator {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn allocate(&self) -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        Ok(port)
    }

    pub fn allocate_addr(&self) -> Result<SocketAddr> {
        let port = self.allocate()?;
        Ok(SocketAddr::from(([127, 0, 0, 1], port)))
    }

    pub fn allocate_many(&self, count: usize) -> Result<Vec<u16>> {
        let mut ports = Vec::with_capacity(count);
        for _ in 0..count {
            ports.push(self.allocate()?);
        }
        Ok(ports)
    }

    pub fn allocate_sequential(&self) -> Result<u16> {
        loop {
            let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
            if port > MAX_PORT {
                NEXT_PORT.store(BASE_PORT, Ordering::SeqCst);
                return Err(Error::PortAllocationFailed("Exhausted port range, resetting".to_string()));
            }

            if TcpListener::bind(("127.0.0.1", port)).is_ok() {
                return Ok(port);
            }
        }
    }
}

pub fn allocate_port() -> Result<u16> {
    PortAllocator::new().allocate()
}

pub fn allocate_ports(count: usize) -> Result<Vec<u16>> {
    PortAllocator::new().allocate_many(count)
}
