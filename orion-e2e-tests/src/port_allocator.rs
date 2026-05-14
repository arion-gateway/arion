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

use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};

use crate::{Error, Result};

const BASE_PORT: u16 = 40000;
const MAX_PORT: u16 = 50000;
const PORT_BLOCK_SIZE: u16 = 50;
const NUM_BLOCKS: u16 = (MAX_PORT - BASE_PORT) / PORT_BLOCK_SIZE;

/// A reserved block of ports for a single test runner.
///
/// Reserves a contiguous block of ports using file-based locking.
/// Other test runners cannot use the same block until this is dropped.
///
/// # Example
///
/// ```no_run
/// use orion_e2e_tests::PortBlock;
///
/// let block = PortBlock::reserve().expect("Failed to reserve port block");
/// let port1 = block.allocate().expect("Failed to allocate port");
/// let port2 = block.allocate().expect("Failed to allocate port");
/// // Block is released when dropped
/// ```
#[derive(Debug)]
pub struct PortBlock {
    block_id: u16,
    start_port: u16,
    next_offset: AtomicU16,
    lock_file: PathBuf,
}

impl PortBlock {
    pub fn reserve() -> Result<Self> {
        let lock_dir = std::env::temp_dir().join("orion-port-locks");
        fs::create_dir_all(&lock_dir)
            .map_err(|e| Error::PortAllocationFailed(format!("Failed to create lock directory: {e}")))?;

        for block_id in 0..NUM_BLOCKS {
            let lock_path = lock_dir.join(format!("block_{block_id}.lock"));

            match OpenOptions::new().write(true).create_new(true).open(&lock_path) {
                Ok(_file) => {
                    let start_port = BASE_PORT + (block_id * PORT_BLOCK_SIZE);
                    tracing::debug!(block_id, start_port, "Reserved port block");
                    return Ok(Self { block_id, start_port, next_offset: AtomicU16::new(0), lock_file: lock_path });
                },
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    continue;
                },
                Err(e) => {
                    return Err(Error::PortAllocationFailed(format!("Lock file error: {e}")));
                },
            }
        }

        Err(Error::PortAllocationFailed("All port blocks are reserved - too many parallel test runners".into()))
    }

    pub fn allocate(&self) -> Result<u16> {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        if offset >= PORT_BLOCK_SIZE {
            return Err(Error::PortAllocationFailed(format!(
                "Port block {} exhausted (used all {} ports)",
                self.block_id, PORT_BLOCK_SIZE
            )));
        }
        let port = self.start_port + offset;
        tracing::trace!(port, block_id = self.block_id, "Allocated port from block");
        Ok(port)
    }

    pub fn allocate_many(&self, count: u16) -> Result<Vec<u16>> {
        (0..count).map(|_| self.allocate()).collect()
    }

    #[must_use]
    pub fn range(&self) -> (u16, u16) {
        (self.start_port, self.start_port + PORT_BLOCK_SIZE - 1)
    }

    #[must_use]
    pub fn block_id(&self) -> u16 {
        self.block_id
    }
}

impl Drop for PortBlock {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.lock_file) {
            tracing::warn!(block_id = self.block_id, error = %e, "Failed to remove port block lock file");
        } else {
            tracing::debug!(block_id = self.block_id, "Released port block");
        }
    }
}
