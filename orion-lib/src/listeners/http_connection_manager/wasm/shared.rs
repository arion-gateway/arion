// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use papaya::HashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64};

#[derive(Clone, Copy, Debug)]
pub enum VarId {
    U64(u32),
    I64(u32),
    Blob(u32),
}

pub struct Blob {
    pub data: Vec<u8>,
    pub version: u64,
}

pub struct SharedMemory {
    pub name_to_id: HashMap<String, VarId>,
    pub u64_vars: Vec<AtomicU64>,
    pub i64_vars: Vec<AtomicI64>,
    pub blob_vars: Vec<RwLock<Blob>>,
    pub next_u64: AtomicU32,
    pub next_i64: AtomicU32,
    pub next_blob: AtomicU32,
}

impl SharedMemory {
    pub fn new(max_size: usize) -> Self {
        let mut u64_vars = Vec::with_capacity(max_size);
        let mut i64_vars = Vec::with_capacity(max_size);
        let mut blob_vars = Vec::with_capacity(max_size);
        for _ in 0..max_size {
            u64_vars.push(AtomicU64::new(0));
            i64_vars.push(AtomicI64::new(0));
            blob_vars.push(RwLock::new(Blob { data: Vec::new(), version: 0 }));
        }
        Self {
            name_to_id: HashMap::new(),
            u64_vars,
            i64_vars,
            blob_vars,
            next_u64: AtomicU32::new(0),
            next_i64: AtomicU32::new(0),
            next_blob: AtomicU32::new(0),
        }
    }
}
