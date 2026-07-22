use papaya::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64};
use std::sync::RwLock;

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
