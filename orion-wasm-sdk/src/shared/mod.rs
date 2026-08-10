pub mod blob;
pub mod i64;
pub mod u64;

pub use blob::{BlobData, SharedBlob};
pub use i64::SharedAtomicI64;
pub use u64::SharedAtomicU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedVarError {
    InitFailed,
}

impl core::fmt::Display for SharedVarError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Failed to initialize shared variable (e.g. type mismatch or capacity exceeded)")
    }
}

impl std::error::Error for SharedVarError {}
