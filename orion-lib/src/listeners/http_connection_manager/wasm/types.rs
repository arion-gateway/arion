#[repr(i32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OrionWasmResult {
    Ok = 0,
    NotFound = 1,
    BufferTooSmall = 2,
    InvalidMemoryAccess = 3,
    InternalError = 4,
}

#[repr(i32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FilterAction {
    Continue = 0,
    PauseAndBufferBody = 1,
    DirectResponse = 2,
}
