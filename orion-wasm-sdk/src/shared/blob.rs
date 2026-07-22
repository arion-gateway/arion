use crate::ffi;
use super::SharedVarError;

#[derive(Clone, Debug)]
pub struct BlobData {
    pub data: Vec<u8>,
    pub version: u64,
}

pub struct SharedBlob {
    id: u32,
}

impl SharedBlob {
    pub fn try_new(name: &str) -> Result<Self, SharedVarError> {
        let id = unsafe { ffi::ext_shared_resolve(name.as_ptr(), name.len() as u32, 2) };
        if id == u32::MAX { Err(SharedVarError::InitFailed) } else { Ok(Self { id }) }
    }

    pub fn read(&self) -> BlobData {
        let mut buffer = vec![0u8; 1024];
        let mut version = 0;
        loop {
            let size = unsafe {
                ffi::ext_shared_blob_read(
                    self.id,
                    buffer.as_mut_ptr(),
                    buffer.len() as u32,
                    &mut version,
                )
            };
            if size <= buffer.len() as u32 {
                buffer.truncate(size as usize);
                return BlobData {
                    data: buffer,
                    version,
                };
            }
            buffer.resize(size as usize, 0);
        }
    }

    pub fn write(&self, data: &[u8]) -> u64 {
        unsafe { ffi::ext_shared_blob_write(self.id, data.as_ptr(), data.len() as u32) }
    }

    pub fn compare_and_swap(&self, data: &[u8], expected_version: u64) -> Result<u64, ()> {
        let mut success: u32 = 0;
        let new_ver = unsafe {
            ffi::ext_shared_blob_cas(
                self.id,
                data.as_ptr(),
                data.len() as u32,
                expected_version,
                &mut success,
            )
        };
        if success == 1 {
            Ok(new_ver)
        } else {
            Err(())
        }
    }
}
