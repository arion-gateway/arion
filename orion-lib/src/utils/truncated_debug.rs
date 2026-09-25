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

use std::fmt::{self, Write};

#[derive(Clone, Copy)]
pub struct TruncatedDebug<'a, T, const N: usize>(pub &'a T);

// A zero-allocation writer adapter that counts characters and truncates the output.
struct TruncatingWriter<'a, 'b> {
    inner: &'a mut fmt::Formatter<'b>,
    chars_remaining: usize,
    truncated: bool,
}

impl<'a, 'b> Write for TruncatingWriter<'a, 'b> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        if self.chars_remaining == 0 {
            self.truncated = true;
            return Ok(());
        }

        let mut char_count = 0;
        for (byte_idx, _) in s.char_indices() {
            if char_count == self.chars_remaining {
                // Character limit reached within this chunk.
                self.truncated = true;
                self.chars_remaining = 0;
                return self.inner.write_str(&s[..byte_idx]);
            }
            char_count += 1;
        }

        // The entire chunk fits. Update remaining count and write.
        self.chars_remaining -= char_count;
        self.inner.write_str(s)
    }
}

impl<T: fmt::Debug, const N: usize> fmt::Debug for TruncatedDebug<'_, T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut writer = TruncatingWriter { inner: f, chars_remaining: N, truncated: false };

        // Format the inner type directly into the adapter. No String is allocated.
        write!(&mut writer, "{:?}", self.0)?;

        if writer.truncated {
            f.write_str("… ")?;
        }

        Ok(())
    }
}
