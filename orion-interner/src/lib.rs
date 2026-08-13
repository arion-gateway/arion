// Copyright 2025 The kmesh Authors
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

use std::{cell::RefCell, ops::Deref};

use http::Version;
use lasso::Rodeo;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smol_str::SmolStr;

// static GLOBAL_INTERNER: OnceLock<ThreadedRodeo> = OnceLock::new();

thread_local! {
    /// Thread-local interner. The `Rodeo` is leaked so interned strings remain
    /// valid after this thread exits (the TLS slot itself is still dropped).
    static THREAD_LOCAL_INTERNER: RefCell<&'static mut Rodeo> = RefCell::new(Box::leak(Box::new(Rodeo::new())));
}

pub trait StringInterner {
    fn to_static_str(&self) -> &'static str;
}

#[inline]
fn intern_str(s: &str) -> &'static str {
    THREAD_LOCAL_INTERNER.with_borrow_mut(|interner| {
        let key = &mut interner.get_or_intern(s);
        // SAFETY: `resolve` ties the `&str` to the temporary `RefMut` of the TLS slot.
        // The `Rodeo` is heap-allocated and leaked (`Box::leak`), so it is never dropped
        // when this thread exits — only the `RefCell` (a pointer) is. Interned slices
        // therefore remain valid for the rest of the process, which is the `'static`
        // lifetime we extend to here.
        unsafe { std::mem::transmute::<&str, &'static str>(interner.resolve(key)) }
    })
}

impl StringInterner for &str {
    fn to_static_str(&self) -> &'static str {
        intern_str(self)
    }
}

impl StringInterner for String {
    fn to_static_str(&self) -> &'static str {
        intern_str(self)
    }
}

impl StringInterner for SmolStr {
    fn to_static_str(&self) -> &'static str {
        intern_str(self)
    }
}

impl StringInterner for Version {
    fn to_static_str(&self) -> &'static str {
        match *self {
            Version::HTTP_09 => "HTTP/0.9",
            Version::HTTP_10 => "HTTP/1.0",
            Version::HTTP_11 => "HTTP/1.1",
            Version::HTTP_2 => "HTTP/2",
            Version::HTTP_3 => "HTTP/3",
            _ => "HTTP/unknown",
        }
    }
}

// Create a wrapper type to hide the 'static lifetime from Serde's macros
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InternedStr(pub &'static str);

impl<'de> Deserialize<'de> for InternedStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        // Assuming your interner has a function that takes a String and returns &'static str.
        // Adjust the function call to match your actual orion_interner API.
        Ok(InternedStr(s.to_static_str()))
    }
}

impl Serialize for InternedStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0)
    }
}

// 4. Implement Deref so you can use InternedStr exactly like a &str in your logic
impl Deref for InternedStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<T: StringInterner> From<T> for InternedStr {
    fn from(value: T) -> Self {
        InternedStr(value.to_static_str())
    }
}
