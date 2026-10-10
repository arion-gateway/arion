// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

use std::{borrow::Borrow, fmt, ops::Deref};

use http::{Method, Version, uri::Scheme};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smol_str::SmolStr;

// Interner implementations. Exposed (hidden) so benchmarks can compare them side by side.
#[doc(hidden)]
pub mod legacy;
#[doc(hidden)]
pub mod papaya;

pub trait StringInterner {
    fn to_static_str(&self) -> &'static str;

    #[inline]
    fn to_interned_str(&self) -> InternedStr {
        InternedStr(self.to_static_str())
    }
}

/// Backend used by the public API.
#[inline]
fn intern_str(s: &str) -> &'static str {
    papaya::intern(s)
}

/// Variant of the backend that ONLY looks up strings without interning them.
/// Returns `None` if the string hasn't been interned yet.
#[inline]
fn lookup_str(s: &str) -> Option<&'static str> {
    papaya::lookup(s)
}

impl StringInterner for str {
    #[inline]
    fn to_static_str(&self) -> &'static str {
        intern_str(self)
    }
}

impl StringInterner for &'static str {
    #[inline]
    fn to_static_str(&self) -> &'static str {
        self
    }

    #[inline]
    fn to_interned_str(&self) -> InternedStr {
        InternedStr(intern_str(self))
    }
}

impl StringInterner for String {
    #[inline]
    fn to_static_str(&self) -> &'static str {
        intern_str(self)
    }
}

impl StringInterner for SmolStr {
    #[inline]
    fn to_static_str(&self) -> &'static str {
        intern_str(self)
    }
}

impl StringInterner for Version {
    #[inline]
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

impl StringInterner for Method {
    /// Standard methods map to constants (no interning).
    ///
    /// Extension methods are interned: they are client-controlled, so on request
    /// paths prefer mapping them to a fixed value (e.g. `OTel`'s `_OTHER`).
    #[inline]
    fn to_static_str(&self) -> &'static str {
        match self.as_str() {
            "GET" => "GET",
            "POST" => "POST",
            "PUT" => "PUT",
            "DELETE" => "DELETE",
            "HEAD" => "HEAD",
            "OPTIONS" => "OPTIONS",
            "CONNECT" => "CONNECT",
            "PATCH" => "PATCH",
            "TRACE" => "TRACE",
            other => intern_str(other),
        }
    }
}

impl StringInterner for Scheme {
    /// `http` / `https` map to constants (no interning); other schemes are interned.
    #[inline]
    fn to_static_str(&self) -> &'static str {
        match self.as_str() {
            "http" => "http",
            "https" => "https",
            other => intern_str(other),
        }
    }
}

// Create a wrapper type to hide the 'static lifetime from Serde's macros
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct InternedStr(&'static str);

impl InternedStr {
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        self.0
    }

    /// Looks up a string without interning it, returning its canonical
    /// `InternedStr` representation if it exists.
    #[inline]
    pub fn lookup(s: &str) -> Option<Self> {
        lookup_str(s).map(InternedStr)
    }
}

impl fmt::Display for InternedStr {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Borrow<str> for InternedStr {
    #[inline]
    fn borrow(&self) -> &str {
        self
    }
}

impl<'de> Deserialize<'de> for InternedStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
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

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<T: StringInterner> From<T> for InternedStr {
    #[inline]
    fn from(value: T) -> Self {
        value.to_interned_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_methods_are_constants() {
        for m in [
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::HEAD,
            Method::OPTIONS,
            Method::CONNECT,
            Method::PATCH,
            Method::TRACE,
        ] {
            assert_eq!(m.to_static_str(), m.as_str());
        }
    }

    #[test]
    fn extension_method_is_interned() {
        let m = Method::from_bytes(b"PURGE").unwrap();
        assert_eq!(m.to_static_str(), "PURGE");
    }

    #[test]
    fn schemes() {
        assert_eq!(Scheme::HTTP.to_static_str(), "http");
        assert_eq!(Scheme::HTTPS.to_static_str(), "https");
        let ws: Scheme = "ws".parse().unwrap();
        assert_eq!(ws.to_static_str(), "ws");
    }

    #[test]
    fn interned_str_roundtrip() {
        let s = InternedStr::from("listener-1");
        assert_eq!(s.as_str(), "listener-1");
        assert_eq!(&*s, "listener-1");
        assert_eq!(format!("{s}"), "listener-1");
        assert_eq!(s.to_static_str(), "listener-1");
    }

    #[test]
    fn static_str_to_static_str_does_not_intern() {
        let s = <&'static str as StringInterner>::to_static_str(&"not-interned-yet");
        assert_eq!(s, "not-interned-yet");
        assert_eq!(InternedStr::lookup("not-interned-yet"), None);
    }

    #[test]
    fn static_str_to_interned_str_interns() {
        let to_intern = "now-interned";
        let interned = to_intern.to_interned_str();
        assert_eq!(interned.as_str(), "now-interned");
        assert_eq!(InternedStr::lookup("now-interned"), Some(interned));
    }

    #[test]
    fn lookup_api() {
        assert_eq!(InternedStr::lookup("never-seen-scheme"), None);
        let seen = InternedStr::from("listener-seen");
        assert_eq!(seen.as_str(), "listener-seen");
        assert!(InternedStr::lookup("listener-seen").is_some());
    }
}
