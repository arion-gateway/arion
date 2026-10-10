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

use arion_configuration::config::metrics::{PartitionKeySource, SourceHeaderName, SourceHeaderNameOrSni};
use atomicoption::AtomicOption;
use http::HeaderMap;
use smol_str::{SmolStr, ToSmolStr};
use std::sync::{OnceLock, atomic::Ordering};

#[macro_export]
macro_rules! with_metric {
    ($counter: expr, $method: ident, $($args: expr),*) => {
        #[cfg(feature = "metrics")]
        {
            $counter.get().inspect(|c| c.value.$method($($args),*));
        }
        #[cfg(not(feature = "metrics"))]
        {

        }
    };
}

#[macro_export]
macro_rules! with_histogram {
    ($counter: expr, $method: ident, $($args: expr),*) => {
        #[cfg(feature = "metrics")]
        {
            $counter.get().inspect(|c| c.value.$method($($args),*));
        }
        #[cfg(not(feature = "metrics"))]
        {

        }
    };
}

#[macro_export]
macro_rules! get_shard_id {
    () => {{
        #[cfg(feature = "metrics")]
        let id = std::thread::current().id();

        #[cfg(not(feature = "metrics"))]
        let id = ();

        id
    }};
}

#[derive(Default)]
pub struct PartitionKey<P: PartitionKeySource> {
    source: AtomicOption<P>,
    attribute_name: AtomicOption<SmolStr>,
}

impl<P> PartitionKey<P>
where
    P: PartitionKeySource,
{
    #[inline]
    pub const fn new() -> PartitionKey<P> {
        PartitionKey { source: AtomicOption::none(), attribute_name: AtomicOption::none() }
    }

    #[inline]
    pub fn source(&self) -> Option<&P> {
        self.source.as_ref(Ordering::Acquire)
    }

    #[inline]
    pub fn set_source(&self, value: P) {
        self.source.store(Ordering::Release, value);
    }

    #[inline]
    pub fn attribute_name(&self) -> Option<&str> {
        self.attribute_name.as_ref(Ordering::Acquire).map(SmolStr::as_str)
    }

    #[inline]
    pub fn set_attribute_name(&self, value: SmolStr) {
        self.attribute_name.store(Ordering::Release, value);
    }
}

pub static USER_KEY: PartitionKey<SourceHeaderNameOrSni> = PartitionKey::new();
pub static CUSTOM_KEYS: OnceLock<Vec<PartitionKey<SourceHeaderName>>> = std::sync::OnceLock::new();

/// Maximum allowed length for partition keys extracted from request headers or SNI (protects against metric DoS).
pub const MAX_PARTITION_KEY_LENGTH: usize = 256;

#[inline]
/// Return the user partition key, extracting it from either headers or sni, if one is present.
pub fn extract_user_partition_key(
    (headers, sni): (&HeaderMap, Option<&SmolStr>),
    source: Option<&SourceHeaderNameOrSni>,
) -> Option<SmolStr> {
    source.and_then(|source| match source {
        SourceHeaderNameOrSni::HeaderName(keym) => {
            headers
                .get(keym)
                .and_then(|value| value.to_str().ok())
                .filter(|s| !s.is_empty() && s.len() <= MAX_PARTITION_KEY_LENGTH)
                .map(ToSmolStr::to_smolstr)
        },
        SourceHeaderNameOrSni::Sni => {
            sni.filter(|s| !s.is_empty() && s.len() <= MAX_PARTITION_KEY_LENGTH).cloned()
        },
    })
}

/// Maximum allowed length for custom partition keys extracted from headers.
pub const MAX_CUSTOM_KEY_LENGTH: usize = 256;

#[inline]
/// Return the custom partition key from headers
pub fn extract_custom_partition_key<'a>(headers: &'a HeaderMap, source: Option<&SourceHeaderName>) -> Option<String> {
    // Extracts the custom partition key from headers based on the provided source name.
    let SourceHeaderName::HeaderName(keym) = source?;
    headers
        .get(keym)?
        .to_str()
        .ok()
        .filter(|s| !s.is_empty() && s.len() <= MAX_CUSTOM_KEY_LENGTH)
        .map(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};

    #[test]
    fn test_extract_user_partition_key_length_validation() {
        let key_name = http::HeaderName::from_static("x-user-id");
        let source = SourceHeaderNameOrSni::HeaderName(key_name.clone());

        // Valid key
        let mut headers = HeaderMap::new();
        headers.insert(key_name.clone(), HeaderValue::from_static("valid-user-123"));
        let extracted = extract_user_partition_key((&headers, None), Some(&source));
        assert_eq!(extracted.as_deref(), Some("valid-user-123"));

        // Empty key -> rejected
        headers.insert(key_name.clone(), HeaderValue::from_static(""));
        let extracted = extract_user_partition_key((&headers, None), Some(&source));
        assert_eq!(extracted, None);

        // Key exceeding MAX_PARTITION_KEY_LENGTH -> rejected
        let too_long = "a".repeat(MAX_PARTITION_KEY_LENGTH + 1);
        headers.insert(key_name.clone(), HeaderValue::try_from(too_long).unwrap());
        let extracted = extract_user_partition_key((&headers, None), Some(&source));
        assert_eq!(extracted, None);

        // SNI validation
        let sni_source = SourceHeaderNameOrSni::Sni;
        let empty_headers = HeaderMap::new();
        let valid_sni = SmolStr::new("example.com");
        let extracted_sni = extract_user_partition_key((&empty_headers, Some(&valid_sni)), Some(&sni_source));
        assert_eq!(extracted_sni.as_deref(), Some("example.com"));

        let too_long_sni = SmolStr::new("a".repeat(MAX_PARTITION_KEY_LENGTH + 1));
        let extracted_sni = extract_user_partition_key((&empty_headers, Some(&too_long_sni)), Some(&sni_source));
        assert_eq!(extracted_sni, None);
    }

    #[test]
    fn test_extract_custom_partition_key_length_validation() {
        let key_name = http::HeaderName::from_static("x-custom-metric");
        let source = SourceHeaderName::HeaderName(key_name.clone());

        // Valid
        let mut headers = HeaderMap::new();
        headers.insert(key_name.clone(), HeaderValue::from_static("custom-value"));
        let extracted = extract_custom_partition_key(&headers, Some(&source));
        assert_eq!(extracted.as_deref(), Some("custom-value"));

        // Empty -> rejected
        headers.insert(key_name.clone(), HeaderValue::from_static(""));
        let extracted = extract_custom_partition_key(&headers, Some(&source));
        assert_eq!(extracted, None);

        // Too long -> rejected
        let too_long = "b".repeat(MAX_CUSTOM_KEY_LENGTH + 1);
        headers.insert(key_name.clone(), HeaderValue::try_from(too_long).unwrap());
        let extracted = extract_custom_partition_key(&headers, Some(&source));
        assert_eq!(extracted, None);
    }
}
