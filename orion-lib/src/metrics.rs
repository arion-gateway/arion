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

use atomicoption::AtomicOption;
use http::HeaderMap;
use orion_configuration::config::metrics::{PartitionKeySource, SourceHeaderName, SourceHeaderNameOrSni};
use orion_interner::StringInterner;
use smol_str::SmolStr;
use std::sync::{atomic::Ordering, OnceLock};

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

#[inline]
/// Return the user partition key, extracting it from either headers or sni, if one is present.
pub fn extract_user_partition_key(
    (headers, sni): (&HeaderMap, Option<&SmolStr>),
    source: Option<&SourceHeaderNameOrSni>,
) -> Option<&'static str> {
    source.and_then(|source| match source {
        SourceHeaderNameOrSni::HeaderName(keym) => {
            headers.get(keym).map(|value| value.to_str()).transpose().ok().flatten().map(|s| s.to_static_str())
        },
        SourceHeaderNameOrSni::Sni => sni.map(orion_interner::StringInterner::to_static_str),
    })
}

#[inline]
/// Return the custom partition key from headers
pub fn extract_custom_partition_key(headers: &HeaderMap, source: Option<&SourceHeaderName>) -> Option<&'static str> {
    // Extracts the custom partition key from headers based on the provided source name.
    let SourceHeaderName::HeaderName(keym) = source?;
    headers.get(keym)?.to_str().ok()?.to_static_str().into()
}
