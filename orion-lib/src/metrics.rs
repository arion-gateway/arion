use atomicoption::AtomicOption;
use http::HeaderMap;
use orion_configuration::config::metrics::PartitionKeySource;
use orion_interner::StringInterner;
use smol_str::SmolStr;
use std::sync::atomic::Ordering;

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

pub struct PartitionKey {
    source: AtomicOption<PartitionKeySource>,
    attribute_name: AtomicOption<String>,
}

impl PartitionKey {
    #[inline]
    pub const fn new() -> PartitionKey {
        PartitionKey { source: AtomicOption::none(), attribute_name: AtomicOption::none() }
    }

    #[inline]
    pub fn source(&self) -> Option<&PartitionKeySource> {
        self.source.as_ref(Ordering::Acquire)
    }

    #[inline]
    pub fn set_source(&self, value: PartitionKeySource) {
        self.source.store(Ordering::Release, value);
    }

    #[inline]
    pub fn attribute_name(&self) -> Option<&str> {
        self.attribute_name.as_ref(Ordering::Acquire).map(String::as_str)
    }

    #[inline]
    pub fn set_attribute_name(&self, value: String) {
        self.attribute_name.store(Ordering::Release, value);
    }
}

pub static USER_KEY: PartitionKey = PartitionKey::new();
pub static CUSTOM_KEY: PartitionKey = PartitionKey::new();

#[inline]
/// Return the partition key from headers, if one is present and the header name is configured.
pub fn get_user_partition_key(
    headers: &HeaderMap,
    sni: Option<&SmolStr>,
    source: Option<&PartitionKeySource>,
) -> Option<&'static str> {
    source.and_then(|key| match key {
        PartitionKeySource::HeaderName(keym) => {
            headers.get(keym).map(|value| value.to_str()).transpose().ok().flatten().map(|s| s.to_static_str())
        },
        PartitionKeySource::Sni => sni.map(orion_interner::StringInterner::to_static_str),
    })
}
