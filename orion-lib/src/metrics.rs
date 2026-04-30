use atomicoption::AtomicOption;
use http::{HeaderMap, HeaderName};
use orion_interner::StringInterner;
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
            ()
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
            ()
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
    header_name: AtomicOption<HeaderName>,
    attribute_name: AtomicOption<String>,
}

impl PartitionKey {
    pub const fn new() -> PartitionKey{
        PartitionKey {
            header_name: AtomicOption::none(),
            attribute_name: AtomicOption::none(),
        }
    }

    pub fn header_name(&self) -> Option<&HeaderName> {
        self.header_name.as_ref(Ordering::Acquire)
    }

    pub fn attribute_name(&self) -> Option<&str> {
        self.attribute_name.as_ref(Ordering::Acquire).map(|s| s.as_str())
    }

    pub fn set_header_name(&self, value: HeaderName) {
        self.header_name.store(Ordering::Release, value);
    }

    pub fn set_attribute_name(&self, value: String) {
        self.attribute_name.store(Ordering::Release, value);
    }
}

pub static USER_KEY : PartitionKey = PartitionKey::new();
pub static CUSTOM_KEY : PartitionKey = PartitionKey::new();

#[inline]
/// Return the partition key from headers, if one is present and the header name is configured.
pub fn get_partition_key_from_headers(headers: &HeaderMap, user_header_name: Option<&HeaderName>) -> Option<&'static str> {
    user_header_name.and_then(|header_name| {
        headers.get(header_name).map(|value| value.to_str()).transpose().ok().flatten().map(|s| s.to_static_str())
    })
}
