use atomicoption::AtomicOption;
use http::HeaderName;
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

static USER_ID_HEADER_NAME: AtomicOption<HeaderName> = AtomicOption::none();

#[inline]
pub fn set_user_header_name(value: HeaderName) {
    USER_ID_HEADER_NAME.store(Ordering::Release, value);
}

#[inline]
pub fn get_user_header_name() -> Option<&'static HeaderName> {
    USER_ID_HEADER_NAME.as_ref(Ordering::Acquire)
}
