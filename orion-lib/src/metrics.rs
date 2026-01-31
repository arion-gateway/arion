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
            $counter.get().inspect(|c| c.$method($($args),*));
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
