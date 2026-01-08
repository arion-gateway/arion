use bytes::Bytes;
use serde::Serialize;
use tracing::error;

#[derive(Debug, Clone, Copy, Default)]
pub enum Transport {
    Sse,
    #[default]
    StreamableHttp
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::Sse => write!(f, "SSE"),
            Transport::StreamableHttp => write!(f, "Streamable HTTP"),
        }
    }
}

pub mod sse {
    use super::*;

    #[derive(Debug)]
    pub enum Event<'a, T: Serialize = ()> {
        Endpoint(&'a str),
        Message(&'a T),
    }

    impl<'a, T: Serialize> Event<'a, T> {
        #[inline]
        pub fn to_bytes(&self) -> Bytes {
            match self {
                Event::Endpoint(endpoint) => Bytes::from(format!("event: endpoint\ndata: {endpoint}\n\n")),
                Event::Message(value) => {
                    let msg = serde_json::to_string(value).unwrap_or_else(|err| {
                        error!(target: "mcp_gateway", "SEE: failed to serialize message: {}", err);
                        "".into()
                    });

                    Bytes::from(format!("event: message\ndata: {msg}\n\n"))
                },
            }
        }
    }
}

pub mod streamable_http {
    use std::{sync::{OnceLock, atomic::{AtomicU64, Ordering}}, time::{SystemTime, UNIX_EPOCH}};

    use bytes::Bytes;
    use serde::Serialize;
    use smol_str::{SmolStr, format_smolstr};


    // Stores the random instance prefix (initialized only once).
    // We use OnceLock for thread-safe, one-time initialization without Mutex overhead on reads.
    static INSTANCE_PREFIX: OnceLock<u128> = OnceLock::new();

    // Stores the monotonic counter.
    // AtomicU64 allows lock-free increments.
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug)]
    pub enum Event<'a, T: Serialize = ()> {
        Endpoint(&'a str),
        Message(&'a T),
        Error(&'a T),
        Ping,
    }

    impl<'a, T: Serialize> Event<'a, T> {
        #[inline]
        pub fn to_bytes(&self) -> Bytes {
            let id = get_sse_id();
            match self {
                Event::Endpoint(endpoint) => Bytes::from(format!("event: message\nid: {id}\ndata: {endpoint}\n\n")),
                Event::Message(value) => {
                    let msg = serde_json::to_string(value).unwrap_or_default();
                    Bytes::from(format!("event: message\nid: {id}\ndata: {msg}\n\n"))
                },
                Event::Ping => Bytes::from(format!("event: ping\nid: {id}\ndata: \n\n")),
                Event::Error(err) => {
                    let msg = serde_json::to_string(err).unwrap_or_default();
                    Bytes::from(format!("event: message\nid: {id}\nerror: {msg}\n\n"))
                },
            }
        }
    }

    fn get_sse_id() -> SmolStr {
        // 1. Get the prefix in nanoseconds.
        let prefix = INSTANCE_PREFIX.get_or_init(|| {
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            since_the_epoch.as_nanos()
        });

        // 2. Increment the counter.
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);

        // 3. Combine them.
        format_smolstr!("{:x}_{}", prefix, count)
    }
}
