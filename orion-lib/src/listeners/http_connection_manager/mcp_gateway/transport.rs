use bytes::Bytes;
use http::Method;
use orion_http_header::MCP_SESSION_ID;
use serde::Serialize;
use smol_str::{SmolStr, ToSmolStr};
use tracing::{debug, error};
use url::form_urlencoded;

pub const SESSION_ID_QUERY_KEY: &str = "sessionId";
pub const SESSION_ID_QUERY_KEY_ALT: &str = "session_id";
pub const MIME_TEXT_EVENT_STREAM: &str = "text/event-stream";
pub const MIME_APPLICATION_JSON: &str = "application/json";

pub const BYTES_MIME_TEXT_EVENT_STREAM: &[u8] = b"text/event-stream";
pub const BYTES_MIME_APPLICATION_JSON: &[u8] = b"application/json";

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct SessionId(pub SmolStr);

impl SessionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub trait RequestExt {
    fn get_mcp_transport(&self) -> Option<Transport>;
    fn get_mcp_session_id(&self) -> Option<SessionId>;
    fn get_mcp_accepted_mime(&self) -> Option<AcceptedMime>;
}

pub enum AcceptedMime {
    EventStream,
    ApplicationJson,
    EventStreamAndJson,
}

impl AcceptedMime {
    #[allow(dead_code)]
    pub fn is_app_json(&self) -> bool {
        match self {
            AcceptedMime::EventStream => false,
            AcceptedMime::ApplicationJson => true,
            AcceptedMime::EventStreamAndJson => true,
        }
    }
    #[allow(dead_code)]
    pub fn is_event_stream(&self) -> bool {
        match self {
            AcceptedMime::EventStream => true,
            AcceptedMime::ApplicationJson => false,
            AcceptedMime::EventStreamAndJson => true,
        }
    }
}

impl TryFrom<&str> for AcceptedMime {
    type Error = ();
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let has_event_stream = s.contains(MIME_TEXT_EVENT_STREAM);
        let has_json = s.contains(MIME_APPLICATION_JSON);
        if has_event_stream && has_json {
            Ok(AcceptedMime::EventStreamAndJson)
        } else if has_event_stream {
            Ok(AcceptedMime::EventStream)
        } else if has_json {
            Ok(AcceptedMime::ApplicationJson)
        } else {
            Err(())
        }
    }
}

impl<B> RequestExt for http::Request<B> {
    fn get_mcp_transport(&self) -> Option<Transport> {
        match *self.method() {
            Method::POST => {
                if let Some(query) = self.uri().query() {
                    if query.contains(SESSION_ID_QUERY_KEY) || query.contains(SESSION_ID_QUERY_KEY_ALT) {
                        return Some(Transport::Sse);
                    }
                }
                return Some(Transport::StreamableHttp);
            },
            Method::GET => {
                if let Some(accept_value) = self.headers().get(http::header::ACCEPT) {
                    let bytes = accept_value.as_bytes();
                    if bytes.windows(BYTES_MIME_TEXT_EVENT_STREAM.len()).any(|w| w == BYTES_MIME_TEXT_EVENT_STREAM) {
                        return Some(Transport::Sse);
                    }
                }
                None
            },

            _ => None,
        }
    }

    fn get_mcp_session_id(&self) -> Option<SessionId> {
        if let Some(id) = self.headers().get(MCP_SESSION_ID) {
            return id.to_str().ok().map(|s| SessionId(s.to_smolstr()));
        }
        self.uri().query().and_then(|query| {
            debug!(target: "mcp_gateway", "get_mcp_session_id: query: {query}...");
            form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == SESSION_ID_QUERY_KEY || key == SESSION_ID_QUERY_KEY_ALT)
                .map(|(_, value)| SessionId(value.to_smolstr()))
        })
    }

    fn get_mcp_accepted_mime(&self) -> Option<AcceptedMime> {
        if let Some(accept_value) = self.headers().get(http::header::ACCEPT) {
            let accept = accept_value.to_str().ok()?;
            return AcceptedMime::try_from(accept).ok();
        }
        None
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Transport {
    Sse,
    #[default]
    StreamableHttp,
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
    use std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            OnceLock,
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use bytes::Bytes;
    use serde::Serialize;
    use smol_str::{format_smolstr, SmolStr};

    // Stores the random instance prefix (initialized only once).
    // We use OnceLock for thread-safe, one-time initialization without Mutex overhead on reads.
    static INSTANCE_PREFIX: OnceLock<u128> = OnceLock::new();

    // Stores the monotonic counter.
    // AtomicU64 allows lock-free increments.
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug)]
    pub enum Event<'a, T: Serialize = ()> {
        Message(&'a T),
        Priming,
    }

    impl<'a, T: Serialize> Event<'a, T> {
        #[inline]
        pub fn to_bytes(&self) -> Bytes {
            let id = get_sse_id();
            match self {
                Event::Message(value) => {
                    let msg = serde_json::to_string(value).unwrap_or_default();
                    Bytes::from(format!("event: message\nid: {id}\ndata: {msg}\n\n"))
                },
                Event::Priming => Bytes::from(format!("event: message\nid: {id}\ndata:\n\n")),
            }
        }
    }

    fn get_sse_id() -> SmolStr {
        // 1. Get the prefix in nanoseconds.
        let prefix = INSTANCE_PREFIX.get_or_init(|| {
            let start = SystemTime::now();
            let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
            since_the_epoch.as_nanos()
        });

        // 2. Increment the counter.
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);

        // 3. Combine them.
        format_smolstr!("{:x}_{}", prefix, count)
    }
}
