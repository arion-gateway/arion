use http::Method;
use orion_http_header::MCP_SESSION_ID;
use serde::Serialize;
use smol_str::{SmolStr, ToSmolStr};
use tracing::{debug, info};
use url::form_urlencoded;

pub const SESSION_ID_QUERY_KEY: &str = "sessionId";
pub const SESSION_ID_QUERY_KEY_ALT: &str = "session_id";
pub const MIME_TEXT_EVENT_STREAM: &str = "text/event-stream";
pub const MIME_APPLICATION_JSON: &str = "application/json";

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct SessionId(pub SmolStr);

impl SessionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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
            AcceptedMime::ApplicationJson | AcceptedMime::EventStreamAndJson => true,
        }
    }
    #[allow(dead_code)]
    pub fn is_event_stream(&self) -> bool {
        match self {
            AcceptedMime::ApplicationJson => false,
            AcceptedMime::EventStreamAndJson | AcceptedMime::EventStream => true,
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
                Some(Transport::StreamableHttp)
            },
            Method::GET => {
                if let Some(accept_value) = self.headers().get(http::header::ACCEPT) {
                    if let Ok(s) = accept_value.to_str() {
                        if s.contains(MIME_TEXT_EVENT_STREAM) {
                            return Some(Transport::Sse);
                        }
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
            Transport::StreamableHttp => write!(f, "StreamableHTTP"),
        }
    }
}

pub mod sse {
    use bytes::{BufMut, BytesMut};

    use super::{info, Serialize};

    #[derive(Debug)]
    pub enum Event<'a, T: Serialize = ()> {
        Endpoint(&'a str),
        Message(&'a T),
    }

    impl<T: Serialize> Event<'_, T> {
        // Write directly to a pre-allocated BytesMut
        pub fn write_to(&self, buf: &mut BytesMut) {
            match self {
                Event::Endpoint(endpoint) => {
                    buf.extend_from_slice(b"event: endpoint\ndata: ");
                    buf.extend_from_slice(endpoint.as_bytes());
                    buf.extend_from_slice(b"\n\n");
                },
                Event::Message(value) => {
                    // Write the SSE header
                    buf.extend_from_slice(b"event: message\ndata: ");

                    // Serialize directly into the IO writer adapter
                    if let Err(err) = serde_json::to_writer(buf.writer(), value) {
                        info!(target: "mcp_gateway", "SSE: failed to serialize message: {}!", err);
                    }

                    // Append closing newlines (acts as fallback if serialization fails)
                    buf.extend_from_slice(b"\n\n");
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

    use bytes::{BufMut, BytesMut};
    use serde::Serialize;
    use std::io::Write;

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

    impl<T: Serialize> Event<'_, T> {
        #[allow(clippy::expect_used)]
        pub fn write_to(&self, buf: &mut BytesMut) {
            let prefix = INSTANCE_PREFIX.get_or_init(|| {
                let start = SystemTime::now();
                let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
                since_the_epoch.as_nanos()
            });
            let count = COUNTER.fetch_add(1, Ordering::Relaxed);

            // Create an adapter that implements std::io::Write
            let mut writer = buf.writer();

            match self {
                Event::Message(value) => {
                    // 1. Write the SSE header using the io::Write trait
                    let _ = write!(writer, "event: message\nid: {prefix:x}_{count}\ndata: ");

                    // 2. Serialize JSON directly into the writer
                    let _ = serde_json::to_writer(&mut writer, value);

                    // 3. Append the closing newlines
                    let _ = writer.write_all(b"\n\n");
                },
                Event::Priming => {
                    let _ = write!(writer, "event: message\nid: {prefix:x}_{count}\ndata:\n\n");
                },
            }
        }
    }
}
