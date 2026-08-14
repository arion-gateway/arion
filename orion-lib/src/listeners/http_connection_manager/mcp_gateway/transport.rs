use orion_http_header::MCP_SESSION_ID;
use smol_str::{SmolStr, ToSmolStr};

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
    fn get_mcp_session_id(&self) -> Option<SessionId> {
        self.headers().get(MCP_SESSION_ID)?.to_str().ok().map(|s| SessionId(s.to_smolstr()))
    }

    fn get_mcp_accepted_mime(&self) -> Option<AcceptedMime> {
        if let Some(accept_value) = self.headers().get(http::header::ACCEPT) {
            let accept = accept_value.to_str().ok()?;
            return AcceptedMime::try_from(accept).ok();
        }
        None
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
    }

    impl<T: Serialize> Event<'_, T> {
        #[allow(clippy::expect_used)]
        pub fn write_to(&self, buf: &mut BytesMut) -> std::io::Result<()> {
            let prefix = INSTANCE_PREFIX.get_or_init(|| {
                let start = SystemTime::now();
                let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
                since_the_epoch.as_nanos()
            });
            let count = COUNTER.fetch_add(1, Ordering::Relaxed);

            // Create an adapter that implements std::io::Write
            let mut writer = buf.writer();

            let Event::Message(value) = self;

            // 1. Write the SSE header using the io::Write trait
            write!(writer, "event: message\nid: {prefix:x}_{count}\ndata: ")?;

            // 2. Serialize JSON directly into the writer
            serde_json::to_writer(&mut writer, value).map_err(std::io::Error::other)?;

            // 3. Append the closing newlines
            writer.write_all(b"\n\n")?;

            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde::Serialize;

        #[derive(Serialize)]
        struct TestMessage<'a> {
            value: &'a str,
        }

        #[test]
        fn message_event_writes_an_sse_frame() {
            let message = TestMessage { value: "hello" };
            let mut buf = BytesMut::new();

            Event::Message(&message).write_to(&mut buf).unwrap();

            let encoded = std::str::from_utf8(&buf).unwrap();
            assert!(encoded.starts_with("event: message\nid: "));
            assert!(encoded.ends_with("\ndata: {\"value\":\"hello\"}\n\n"));
        }
    }
}
