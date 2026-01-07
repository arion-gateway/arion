pub mod transport {
    use bytes::Bytes;
    use serde::Serialize;
    use tracing::error;

    #[derive(Debug)]
    pub enum Event<'a, T: Serialize = ()> {
        Endpoint(&'a str),
        Message(&'a T),
    }

    impl<'a, T: Serialize> Event<'a, T> {
        #[inline]
        pub fn to_bytes(&self) -> Bytes {
            match self {
                Event::Endpoint(endpoint) => Bytes::from(format!("event: endpoint\ndata: {}\n\n", endpoint)),
                Event::Message(value) => {
                    let msg = serde_json::to_string(value).unwrap_or_else(|err| {
                        error!(target: "mcp_gateway", "SEE: failed to serialize message: {}", err);
                        "".into()
                    });

                    Bytes::from(format!("event: message\ndata: {}\n\n", msg))
                },
            }
        }
    }
}
