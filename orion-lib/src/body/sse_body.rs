use bytes::Bytes;
use futures::{Sink, Stream, StreamExt};
use http_body::{Body, Frame};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

type FrameResult = Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>;

/// A wrapper for any Body that allows observing and modifying frames in real-time.
pub struct SseBody {
    stream: ReceiverStream<FrameResult>,
}

impl SseBody {
    /// Creates a new `SseBody` wrapping an existing body.
    ///
    /// Returns a tuple of (`SseBody`, `SseBridge`). `SseBridge` must be used
    /// to inject frames (either manually or via `complete()`), otherwise the `SseBody`
    /// will never produce any frames.
    pub fn new<B>(body: B) -> (Self, SseBridge)
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        // Create a channel for injecting frames
        let (tx, rx) = mpsc::channel(8);

        // Convert the receiver into a StreamBody
        let stream_of_body = ReceiverStream::new(rx);

        // Create the bridge with the original body
        let bridge = SseBridge::new(body, tx);

        (SseBody { stream: stream_of_body }, bridge)
    }
}

impl std::fmt::Debug for SseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelBody").field("stream_body", &self.stream).finish()
    }
}

impl Body for SseBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

/// A stream that allows observing frames from a body and simultaneously
/// injecting them into the `ChannelBody`.
///
/// The `FrameBridge` acts as a bridge between the original body and the `ChannelBody`.
/// Frames read from the original body must be injected into the `ChannelBody` for it
/// to produce any output.
pub struct SseBridge {
    body_stream: Pin<Box<dyn Stream<Item = FrameResult> + Send>>,
    injector: Option<mpsc::Sender<FrameResult>>,
}

impl std::fmt::Debug for SseBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameBridge").finish()
    }
}

impl Default for SseBridge {
    fn default() -> Self {
        Self { body_stream: Box::pin(futures::stream::empty()), injector: None }
    }
}

impl SseBridge {
    fn new<B>(body: B, injector: mpsc::Sender<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>) -> Self
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        // Convert the body into a stream using http_body_util and map errors to Box
        let body_stream: Pin<Box<dyn Stream<Item = FrameResult> + Send>> =
            Box::pin(http_body_util::BodyStream::new(body).map(|result| result.map_err(Into::into)));

        Self { body_stream, injector: Some(injector) }
    }

    /// Close the `FrameBridge` to prevent further frame injections.
    pub fn close(&mut self) {
        self.injector.take();
    }
}

impl Stream for SseBridge {
    type Item = Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.body_stream.as_mut().poll_next(cx)
    }
}

impl Sink<Bytes> for SseBridge {
    type Error = mpsc::error::SendError<FrameResult>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.injector.is_some() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(mpsc::error::SendError(Ok(Frame::data(Bytes::new())))))
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        if let Some(injector) = &mut self.injector {
            injector.try_send(Ok(Frame::data(item))).map_err(|e| match e {
                mpsc::error::TrySendError::Full(frame) | mpsc::error::TrySendError::Closed(frame) => {
                    mpsc::error::SendError(frame)
                },
            })
        } else {
            Err(mpsc::error::SendError(Ok(Frame::data(item))))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // mpsc channels don't need explicit flushing
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.injector.take();
        Poll::Ready(Ok(()))
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use futures::future;
//     use http_body_util::Full;
//     use std::{
//         num::NonZeroUsize,
//         task::{Context, Poll, Waker},
//     };
//
//     #[tokio::test]
//     async fn test_complete() {
//         let body = Full::new(Bytes::from("Hello, World!"));
//         let (mut channel_body, mut bridge) = SseBody::new(body);
//
//         // Spawn bridge task
//         let bridge_handle = tokio::spawn(async move {
//             bridge.complete().await;
//         });
//
//         // Consume the channel body
//         let frame = future::poll_fn(|cx| Pin::new(&mut channel_body).poll_frame(cx)).await.unwrap().unwrap();
//
//         if let Ok(data) = frame.into_data() {
//             assert_eq!(data, Bytes::from("Hello, World!"));
//         } else {
//             panic!("Expected data frame");
//         }
//
//         bridge_handle.await.unwrap();
//     }
//
//     #[tokio::test]
//     async fn test_manual_injection() {
//         let body = Full::new(Bytes::from("Test"));
//         let (mut channel_body, mut bridge) = SseBody::new(body);
//
//         // Spawn a task that consumes the ChannelBody
//         let consumer_handle = tokio::spawn(async move {
//             let frame = future::poll_fn(|cx| Pin::new(&mut channel_body).poll_frame(cx)).await.unwrap().unwrap();
//
//             if let Ok(data) = frame.into_data() {
//                 data
//             } else {
//                 panic!("Expected data frame");
//             }
//         });
//
//         // Manually observe and inject the frame
//         if let Some(frame) = bridge.next_frame().await {
//             bridge.inject_frame(frame).await.unwrap();
//         }
//
//         // Complete the bridge
//         drop(bridge);
//
//         // Verify that the consumer received the data
//         let result = consumer_handle.await.unwrap();
//         assert_eq!(result, Bytes::from("Test"));
//     }
//
//     fn dummy_context() -> Context<'static> {
//         // Waker::noop() creates a waker that does nothing when woken.
//         // This has been stable since Rust 1.58.
//         let waker = Waker::noop();
//         Context::from_waker(waker)
//     }
//
//     #[tokio::test]
//     async fn test_channel_body_debug() {
//         let body = Full::new(Bytes::from("Debug Test"));
//         let (mut channel_body, mut bridge) = SseBody::new(body);
//         assert!(!channel_body.is_end_stream());
//         let mut ctx = dummy_context();
//         assert!(matches!(Pin::new(&mut channel_body).poll_frame(&mut ctx), Poll::Pending));
//
//         let bridge_handle = tokio::spawn(async move {
//             bridge.complete().await;
//         });
//         bridge_handle.await.unwrap();
//
//         assert!(matches!(Pin::new(&mut channel_body).poll_frame(&mut ctx), Poll::Ready(Some(Ok(_)))));
//         assert!(matches!(Pin::new(&mut channel_body).poll_frame(&mut ctx), Poll::Ready(None)));
//
//         println!("{channel_body:?}");
//     }
// }
