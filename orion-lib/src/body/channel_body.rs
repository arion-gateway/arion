use bytes::Bytes;
use futures::{Sink, Stream, StreamExt};
use http_body::{Body, Frame, SizeHint};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

type FrameResult = Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>;

/// A wrapper for any Body that allows observing and modifying frames in real-time.
pub struct ChannelBody {
    prefetch: VecDeque<Option<FrameResult>>,
    prefetch_num_frames: NonZeroUsize,
    stream: ReceiverStream<FrameResult>,
    is_end_stream: bool,
}

impl ChannelBody {
    /// Creates a new `ChannelBody` wrapping an existing body.
    ///
    /// Returns a tuple of (`ChannelBody`, `FrameBridge`). `FrameBridge` must be used
    /// to inject frames (either manually or via `complete()`), otherwise the `ChannelBody`
    /// will never produce any frames.
    pub fn new<B>(body: B, prefetch_num_frames: NonZeroUsize) -> (Self, FrameBridge)
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        // Create a channel for injecting frames
        let (tx, rx) = mpsc::channel(8);

        // Convert the receiver into a StreamBody
        let stream_of_body = ReceiverStream::new(rx);

        // Create the bridge with the original body
        let bridge = FrameBridge::new(body, tx);

        (
            ChannelBody {
                stream: stream_of_body,
                prefetch: VecDeque::new(),
                prefetch_num_frames,
                is_end_stream: false,
            },
            bridge,
        )
    }

    /// Asynchronously waits until the given number of frames are available in the body.
    ///
    /// This method does not consume the frame.
    pub async fn prefetch_frames(&mut self) {
        self.prefetch.reserve(self.prefetch_num_frames.get());
        for _ in 0..self.prefetch_num_frames.get() {
            let r = Pin::new(&mut self.stream).next().await;
            self.is_end_stream = r.is_none();
            self.prefetch.push_back(r);
            if self.is_end_stream {
                return;
            }
        }
    }
}

impl std::fmt::Debug for ChannelBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelBody").field("stream_body", &self.stream).field("buffered", &self.prefetch).finish()
    }
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        while self.prefetch.len() < self.prefetch_num_frames.get() {
            if let Poll::Ready(something) = Pin::new(&mut self.stream).poll_next(cx) {
                self.is_end_stream = something.is_none();
                self.prefetch.push_back(something);
                if self.is_end_stream {
                    break;
                }
            } else {
                break;
            }
        }

        if let Some(frame) = self.prefetch.pop_front() {
            return Poll::Ready(frame);
        }

        if self.is_end_stream {
            return Poll::Ready(None);
        }

        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        self.is_end_stream && self.prefetch.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        // Calculates the length of all data frames currently in the prefetch buffer.
        // Iterates over options, flattens them, extracts data frames, and sums their lengths.
        let prefetched_len: u64 = self
            .prefetch
            .iter()
            .flatten()
            .filter_map(|res| res.as_ref().ok()) // ignoring any Err in the buffer for the size calculation.
            .filter_map(|f| f.data_ref()) // Keeps only frames with some data (ignores trailers), returns &Data
            .map(|b| b.len() as u64)
            .sum();

        if self.is_end_stream {
            SizeHint::with_exact(prefetched_len)
        } else {
            let mut sh = SizeHint::new();
            sh.set_lower(prefetched_len);
            sh
        }
    }
}

/// A stream that allows observing frames from a body and simultaneously
/// injecting them into the `ChannelBody`.
///
/// The `FrameBridge` acts as a bridge between the original body and the `ChannelBody`.
/// Frames read from the original body must be injected into the `ChannelBody` for it
/// to produce any output.
pub struct FrameBridge {
    body_stream: Pin<Box<dyn Stream<Item = FrameResult> + Send>>,
    injector: Option<mpsc::Sender<FrameResult>>,
    orig_empty_body: bool,
}

impl std::fmt::Debug for FrameBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameBridge").finish()
    }
}

impl Default for FrameBridge {
    fn default() -> Self {
        Self { body_stream: Box::pin(futures::stream::empty()), injector: None, orig_empty_body: true }
    }
}

impl FrameBridge {
    fn new<B>(body: B, injector: mpsc::Sender<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>) -> Self
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let end_of_stream = body.is_end_stream();

        // Convert the body into a stream using http_body_util and map errors to Box
        let body_stream: Pin<Box<dyn Stream<Item = FrameResult> + Send>> =
            Box::pin(http_body_util::BodyStream::new(body).map(|result| result.map_err(Into::into)));

        Self { body_stream, injector: Some(injector), orig_empty_body: end_of_stream }
    }

    /// Close the `FrameBridge` to prevent further frame injections.
    pub fn close(&mut self) {
        self.injector.take();
    }

    /// Consumes the entire original body, injecting each frame into the `ChannelBody`.
    pub async fn complete(&mut self) {
        let Some(injector) = &mut self.injector else {
            return;
        };
        while let Some(frame_result) = self.body_stream.next().await {
            // If sending fails, it means the receiver has been dropped
            if injector.send(frame_result).await.is_err() {
                break;
            }
        }
    }

    /// Consumes the DATA frame of the entire original body, injecting each frame into the `ChannelBody`,
    /// and returns when a non-DATA frame is encountered.
    pub async fn complete_data(
        &mut self,
    ) -> Option<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync + 'static>>> {
        let Some(injector) = &mut self.injector else {
            return None;
        };
        while let Some(frame_result) = self.body_stream.next().await {
            match &frame_result {
                Ok(frame) if frame.is_data() => {
                    // If sending fails, it means the receiver has been dropped
                    if injector.send(frame_result).await.is_err() {
                        break;
                    }
                },
                _ => {
                    return Some(frame_result);
                },
            }
        }
        None
    }

    /// Consumes the entire original body by applying a transformation function
    /// to each frame before injecting it into the `ChannelBody`.
    pub async fn complete_with<F>(mut self, mut transform: F)
    where
        F: FnMut(Frame<Bytes>) -> Frame<Bytes>,
    {
        while let Some(frame_result) = self.body_stream.next().await {
            let Some(injector) = &mut self.injector else {
                break;
            };
            let transformed = frame_result.map(&mut transform);
            if injector.send(transformed).await.is_err() {
                break;
            }
        }
    }

    /// Gets the next frame from the original body stream.
    ///
    /// Returns None when the body is completely consumed.
    pub async fn next_frame(&mut self) -> Option<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>> {
        self.body_stream.as_mut().next().await
    }

    /// Injects a frame into the `ChannelBody`.
    ///
    /// Returns an error if the receiver has been dropped (i.e., the `ChannelBody`
    /// has been consumed or dropped).
    pub async fn inject_frame(
        &mut self,
        frame: Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), mpsc::error::SendError<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>> {
        let Some(injector) = &mut self.injector else {
            return Err(mpsc::error::SendError(frame));
        };
        injector.send(frame).await
    }

    /// Observes the next frame and automatically injects it into the `ChannelBody`.
    ///
    /// Returns a copy of the frame to allow observation, None when the body is completely consumed.
    pub async fn observe_and_inject(
        &mut self,
    ) -> Option<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>> {
        let frame = self.body_stream.as_mut().next().await?;

        // Clone the frame to be able to return it
        let cloned = match &frame {
            Ok(f) => {
                // Clone the frame
                let cloned_frame = if let Some(data) = f.data_ref() {
                    Frame::data(data.clone())
                } else if let Some(trailers) = f.trailers_ref() {
                    Frame::trailers(trailers.clone())
                } else {
                    return Some(frame);
                };
                Ok(cloned_frame)
            },
            Err(e) => Err(e.to_string().into()),
        };

        // Inject the original frame
        if let Some(injector) = &mut self.injector {
            let _ = injector.send(frame).await;
        }

        Some(cloned)
    }

    /// Check if the `FrameBridge` has been constructed with an empty body.
    pub fn is_orig_empty_body(&self) -> bool {
        self.orig_empty_body
    }
}

impl Stream for FrameBridge {
    type Item = Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.body_stream.as_mut().poll_next(cx)
    }
}

impl Sink<Bytes> for FrameBridge {
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    use http_body_util::Full;
    use std::task::{Context, Poll, Waker};

    #[tokio::test]
    async fn test_complete() {
        let body = Full::new(Bytes::from("Hello, World!"));
        let (mut channel_body, mut bridge) = ChannelBody::new(body, 1);

        // Spawn bridge task
        let bridge_handle = tokio::spawn(async move {
            bridge.complete().await;
        });

        // Consume the channel body
        let frame = future::poll_fn(|cx| Pin::new(&mut channel_body).poll_frame(cx)).await.unwrap().unwrap();

        if let Ok(data) = frame.into_data() {
            assert_eq!(data, Bytes::from("Hello, World!"));
        } else {
            panic!("Expected data frame");
        }

        bridge_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_manual_injection() {
        let body = Full::new(Bytes::from("Test"));
        let (mut channel_body, mut bridge) = ChannelBody::new(body, 1);

        // Spawn a task that consumes the ChannelBody
        let consumer_handle = tokio::spawn(async move {
            let frame = future::poll_fn(|cx| Pin::new(&mut channel_body).poll_frame(cx)).await.unwrap().unwrap();

            if let Ok(data) = frame.into_data() {
                data
            } else {
                panic!("Expected data frame");
            }
        });

        // Manually observe and inject the frame
        if let Some(frame) = bridge.next_frame().await {
            bridge.inject_frame(frame).await.unwrap();
        }

        // Complete the bridge
        drop(bridge);

        // Verify that the consumer received the data
        let result = consumer_handle.await.unwrap();
        assert_eq!(result, Bytes::from("Test"));
    }

    fn dummy_context() -> Context<'static> {
        // Waker::noop() creates a waker that does nothing when woken.
        // This has been stable since Rust 1.58.
        let waker = Waker::noop();
        Context::from_waker(waker)
    }

    #[tokio::test]
    async fn test_channel_body_debug() {
        let body = Full::new(Bytes::from("Debug Test"));
        let (mut channel_body, mut bridge) = ChannelBody::new(body, 1);
        assert!(!channel_body.is_end_stream());
        let mut ctx = dummy_context();
        assert!(matches!(Pin::new(&mut channel_body).poll_frame(&mut ctx), Poll::Pending));

        let bridge_handle = tokio::spawn(async move {
            bridge.complete().await;
        });
        bridge_handle.await.unwrap();

        assert!(matches!(Pin::new(&mut channel_body).poll_frame(&mut ctx), Poll::Ready(Some(Ok(_)))));
        assert!(matches!(Pin::new(&mut channel_body).poll_frame(&mut ctx), Poll::Ready(None)));

        println!("{channel_body:?}");
    }
}
