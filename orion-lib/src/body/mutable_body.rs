use bytes::Bytes;
use futures::{Stream, StreamExt};
use http_body::{Body, Frame};
use http_body_util::StreamBody;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// A wrapper for any Body that allows observing and modifying frames in real-time.
///
pub struct MutableBody {
    stream_body: StreamBody<ReceiverStream<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>>,
}

impl MutableBody {
    /// Creates a new MutableBody wrapping an existing body.
    ///
    /// Returns a tuple of (MutableBody, FrameObserver). FrameObserver must be used
    /// to inject frames (either manually or via `complete()`), otherwise the MutableBody
    /// will never produce any frames.

    pub fn new<B>(body: B) -> (Self, FrameObserver<B>)
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        // Create a channel for injecting frames
        let (tx, rx) = mpsc::channel(32);

        // Convert the receiver into a StreamBody
        let stream_body = StreamBody::new(ReceiverStream::new(rx));

        // Create the observer with the original body
        let observer = FrameObserver::new(body, tx);

        (MutableBody { stream_body }, observer)
    }
}

impl Body for MutableBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.stream_body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.stream_body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        Body::size_hint(&self.stream_body)
    }
}

/// A stream that allows observing frames from a body and simultaneously
/// injecting them into the MutableBody.
///
/// The FrameObserver acts as a bridge between the original body and the MutableBody.
/// Frames read from the original body must be injected into the MutableBody for it
/// to produce any output.
pub struct FrameObserver<B>
where
    B: Body<Data = Bytes>,
{
    body_stream: Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, B::Error>> + Send>>,
    injector: Option<mpsc::Sender<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>>,
}

impl<B> FrameObserver<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn new(body: B, injector: mpsc::Sender<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>) -> Self {
        // Convert the body into a stream using http_body_util
        let body_stream: Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, B::Error>> + Send>> =
            Box::pin(http_body_util::BodyStream::new(body));

        Self {
            body_stream,
            injector: Some(injector),
        }
    }

    /// Close the FrameObserver to prevent further frame injections.
    ///
    pub async fn close(mut self) {
        self.injector.take();
    }

    /// Consumes the entire original body, injecting each frame into the MutableBody.
    ///
    pub async fn complete(mut self) {
        while let Some(frame_result) = self.body_stream.next().await {
            let converted = frame_result.map_err(|e| e.into());
            let Some(injector) = &mut self.injector else {
                break;
            };
            // If sending fails, it means the receiver has been dropped
            if injector.send(converted).await.is_err() {
                break;
            }
        }
    }

    /// Consumes the entire original body by applying a transformation function
    /// to each frame before injecting it into the MutableBody.
    ///
    pub async fn complete_with<F>(mut self, mut transform: F)
    where
        F: FnMut(Frame<Bytes>) -> Frame<Bytes>,
    {
        while let Some(frame_result) = self.body_stream.next().await {
            let Some(injector) = &mut self.injector else {
                break;
            };
            let transformed = frame_result.map(&mut transform).map_err(|e| e.into());
            if injector.send(transformed).await.is_err() {
                break;
            }
        }
    }

    /// Gets the next frame from the original body stream.
    ///
    /// Returns None when the body is completely consumed.
    ///
    pub async fn next_frame(&mut self) -> Option<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>> {
        self.body_stream.as_mut().next().await.map(|result| result.map_err(|e| e.into()))
    }

    /// Injects a frame into the MutableBody.
    ///
    /// Returns an error if the receiver has been dropped (i.e., the MutableBody
    /// has been consumed or dropped).
    ///
    pub async fn inject_frame(
        &mut self,
        frame: Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), mpsc::error::SendError<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>> {
        let Some(injector) = &mut self.injector else {
            return Err(mpsc::error::SendError(frame));
        };
        injector.send(frame).await
    }

    /// Observes the next frame and automatically injects it into the MutableBody.
    ///
    /// Returns a copy of the frame to allow observation, None when the body is completely consumed.
    ///
    pub async fn observe_and_inject(&mut self) -> Option<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>> {
        let frame = self.body_stream.as_mut().next().await?.map_err(|e| e.into());

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
            }
            Err(e) => Err(e.to_string().into()),
        };

        // Inject the original frame
        if let Some(injector) = &mut self.injector {
            let _ = injector.send(frame).await;
        };

        Some(cloned)
    }
}

impl<B> Stream for FrameObserver<B>
where
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Item = Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.body_stream.as_mut().poll_next(cx).map(|opt| {
            opt.map(|result| result.map_err(|e| e.into()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    use http_body_util::Full;

    #[tokio::test]
    async fn test_complete() {
        let body = Full::new(Bytes::from("Hello, World!"));
        let (mut mutable_body, observer) = MutableBody::new(body);

        // Spawn observer task
        let observer_handle = tokio::spawn(async move {
            observer.complete().await;
        });

        // Consume the mutable body
        let frame = future::poll_fn(|cx| Pin::new(&mut mutable_body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();

        if let Some(data) = frame.into_data().ok() {
            assert_eq!(data, Bytes::from("Hello, World!"));
        } else {
            panic!("Expected data frame");
        }

        observer_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_manual_injection() {
        let body = Full::new(Bytes::from("Test"));
        let (mut mutable_body, mut observer) = MutableBody::new(body);

        // Spawn a task that consumes the MutableBody
        let consumer_handle = tokio::spawn(async move {
            let frame = future::poll_fn(|cx| Pin::new(&mut mutable_body).poll_frame(cx))
                .await
                .unwrap()
                .unwrap();

            if let Ok(data) = frame.into_data() {
                data
            } else {
                panic!("Expected data frame");
            }
        });

        // Manually observe and inject the frame
        if let Some(frame) = observer.next_frame().await {
            observer.inject_frame(frame).await.unwrap();
        }

        // Complete the observer
        drop(observer);

        // Verify that the consumer received the data
        let result = consumer_handle.await.unwrap();
        assert_eq!(result, Bytes::from("Test"));
    }
}
