use bytes::Bytes;
use futures::{Sink, Stream};
use http_body::{Body, Frame};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{PollSendError, PollSender};

use thiserror::Error;

type FrameResult = Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>;

/// A Body that allows streaming bytes into its body.
pub struct SseBody {
    stream: ReceiverStream<FrameResult>,
}

impl SseBody {
    /// Creates a new `SseBody` wrapping an existing body.
    ///
    /// Returns a tuple of (`SseBody`, `SseSender`). `SseSender` must be used
    /// to inject frames into.
    pub fn new() -> (Self, SseSender) {
        // Create a channel for injecting frames
        let (tx, rx) = mpsc::channel(16);

        // Convert the receiver into a StreamBody
        let stream_of_body = ReceiverStream::new(rx);

        // Create the bridge linked to the body
        (SseBody { stream: stream_of_body }, SseSender::new(tx))
    }
}

impl std::fmt::Debug for SseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseBody").field("stream", &self.stream).finish()
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

/// The `SseSender` acts as a bridge to the `SseBody`.
/// Frames written from the original body must be injected into the `SseBody` for it
/// to produce any output.
#[pin_project]
#[derive(Default)]
pub struct SseSender {
    #[pin]
    injector: Option<PollSender<FrameResult>>,
}

impl std::fmt::Debug for SseSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseSender").finish()
    }
}

impl SseSender {
    fn new(injector: mpsc::Sender<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>) -> Self {
        Self { injector: Some(PollSender::new(injector)) }
    }

    /// Close the `SseSender` to prevent further frame injections.
    pub fn close(&mut self) {
        self.injector.take();
    }
}

type PollSenderError = PollSendError<std::result::Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>;

#[derive(Debug, Error)]
pub enum SseSenderError {
    #[error("PollSendError: {0}")]
    PollSendError(#[from] PollSenderError),
    #[error("SenderClosed")]
    SenderClosed,
}

impl Sink<Bytes> for SseSender {
    type Error = SseSenderError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        match this.injector.as_pin_mut() {
            Some(injector) => injector.poll_ready(cx).map_err(Into::into),
            None => Poll::Ready(Err(SseSenderError::SenderClosed)),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let this = self.project();
        match this.injector.as_pin_mut() {
            Some(injector) => injector.start_send(Ok(Frame::data(item))).map_err(Into::into),
            None => Err(SseSenderError::SenderClosed),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        match this.injector.as_pin_mut() {
            Some(injector) => injector.poll_flush(cx).map_err(Into::into),
            None => Poll::Ready(Err(SseSenderError::SenderClosed)),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        match this.injector.as_pin_mut() {
            Some(injector) => injector.poll_close(cx).map_err(Into::into),
            None => Poll::Ready(Err(SseSenderError::SenderClosed)),
        }
    }
}
