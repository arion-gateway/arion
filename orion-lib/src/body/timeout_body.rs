// Copyright 2025 The kmesh Authors
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

/// Middleware that applies a timeout to request and response bodies.
///
/// Wrapper around a [`http_body::Body`] to time out if data is not ready within the specified duration.
///
/// Bodies must produce data at most within the specified timeout.
/// If the body does not produce a requested data frame within the timeout period, it will return an error.
///
/// This `TimeoutBody` variant differs from `tower_http::timeout::TimeoutBody` in two ways:
/// 1. Unpin: The original `TimeoutBody` is !Unpin, while this version is Unpin to enable use in certain asynchronous contexts.
/// 2. Optional Timeout: The timeout is wrapped in `Option`, allowing for cases where a timeout may not be necessary.
///
use super::h1_permit::Http1Permit;
use http_body::{Body, SizeHint};
use pin_project::pin_project;
use pingora_timeout::{
    fast_timeout::{fast_timeout, FastTimeout},
    Timeout as PingoraTimeout,
};
use std::any::type_name;
use std::{
    future::{pending, Future, Pending},
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

pub type Timeout = PingoraTimeout<Pending<()>, FastTimeout>;

/// Dropping the hook without `notify_complete` treats the body as aborted.
struct BodyEndHook {
    inner: Option<Http1Permit>,
}

impl BodyEndHook {
    fn none() -> Self {
        Self { inner: None }
    }

    fn set(&mut self, hook: Http1Permit) {
        self.inner = Some(hook);
    }

    fn notify_complete(&mut self) {
        if let Some(hook) = self.inner.take() {
            hook.on_body_end(true);
        }
    }

    fn notify_abort(&mut self) {
        if let Some(hook) = self.inner.take() {
            hook.on_body_end(false);
        }
    }
}

impl Drop for BodyEndHook {
    fn drop(&mut self) {
        self.notify_abort();
    }
}

#[pin_project]
pub struct TimeoutBody<B> {
    #[pin]
    pub inner: B,
    pub timeout: Option<Duration>,
    #[pin]
    pub sleep: Option<Pin<Box<Timeout>>>,
    on_end: BodyEndHook,
}

impl<B> std::fmt::Debug for TimeoutBody<B>
where
    B: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(type_name::<TimeoutBody<B>>())
            .field("timeout", &self.timeout)
            .field("body", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<B> Default for TimeoutBody<B>
where
    B: Default,
{
    fn default() -> Self {
        Self { inner: Default::default(), timeout: None, sleep: None, on_end: BodyEndHook::none() }
    }
}

impl<B> TimeoutBody<B> {
    /// Creates a new [`TimeoutBody`].
    pub fn new(timeout: Option<Duration>, body: B) -> Self {
        TimeoutBody { inner: body, timeout, sleep: None, on_end: BodyEndHook::none() }
    }

    #[must_use]
    pub fn with_on_end(mut self, hook: Http1Permit) -> Self {
        self.on_end.set(hook);
        self
    }

    pub fn map_into<B2>(self) -> TimeoutBody<B2>
    where
        B: Into<B2>,
    {
        self.map_inner(Into::into)
    }

    pub fn map_inner<B2, F>(self, f: F) -> TimeoutBody<B2>
    where
        F: FnOnce(B) -> B2,
    {
        TimeoutBody { inner: f(self.inner), timeout: self.timeout, sleep: self.sleep, on_end: self.on_end }
    }
}

impl<B> Body for TimeoutBody<B>
where
    B: Body,
    B::Error: std::error::Error,
{
    type Data = B::Data;
    type Error = TimeoutBodyError<B::Error>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        if let Some(timeout) = this.timeout {
            // Start the `Sleep` if not active.
            let sleep_pinned = if let Some(some) = this.sleep.as_mut().as_pin_mut() {
                some
            } else {
                Pin::new(this.sleep.insert(Box::pin(fast_timeout(*timeout, pending()))))
            };

            // Error if the timeout has expired.
            if sleep_pinned.poll(cx).is_ready() {
                this.on_end.notify_abort();
                return Poll::Ready(Some(Err(TimeoutBodyError::TimedOut)));
            }

            // Check for body data.
            let frame = ready!(this.inner.poll_frame(cx));

            // A frame is ready. Reset the `Sleep`...
            this.sleep.set(None);

            match frame {
                None => {
                    this.on_end.notify_complete();
                    Poll::Ready(None)
                },
                Some(Ok(frame)) => Poll::Ready(Some(Ok(frame))),
                Some(Err(err)) => {
                    this.on_end.notify_abort();
                    Poll::Ready(Some(Err(TimeoutBodyError::BodyError(err))))
                },
            }
        } else {
            match this.inner.poll_frame(cx) {
                Poll::Ready(None) => {
                    this.on_end.notify_complete();
                    Poll::Ready(None)
                },
                Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
                Poll::Ready(Some(Err(err))) => {
                    this.on_end.notify_abort();
                    Poll::Ready(Some(Err(TimeoutBodyError::BodyError(err))))
                },
                Poll::Pending => Poll::Pending,
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Error for [`TimeoutBody`].
#[derive(thiserror::Error, Debug)]
pub enum TimeoutBodyError<E: std::error::Error> {
    #[error("data was not received within the designated timeout")]
    TimedOut,
    #[error(transparent)]
    BodyError(E),
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use http_body::Frame;
    use http_body_util::BodyExt;
    use pin_project::pin_project;
    use std::{error::Error, fmt::Display};
    use tokio::time::{sleep, Sleep};

    #[derive(Debug)]
    struct MockError;

    impl Error for MockError {}

    impl Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "mock error")
        }
    }

    #[pin_project]
    struct MockBody {
        #[pin]
        sleep: Sleep,
    }

    impl Body for MockBody {
        type Data = Bytes;
        type Error = MockError;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            let this = self.project();
            this.sleep.poll(cx).map(|()| Some(Ok(Frame::data(vec![].into()))))
        }
    }

    #[tokio::test]
    async fn test_body_available_within_timeout() {
        let mock_sleep = Duration::from_secs(1);
        let timeout_sleep = Duration::from_secs(2);

        let mock_body = MockBody { sleep: sleep(mock_sleep) };
        let body_with_timeout = TimeoutBody::new(Some(timeout_sleep), mock_body);

        body_with_timeout.boxed_unsync().frame().await.expect("no frame").unwrap();
    }

    #[tokio::test]
    async fn test_body_unavailable_within_timeout_error() {
        let mock_sleep = Duration::from_secs(2);
        let timeout_sleep = Duration::from_secs(1);

        let mock_body = MockBody { sleep: sleep(mock_sleep) };
        let body_with_timeout = TimeoutBody::new(Some(timeout_sleep), mock_body);

        body_with_timeout.boxed_unsync().frame().await.unwrap().unwrap_err();
    }
}
