// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

use http_body::{Body, SizeHint};
use pin_project::pin_project;
use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
};

/// Action run once when the wrapped body ends.
///
/// `completed` is `true` when the body reached its end, `false` when it failed or was dropped
/// before the end.
pub trait OnBodyEnd {
    fn on_body_end(self, completed: bool);
}

/// No action.
impl OnBodyEnd for () {
    #[inline]
    fn on_body_end(self, _completed: bool) {}
}

/// Holds the hook inline and runs it at most once. Dropping it without [`Self::complete`]
/// treats the body as aborted.
struct HookSlot<H: OnBodyEnd>(Option<H>);

impl<H: OnBodyEnd> HookSlot<H> {
    #[inline]
    fn complete(&mut self) {
        if let Some(hook) = self.0.take() {
            hook.on_body_end(true);
        }
    }

    #[inline]
    fn abort(&mut self) {
        if let Some(hook) = self.0.take() {
            hook.on_body_end(false);
        }
    }
}

impl<H: OnBodyEnd> Drop for HookSlot<H> {
    fn drop(&mut self) {
        self.abort();
    }
}

/// Wraps a body and runs an [`OnBodyEnd`] hook when the stream ends, fails or is dropped.
#[pin_project]
pub struct OnEndBody<B, H: OnBodyEnd> {
    #[pin]
    body: B,
    hook: HookSlot<H>,
}

impl<B, H> std::fmt::Debug for OnEndBody<B, H>
where
    B: std::fmt::Debug,
    H: OnBodyEnd,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnEndBody").field("body", &self.body).finish_non_exhaustive()
    }
}

impl<B, H> Default for OnEndBody<B, H>
where
    B: Default,
    H: OnBodyEnd,
{
    fn default() -> Self {
        Self::new(B::default())
    }
}

impl<B, H: OnBodyEnd> OnEndBody<B, H> {
    #[inline]
    pub fn new(inner: B) -> Self {
        Self { body: inner, hook: HookSlot(None) }
    }

    /// Attaches the hook. When the body is already at end of stream (empty bodies are often
    /// never polled) the hook runs right away.
    #[must_use]
    pub fn with_on_end(mut self, hook: H) -> Self
    where
        B: Body,
    {
        self.hook.0 = Some(hook);
        if self.body.is_end_stream() {
            self.hook.complete();
        }
        self
    }

    pub fn map_into<B2>(self) -> OnEndBody<B2, H>
    where
        B: Into<B2>,
    {
        self.map_inner(Into::into)
    }

    pub fn map_inner<B2, F>(self, f: F) -> OnEndBody<B2, H>
    where
        F: FnOnce(B) -> B2,
    {
        OnEndBody { body: f(self.body), hook: self.hook }
    }
}

impl<B, H: OnBodyEnd> From<B> for OnEndBody<B, H> {
    #[inline]
    fn from(inner: B) -> Self {
        Self::new(inner)
    }
}

impl<B, H: OnBodyEnd> Deref for OnEndBody<B, H> {
    type Target = B;

    #[inline]
    fn deref(&self) -> &B {
        &self.body
    }
}

impl<B, H: OnBodyEnd> DerefMut for OnEndBody<B, H> {
    #[inline]
    fn deref_mut(&mut self) -> &mut B {
        &mut self.body
    }
}

impl<B, H> Body for OnEndBody<B, H>
where
    B: Body,
    H: OnBodyEnd,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        if this.hook.0.is_none() {
            return this.body.poll_frame(cx);
        }
        match this.body.as_mut().poll_frame(cx) {
            Poll::Ready(None) => {
                this.hook.complete();
                Poll::Ready(None)
            },
            Poll::Ready(Some(Ok(frame))) => {
                // Consumers often stop after the last data frame when `is_end_stream` is true
                // and never poll `None`. Run the hook now or Drop would treat the body as aborted.
                if this.body.as_ref().is_end_stream() {
                    this.hook.complete();
                }
                Poll::Ready(Some(Ok(frame)))
            },
            Poll::Ready(Some(Err(err))) => {
                this.hook.abort();
                Poll::Ready(Some(Err(err)))
            },
            Poll::Pending => Poll::Pending,
        }
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::{BodyExt, Empty, Full};
    use std::{cell::Cell, rc::Rc};

    struct Record(Rc<Cell<Option<bool>>>);

    impl OnBodyEnd for Record {
        fn on_body_end(self, completed: bool) {
            assert!(self.0.replace(Some(completed)).is_none(), "hook must run once");
        }
    }

    fn recorded() -> (Record, Rc<Cell<Option<bool>>>) {
        let cell = Rc::new(Cell::new(None));
        (Record(Rc::clone(&cell)), cell)
    }

    #[tokio::test]
    async fn hook_reports_completion_when_body_is_consumed() {
        let (hook, seen) = recorded();
        let body = OnEndBody::new(Full::new(Bytes::from_static(b"data"))).with_on_end(hook);
        body.collect().await.unwrap();
        assert_eq!(seen.get(), Some(true));
    }

    #[test]
    fn hook_reports_completion_immediately_for_empty_body() {
        let (hook, seen) = recorded();
        let body = OnEndBody::new(Empty::<Bytes>::new()).with_on_end(hook);
        assert_eq!(seen.get(), Some(true));
        drop(body);
        assert_eq!(seen.get(), Some(true));
    }

    #[test]
    fn hook_reports_abort_when_body_is_dropped_unread() {
        let (hook, seen) = recorded();
        drop(OnEndBody::new(Full::new(Bytes::from_static(b"data"))).with_on_end(hook));
        assert_eq!(seen.get(), Some(false));
    }

    #[test]
    fn map_inner_keeps_the_hook() {
        let (hook, seen) = recorded();
        let body = OnEndBody::new(Full::new(Bytes::from_static(b"data"))).with_on_end(hook);
        let mapped: OnEndBody<Option<()>, Record> = body.map_inner(|_| None);
        assert_eq!(seen.get(), None);
        drop(mapped);
        assert_eq!(seen.get(), Some(false));
    }
}
