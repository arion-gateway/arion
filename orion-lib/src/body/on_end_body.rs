// Copyright 2025 The kmesh Authors
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

use crate::transport::http1_pool::Http1Permit;
use crate::transport::http2_pool::Http2StreamPermit;
use http_body::{Body, SizeHint};
use pin_project::pin_project;
use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
};

/// Recycles an HTTP/1 connection or releases an HTTP/2 stream slot when the body ends.
pub enum BodyEndPermit {
    Http1(Http1Permit),
    Http2(Http2StreamPermit),
}

impl BodyEndPermit {
    fn on_body_end(self, completed: bool) {
        match self {
            Self::Http1(permit) => permit.on_body_end(completed),
            Self::Http2(permit) => permit.on_body_end(completed),
        }
    }
}

/// Dropping the hook without `notify_complete` treats the body as aborted.
struct BodyEndHook {
    inner: Option<Box<BodyEndPermit>>,
}

impl BodyEndHook {
    fn none() -> Self {
        Self { inner: None }
    }

    fn set(&mut self, hook: BodyEndPermit) {
        self.inner = Some(Box::new(hook));
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

/// Wraps a response body and runs a pool permit when the stream ends or is dropped.
#[pin_project]
pub struct OnEndBody<B> {
    #[pin]
    body: B,
    on_end: BodyEndHook,
}

impl<B> std::fmt::Debug for OnEndBody<B>
where
    B: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnEndBody").field("body", &self.body).finish_non_exhaustive()
    }
}

impl<B> Default for OnEndBody<B>
where
    B: Default,
{
    fn default() -> Self {
        Self::new(B::default())
    }
}

impl<B> OnEndBody<B> {
    #[inline]
    pub fn new(inner: B) -> Self {
        Self { body: inner, on_end: BodyEndHook::none() }
    }

    #[must_use]
    pub fn with_on_end(mut self, hook: BodyEndPermit) -> Self {
        self.on_end.set(hook);
        self
    }

    pub fn map_into<B2>(self) -> OnEndBody<B2>
    where
        B: Into<B2>,
    {
        self.map_inner(Into::into)
    }

    pub fn map_inner<B2, F>(self, f: F) -> OnEndBody<B2>
    where
        F: FnOnce(B) -> B2,
    {
        OnEndBody { body: f(self.body), on_end: self.on_end }
    }
}

impl<B> From<B> for OnEndBody<B> {
    #[inline]
    fn from(inner: B) -> Self {
        Self::new(inner)
    }
}

impl<B> Deref for OnEndBody<B> {
    type Target = B;

    #[inline]
    fn deref(&self) -> &B {
        &self.body
    }
}

impl<B> DerefMut for OnEndBody<B> {
    #[inline]
    fn deref_mut(&mut self) -> &mut B {
        &mut self.body
    }
}

impl<B> Body for OnEndBody<B>
where
    B: Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        if this.on_end.inner.is_none() {
            return this.body.poll_frame(cx);
        }
        match this.body.poll_frame(cx) {
            Poll::Ready(None) => {
                this.on_end.notify_complete();
                Poll::Ready(None)
            },
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(Some(Err(err))) => {
                this.on_end.notify_abort();
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
