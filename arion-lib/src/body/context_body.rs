// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use http_body::{Body, SizeHint};
use pin_project::pin_project;

#[pin_project]
pub struct ContextBody<T, B> {
    ctx: Option<Box<T>>,
    #[pin]
    inner: B,
}

#[allow(dead_code)]
impl<T, B> ContextBody<T, B> {
    #[inline]
    pub fn new(body: B) -> Self {
        ContextBody { ctx: None, inner: body }
    }

    #[inline]
    pub fn with_ctx(self, ctx: T) -> Self {
        ContextBody { ctx: Some(Box::new(ctx)), inner: self.inner }
    }

    #[inline]
    pub fn ctx(&self) -> Option<&T> {
        self.ctx.as_deref()
    }

    #[inline]
    pub fn ctx_mut(&mut self) -> Option<&mut T> {
        self.ctx.as_deref_mut()
    }

    #[inline]
    pub fn map_inner<B2, F>(self, f: F) -> ContextBody<T, B2>
    where
        F: FnOnce(B) -> B2,
    {
        ContextBody { ctx: self.ctx, inner: f(self.inner) }
    }
}

impl<T, B> Body for ContextBody<T, B>
where
    B: Body,
    B::Error: std::error::Error,
{
    type Data = B::Data;
    type Error = B::Error;

    #[inline]
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        this.inner.poll_frame(cx)
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
