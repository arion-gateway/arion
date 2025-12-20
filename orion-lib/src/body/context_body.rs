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
