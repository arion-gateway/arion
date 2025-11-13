use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use hyper::rt::{Sleep, Timer};
use pin_project::pin_project;

/// A wrapper around the future returned by `pingora_timeout::sleep`
#[pin_project]
#[derive(Debug)]
pub struct PingoraSleep<F: Future<Output = ()>> {
    #[pin]
    inner: F,
}

// PingoraSleep implements Future
impl<F> Future for PingoraSleep<F>
where
    F: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// `PingoraSleep` implements hyper Sleep
impl<F> Sleep for PingoraSleep<F> where F: Future<Output = ()> + Send + Sync {}

/// A `hyper::rt::Timer` implementation using pingora's sleep function
#[derive(Clone, Debug, Default)]
pub struct PingoraTimer;

impl Timer for PingoraTimer {
    fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn Sleep>> {
        let sleep_future = pingora_timeout::sleep(duration);
        Box::pin(PingoraSleep { inner: sleep_future })
    }

    fn sleep_until(&self, deadline: std::time::Instant) -> Pin<Box<dyn Sleep>> {
        let duration = deadline.saturating_duration_since(std::time::Instant::now());
        self.sleep(duration)
    }
}
