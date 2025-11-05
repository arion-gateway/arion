#[derive(Debug, Clone, Default)]
pub struct Observability {}

#[derive(Debug, Copy, Clone, Default)]
pub struct Processing {}

pub trait Mode {
    fn is_observability(&self) -> bool;
}

impl Mode for Observability {
    #[inline]
    fn is_observability(&self) -> bool {
        true
    }
}

impl Mode for Processing {
    #[inline]
    fn is_observability(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Request;
#[derive(Debug, Clone, Copy, Default)]
pub struct Response;

pub trait Message {}
impl Message for Request {}
impl Message for Response {}
