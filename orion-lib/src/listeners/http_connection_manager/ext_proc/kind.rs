#[derive(Debug, Clone, Default)]
pub struct Observability {}

#[derive(Debug, Copy, Clone, Default)]
pub struct Processing {}

pub trait Mode {
    const OBSERVABILITY : bool;
}

impl Mode for Observability {
    const OBSERVABILITY : bool = true;
}

impl Mode for Processing {
    const OBSERVABILITY : bool = false;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Request;
#[derive(Debug, Clone, Copy, Default)]
pub struct Response;

pub trait Message {
    const IS_REQUEST: bool;
    const IS_RESPONSE: bool;
}

impl Message for Request {
    const IS_REQUEST: bool = true;
    const IS_RESPONSE: bool = false;
}

impl Message for Response {
    const IS_REQUEST: bool = false;
    const IS_RESPONSE: bool = true;
}
