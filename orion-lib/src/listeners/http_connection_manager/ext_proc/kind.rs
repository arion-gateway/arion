#[derive(Debug, Clone, Default)]
pub struct Observability {}

#[derive(Debug, Copy, Clone, Default)]
pub struct Processing {}

pub trait Mode {
    const OBSERVABILITY: bool;
}

impl Mode for Observability {
    const OBSERVABILITY: bool = true;
}

impl Mode for Processing {
    const OBSERVABILITY: bool = false;
}

pub type Request = http::Request<()>;
pub type Response = http::Response<()>;

pub trait Phase {
    const IS_REQUEST: bool;
    #[allow(dead_code)]
    const IS_RESPONSE: bool;
}

impl Phase for Request {
    const IS_REQUEST: bool = true;
    const IS_RESPONSE: bool = false;
}

impl Phase for Response {
    const IS_REQUEST: bool = false;
    const IS_RESPONSE: bool = true;
}
