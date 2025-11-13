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

#[derive(Debug, Clone, Default)]
pub struct RequestMsg {}
#[derive(Debug, Clone, Default)]
pub struct ResponseMsg {}

pub trait MsgType {
    const IS_REQUEST: bool;
    #[allow(dead_code)]
    const IS_RESPONSE: bool;
}

impl MsgType for RequestMsg {
    const IS_REQUEST: bool = true;
    const IS_RESPONSE: bool = false;
}

impl MsgType for ResponseMsg {
    const IS_REQUEST: bool = false;
    const IS_RESPONSE: bool = true;
}
