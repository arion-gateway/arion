pub type PseudoHeaderName = &'static str;

pub const METHOD: PseudoHeaderName = ":method";
pub const SCHEME: PseudoHeaderName = ":scheme";
pub const AUTHORITY: PseudoHeaderName = ":authority";
pub const PATH: PseudoHeaderName = ":path";
pub const STATUS: PseudoHeaderName = ":status";
