use smol_str::SmolStr;

pub type PseudoHeaderName = &'static str;

pub const METHOD: PseudoHeaderName = ":method";
pub const SCHEME: PseudoHeaderName = ":scheme";
pub const AUTHORITY: PseudoHeaderName = ":authority";
pub const PATH: PseudoHeaderName = ":path";
pub const STATUS: PseudoHeaderName = ":status";

#[derive(Debug, Clone)]
pub struct CombinedHeaderMap {
    pub regular: http::HeaderMap,
    pub pseudo: Vec<(PseudoHeaderName, SmolStr)>,
}
