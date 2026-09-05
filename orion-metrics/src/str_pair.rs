use ahash::RandomState;

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct StrPair<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

pub type StrMap<'a> = std::collections::HashMap<&'a str, &'a str, RandomState>;
