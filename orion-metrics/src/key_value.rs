use ahash::RandomState;

pub struct KeyValue<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

pub type KeyValueMap<'a> = std::collections::HashMap<&'a str, &'a str, RandomState>;
