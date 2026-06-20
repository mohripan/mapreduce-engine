pub type Key = String;
pub type Value = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputRecord {
    pub offset: u64,
    pub value: String,
}

impl InputRecord {
    pub fn new(offset: u64, value: impl Into<String>) -> Self {
        Self {
            offset,
            value: value.into(),
        }
    }
}