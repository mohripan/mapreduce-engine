use crate::{Emitter, Key, Result, Value};

pub trait Reducer: Send + Sync {
    fn reduce(&self, key: Key, values: Vec<Value>, emitter: &mut dyn Emitter) -> Result<()>;
}
