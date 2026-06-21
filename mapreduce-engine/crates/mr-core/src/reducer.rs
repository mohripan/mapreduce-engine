use crate::emitter::{Emitter};
use crate::error::Result;
use crate::record::{Key, Value};

pub trait Reducer: Send + Sync {
    fn reduce(&self, key: Key, values: Vec<Value>, emitter: &mut dyn Emitter) -> Result<()>;
}
