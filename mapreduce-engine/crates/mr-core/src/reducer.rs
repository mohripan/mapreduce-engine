use crate::emitter::{Emitter, VecEmitter};
use crate::record::{Key, Value};
use crate::error::Result;

pub trait Reducer: Send + Sync {
    fn reduce(
        &self,
        key: Key,
        values: Vec<Value>,
        emitter: &mut dyn Emitter,
    ) -> Result<()>;
}