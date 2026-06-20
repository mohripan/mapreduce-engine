use crate::emitter::Emitter;
use crate::record::InputRecord;
use crate::error::Result;

pub trait Mapper: Send + Sync {
    fn map(&self, record: InputRecord, emitter: &mut dyn Emitter) -> Result<()>;
}