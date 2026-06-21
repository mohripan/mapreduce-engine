use crate::emitter::Emitter;
use crate::error::Result;
use crate::record::InputRecord;

pub trait Mapper: Send + Sync {
    fn map(&self, record: InputRecord, emitter: &mut dyn Emitter) -> Result<()>;
}
