use mr_core::Result;

use crate::Task;

pub trait Executor: Send + Sync {
    fn submit(&self, task: Task) -> Result<()>;
}
