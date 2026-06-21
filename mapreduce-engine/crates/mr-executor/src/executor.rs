use mr_core::Result;

use crate::{Task, TaskHandle};

pub trait Executor: Send + Sync {
    fn submit(&self, task: Task) -> Result<TaskHandle>;
}
