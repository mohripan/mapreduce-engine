use mr_core::{MapReduceError, Result};
use std::sync::mpsc;
use std::sync::mpsc::Receiver;

pub struct Task {
    job: Box<dyn FnOnce() -> Result<()> + Send + 'static>,
}

impl Task {
    pub fn new<F>(job: F) -> Self
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        Self { job: Box::new(job) }
    }

    pub(crate) fn run(self) -> Result<()> {
        (self.job)()
    }
}

pub struct TaskHandle {
    receiver: mpsc::Receiver<Result<()>>,
}

impl TaskHandle {
    pub(crate) fn new(receiver: mpsc::Receiver<Result<()>>) -> Self {
        Self { receiver }
    }

    pub fn wait(self) -> Result<()> {
        self.receiver.recv().map_err(|_| {
            MapReduceError::Executor("task completed without reporting a result".to_string())
        })?
    }
}
