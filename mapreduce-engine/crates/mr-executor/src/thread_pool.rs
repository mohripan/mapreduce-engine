use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use mr_core::{MapReduceError, Result};

use crate::{Executor, Task};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Mutex<Option<mpsc::Sender<Task>>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> Result<Self> {
        if size == 0 {
            return Err(MapReduceError::Executor(
                "thread pool size must be greater than zero".to_string(),
            ));
        }

        let (sender, receiver) = mpsc::channel::<Task>();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        Ok(Self {
            workers,
            sender: Mutex::new(Some(sender)),
        })
    }

    pub fn shutdown(&mut self) -> Result<()> {
        {
            let mut sender_guard = self.sender.lock().map_err(|_| {
                MapReduceError::Executor("thread pool sender lock was poisoned".to_string())
            })?;

            // Dropping the sender closes the channel.
            // Workers finish any already-queued tasks, then exit.
            sender_guard.take();
        }

        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                handle.join().map_err(|_| {
                    MapReduceError::Executor(format!(
                        "worker {} panicked while shutting down",
                        worker.id
                    ))
                })?;
            }
        }

        Ok(())
    }
}

impl Executor for ThreadPool {
    fn submit(&self, task: Task) -> Result<()> {
        let sender_guard = self.sender.lock().map_err(|_| {
            MapReduceError::Executor("thread pool sender lock was poisoned".to_string())
        })?;

        let sender = sender_guard.as_ref().ok_or_else(|| {
            MapReduceError::Executor(
                "cannot submit task because thread pool is shut down".to_string(),
            )
        })?;

        sender.send(task).map_err(|_| {
            MapReduceError::Executor(
                "cannot submit task because all workers have stopped".to_string(),
            )
        })
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

struct Worker {
    id: usize,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Task>>>) -> Self {
        let handle = thread::spawn(move || {
            loop {
                let task_result = {
                    let receiver = match receiver.lock() {
                        Ok(receiver) => receiver,
                        Err(_) => {
                            // If the receiver mutex is poisoned, something went very wrong.
                            // The safest things this worker can do is stop.
                            break;
                        }
                    };

                    receiver.recv()
                };

                match task_result {
                    Ok(task) => {
                        task.run();
                    }
                    Err(_) => {
                        // Channel closed. Time to stop this worker.
                        break;
                    }
                }
            }
        });

        Self {
            id,
            handle: Some(handle),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[test]
    fn rejects_zero_workers() {
        let result = ThreadPool::new(0);

        assert!(result.is_err());
    }

    #[test]
    fn reports_pool_size() {
        let pool = ThreadPool::new(4).unwrap();

        assert_eq!(pool.workers.len(), 4);
    }

    #[test]
    fn executes_submitted_tasks() {
        let counter = Arc::new(AtomicUsize::new(0));

        {
            let pool = ThreadPool::new(4).unwrap();

            for _ in 0..100 {
                let counter = Arc::clone(&counter);

                pool.submit(Task::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                }))
                .unwrap();
            }
        }

        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }

    #[test]
    fn shutdown_waits_for_submitted_tasks() {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut pool = ThreadPool::new(4).unwrap();

        for _ in 0..50 {
            let counter = Arc::clone(&counter);

            pool.submit(Task::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }))
            .unwrap();
        }

        pool.shutdown().unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 50);
    }

    #[test]
    fn cannot_submit_after_shutdown() {
        let mut pool = ThreadPool::new(4).unwrap();

        pool.shutdown().unwrap();

        let result = pool.submit(Task::new(|| {}));

        assert!(result.is_err());
    }
}
