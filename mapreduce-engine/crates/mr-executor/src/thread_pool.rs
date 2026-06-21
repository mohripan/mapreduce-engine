use std::any::Any;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use mr_core::{MapReduceError, Result};

use crate::{Executor, Task, TaskHandle};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Mutex<Option<mpsc::Sender<ScheduledTask>>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> Result<Self> {
        if size == 0 {
            return Err(MapReduceError::Executor(
                "thread pool size must be greater than zero".to_string(),
            ));
        }

        let (sender, receiver) = mpsc::channel::<ScheduledTask>();
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

    pub fn size(&self) -> usize {
        self.workers.len()
    }

    pub fn shutdown(&mut self) -> Result<()> {
        {
            let mut sender_guard = self.sender.lock().map_err(|_| {
                MapReduceError::Executor("thread pool sender lock was poisoned".to_string())
            })?;

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
    fn submit(&self, task: Task) -> Result<TaskHandle> {
        let sender_guard = self.sender.lock().map_err(|_| {
            MapReduceError::Executor("thread pool sender lock was poisoned".to_string())
        })?;

        let sender = sender_guard.as_ref().ok_or_else(|| {
            MapReduceError::Executor(
                "cannot submit task because thread pool is shut down".to_string(),
            )
        })?;

        let (completion_sender, completion_receiver) = mpsc::channel();

        let scheduled_task = ScheduledTask {
            task,
            completion_sender,
        };

        sender.send(scheduled_task).map_err(|_| {
            MapReduceError::Executor(
                "cannot submit task because all workers have stopped".to_string(),
            )
        })?;

        Ok(TaskHandle::new(completion_receiver))
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

struct ScheduledTask {
    task: Task,
    completion_sender: mpsc::Sender<Result<()>>,
}

impl ScheduledTask {
    fn run(self) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.task.run()))
            .unwrap_or_else(|panic_payload| {
                Err(MapReduceError::Executor(format!(
                    "task panicked: {}",
                    panic_payload_to_string(panic_payload)
                )))
            });

        let _ = self.completion_sender.send(result);
    }
}

struct Worker {
    id: usize,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<ScheduledTask>>>) -> Self {
        let handle = thread::spawn(move || {
            loop {
                let task_result = {
                    let receiver = match receiver.lock() {
                        Ok(receiver) => receiver,
                        Err(_) => {
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

fn panic_payload_to_string(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return message.to_string();
    }

    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }

    "unknown panic payload".to_string()
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

        assert_eq!(pool.size(), 4);
    }

    #[test]
    fn executes_submitted_tasks() {
        let counter = Arc::new(AtomicUsize::new(0));
        let pool = ThreadPool::new(4).unwrap();
        let mut handles = Vec::new();

        for _ in 0..100 {
            let counter = Arc::clone(&counter);

            let handle = pool
                .submit(Task::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }))
                .unwrap();

            handles.push(handle);
        }

        for handle in handles {
            handle.wait().unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }

    #[test]
    fn shutdown_waits_for_submitted_tasks() {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut pool = ThreadPool::new(4).unwrap();
        let mut handles = Vec::new();

        for _ in 0..50 {
            let counter = Arc::clone(&counter);

            let handle = pool
                .submit(Task::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }))
                .unwrap();

            handles.push(handle);
        }

        pool.shutdown().unwrap();

        for handle in handles {
            handle.wait().unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 50);
    }

    #[test]
    fn cannot_submit_after_shutdown() {
        let mut pool = ThreadPool::new(4).unwrap();

        pool.shutdown().unwrap();

        let result = pool.submit(Task::new(|| Ok(())));

        assert!(result.is_err());
    }

    #[test]
    fn task_error_is_returned_through_handle() {
        let pool = ThreadPool::new(2).unwrap();

        let handle = pool
            .submit(Task::new(|| {
                Err(MapReduceError::Executor(
                    "intentional task failure".to_string(),
                ))
            }))
            .unwrap();

        let result = handle.wait();

        assert!(result.is_err());
    }

    #[test]
    fn task_panic_is_returned_through_handle() {
        let pool = ThreadPool::new(2).unwrap();

        let handle = pool
            .submit(Task::new(|| {
                panic!("intentional panic");
            }))
            .unwrap();

        let result = handle.wait();

        assert!(result.is_err());

        let message = result.unwrap_err().to_string();

        assert!(message.contains("intentional panic"));
    }

    #[test]
    fn worker_survives_task_panic() {
        let counter = Arc::new(AtomicUsize::new(0));
        let pool = ThreadPool::new(1).unwrap();

        let panic_handle = pool
            .submit(Task::new(|| {
                panic!("boom");
            }))
            .unwrap();

        assert!(panic_handle.wait().is_err());

        let counter_clone = Arc::clone(&counter);

        let normal_handle = pool
            .submit(Task::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        normal_handle.wait().unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
