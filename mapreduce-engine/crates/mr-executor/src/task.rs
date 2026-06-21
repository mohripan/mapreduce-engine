pub struct Task {
    job: Box<dyn FnOnce() + Send + 'static>,
}

impl Task {
    pub fn new<F>(job: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self { job: Box::new(job) }
    }

    pub(crate) fn run(self) {
        (self.job)();
    }
}
