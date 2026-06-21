use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{
    DefaultPartitioner, MapReduceError, Mapper, Partitioner, Reducer, Result,
};

pub struct Job {
    name: String,
    input_paths: Vec<PathBuf>,
    output_path: PathBuf,
    mapper: Arc<dyn Mapper>,
    reducer: Arc<dyn Reducer>,
    partitioner: Arc<dyn Partitioner>,
    num_map_workers: usize,
    num_reduce_workers: usize,
    num_reducers: usize,
}

impl Job {
    pub fn builder() -> JobBuilder {
        JobBuilder::default()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn input_paths(&self) -> &[PathBuf] {
        &self.input_paths
    }

    pub fn output_path(&self) -> &Path {
        &self.output_path
    }

    pub fn mapper(&self) -> Arc<dyn Mapper> {
        Arc::clone(&self.mapper)
    }

    pub fn reducer(&self) -> Arc<dyn Reducer> {
        Arc::clone(&self.reducer)
    }

    pub fn partitioner(&self) -> Arc<dyn Partitioner> {
        Arc::clone(&self.partitioner)
    }

    pub fn num_map_workers(&self) -> usize {
        self.num_map_workers
    }

    pub fn num_reduce_workers(&self) -> usize {
        self.num_reduce_workers
    }

    pub fn num_reducers(&self) -> usize {
        self.num_reducers
    }
}

pub struct JobBuilder {
    name: Option<String>,
    input_paths: Vec<PathBuf>,
    output_path: Option<PathBuf>,
    mapper: Option<Arc<dyn Mapper>>,
    reducer: Option<Arc<dyn Reducer>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    num_map_workers: usize,
    num_reduce_workers: usize,
    num_reducers: usize,
}

impl Default for JobBuilder {
    fn default() -> Self {
        Self {
            name: None,
            input_paths: Vec::new(),
            output_path: None,
            mapper: None,
            reducer: None,
            partitioner: None,
            num_map_workers: 1,
            num_reduce_workers: 1,
            num_reducers: 1,
        }
    }
}

impl JobBuilder {
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn input_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.input_paths.push(path.into());
        self
    }

    pub fn input_paths<I, P>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.input_paths.extend(paths.into_iter().map(Into::into));
        self
    }

    pub fn output_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.output_path = Some(path.into());
        self
    }

    pub fn mapper<M>(mut self, mapper: M) -> Self
    where
        M: Mapper + 'static,
    {
        self.mapper = Some(Arc::new(mapper));
        self
    }

    pub fn mapper_arc(mut self, mapper: Arc<dyn Mapper>) -> Self {
        self.mapper = Some(mapper);
        self
    }

    pub fn reducer<R>(mut self, reducer: R) -> Self
    where
        R: Reducer + 'static,
    {
        self.reducer = Some(Arc::new(reducer));
        self
    }

    pub fn reducer_arc(mut self, reducer: Arc<dyn Reducer>) -> Self {
        self.reducer = Some(reducer);
        self
    }

    pub fn partitioner<P>(mut self, partitioner: P) -> Self
    where
        P: Partitioner + 'static,
    {
        self.partitioner = Some(Arc::new(partitioner));
        self
    }

    pub fn partitioner_arc(mut self, partitioner: Arc<dyn Partitioner>) -> Self {
        self.partitioner = Some(partitioner);
        self
    }

    pub fn num_map_workers(mut self, workers: usize) -> Self {
        self.num_map_workers = workers;
        self
    }

    pub fn num_reduce_workers(mut self, workers: usize) -> Self {
        self.num_reduce_workers = workers;
        self
    }

    pub fn num_reducers(mut self, workers: usize) -> Self {
        self.num_reducers = workers;
        self
    }

    pub fn build(self) -> Result<Job> {
        let name = self
            .name
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| {
                MapReduceError::InvalidJobConfig("job name must not be empty".to_string())
            })?;

        if self.input_paths.is_empty() {
            return Err(MapReduceError::InvalidJobConfig(
                "at least one input is required".to_string(),
            ));
        }

        let output_path = self.output_path.ok_or_else(|| {
            MapReduceError::InvalidJobConfig("output path is required".to_string())
        })?;

        let mapper = self
            .mapper
            .ok_or_else(|| MapReduceError::InvalidJobConfig("mapper is required".to_string()))?;

        let reducer = self
            .reducer
            .ok_or_else(|| MapReduceError::InvalidJobConfig("reducer is required".to_string()))?;

        if self.num_map_workers == 0 {
            return Err(MapReduceError::InvalidJobConfig(
                "number of map workers must be greater than zero".to_string(),
            ));
        }

        if self.num_reduce_workers == 0 {
            return Err(MapReduceError::InvalidJobConfig(
                "number of reduce workers must be greater than zero".to_string(),
            ));
        }

        if self.num_reducers == 0 {
            return Err(MapReduceError::InvalidJobConfig(
                "number of reducers must be greater than zero".to_string(),
            ));
        }

        let partitioner = self
            .partitioner
            .unwrap_or_else(|| Arc::new(DefaultPartitioner) as Arc<dyn Partitioner>);

        Ok(Job {
            name,
            input_paths: self.input_paths,
            output_path,
            mapper,
            reducer,
            partitioner,
            num_map_workers: self.num_map_workers,
            num_reduce_workers: self.num_reduce_workers,
            num_reducers: self.num_reducers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emitter::Emitter;
    use crate::record::{InputRecord, Key, Value};

    struct TestMapper;

    impl Mapper for TestMapper {
        fn map(&self, _record: InputRecord, _emitter: &mut dyn Emitter) -> Result<()> {
            Ok(())
        }
    }

    struct TestReducer;

    impl Reducer for TestReducer {
        fn reduce(&self, _key: Key, _values: Vec<Value>, _emitter: &mut dyn Emitter) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn valid_job_can_be_built() {
        let job = Job::builder()
            .name("test-job")
            .input_path("input.txt")
            .output_path("output")
            .mapper(TestMapper)
            .reducer(TestReducer)
            .num_map_workers(2)
            .num_reduce_workers(2)
            .num_reducers(2)
            .build()
            .unwrap();

        assert_eq!(job.name(), "test-job");
        assert_eq!(job.num_map_workers(), 2);
        assert_eq!(job.num_reduce_workers(), 2);
        assert_eq!(job.num_reducers(), 2);
    }

    #[test]
    fn job_requires_mapper() {
        let result = Job::builder()
            .name("test-job")
            .input_path("input.txt")
            .output_path("output")
            .reducer(TestReducer)
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn job_requires_reducer() {
        let result = Job::builder()
            .name("test-job")
            .input_path("input.txt")
            .output_path("output")
            .mapper(TestMapper)
            .build();

        assert!(result.is_err());
    }
}
