use std::collections::BTreeMap;
use std::sync::mpsc;

use mr_core::{InputRecord, Job, Key, MapReduceError, Result, Value, VecEmitter};

use mr_executor::{Executor, Task, ThreadPool};

pub struct InMemoryEngine;

impl InMemoryEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn run_records(&self, job: &Job, records: Vec<InputRecord>) -> Result<Vec<(Key, Value)>> {
        let intermediate = self.run_map_phase(job, records)?;
        let shuffled = self.run_shuffle_phase(job, intermediate)?;
        let mut outputs = self.run_reduce_phase(job, shuffled)?;

        outputs.sort_by(|left, right| left.0.cmp(&right.0));

        Ok(outputs)
    }

    fn run_map_phase(&self, job: &Job, records: Vec<InputRecord>) -> Result<Vec<(Key, Value)>> {
        let mut pool = ThreadPool::new(job.num_map_workers())?;
        let mapper = job.mapper();

        let (result_sender, result_receiver) = mpsc::channel::<Result<Vec<(Key, Value)>>>();

        for record in records {
            let mapper = mapper.clone();
            let result_sender = result_sender.clone();

            pool.submit(Task::new(move || {
                let mut emitter = VecEmitter::new();

                let result = mapper
                    .map(record, &mut emitter)
                    .map(|_| emitter.into_outputs());

                let _ = result_sender.send(result);
            }))?;
        }

        drop(result_sender);

        pool.shutdown()?;

        let mut intermediate = Vec::new();

        for result in result_receiver {
            match result {
                Ok(mut outputs) => intermediate.append(&mut outputs),
                Err(error) => return Err(error),
            }
        }

        Ok(intermediate)
    }

    fn run_shuffle_phase(
        &self,
        job: &Job,
        intermediate: Vec<(Key, Value)>,
    ) -> Result<Vec<BTreeMap<Key, Vec<Value>>>> {
        let num_reducers = job.num_reducers();
        let partitioner = job.partitioner();

        let mut partitions = Vec::with_capacity(num_reducers);

        for _ in 0..num_reducers {
            partitions.push(BTreeMap::<Key, Vec<Value>>::new());
        }

        for (key, value) in intermediate {
            let partition = partitioner.partition(&key, num_reducers)?;

            let bucket = partitions.get_mut(partition).ok_or_else(|| {
                MapReduceError::Shuffle(format!(
                    "partitioner returned invalid partition {partition}"
                ))
            })?;

            bucket.entry(key).or_default().push(value);
        }

        Ok(partitions)
    }

    fn run_reduce_phase(
        &self,
        job: &Job,
        partitions: Vec<BTreeMap<Key, Vec<Value>>>,
    ) -> Result<Vec<(Key, Value)>> {
        let mut pool = ThreadPool::new(job.num_reduce_workers())?;
        let reducer = job.reducer();

        let (result_sender, result_receiver) = mpsc::channel::<Result<Vec<(Key, Value)>>>();

        for partition in partitions {
            let reducer = reducer.clone();
            let result_sender = result_sender.clone();

            pool.submit(Task::new(move || {
                let mut emitter = VecEmitter::new();

                for (key, values) in partition {
                    if let Err(error) = reducer.reduce(key, values, &mut emitter) {
                        let _ = result_sender.send(Err(error));
                        return;
                    }
                }

                let _ = result_sender.send(Ok(emitter.into_outputs()));
            }))?;
        }

        drop(result_sender);

        pool.shutdown()?;

        let mut outputs = Vec::new();

        for result in result_receiver {
            match result {
                Ok(mut partition_outputs) => {
                    outputs.append(&mut partition_outputs);
                }
                Err(error) => return Err(error),
            }
        }

        Ok(outputs)
    }
}

impl Default for InMemoryEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use mr_core::{Emitter, Mapper, Reducer};

    struct WordCountMapper;

    impl Mapper for WordCountMapper {
        fn map(&self, record: InputRecord, emitter: &mut dyn Emitter) -> Result<()> {
            for word in record.value.split_whitespace() {
                emitter.emit(word.to_lowercase(), "1".to_string());
            }

            Ok(())
        }
    }

    struct WordCountReducer;

    impl Reducer for WordCountReducer {
        fn reduce(&self, key: Key, values: Vec<Value>, emitter: &mut dyn Emitter) -> Result<()> {
            emitter.emit(key, values.len().to_string());
            Ok(())
        }
    }

    #[test]
    fn runs_word_count_job() {
        let job = Job::builder()
            .name("word-count")
            .input_path("unused-input.txt")
            .output_path("unused-output")
            .mapper(WordCountMapper)
            .reducer(WordCountReducer)
            .num_map_workers(4)
            .num_reduce_workers(2)
            .num_reducers(2)
            .build()
            .unwrap();

        let records = vec![
            InputRecord::new(0, "hello rust"),
            InputRecord::new(1, "hello mapreduce"),
            InputRecord::new(2, "rust rust"),
        ];

        let engine = InMemoryEngine::new();
        let outputs = engine.run_records(&job, records).unwrap();

        let actual = outputs.into_iter().collect::<BTreeMap<_, _>>();

        let expected = BTreeMap::from([
            ("hello".to_string(), "2".to_string()),
            ("mapreduce".to_string(), "1".to_string()),
            ("rust".to_string(), "3".to_string()),
        ]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn handles_empty_input() {
        let job = Job::builder()
            .name("empty-word-count")
            .input_path("unused-input.txt")
            .output_path("unused-output")
            .mapper(WordCountMapper)
            .reducer(WordCountReducer)
            .num_map_workers(2)
            .num_reduce_workers(2)
            .num_reducers(2)
            .build()
            .unwrap();

        let engine = InMemoryEngine::new();
        let outputs = engine.run_records(&job, Vec::new()).unwrap();

        assert!(outputs.is_empty());
    }
}
