use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use log::error;
use crate::record::Key;
use crate::error::{MapReduceError, Result};

pub trait Partitioner: Send + Sync {
    fn partition(&self, key: &Key, reducers: usize) -> Result<usize>;
}

#[derive(Debug, Default)]
pub struct DefaultPartitioner;

impl Partitioner for DefaultPartitioner {
    fn partition(&self, key: &Key, reducers: usize) -> Result<usize> {
        if reducers == 0 {
            return Err(MapReduceError::InvalidJobConfig(
                "number of reducers must be greater than zero".to_string()
            ));
        }

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);

        Ok((hasher.finish() as usize) % reducers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_is_within_reducer_range() {
        let partitioner = DefaultPartitioner;

        let partition = partitioner.partition(&"hello".to_string(), 4).unwrap();

        assert!(partition < 4);
    }

    #[test]
    fn partition_rejects_zero_reducers() {
        let partitioner = DefaultPartitioner;

        let result = partitioner.partition(&"hello".to_string(), 0);

        assert!(result.is_err());
    }
}