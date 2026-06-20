use std::error::Error;
use std::fmt;
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, MapReduceError>;

#[derive(Debug)]
pub enum MapReduceError {
    InvalidJobConfig(String),
    Map(String),
    Reduce(String),
    Shuffle(String),
    Executor(String),
    Io {
        path: Option<PathBuf>,
        source: std::io::Error,
    },
}

