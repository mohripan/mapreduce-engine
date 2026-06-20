use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
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

impl MapReduceError {
    pub fn io(source: std::io::Error) -> Self {
        Self::Io {path: None, source}
    }

    pub fn io_with_path(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: Some(path.into()),
            source,
        }
    }
}

impl fmt::Display for MapReduceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobConfig(message) => {
                write!(f, "Invalid job configuration: {message}")
            }
            Self::Map(message) => {
                write!(f, "map phase failed: {message}")
            }
            Self::Reduce(message) => {
                write!(f, "reduce phase failed: {message}")
            }
            Self::Shuffle(message) => {
                write!(f, "shuffle phase failed: {message}")
            }
            Self::Executor(message) => {
                write!(f, "executor phase failed: {message}")
            }
            Self::Io {path, source} => {
                if let Some(path) = path {
                    write!(f, "I/O error at {}: {source}", path.display())
                } else {
                    write!(f, "I/O error: {source}")
                }
            }
        }
    }
}

impl Error for MapReduceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io {source, ..} => Some(source),
            _ => None,
        }
    }
}

impl From<std::io::Error> for MapReduceError {
    fn from(source: std::io::Error) -> Self {
        Self::io(source)
    }
}