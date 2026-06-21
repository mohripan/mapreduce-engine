use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use mr_core::{Key, MapReduceError, Result, Value};

pub struct LocalTextOutput;

impl LocalTextOutput {
    pub fn write_outputs<P>(output_dir: P, outputs: &[(Key, Value)]) -> Result<PathBuf>
    where
        P: AsRef<Path>,
    {
        let output_dir = output_dir.as_ref();

        prepare_output_dir(output_dir)?;

        let output_file = output_dir.join("part-00000");

        let file = fs::File::create(&output_file)
            .map_err(|source| MapReduceError::io_with_path(&output_file, source))?;

        let mut writer = BufWriter::new(file);

        for (key, value) in outputs {
            writeln!(writer, "{key}\t{value}")
                .map_err(|source| MapReduceError::io_with_path(&output_file, source))?;
        }

        writer
            .flush()
            .map_err(|source| MapReduceError::io_with_path(&output_file, source))?;

        Ok(output_file)
    }
}

fn prepare_output_dir(output_dir: &Path) -> Result<()> {
    if output_dir.exists() {
        if !output_dir.is_dir() {
            return Err(MapReduceError::InvalidJobConfig(format!(
                "output path exists but is not a directory: {}",
                output_dir.display()
            )));
        }

        let mut entries = fs::read_dir(output_dir)
            .map_err(|source| MapReduceError::io_with_path(output_dir, source))?;

        if entries.next().is_some() {
            return Err(MapReduceError::InvalidJobConfig(format!(
                "output directory already exists and is not empty: {}",
                output_dir.display()
            )));
        }

        return Ok(());
    }

    fs::create_dir_all(output_dir)
        .map_err(|source| MapReduceError::io_with_path(output_dir, source))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn writes_outputs_to_part_file() {
        let dir = create_test_dir("write-outputs");

        let output_file = LocalTextOutput::write_outputs(
            &dir,
            &[
                ("hello".to_string(), "2".to_string()),
                ("rust".to_string(), "1".to_string()),
            ],
        )
        .unwrap();

        let content = fs::read_to_string(output_file).unwrap();

        assert_eq!(content, "hello\t2\nrust\t1\n");

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_non_empty_output_directory() {
        let dir = create_test_dir("non-empty-output");

        fs::write(dir.join("existing.txt"), "already here").unwrap();

        let result =
            LocalTextOutput::write_outputs(&dir, &[("hello".to_string(), "1".to_string())]);

        assert!(result.is_err());

        fs::remove_dir_all(dir).unwrap();
    }

    fn create_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let dir = std::env::temp_dir().join(format!("mr-local-fs-{name}-{nanos}"));

        fs::create_dir_all(&dir).unwrap();

        dir
    }
}
