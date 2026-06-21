use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use mr_core::{InputRecord, MapReduceError, Result};

pub struct LocalTextInput;

impl LocalTextInput {
    pub fn read_records<P>(paths: impl IntoIterator<Item = P>) -> Result<Vec<InputRecord>>
    where
        P: AsRef<Path>,
    {
        let input_files = collect_input_files(paths)?;

        let mut records = Vec::new();
        let mut offset = 0_u64;

        for input_file in input_files {
            let file = fs::File::open(&input_file)
                .map_err(|source| MapReduceError::io_with_path(&input_file, source))?;

            let mut reader = BufReader::new(file);
            let mut line = String::new();

            loop {
                line.clear();

                let bytes_read = reader
                    .read_line(&mut line)
                    .map_err(|source| MapReduceError::io_with_path(&input_file, source))?;

                if bytes_read == 0 {
                    break;
                }

                strip_line_ending(&mut line);

                records.push(InputRecord::new(offset, line.clone()));
                offset += 1;
            }
        }

        Ok(records)
    }
}

fn collect_input_files<P>(paths: impl IntoIterator<Item = P>) -> Result<Vec<PathBuf>>
where
    P: AsRef<Path>,
{
    let mut files = Vec::new();

    for path in paths {
        collect_path(path.as_ref(), &mut files)?;
    }

    files.sort();

    Ok(files)
}

fn collect_path(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let metadata =
        fs::metadata(path).map_err(|source| MapReduceError::io_with_path(path, source))?;

    if metadata.is_file() {
        files.push(path.to_path_buf());
        return Ok(());
    }

    if metadata.is_dir() {
        let mut entries = fs::read_dir(path)
            .map_err(|source| MapReduceError::io_with_path(path, source))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|source| MapReduceError::io_with_path(path, source))?;

        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            collect_path(&entry.path(), files)?;
        }

        return Ok(());
    }

    Err(MapReduceError::InvalidJobConfig(format!(
        "input path is neither a file nor a directory: {}",
        path.display()
    )))
}

fn strip_line_ending(line: &mut String) {
    if line.ends_with('\n') {
        line.pop();

        if line.ends_with('\r') {
            line.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn reads_records_from_single_file() {
        let dir = create_test_dir("single-file");
        let input_path = dir.join("input.txt");

        fs::write(&input_path, "hello rust\nhello mapreduce\n").unwrap();

        let records = LocalTextInput::read_records(&[input_path]).unwrap();

        assert_eq!(
            records,
            vec![
                InputRecord::new(0, "hello rust"),
                InputRecord::new(1, "hello mapreduce"),
            ]
        );

        fs::remove_dir_all(dir).unwrap()
    }

    #[test]
    fn reads_records_from_directory_recursively() {
        let dir = create_test_dir("directory");
        let nested = dir.join("nested");

        fs::create_dir_all(&nested).unwrap();
        fs::write(dir.join("a.txt"), "alpha\n").unwrap();
        fs::write(nested.join("b.txt"), "beta\n").unwrap();

        let records = LocalTextInput::read_records([&dir]).unwrap();

        let values = records
            .into_iter()
            .map(|record| record.value)
            .collect::<Vec<_>>();

        assert_eq!(values, vec!["alpha".to_string(), "beta".to_string()]);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn preserves_last_line_without_trailing_newline() {
        let dir = create_test_dir("no-trailing-newline");
        let input_path = dir.join("input.txt");

        fs::write(&input_path, "hello\nrust").unwrap();

        let records = LocalTextInput::read_records(&[input_path]).unwrap();

        assert_eq!(
            records,
            vec![InputRecord::new(0, "hello"), InputRecord::new(1, "rust")]
        );

        fs::remove_dir_all(dir).unwrap()
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
