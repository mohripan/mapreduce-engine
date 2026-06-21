use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use mr_core::{Emitter, InputRecord, Job, Key, MapReduceError, Mapper, Reducer, Result, Value};
use mr_engine::InMemoryEngine;
use mr_local_fs::{LocalTextInput, LocalTextOutput};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            eprintln!();
            eprintln!("{}", usage());

            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let raw_args = env::args().skip(1).collect::<Vec<_>>();

    if raw_args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{}", usage());
        return Ok(());
    }

    let args = CliArgs::parse(raw_args)?;

    let records = LocalTextInput::read_records(&args.input_paths)?;

    let job = Job::builder()
        .name("word-count")
        .input_paths(args.input_paths.clone())
        .output_path(args.output_path.clone())
        .mapper(WordCountMapper)
        .reducer(WordCountReducer)
        .num_map_workers(args.map_workers)
        .num_reduce_workers(args.reduce_workers)
        .num_reducers(args.reducers)
        .build()?;

    let engine = InMemoryEngine::new();
    let outputs = engine.run_records(&job, records)?;

    let output_file = LocalTextOutput::write_outputs(job.output_path(), &outputs)?;

    println!("job completed successfully");
    println!("job name: {}", job.name());
    println!("output file: {}", output_file.display());
    println!("records written: {}", outputs.len());

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliArgs {
    input_paths: Vec<PathBuf>,
    output_path: PathBuf,
    map_workers: usize,
    reduce_workers: usize,
    reducers: usize,
}

impl CliArgs {
    fn parse<I, S>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut args = args.into_iter().map(Into::into);

        let command = args
            .next()
            .ok_or_else(|| MapReduceError::InvalidJobConfig("missing command".to_string()))?;

        if command != "word-count" {
            return Err(MapReduceError::InvalidJobConfig(format!(
                "unknown command: {command}"
            )));
        }

        let default_parallelism = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(4);

        let mut input_paths = Vec::new();
        let mut output_path = None;
        let mut map_workers = default_parallelism;
        let mut reduce_workers = default_parallelism;
        let mut reducers = 1;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--input" | "-i" => {
                    let value = next_value(&mut args, "--input")?;
                    input_paths.push(PathBuf::from(value));
                }
                "--output" | "-o" => {
                    let value = next_value(&mut args, "--output")?;
                    output_path = Some(PathBuf::from(value));
                }
                "--map-workers" => {
                    let value = next_value(&mut args, "--map-workers")?;
                    map_workers = parse_positive_usize("--map-workers", &value)?;
                }
                "--reduce-workers" => {
                    let value = next_value(&mut args, "--reduce-workers")?;
                    reduce_workers = parse_positive_usize("--reduce-workers", &value)?;
                }
                "--reducers" => {
                    let value = next_value(&mut args, "--reducers")?;
                    reducers = parse_positive_usize("--reducers", &value)?;
                }
                unknown => {
                    return Err(MapReduceError::InvalidJobConfig(format!(
                        "unknown argument: {unknown}"
                    )));
                }
            }
        }

        if input_paths.is_empty() {
            return Err(MapReduceError::InvalidJobConfig(
                "at least one --input path is required".to_string(),
            ));
        }

        let output_path = output_path.ok_or_else(|| {
            MapReduceError::InvalidJobConfig("--output path is required".to_string())
        })?;

        Ok(Self {
            input_paths,
            output_path,
            map_workers,
            reduce_workers,
            reducers,
        })
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, option_name: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| MapReduceError::InvalidJobConfig(format!("missing value for {option_name}")))
}

fn parse_positive_usize(option_name: &str, value: &str) -> Result<usize> {
    let parsed = value.parse::<usize>().map_err(|_| {
        MapReduceError::InvalidJobConfig(format!(
            "{option_name} must be a positive integer, got {value}"
        ))
    })?;

    if parsed == 0 {
        return Err(MapReduceError::InvalidJobConfig(format!(
            "{option_name} must be greater than zero"
        )));
    }

    Ok(parsed)
}

fn usage() -> &'static str {
    r#"Usage:
  mr-cli word-count --input <path> --output <path> [options]

Commands:
  word-count
      Count words from one or more local text files/directories.

Required:
  -i, --input <path>
      Input file or directory. Can be repeated.

  -o, --output <path>
      Output directory. Must not already contain files.

Options:
  --map-workers <n>
      Number of mapper worker threads.

  --reduce-workers <n>
      Number of reducer worker threads.

  --reducers <n>
      Number of logical reducer partitions.

  -h, --help
      Show this help message.

Example:
  cargo run -p mr-cli -- word-count --input ./data/input.txt --output ./data/out --map-workers 4 --reduce-workers 2 --reducers 2
"#
}

struct WordCountMapper;

impl Mapper for WordCountMapper {
    fn map(&self, record: InputRecord, emitter: &mut dyn Emitter) -> Result<()> {
        for raw_word in record.value.split_whitespace() {
            let word = normalize_word(raw_word);

            if !word.is_empty() {
                emitter.emit(word, "1".to_string());
            }
        }

        Ok(())
    }
}

struct WordCountReducer;

impl Reducer for WordCountReducer {
    fn reduce(&self, key: Key, values: Vec<Value>, emitter: &mut dyn Emitter) -> Result<()> {
        let mut count = 0_u64;

        for value in values {
            let parsed = value.parse::<u64>().map_err(|_| {
                MapReduceError::Reduce(format!(
                    "word-count reducer expected numeric value for key {key}"
                ))
            })?;

            count += parsed;
        }

        emitter.emit(key, count.to_string());

        Ok(())
    }
}

fn normalize_word(word: &str) -> String {
    word.trim_matches(|ch: char| !ch.is_alphanumeric())
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_word_count_command() {
        let args =
            CliArgs::parse(["word-count", "--input", "input.txt", "--output", "out"]).unwrap();

        assert_eq!(args.input_paths, vec![PathBuf::from("input.txt")]);
        assert_eq!(args.output_path, PathBuf::from("out"));
        assert_eq!(args.reducers, 1);
    }

    #[test]
    fn parses_repeated_input_paths() {
        let args = CliArgs::parse([
            "word-count",
            "--input",
            "a.txt",
            "--input",
            "b.txt",
            "--output",
            "out",
        ])
        .unwrap();

        assert_eq!(
            args.input_paths,
            vec![PathBuf::from("a.txt"), PathBuf::from("b.txt")]
        );
    }

    #[test]
    fn parses_worker_options() {
        let args = CliArgs::parse([
            "word-count",
            "--input",
            "input.txt",
            "--output",
            "out",
            "--map-workers",
            "4",
            "--reduce-workers",
            "2",
            "--reducers",
            "3",
        ])
        .unwrap();

        assert_eq!(args.map_workers, 4);
        assert_eq!(args.reduce_workers, 2);
        assert_eq!(args.reducers, 3);
    }

    #[test]
    fn rejects_missing_input() {
        let result = CliArgs::parse(["word-count", "--output", "out"]);

        assert!(result.is_err());
    }

    #[test]
    fn rejects_missing_output() {
        let result = CliArgs::parse(["word-count", "--input", "input.txt"]);

        assert!(result.is_err());
    }

    #[test]
    fn rejects_zero_workers() {
        let result = CliArgs::parse([
            "word-count",
            "--input",
            "input.txt",
            "--output",
            "out",
            "--map-workers",
            "0",
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn normalizes_words() {
        assert_eq!(normalize_word("Rust!"), "rust");
        assert_eq!(normalize_word("(MapReduce)"), "mapreduce");
        assert_eq!(normalize_word("hello"), "hello");
    }
}
