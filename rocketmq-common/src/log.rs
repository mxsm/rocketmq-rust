/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::fmt;
use std::path::Path;
use std::str::FromStr;

use tracing_appender::rolling;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

/// Initializes the logger with the specified configuration.
///
/// This function sets up the logger using the `tracing_subscriber` crate.
/// It reads the log level from the `RUST_LOG` environment variable, defaulting to "INFO" if not
/// set. The logger is configured to include thread names, log levels, line numbers, and thread IDs
/// in the log output.
pub fn init_logger() {
    let info_level = std::env::var("RUST_LOG").unwrap_or(String::from("INFO"));
    tracing_subscriber::fmt()
        .with_thread_names(true)
        .with_level(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::from_str(info_level.as_str()).expect("Invalid log level"))
        .init();
}

/// Initializes the logger with the specified log level.
///
/// This function sets up the logger using the `tracing_subscriber` crate.
/// It configures the logger to include thread names, log levels, line numbers, and thread IDs
/// in the log output. The maximum log level is set based on the provided `level`.
///
/// # Arguments
///
/// * `level` - A `Level` representing the desired log level.
///
/// # Panics
///
/// This function will panic if the provided log level is invalid.
pub fn init_logger_with_level(level: Level) {
    tracing_subscriber::fmt()
        .with_thread_names(true)
        .with_level(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::from_str(level.as_str()).expect("Invalid log level"))
        .init();
}

/// Initializes the logger with both file and console output.
///
/// This function configures logging to output to both a file and the console simultaneously.
/// The file logger uses daily rotation to create new log files each day.
///
/// # Arguments
///
/// * `level` - The logging level to use for both outputs.
/// * `directory` - The directory where log files will be stored.
/// * `file_name_prefix` - The prefix to use for log filenames.
///
/// # Notes
///
/// This function creates a non-blocking file writer to improve performance.
/// The returned guard should be stored in a static or long-lived variable to prevent
/// premature flushing of logs when the guard is dropped.
///
/// # Panics
///
/// This function will panic if the provided log level is invalid.
pub fn init_logger_with_file(
    level: Level,
    directory: impl AsRef<Path>,
    file_name_prefix: impl AsRef<Path>,
) {
    // log file output (daily rolling)
    let file_appender = rolling::daily(directory, file_name_prefix);
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    let max_level = tracing_subscriber::filter::LevelFilter::from_str(level.as_str())
        .expect("Invalid log level");

    // console layer (colorful output)
    let console_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_level(true)
        .with_ansi(true)
        .with_filter(max_level);

    // file layer(non-color output)
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_writer)
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_level(true)
        .with_ansi(false)
        .with_filter(max_level);

    // Register two layers
    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();

    // Store the guard in a static variable to prevent it from being dropped
    Box::leak(Box::new(guard));
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Level(&'static str);

impl Level {
    /// Constant representing the ERROR log level.
    pub const ERROR: Level = Level("ERROR");

    /// Constant representing the WARN log level.
    pub const WARN: Level = Level("WARN");

    /// Constant representing the INFO log level.
    pub const INFO: Level = Level("INFO");

    /// Constant representing the DEBUG log level.
    pub const DEBUG: Level = Level("DEBUG");

    /// Constant representing the TRACE log level.
    pub const TRACE: Level = Level("TRACE");

    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl From<&'static str> for Level {
    fn from(level: &'static str) -> Self {
        match level {
            "ERROR" | "WARN" | "INFO" | "DEBUG" | "TRACE" => Level(level),
            _ => panic!("Invalid log level: {}", level),
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_as_str_returns_correct_value() {
        assert_eq!(Level::ERROR.as_str(), "ERROR");
        assert_eq!(Level::WARN.as_str(), "WARN");
        assert_eq!(Level::INFO.as_str(), "INFO");
        assert_eq!(Level::DEBUG.as_str(), "DEBUG");
        assert_eq!(Level::TRACE.as_str(), "TRACE");
    }

    #[test]
    fn level_from_str_creates_correct_level() {
        assert_eq!(Level::from("ERROR"), Level::ERROR);
        assert_eq!(Level::from("WARN"), Level::WARN);
        assert_eq!(Level::from("INFO"), Level::INFO);
        assert_eq!(Level::from("DEBUG"), Level::DEBUG);
        assert_eq!(Level::from("TRACE"), Level::TRACE);
    }

    #[test]
    fn level_display_formats_correctly() {
        assert_eq!(format!("{}", Level::ERROR), "ERROR");
        assert_eq!(format!("{}", Level::WARN), "WARN");
        assert_eq!(format!("{}", Level::INFO), "INFO");
        assert_eq!(format!("{}", Level::DEBUG), "DEBUG");
        assert_eq!(format!("{}", Level::TRACE), "TRACE");
    }
}
