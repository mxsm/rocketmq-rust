// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::Offset;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::OnceLock;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use chrono_tz::Tz;
use time::UtcOffset;
use tracing_subscriber::fmt::time::OffsetTime;

/// Static storage for the worker guard to prevent premature log flushing.
/// This ensures logs are properly written before the program exits.
static WORKER_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

/// Cached local timezone to avoid repeated system calls
static LOCAL_TIMEZONE: OnceLock<Tz> = OnceLock::new();

/// Initializes the logger with the specified configuration.
///
/// This function sets up the logger using the `tracing_subscriber` crate.
/// It reads the log level from the `RUST_LOG` environment variable, defaulting to "INFO" if not
/// set. The logger is configured to include thread names, log levels, line numbers, and thread IDs
/// in the log output.
///
/// # Returns
///
/// Returns `Ok(())` on success, or a `RocketMQError` if initialization fails.
pub fn init_logger() -> RocketMQResult<()> {
    let info_level = std::env::var("RUST_LOG").unwrap_or_else(|_| String::from("INFO"));
    let level = tracing::Level::from_str(&info_level)
        .map_err(|_| RocketMQError::illegal_argument(format!("Invalid log level: {}", info_level)))?;

    tracing_subscriber::fmt()
        .with_timer(get_timer_from_env())
        .with_thread_names(true)
        .with_level(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_max_level(level)
        .try_init()
        .map_err(|e| RocketMQError::Internal(format!("Failed to initialize logger: {}", e)))?;

    Ok(())
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
/// # Returns
///
/// Returns `Ok(())` on success, or a `RocketMQError` if initialization fails.
pub fn init_logger_with_level(level: Level) -> RocketMQResult<()> {
    let tracing_level = level.to_tracing_level()?;

    tracing_subscriber::fmt()
        .with_timer(get_timer_from_env())
        .with_thread_names(true)
        .with_level(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_max_level(tracing_level)
        .try_init()
        .map_err(|e| RocketMQError::Internal(format!("Failed to initialize logger: {}", e)))?;

    Ok(())
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
/// # Returns
///
/// Returns `Ok(())` on success, or a `RocketMQError` if initialization fails.
///
/// # Notes
///
/// This function creates a non-blocking file writer to improve performance.
/// The worker guard is stored in a static `OnceLock` to prevent premature flushing.
/// This ensures logs are properly written before the program exits.
pub fn init_logger_with_file(
    level: Level,
    directory: impl AsRef<Path>,
    file_name_prefix: impl AsRef<Path>,
) -> RocketMQResult<()> {
    // Convert custom Level to tracing LevelFilter
    let max_level = tracing_subscriber::filter::LevelFilter::from_str(level.as_str())
        .map_err(|_| RocketMQError::illegal_argument(format!("Invalid log level: {}", level)))?;

    // log file output (daily rolling)
    let file_appender = rolling::daily(directory, file_name_prefix);
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    // console layer (colorful output)
    let console_layer = tracing_subscriber::fmt::layer()
        .with_timer(get_timer_from_env())
        .with_writer(std::io::stdout)
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_level(true)
        .with_ansi(true)
        .with_filter(max_level);

    // file layer (non-color output)
    let file_layer = tracing_subscriber::fmt::layer()
        .with_timer(get_timer_from_env())
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
        .try_init()
        .map_err(|e| RocketMQError::Internal(format!("Failed to initialize logger: {}", e)))?;

    // Store the guard in a static variable to prevent it from being dropped
    // Using OnceLock instead of Box::leak to avoid memory leak
    WORKER_GUARD
        .set(guard)
        .map_err(|_| RocketMQError::Internal("Logger already initialized".to_string()))?;

    Ok(())
}

/// decides the time zone of the logs
///
/// this function gets the LOG_TIMEZONE from environment variables
/// defaults to system local timezone if nothing is provided
/// the IANA timezone has to be provided in the env file
/// ex LOG_TIMEZONE=Asia/Kolkata
/// ex LOG_TIMEZONE=Europe/Berlin
/// ex LOG_TIMEZONE=UTC (use UTC timezone)
/// ex LOG_TIMEZONE=Local (use system local timezone)
/// we use the IANA timezone instead of just the abbreviations
/// because the abbreviations can refer to different countries
/// for ex IST can refer to Indian Standard Time or Irish Standard Time or Israel Standard Time
/// added to this we also have the benefit of taking DST into consideration with IANA timezones
/// the usage of LocalTime provided by tracing subscriber is not recommended
/// hence going with OffsetTime
///
/// # Returns
///
/// Returns 'OffsetTime'
pub fn get_timer_from_env() -> OffsetTime<time::format_description::well_known::Rfc3339> {
    let tz: Tz = if let Ok(tz_str) = std::env::var("LOG_TIMEZONE") {
        // If LOG_TIMEZONE is explicitly set
        if tz_str.eq_ignore_ascii_case("Local") {
            // Use system local timezone
            get_local_timezone()
        } else {
            // Parse the provided timezone string
            tz_str.parse().unwrap_or_else(|e| {
                eprintln!(
                    "Warning: Invalid timezone '{}': {}. Falling back to local timezone.",
                    tz_str, e
                );
                get_local_timezone()
            })
        }
    } else {
        // Default: use system local timezone if LOG_TIMEZONE is not set
        get_local_timezone()
    };

    let now = chrono::Utc::now().with_timezone(&tz);

    let offset_seconds = now.offset().fix().local_minus_utc();

    let offset = UtcOffset::from_whole_seconds(offset_seconds).unwrap_or_else(|e| {
        eprintln!(
            "Warning: Invalid offset {} seconds: {}. Falling back to UTC.",
            offset_seconds, e
        );
        UtcOffset::UTC
    });

    OffsetTime::new(offset, time::format_description::well_known::Rfc3339)
}

/// Get the system local timezone
///
/// Attempts to get the system timezone directly from the OS using `iana-time-zone` crate.
/// If that fails, tries the `TZ` environment variable.
/// As a last resort, guesses the timezone based on the UTC offset.
/// Results are cached to avoid repeated system calls.
///
/// # Returns
///
/// Returns 'Tz'
fn get_local_timezone() -> Tz {
    *LOCAL_TIMEZONE.get_or_init(|| {
        // Try to get timezone directly from the system
        if let Ok(tz_name) = iana_time_zone::get_timezone() {
            if let Ok(tz) = tz_name.parse::<Tz>() {
                return tz;
            } else {
                eprintln!(
                    "Warning: System returned timezone '{}' but it could not be parsed. Trying fallback methods.",
                    tz_name
                );
            }
        }

        // Try to get timezone name from environment variable
        if let Ok(tz_name) = std::env::var("TZ") {
            if let Ok(tz) = tz_name.parse::<Tz>() {
                return tz;
            }
        }

        // Get the UTC offset of the local time
        let local_now = chrono::Local::now();
        let offset_seconds = local_now.offset().fix().local_minus_utc();
        let offset_hours = offset_seconds / 3600;

        // Guess common timezones based on offset as last resort
        eprintln!(
            "Warning: Could not determine system timezone. Guessing based on UTC offset ({} hours).",
            offset_hours
        );
        match offset_hours {
            -12 => chrono_tz::Pacific::Fiji,
            -11 => chrono_tz::Pacific::Midway,
            -10 => chrono_tz::Pacific::Honolulu,
            -9 => chrono_tz::America::Anchorage,
            -8 => chrono_tz::America::Los_Angeles,
            -7 => chrono_tz::America::Denver,
            -6 => chrono_tz::America::Chicago,
            -5 => chrono_tz::America::New_York,
            -4 => chrono_tz::America::Halifax,
            -3 => chrono_tz::America::Sao_Paulo,
            -2 => chrono_tz::Atlantic::South_Georgia,
            -1 => chrono_tz::Atlantic::Azores,
            0 => chrono_tz::UTC,
            1 => chrono_tz::Europe::Paris,
            2 => chrono_tz::Europe::Helsinki,
            3 => chrono_tz::Europe::Moscow,
            4 => chrono_tz::Asia::Dubai,
            5 => chrono_tz::Asia::Karachi,
            6 => chrono_tz::Asia::Dhaka,
            7 => chrono_tz::Asia::Bangkok,
            8 => chrono_tz::Asia::Shanghai,
            9 => chrono_tz::Asia::Tokyo,
            10 => chrono_tz::Australia::Sydney,
            11 => chrono_tz::Pacific::Noumea,
            12 => chrono_tz::Pacific::Auckland,
            _ => {
                eprintln!(
                    "Warning: Unsupported local timezone offset {} hours. Falling back to UTC.",
                    offset_hours
                );
                chrono_tz::UTC
            }
        }
    })
}

/// Custom log level type that wraps a static string.
///
/// This type provides a type-safe way to represent log levels while maintaining
/// compatibility with the tracing crate's log levels.
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

    /// Returns the string representation of the log level.
    #[inline]
    pub fn as_str(&self) -> &'static str {
        self.0
    }

    /// Converts this Level to a tracing::Level.
    ///
    /// # Returns
    ///
    /// Returns `Ok(tracing::Level)` if the conversion is successful,
    /// or a `RocketMQError` if the level string is invalid.
    pub fn to_tracing_level(&self) -> RocketMQResult<tracing::Level> {
        tracing::Level::from_str(self.0)
            .map_err(|_| RocketMQError::illegal_argument(format!("Invalid log level: {}", self.0)))
    }
}

impl From<&'static str> for Level {
    fn from(level: &'static str) -> Self {
        match level {
            "ERROR" | "WARN" | "INFO" | "DEBUG" | "TRACE" => Level(level),
            _ => panic!("Invalid log level: {level}"),
        }
    }
}

impl FromStr for Level {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ERROR" => Ok(Level::ERROR),
            "WARN" => Ok(Level::WARN),
            "INFO" => Ok(Level::INFO),
            "DEBUG" => Ok(Level::DEBUG),
            "TRACE" => Ok(Level::TRACE),
            _ => Err(format!("Invalid log level: {s}")),
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
    fn level_from_str_trait_works() {
        assert_eq!("ERROR".parse::<Level>().unwrap(), Level::ERROR);
        assert_eq!("INFO".parse::<Level>().unwrap(), Level::INFO);
        assert!("invalid".parse::<Level>().is_err());
    }

    #[test]
    fn level_display_formats_correctly() {
        assert_eq!(format!("{}", Level::ERROR), "ERROR");
        assert_eq!(format!("{}", Level::WARN), "WARN");
        assert_eq!(format!("{}", Level::INFO), "INFO");
        assert_eq!(format!("{}", Level::DEBUG), "DEBUG");
        assert_eq!(format!("{}", Level::TRACE), "TRACE");
    }

    #[test]
    fn level_to_tracing_level_success() {
        assert!(Level::ERROR.to_tracing_level().is_ok());
        assert!(Level::WARN.to_tracing_level().is_ok());
        assert!(Level::INFO.to_tracing_level().is_ok());
        assert!(Level::DEBUG.to_tracing_level().is_ok());
        assert!(Level::TRACE.to_tracing_level().is_ok());
    }

    #[test]
    fn level_clone_works() {
        let level = Level::INFO;
        let cloned = level.clone();
        assert_eq!(level, cloned);
    }

    #[test]
    fn level_hash_works() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Level::ERROR);
        set.insert(Level::INFO);
        assert!(set.contains(&Level::ERROR));
        assert!(!set.contains(&Level::DEBUG));
    }
}
