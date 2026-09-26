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

//! Wall-clock time helpers.
//!
//! The `current_*` functions query [`std::time::SystemTime::now`] and return the
//! elapsed duration since the Unix epoch in the requested unit. The remaining
//! helpers format, parse and align timestamps in the local time zone, following
//! the Java broker's formats.

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use chrono::DateTime;
use chrono::Datelike;
use chrono::Local;
use chrono::LocalResult;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Timelike;

/// The yyyy mm dd hh mm ss sss constant.
pub const YYYY_MM_DD_HH_MM_SS_SSS: &str = "%Y-%m-%d#%H:%M:%S:%3f";
/// The yyyymmddhhmmss constant.
pub const YYYYMMDDHHMMSS: &str = "%Y%m%d%H%M%S";

#[inline(always)]
fn duration_since_unix_epoch(time: std::time::SystemTime) -> std::time::Duration {
    time.duration_since(std::time::UNIX_EPOCH).unwrap_or_default()
}

/// Returns the current Unix timestamp in milliseconds.
///
/// Returns 0 if the system clock is set before the Unix epoch.
#[inline(always)]
pub fn current_millis() -> u64 {
    duration_since_unix_epoch(std::time::SystemTime::now()).as_millis() as u64
}

/// Returns the current Unix timestamp in nanoseconds.
///
/// Returns 0 if the system clock is set before the Unix epoch.
#[inline(always)]
pub fn current_nano() -> u64 {
    duration_since_unix_epoch(std::time::SystemTime::now()).as_nanos() as u64
}

/// Returns the current Unix timestamp in whole seconds.
///
/// Returns 0 if the system clock is set before the Unix epoch.
#[inline(always)]
pub fn current_secs() -> u64 {
    duration_since_unix_epoch(std::time::SystemTime::now()).as_secs()
}

fn local_timestamp_millis_or_epoch(t: i64) -> DateTime<Local> {
    Local
        .timestamp_millis_opt(t)
        .single()
        .or_else(|| Local.timestamp_millis_opt(0).single())
        .unwrap_or_else(Local::now)
}

/// Returns whether the current local hour is one of the `;`-separated hours
/// in `when`, such as a `deleteWhen` setting of `"04;16"`.
pub fn is_it_time_to_do(when: &str) -> bool {
    let hours: Vec<&str> = when.split(";").collect();
    if !hours.is_empty() {
        let now = Local::now();
        for hour in hours {
            let now_hour: i32 = hour.parse().unwrap_or(0);
            if now_hour == now.hour() as i32 {
                return true;
            }
        }
    }
    false
}

/// Converts a timestamp in milliseconds to a human-readable string format.
///
/// The format is: yyyy-MM-dd HH:mm:ss,SSS
///
/// # Arguments
///
/// * `t` - Timestamp in milliseconds
///
/// # Returns
///
/// Formatted date-time string
pub fn time_millis_to_human_string2(t: i64) -> String {
    let dt = local_timestamp_millis_or_epoch(t);

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02},{:03}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.timestamp_subsec_millis()
    )
}

/// Converts a timestamp in milliseconds to a compact human-readable string format.
///
/// The format is: yyyyMMddHHmmss
///
/// # Arguments
///
/// * `t` - Timestamp in milliseconds
///
/// # Returns
///
/// Formatted date-time string
pub fn time_millis_to_human_string3(t: i64) -> String {
    let dt = local_timestamp_millis_or_epoch(t);

    format!(
        "{:04}{:02}{:02}{:02}{:02}{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

/// Converts a timestamp in milliseconds to a human-readable string format.
///
/// The format is: yyyyMMddHHmmssSSS (year, month, day, hour, minute, second, millisecond)
///
/// # Arguments
///
/// * `t` - Timestamp in milliseconds
///
/// # Returns
///
/// Formatted date-time string
pub fn time_millis_to_human_string(t: i64) -> String {
    let dt = local_timestamp_millis_or_epoch(t);

    format!(
        "{:04}{:02}{:02}{:02}{:02}{:02}{:03}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.timestamp_subsec_millis()
    )
}

/// Executes compute next minutes time millis.
pub fn compute_next_minutes_time_millis() -> u64 {
    let now = SystemTime::now();
    let millis_since_epoch = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64);

    let millis_in_minute = 60 * 1000;
    ((millis_since_epoch / millis_in_minute) + 1) * millis_in_minute
}

/// Executes compute next morning time millis.
pub fn compute_next_morning_time_millis() -> u64 {
    let now = Local::now();
    let tomorrow = now.date_naive().succ_opt().unwrap_or_else(|| now.date_naive());
    let next_morning = match Local.with_ymd_and_hms(tomorrow.year(), tomorrow.month(), tomorrow.day(), 0, 0, 0) {
        LocalResult::Single(value) => value,
        LocalResult::Ambiguous(earliest, _) => earliest,
        LocalResult::None => now,
    };
    next_morning.timestamp_millis().max(0) as u64
}

/// Parses date.
pub fn parse_date(date: &str, pattern: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(date, pattern).ok()
}

/// Parses date to millis.
pub fn parse_date_to_millis(date: &str, pattern: &str) -> Option<i64> {
    let parsed = parse_date(date, pattern)?;
    let timestamp = match Local.from_local_datetime(&parsed) {
        LocalResult::Single(timestamp) => timestamp,
        LocalResult::Ambiguous(earliest, _) => earliest,
        LocalResult::None => return None,
    };
    Some(timestamp.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use super::*;

    #[test]
    fn duration_since_unix_epoch_returns_zero_for_before_epoch() {
        let before_epoch = UNIX_EPOCH - Duration::from_secs(1);

        assert_eq!(duration_since_unix_epoch(before_epoch), Duration::ZERO);
    }

    #[test]
    fn current_millis_returns_correct_value() {
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let current = current_millis();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        assert!(current >= before && current <= after);
    }

    #[test]
    fn current_nano_returns_correct_value() {
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        let current = current_nano();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        assert!(current >= before && current <= after);
    }

    #[test]
    fn current_secs_returns_correct_value() {
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let current = current_secs();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        assert!(current >= before && current <= after);
    }

    #[test]
    fn all_timestamps_are_positive() {
        assert!(current_millis() > 0);
        assert!(current_nano() > 0);
        assert!(current_secs() > 0);
    }

    #[test]
    fn millis_greater_than_secs() {
        let secs = current_secs();
        let millis = current_millis();
        assert!(millis > secs);
    }

    #[test]
    fn nanos_greater_than_millis() {
        let millis = current_millis();
        let nanos = current_nano();
        assert!(nanos > millis);
    }

    #[test]
    fn timestamps_are_non_decreasing() {
        let t1 = current_millis();
        std::thread::sleep(Duration::from_millis(5));
        let t2 = current_millis();
        assert!(t2 >= t1);
    }

    #[test]
    fn secs_consistent_with_millis() {
        let secs = current_secs();
        let millis_as_secs = current_millis() / 1000;
        // millis is sampled after secs; millis_as_secs is equal to secs or one ahead.
        assert!(millis_as_secs == secs || millis_as_secs == secs + 1);
    }

    #[test]
    fn is_it_time_to_do_returns_true_when_current_hour_is_in_input() {
        let current_hour = Local::now().hour();
        assert!(is_it_time_to_do(&current_hour.to_string()));
    }

    #[test]
    fn is_it_time_to_do_returns_false_when_current_hour_is_not_in_input() {
        let current_hour = (Local::now().hour() + 1) % 24;
        assert!(!is_it_time_to_do(&current_hour.to_string()));
    }

    #[test]
    fn time_millis_to_human_string_formats_correctly() {
        let timestamp = 1743239631601;
        let expected = Local
            .timestamp_millis_opt(timestamp)
            .unwrap()
            .format("%Y%m%d%H%M%S%3f")
            .to_string();
        assert_eq!(time_millis_to_human_string(timestamp), expected);
    }

    #[test]
    fn time_millis_to_human_string_falls_back_to_epoch_for_invalid_timestamp() {
        assert_eq!(time_millis_to_human_string(i64::MAX), time_millis_to_human_string(0));
        assert_eq!(time_millis_to_human_string2(i64::MAX), time_millis_to_human_string2(0));
        assert_eq!(time_millis_to_human_string3(i64::MAX), time_millis_to_human_string3(0));
    }

    #[test]
    fn test_compute_next_minutes_time_millis() {
        let next_minute = compute_next_minutes_time_millis();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        assert!(next_minute > now);
        assert_eq!(next_minute % (60 * 1000), 0);
    }

    #[test]
    fn compute_next_morning_time_millis_returns_non_zero_timestamp() {
        assert!(compute_next_morning_time_millis() > 0);
    }

    #[test]
    fn time_millis_to_human_string2_formats_correctly_with_valid_timestamp() {
        use chrono::TimeZone;

        let timestamp = 1625140800000;
        let expected = Local
            .timestamp_millis_opt(timestamp)
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S,%3f")
            .to_string();
        assert_eq!(time_millis_to_human_string2(timestamp), expected);
    }

    #[test]
    fn time_millis_to_human_string3_formats_correctly_with_valid_timestamp() {
        let timestamp = 1625140800000;
        let expect = Local
            .timestamp_millis_opt(timestamp)
            .unwrap()
            .format("%Y%m%d%H%M%S")
            .to_string();
        assert_eq!(time_millis_to_human_string3(timestamp), expect);
    }

    #[test]
    fn java_date_format_constants_parse_java_shapes() {
        assert!(parse_date("20250102030405", YYYYMMDDHHMMSS).is_some());

        let parsed = parse_date("2025-01-02#03:04:05:006", YYYY_MM_DD_HH_MM_SS_SSS)
            .expect("Java millisecond format should parse");
        assert_eq!(parsed.and_utc().timestamp_subsec_millis(), 6);
    }

    #[test]
    fn parse_date_to_millis_uses_local_timezone_like_java_simple_date_format() {
        let expected = match Local.with_ymd_and_hms(2025, 1, 2, 3, 4, 5) {
            LocalResult::Single(timestamp) => timestamp,
            LocalResult::Ambiguous(earliest, _) => earliest,
            LocalResult::None => panic!("test timestamp should be valid in the local timezone"),
        };

        assert_eq!(
            parse_date_to_millis("20250102030405", YYYYMMDDHHMMSS),
            Some(expected.timestamp_millis())
        );
    }
}
