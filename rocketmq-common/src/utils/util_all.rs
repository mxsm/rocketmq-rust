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

use std::env;
use std::fs;
use std::io;
use std::net::IpAddr;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use cheetah_string::CheetahString;
use chrono::DateTime;
use chrono::Datelike;
use chrono::Local;
use chrono::NaiveDateTime;
use chrono::ParseError;
use chrono::ParseResult;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use local_ip_address::Error;
use once_cell::sync::Lazy;
use tracing::error;
use tracing::info;

use crate::common::mix_all::MULTI_PATH_SPLITTER;

pub const YYYY_MM_DD_HH_MM_SS: &str = "%Y-%m-%d %H:%M:%S%";
pub const YYYY_MM_DD_HH_MM_SS_SSS: &str = "%Y-%m-%d %H:%M:%S%.f";
pub const YYYYMMDDHHMMSS: &str = "%Y%m%d%H%M%S%";

const HEX_ARRAY: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
];

pub fn compute_elapsed_time_milliseconds(begin_time: Instant) -> u64 {
    let elapsed = begin_time.elapsed();
    elapsed.as_millis() as u64
}

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
    let dt: DateTime<Local> = Local.timestamp_millis_opt(t).unwrap();

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
    let dt: DateTime<Local> = Local.timestamp_millis_opt(t).unwrap();

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
    let dt: DateTime<Local> = Local.timestamp_millis_opt(t).unwrap();

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

pub fn is_path_exists(path: &str) -> bool {
    Path::new(path).exists()
}

pub fn get_disk_partition_space_used_percent(path: &str) -> f64 {
    if path.is_empty() {
        error!(
            "Error when measuring disk space usage, path is null or empty, path: {}",
            path
        );
        return -1.0;
    }

    let path = Path::new(path);
    if !path.exists() {
        error!(
            "Error when measuring disk space usage, file doesn't exist on this path: {}",
            path.to_string_lossy()
        );
        return -1.0;
    }

    match fs::metadata(path) {
        Ok(metadata) => {
            let total_space = metadata.len();
            if total_space > 0 {
                match (fs::metadata(path), fs::metadata(path)) {
                    (Ok(metadata1), Ok(metadata2)) => {
                        let free_space = metadata1.len();
                        let usable_space = metadata2.len();
                        let used_space = total_space.saturating_sub(free_space);
                        let entire_space = used_space + usable_space;
                        let round_num = if used_space * 100 % entire_space != 0 { 1 } else { 0 };
                        let result = used_space * 100 / entire_space + round_num;
                        return result as f64 / 100.0;
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        error!("Error when measuring disk space usage, got exception: {:?}", e);
                        return -1.0;
                    }
                }
            }
        }
        Err(e) => {
            error!("Error when measuring disk space usage, got exception: {:?}", e);
            return -1.0;
        }
    }

    -1.0
}

pub fn bytes_to_string(src: &[u8]) -> String {
    let mut hex_chars = Vec::with_capacity(src.len() * 2);
    for &byte in src {
        let v = byte as usize;
        hex_chars.push(HEX_ARRAY[v >> 4]);
        hex_chars.push(HEX_ARRAY[v & 0x0F]);
    }
    hex_chars.into_iter().collect()
}

pub fn write_int(buffer: &mut [char], pos: usize, value: i32) {
    let mut current_pos = pos;
    for move_bits in (0..=28).rev().step_by(4) {
        buffer[current_pos] = HEX_ARRAY[((value >> move_bits) & 0xF) as usize];
        current_pos += 1;
    }
}

pub fn write_short(buffer: &mut [char], pos: usize, value: i16) {
    let mut current_pos = pos;
    for move_bits in (0..=12).rev().step_by(4) {
        buffer[current_pos] = HEX_ARRAY[((value >> move_bits) & 0xF) as usize];
        current_pos += 1;
    }
}

pub fn string_to_bytes(hex_string: impl Into<String>) -> Option<Vec<u8>> {
    let hex_string = hex_string.into();
    if hex_string.is_empty() {
        return None;
    }

    let hex_string = hex_string.to_uppercase();
    let length = hex_string.len() / 2;
    let mut bytes = Vec::<u8>::with_capacity(length);

    for i in 0..length {
        let pos = i * 2;
        let byte = (char_to_byte(hex_string.chars().nth(pos)?) << 4) | char_to_byte(hex_string.chars().nth(pos + 1)?);

        bytes.push(byte);
    }

    Some(bytes)
}

/// Converts a hexadecimal character to its corresponding byte value.
///
/// # Arguments
///
/// * `c` - A character representing a hexadecimal digit (0-9, A-F).
///
/// # Returns
///
/// The byte value of the hexadecimal character. If the character is not a valid
/// hexadecimal digit, returns 0.
#[inline]
fn char_to_byte(c: char) -> u8 {
    match c {
        '0'..='9' => c as u8 - b'0',
        'A'..='F' => c as u8 - b'A' + 10,
        _ => 0,
    }
}

/// Converts an offset value to a zero-padded string of length 20.
///
/// # Arguments
///
/// * `offset` - A 64-bit unsigned integer representing the offset.
///
/// # Returns
///
/// A string representation of the offset, zero-padded to a length of 20 characters.
///
/// # Examples
///
/// ```rust
/// use rocketmq_common::UtilAll::offset_to_file_name;
/// assert_eq!(offset_to_file_name(123), "00000000000000000123");
/// assert_eq!(offset_to_file_name(0), "00000000000000000000");
/// ```
#[inline]
pub fn offset_to_file_name(offset: u64) -> String {
    format!("{offset:020}")
}

pub fn ensure_dir_ok(dir_name: &str) {
    if !dir_name.is_empty() {
        let multi_path_splitter = MULTI_PATH_SPLITTER.as_str();
        if dir_name.contains(multi_path_splitter) {
            for dir in dir_name.trim().split(&multi_path_splitter) {
                create_dir_if_not_exist(dir);
            }
        } else {
            create_dir_if_not_exist(dir_name);
        }
    }
}

fn create_dir_if_not_exist(dir_name: &str) {
    let path = Path::new(dir_name);
    if !path.exists() {
        match fs::create_dir_all(path) {
            Ok(_) => info!("{} mkdir OK", dir_name),
            Err(_) => info!("{} mkdir Failed", dir_name),
        }
    }
}

pub fn compute_next_minutes_time_millis() -> u64 {
    let now = SystemTime::now();
    let duration_since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
    let millis_since_epoch = duration_since_epoch.as_millis() as u64;

    let millis_in_minute = 60 * 1000;
    ((millis_since_epoch / millis_in_minute) + 1) * millis_in_minute
}

pub fn compute_next_morning_time_millis() -> u64 {
    let now = Local::now();
    let tomorrow = now.date_naive().succ_opt().unwrap();
    let next_morning = Local
        .with_ymd_and_hms(tomorrow.year(), tomorrow.month(), tomorrow.day(), 0, 0, 0)
        .unwrap();
    next_morning.timestamp_millis() as u64
}

pub fn delete_empty_directory<P: AsRef<Path>>(path: P) {
    let path = path.as_ref();
    if !path.exists() {
        return;
    }
    if !path.is_dir() {
        return;
    }
    match fs::read_dir(path) {
        Ok(entries) => {
            if entries.count() == 0 {
                match fs::remove_dir(path) {
                    Ok(_) => info!("delete empty directory, {}", path.display()),
                    Err(e) => error!("Error deleting directory: {}", e),
                }
            }
        }
        Err(e) => error!("Error reading directory: {}", e),
    }
}

pub fn get_ip() -> rocketmq_error::RocketMQResult<Vec<u8>> {
    match local_ip_address::local_ip() {
        Ok(value) => match value {
            IpAddr::V4(ip) => Ok(ip.octets().to_vec()),
            IpAddr::V6(ip) => Ok(ip.octets().to_vec()),
        },
        Err(_) => match local_ip_address::local_ipv6() {
            Ok(value) => match value {
                IpAddr::V4(ip) => Ok(ip.octets().to_vec()),
                IpAddr::V6(ip) => Ok(ip.octets().to_vec()),
            },
            Err(value) => Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                "IP error: {}",
                value
            ))),
        },
    }
}

pub fn get_ip_str() -> CheetahString {
    match local_ip_address::local_ip() {
        Ok(value) => match value {
            IpAddr::V4(ip) => CheetahString::from_string(ip.to_string()),
            IpAddr::V6(ip) => CheetahString::from_string(ip.to_string()),
        },
        Err(_) => match local_ip_address::local_ipv6() {
            Ok(value) => match value {
                IpAddr::V4(ip) => CheetahString::from_string(ip.to_string()),
                IpAddr::V6(ip) => CheetahString::from_string(ip.to_string()),
            },
            Err(value) => CheetahString::empty(),
        },
    }
}

pub fn parse_date(date: &str, pattern: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(date, pattern).ok()
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[test]
    fn compute_elapsed_time_milliseconds_returns_correct_duration() {
        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(100));
        let elapsed = compute_elapsed_time_milliseconds(start);
        assert!(elapsed >= 100);
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
    fn is_path_exists_returns_true_for_existing_path() {
        assert!(is_path_exists("."));
    }

    #[test]
    fn is_path_exists_returns_false_for_non_existing_path() {
        assert!(!is_path_exists("./non_existing_path"));
    }

    #[test]
    fn bytes_to_string_converts_correctly() {
        let bytes = [0x41, 0x42, 0x43];
        assert_eq!(bytes_to_string(&bytes), "414243");
    }

    #[test]
    fn offset_to_file_name_formats_correctly() {
        assert_eq!(offset_to_file_name(123), "00000000000000000123");
    }

    #[test]
    fn ensure_dir_ok_creates_directory_if_not_exists() {
        let dir_name = "./test_dir";
        ensure_dir_ok(dir_name);
        assert!(is_path_exists(dir_name));
        std::fs::remove_dir(dir_name).unwrap();
    }

    #[test]
    fn test_compute_next_minutes_time_millis() {
        let next_minute = compute_next_minutes_time_millis();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        assert!(next_minute > now);
        assert_eq!(next_minute % (60 * 1000), 0);
    }

    /*    #[test]
    fn compute_next_morning_time_millis_returns_correct_time() {
        let now = Local::now();
        let next_morning = compute_next_morning_time_millis();
        let expected_next_morning = Local
            .ymd(now.year(), now.month(), now.day() + 1)
            .and_hms(0, 0, 0)
            .timestamp_millis();
        assert_eq!(next_morning, expected_next_morning as u64);
    }*/
    #[test]
    fn time_millis_to_human_string2_formats_correctly_with_valid_timestamp() {
        use chrono::TimeZone;
        use chrono::Utc;
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
}
