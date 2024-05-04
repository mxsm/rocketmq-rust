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

use chrono::{Datelike, TimeZone, Timelike, Utc};

const HEX_ARRAY: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
];

pub fn bytes_to_string(src: &[u8]) -> String {
    let mut hex_chars = Vec::with_capacity(src.len() * 2);
    for &byte in src {
        let v = byte as usize;
        hex_chars.push(HEX_ARRAY[v >> 4]);
        hex_chars.push(HEX_ARRAY[v & 0x0F]);
    }
    hex_chars.into_iter().collect()
}

fn string_to_bytes(hex_string: impl Into<String>) -> Option<Vec<u8>> {
    let hex_string = hex_string.into();
    if hex_string.is_empty() {
        return None;
    }

    let hex_string = hex_string.to_uppercase();
    let length = hex_string.len() / 2;
    let mut bytes = Vec::<u8>::with_capacity(length);

    for i in 0..length {
        let pos = i * 2;
        let byte = char_to_byte(hex_string.chars().nth(pos)?) << 4
            | char_to_byte(hex_string.chars().nth(pos + 1)?);

        bytes.push(byte);
    }

    Some(bytes)
}

fn char_to_byte(c: char) -> u8 {
    let hex_chars = "0123456789ABCDEF";
    hex_chars.find(c).unwrap_or(0) as u8
}

pub fn time_millis_to_human_string2(t: i64) -> String {
    let dt = Utc.timestamp_millis_opt(t).unwrap();
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02},{:03}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.timestamp_subsec_millis(),
    )
}

pub fn time_millis_to_human_string3(t: i64) -> String {
    let dt = Utc.timestamp_millis_opt(t).unwrap();
    format!(
        "{:04}{:02}{:02}{:02}{:02}{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
    )
}

pub fn offset_to_file_name(offset: u64) -> String {
    format!("{:020}", offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_string() {
        let src = b"hello";
        let expected = "68656C6C6F";
        let result = bytes_to_string(src);
        assert_eq!(result, String::from(expected));
    }

    #[test]
    fn test_bytes_to_string_empty() {
        let src = &[];
        let expected = "";
        let result = bytes_to_string(src);
        assert_eq!(result, String::from(expected));
    }

    #[test]
    fn test_bytes_to_string_large() {
        let src = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let expected = "6162636465666768696A6B6C6D6E6F707172737475767778797A4142434445464748494A4B4C4D4E4F505152535455565758595A";
        let result = bytes_to_string(src);
        assert_eq!(result, String::from(expected));
    }

    #[test]
    fn test_offset_to_file_name() {
        assert_eq!(offset_to_file_name(0), "00000000000000000000");
        assert_eq!(
            offset_to_file_name(2000000000000000000),
            "02000000000000000000"
        );
    }
}
