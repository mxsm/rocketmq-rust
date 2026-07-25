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

pub const NAME_VALUE_SEPARATOR: char = '\u{0001}';
pub const PROPERTY_SEPARATOR: char = '\u{0002}';

use std::collections::HashMap;

use cheetah_string::CheetahString;

const HEX: &[u8; 16] = b"0123456789ABCDEF";

pub fn bytes_to_string(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 0x0f) as usize] as char);
    }
    value
}

pub fn string_to_bytes(value: &str) -> Option<Vec<u8>> {
    if value.is_empty() || !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = hex_value(pair[0])?;
            let low = hex_value(pair[1])?;
            Some((high << 4) | low)
        })
        .collect()
}

pub fn write_int(buffer: &mut [char], position: usize, value: i32) {
    for (offset, shift) in (0..8).zip((0..=28).rev().step_by(4)) {
        buffer[position + offset] = HEX[((value >> shift) & 0x0f) as usize] as char;
    }
}

pub fn write_short(buffer: &mut [char], position: usize, value: i16) {
    for (offset, shift) in (0..4).zip((0..=12).rev().step_by(4)) {
        buffer[position + offset] = HEX[((value >> shift) & 0x0f) as usize] as char;
    }
}

pub fn message_properties_to_string(properties: &HashMap<CheetahString, CheetahString>) -> CheetahString {
    let mut value = String::new();
    for (name, property) in properties {
        value.push_str(name);
        value.push(NAME_VALUE_SEPARATOR);
        value.push_str(property);
        value.push(PROPERTY_SEPARATOR);
    }
    CheetahString::from_string_owned(value)
}

pub fn string_to_message_properties(properties: Option<&CheetahString>) -> HashMap<CheetahString, CheetahString> {
    let Some(properties) = properties else {
        return HashMap::new();
    };
    properties
        .split(PROPERTY_SEPARATOR)
        .filter_map(|entry| {
            let (name, value) = entry.split_once(NAME_VALUE_SEPARATOR)?;
            (!name.is_empty()).then(|| (CheetahString::from_slice(name), CheetahString::from_slice(value)))
        })
        .collect()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
