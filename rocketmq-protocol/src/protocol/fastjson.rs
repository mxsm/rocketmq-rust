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

/// Quotes signed decimal object keys emitted by Java FastJSON, leaving string literals intact.
pub(super) fn quote_unquoted_numeric_object_keys(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len() + 8);
    let mut last_copied = 0;
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    let mut changed = false;

    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }

        if byte == b'"' {
            in_string = true;
            index += 1;
            continue;
        }

        if byte == b'{' || byte == b',' {
            let mut key_start = index + 1;
            while key_start < bytes.len() && bytes[key_start].is_ascii_whitespace() {
                key_start += 1;
            }

            let mut digit_start = key_start;
            if digit_start < bytes.len() && bytes[digit_start] == b'-' {
                digit_start += 1;
            }

            if digit_start < bytes.len() && bytes[digit_start].is_ascii_digit() {
                let mut digit_end = digit_start + 1;
                while digit_end < bytes.len() && bytes[digit_end].is_ascii_digit() {
                    digit_end += 1;
                }

                let mut colon_index = digit_end;
                while colon_index < bytes.len() && bytes[colon_index].is_ascii_whitespace() {
                    colon_index += 1;
                }

                if colon_index < bytes.len() && bytes[colon_index] == b':' {
                    output.push_str(&input[last_copied..key_start]);
                    output.push('"');
                    output.push_str(&input[key_start..digit_end]);
                    output.push('"');
                    last_copied = digit_end;
                    changed = true;
                    index = digit_end;
                    continue;
                }
            }
        }

        index += 1;
    }

    if changed {
        output.push_str(&input[last_copied..]);
        Some(output)
    } else {
        None
    }
}
