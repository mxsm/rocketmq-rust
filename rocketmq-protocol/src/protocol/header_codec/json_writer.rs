// Copyright 2026 The RocketMQ Rust Authors
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

use bytes::BytesMut;

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Appends one JSON string without allocating a temporary escaped value.
pub(crate) fn write_json_string(out: &mut BytesMut, value: &str) {
    out.extend_from_slice(b"\"");
    let bytes = value.as_bytes();
    if bytes
        .iter()
        .all(|byte| *byte >= 0x20 && *byte != b'"' && *byte != b'\\')
    {
        out.extend_from_slice(bytes);
        out.extend_from_slice(b"\"");
        return;
    }
    let mut copied_until = 0usize;

    for (index, byte) in bytes.iter().copied().enumerate() {
        let escape = match byte {
            b'"' => Some(b"\\\"".as_slice()),
            b'\\' => Some(b"\\\\".as_slice()),
            b'\x08' => Some(b"\\b".as_slice()),
            b'\t' => Some(b"\\t".as_slice()),
            b'\n' => Some(b"\\n".as_slice()),
            b'\x0c' => Some(b"\\f".as_slice()),
            b'\r' => Some(b"\\r".as_slice()),
            0x00..=0x1f => {
                out.extend_from_slice(&bytes[copied_until..index]);
                out.extend_from_slice(b"\\u00");
                out.extend_from_slice(&[HEX[(byte >> 4) as usize], HEX[(byte & 0x0f) as usize]]);
                copied_until = index + 1;
                continue;
            }
            _ => None,
        };

        if let Some(escape) = escape {
            out.extend_from_slice(&bytes[copied_until..index]);
            out.extend_from_slice(escape);
            copied_until = index + 1;
        }
    }

    out.extend_from_slice(&bytes[copied_until..]);
    out.extend_from_slice(b"\"");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_string_writer_matches_serde_json_escaping() {
        for value in [
            "plain",
            "quote\"slash\\",
            "line\nfeed\ttab\rcarriage\u{0008}back\u{000c}form",
            "\u{0000}\u{001f}",
            "RocketMQ-主题-🚀",
        ] {
            let mut actual = BytesMut::new();
            write_json_string(&mut actual, value);
            assert_eq!(actual.as_ref(), serde_json::to_string(value).unwrap().as_bytes());
        }
    }
}
