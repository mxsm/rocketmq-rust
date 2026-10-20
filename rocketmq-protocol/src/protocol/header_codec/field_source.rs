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

use bytes::Bytes;
use cheetah_string::CheetahString;

use super::private::FieldSourceSealed;
use super::text_runs::TextRuns;
use crate::HeaderMap;

const KEY_LENGTH_BYTES: usize = 2;
const VALUE_LENGTH_BYTES: usize = 4;
const MAX_INITIAL_MAP_CAPACITY: usize = 1024;

/// A validated, borrowed view of remoting command extension fields.
///
/// This trait is sealed because source implementations must guarantee that
/// every visited key and value is valid immutable UTF-8 for the full source
/// borrow. Returning `false` from the visitor stops the scan early.
pub trait HeaderFieldSource: FieldSourceSealed {
    /// Visits fields without allocating owned key/value strings.
    fn visit_fields_while<'a>(&'a self, visitor: &mut dyn FnMut(&'a str, &'a str) -> bool);

    /// Produces the owned compatibility representation.
    fn to_header_map(&self) -> HeaderMap;
}

impl FieldSourceSealed for HeaderMap {}

impl HeaderFieldSource for HeaderMap {
    #[inline]
    fn visit_fields_while<'a>(&'a self, visitor: &mut dyn FnMut(&'a str, &'a str) -> bool) {
        for (key, value) in self {
            if !visitor(key.as_str(), value.as_str()) {
                break;
            }
        }
    }

    #[inline]
    fn to_header_map(&self) -> HeaderMap {
        self.clone()
    }
}

#[cold]
#[inline(never)]
fn malformed_binary_fields(reason: &'static str) -> rocketmq_error::Error {
    crate::error::serialization_decode_failed("binary-header-fields", reason)
}

/// A validated, immutable ROCKETMQ extension-field payload.
///
/// Construction validates the complete payload before it is retained by a
/// remoting command. Subsequent scans therefore need no allocation and cannot
/// observe mutable bytes.
#[derive(Clone)]
pub(crate) struct BinaryHeaderFields {
    payload: Bytes,
    entry_count: usize,
}

impl BinaryHeaderFields {
    /// Validates and retains one complete extension-field payload.
    pub(crate) fn new(payload: Bytes) -> rocketmq_error::Result<Self> {
        let entry_count = Self::validate(&payload)?;
        Ok(Self { payload, entry_count })
    }

    pub(crate) const fn len(&self) -> usize {
        self.entry_count
    }

    /// Materializes the compatibility map. The payload has already been
    /// validated, so iteration uses its immutable representation invariant.
    pub(crate) fn materialize(&self) -> HeaderMap {
        let mut map = HeaderMap::with_capacity(self.entry_count.min(MAX_INITIAL_MAP_CAPACITY));
        for (key, value) in self.iter() {
            map.insert(CheetahString::from_slice(key), CheetahString::from_slice(value));
        }
        map
    }

    #[inline]
    fn iter(&self) -> BinaryHeaderFieldIter<'_> {
        BinaryHeaderFieldIter {
            payload: &self.payload,
            cursor: 0,
            text: TextRuns::new(&self.payload),
        }
    }

    fn validate(payload: &[u8]) -> rocketmq_error::Result<usize> {
        let mut cursor = 0usize;
        let mut entry_count = 0usize;
        let mut text = TextRuns::new(payload);
        while cursor < payload.len() {
            let key_length = Self::read_u16(payload, &mut cursor)?;
            if key_length == 0 {
                return Err(malformed_binary_fields("extension-field key is empty"));
            }
            Self::read_utf8(
                &mut text,
                payload,
                &mut cursor,
                key_length,
                "truncated extension-field key",
            )?;

            let value_length = Self::read_i32(payload, &mut cursor)?;
            if value_length < 0 {
                return Err(malformed_binary_fields("extension-field value length is negative"));
            }
            let value = Self::read_utf8(
                &mut text,
                payload,
                &mut cursor,
                value_length as usize,
                "truncated extension-field value",
            )?;

            // Java-compatible ROCKETMQ map decoding treats zero-length values
            // as absent rather than storing an empty string.
            if !value.is_empty() {
                entry_count = entry_count.saturating_add(1);
            }
        }
        Ok(entry_count)
    }

    fn read_u16(payload: &[u8], cursor: &mut usize) -> rocketmq_error::Result<usize> {
        let bytes = Self::take(payload, cursor, KEY_LENGTH_BYTES, "missing extension-field key length")?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]) as usize)
    }

    fn read_i32(payload: &[u8], cursor: &mut usize) -> rocketmq_error::Result<i32> {
        let bytes = Self::take(
            payload,
            cursor,
            VALUE_LENGTH_BYTES,
            "missing extension-field value length",
        )?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    #[inline(never)]
    fn read_utf8<'a>(
        text: &mut TextRuns<'a>,
        payload: &'a [u8],
        cursor: &mut usize,
        length: usize,
        truncated_reason: &'static str,
    ) -> rocketmq_error::Result<&'a str> {
        let start = *cursor;
        Self::take(payload, cursor, length, truncated_reason)?;
        text.text(start, *cursor)
            .ok_or_else(|| malformed_binary_fields("extension-field text is not valid UTF-8"))
    }

    fn take<'a>(
        payload: &'a [u8],
        cursor: &mut usize,
        length: usize,
        reason: &'static str,
    ) -> rocketmq_error::Result<&'a [u8]> {
        let end = cursor
            .checked_add(length)
            .ok_or_else(|| malformed_binary_fields(reason))?;
        let bytes = payload
            .get(*cursor..end)
            .ok_or_else(|| malformed_binary_fields(reason))?;
        *cursor = end;
        Ok(bytes)
    }
}

impl FieldSourceSealed for BinaryHeaderFields {}

impl HeaderFieldSource for BinaryHeaderFields {
    #[inline]
    fn visit_fields_while<'a>(&'a self, visitor: &mut dyn FnMut(&'a str, &'a str) -> bool) {
        for (key, value) in self.iter() {
            if !visitor(key, value) {
                break;
            }
        }
    }

    #[inline]
    fn to_header_map(&self) -> HeaderMap {
        self.materialize()
    }
}

struct BinaryHeaderFieldIter<'a> {
    payload: &'a [u8],
    cursor: usize,
    text: TextRuns<'a>,
}

impl<'a> BinaryHeaderFieldIter<'a> {
    #[inline]
    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let start = self.cursor;
        let end = start.checked_add(length)?;
        let bytes = self.payload.get(start..end)?;
        self.cursor = end;
        Some(bytes)
    }

    #[inline]
    fn read_u16(&mut self) -> Option<usize> {
        let bytes: [u8; KEY_LENGTH_BYTES] = self.take(KEY_LENGTH_BYTES)?.try_into().ok()?;
        Some(u16::from_be_bytes(bytes) as usize)
    }

    #[inline]
    fn read_i32(&mut self) -> Option<i32> {
        let bytes: [u8; VALUE_LENGTH_BYTES] = self.take(VALUE_LENGTH_BYTES)?.try_into().ok()?;
        Some(i32::from_be_bytes(bytes))
    }

    #[inline]
    fn read_utf8(&mut self, length: usize) -> Option<&'a str> {
        let start = self.cursor;
        let end = start.checked_add(length)?;
        let text = self.text.text(start, end)?;
        self.cursor = end;
        Some(text)
    }
}

impl<'a> Iterator for BinaryHeaderFieldIter<'a> {
    type Item = (&'a str, &'a str);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor < self.payload.len() {
            let key_length = self.read_u16()?;
            let key = self.read_utf8(key_length)?;
            let value_length = usize::try_from(self.read_i32()?).ok()?;
            let value = self.read_utf8(value_length)?;
            if !value.is_empty() {
                return Some((key, value));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use bytes::BytesMut;

    use super::*;

    fn entry(out: &mut BytesMut, key: &[u8], value: &[u8]) {
        out.put_u16(key.len() as u16);
        out.extend_from_slice(key);
        out.put_i32(value.len() as i32);
        out.extend_from_slice(value);
    }

    #[test]
    fn validates_and_materializes_duplicate_and_empty_values() {
        let mut payload = BytesMut::new();
        entry(&mut payload, b"key", b"first");
        entry(&mut payload, b"empty", b"");
        entry(&mut payload, b"key", b"last");

        let fields = BinaryHeaderFields::new(payload.freeze()).unwrap();
        let map = fields.materialize();

        assert_eq!(map.len(), 1);
        assert_eq!(map.get("key").map(CheetahString::as_str), Some("last"));
        assert!(!map.contains_key("empty"));
    }

    #[test]
    fn validates_and_materializes_two_binary_extension_fields() {
        let mut payload = BytesMut::new();
        entry(&mut payload, b"alpha", b"first");
        entry(&mut payload, b"zeta", b"last");

        let fields = BinaryHeaderFields::new(payload.freeze()).expect("valid binary extension fields");
        let map = fields.materialize();

        assert_eq!(map.get("alpha").map(CheetahString::as_str), Some("first"));
        assert_eq!(map.get("zeta").map(CheetahString::as_str), Some("last"));
    }

    #[test]
    fn rejects_truncated_negative_empty_key_and_invalid_utf8_payloads() {
        let invalid_payloads = [
            Bytes::from_static(&[0]),
            Bytes::from_static(&[0, 1]),
            Bytes::from_static(&[0, 1, b'k', 0, 0, 0]),
            Bytes::from_static(&[0, 1, b'k', 0xff, 0xff, 0xff, 0xff]),
            Bytes::from_static(&[0, 0, 0, 0, 0, 1, b'v']),
            Bytes::from_static(&[0, 1, 0xff, 0, 0, 0, 1, b'v']),
            Bytes::from_static(&[0, 1, b'k', 0, 0, 0, 1, 0xff]),
            Bytes::from_static(&[0, 1, b'k', 0, 0, 4, 0]),
        ];

        for payload in invalid_payloads {
            assert!(BinaryHeaderFields::new(payload).is_err());
        }
    }

    /// Deterministic generator so every run checks the same payloads.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next() % bound as u64) as usize
        }

        fn text(&mut self, bytes: usize) -> String {
            const ALPHABET: [&str; 6] = ["a", "Z", "7", "é", "主", "🚀"];
            // Mostly ASCII, as on the wire, with an occasional multibyte character.
            let ascii_only = self.below(4) != 0;
            let mut text = String::new();
            while text.len() < bytes {
                let choice = if ascii_only { self.below(3) } else { self.below(6) };
                text.push_str(ALPHABET[choice]);
            }
            text
        }
    }

    /// Reads fields by validating each key and value on its own, stopping at the first malformed entry.
    fn reference_fields(payload: &[u8], reject_empty_keys: bool) -> (Vec<(String, String)>, bool) {
        fn read<'a>(payload: &'a [u8], cursor: &mut usize, length: usize) -> Option<&'a [u8]> {
            let bytes = payload.get(*cursor..cursor.checked_add(length)?)?;
            *cursor += length;
            Some(bytes)
        }
        fn entry<'a>(payload: &'a [u8], cursor: &mut usize, reject_empty_keys: bool) -> Option<(&'a str, &'a str)> {
            let key_length = u16::from_be_bytes(read(payload, cursor, 2)?.try_into().ok()?) as usize;
            if reject_empty_keys && key_length == 0 {
                return None;
            }
            let key = std::str::from_utf8(read(payload, cursor, key_length)?).ok()?;
            let value_length = i32::from_be_bytes(read(payload, cursor, 4)?.try_into().ok()?);
            let value = std::str::from_utf8(read(payload, cursor, usize::try_from(value_length).ok()?)?).ok()?;
            Some((key, value))
        }

        let mut cursor = 0;
        let mut fields = Vec::new();
        while cursor < payload.len() {
            let Some((key, value)) = entry(payload, &mut cursor, reject_empty_keys) else {
                return (fields, false);
            };
            if !value.is_empty() {
                fields.push((key.to_owned(), value.to_owned()));
            }
        }
        (fields, true)
    }

    fn owned(fields: &BinaryHeaderFields) -> Vec<(String, String)> {
        fields
            .iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }

    #[test]
    fn run_based_text_matches_per_field_validation_for_valid_and_corrupted_payloads() {
        // Value lengths around 128 and 256 put bytes >= 0x80 into the length prefix.
        const VALUE_BYTES: [usize; 12] = [0, 1, 2, 9, 30, 127, 128, 129, 200, 255, 256, 300];
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);

        for _ in 0..3000 {
            let mut payload = BytesMut::new();
            for _ in 0..rng.below(9) {
                let key_bytes = 1 + rng.below(20);
                let key = rng.text(key_bytes);
                let value_bytes = VALUE_BYTES[rng.below(VALUE_BYTES.len())];
                let value = rng.text(value_bytes);
                entry(&mut payload, key.as_bytes(), value.as_bytes());
            }
            let payload = payload.freeze();

            let (expected, complete) = reference_fields(&payload, true);
            assert!(complete);
            let fields = BinaryHeaderFields::new(payload.clone()).expect("generated payload is valid");
            assert_eq!(fields.len(), expected.len());
            assert_eq!(owned(&fields), expected);

            if payload.is_empty() {
                continue;
            }
            for _ in 0..3 {
                let mut corrupted = payload.to_vec();
                for _ in 0..1 + rng.below(3) {
                    let index = rng.below(corrupted.len());
                    corrupted[index] = rng.next() as u8;
                }
                let corrupted = Bytes::from(corrupted);

                let (expected, complete) = reference_fields(&corrupted, true);
                match BinaryHeaderFields::new(corrupted.clone()) {
                    Ok(fields) => {
                        assert!(complete);
                        assert_eq!(owned(&fields), expected);
                    }
                    Err(_) => assert!(!complete),
                }

                // The iterator alone must stop exactly where per-field validation stops.
                let unchecked = BinaryHeaderFields {
                    payload: corrupted.clone(),
                    entry_count: 0,
                };
                assert_eq!(owned(&unchecked), reference_fields(&corrupted, false).0);
            }
        }
    }

    #[test]
    fn iterator_fails_closed_if_an_internal_payload_invariant_is_broken() {
        let malformed_payloads = [
            Bytes::from_static(&[0, 2, b'k']),
            Bytes::from_static(&[0, 1, b'k', 0xff, 0xff, 0xff, 0xff]),
            Bytes::from_static(&[0, 1, 0xff, 0, 0, 0, 1, b'v']),
        ];

        for payload in malformed_payloads {
            let fields = BinaryHeaderFields {
                payload,
                entry_count: 1,
            };
            assert!(fields.iter().next().is_none());
        }
    }
}
