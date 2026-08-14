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
fn malformed_binary_fields(reason: &'static str) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
        format: "binary-header-fields",
        message: reason.to_string(),
    })
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
    pub(crate) fn new(payload: Bytes) -> rocketmq_error::RocketMQResult<Self> {
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
        }
    }

    fn validate(payload: &[u8]) -> rocketmq_error::RocketMQResult<usize> {
        let mut cursor = 0usize;
        let mut entry_count = 0usize;
        while cursor < payload.len() {
            let key_length = Self::read_u16(payload, &mut cursor)?;
            if key_length == 0 {
                return Err(malformed_binary_fields("extension-field key is empty"));
            }
            Self::read_utf8(payload, &mut cursor, key_length, "truncated extension-field key")?;

            let value_length = Self::read_i32(payload, &mut cursor)?;
            if value_length < 0 {
                return Err(malformed_binary_fields("extension-field value length is negative"));
            }
            let value = Self::read_utf8(
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

    fn read_u16(payload: &[u8], cursor: &mut usize) -> rocketmq_error::RocketMQResult<usize> {
        let bytes = Self::take(payload, cursor, KEY_LENGTH_BYTES, "missing extension-field key length")?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]) as usize)
    }

    fn read_i32(payload: &[u8], cursor: &mut usize) -> rocketmq_error::RocketMQResult<i32> {
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
        payload: &'a [u8],
        cursor: &mut usize,
        length: usize,
        truncated_reason: &'static str,
    ) -> rocketmq_error::RocketMQResult<&'a str> {
        let bytes = Self::take(payload, cursor, length, truncated_reason)?;
        if bytes.is_ascii() {
            // SAFETY: ASCII is valid UTF-8.
            return Ok(unsafe { std::str::from_utf8_unchecked(bytes) });
        }
        std::str::from_utf8(bytes).map_err(|_| malformed_binary_fields("extension-field text is not valid UTF-8"))
    }

    fn take<'a>(
        payload: &'a [u8],
        cursor: &mut usize,
        length: usize,
        reason: &'static str,
    ) -> rocketmq_error::RocketMQResult<&'a [u8]> {
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
}

impl<'a> BinaryHeaderFieldIter<'a> {
    #[inline]
    fn take(&mut self, length: usize) -> &'a [u8] {
        let start = self.cursor;
        let end = start + length;
        self.cursor = end;
        // SAFETY: BinaryHeaderFields::new validates every length and boundary
        // before retaining this immutable payload. The iterator advances using
        // exactly the same wire lengths and never exposes mutable access.
        unsafe { self.payload.get_unchecked(start..end) }
    }

    #[inline]
    fn read_u16(&mut self) -> usize {
        let bytes = self.take(KEY_LENGTH_BYTES);
        u16::from_be_bytes([bytes[0], bytes[1]]) as usize
    }

    #[inline]
    fn read_i32(&mut self) -> i32 {
        let bytes = self.take(VALUE_LENGTH_BYTES);
        i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    #[inline]
    fn read_utf8(&mut self, length: usize) -> &'a str {
        let bytes = self.take(length);
        // SAFETY: BinaryHeaderFields::new validates UTF-8 for every key and
        // value, and the immutable payload boundaries are preserved by take.
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }
}

impl<'a> Iterator for BinaryHeaderFieldIter<'a> {
    type Item = (&'a str, &'a str);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor < self.payload.len() {
            let key_length = self.read_u16();
            let key = self.read_utf8(key_length);
            let value_length = self.read_i32() as usize;
            let value = self.read_utf8(value_length);
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
    fn rejects_truncated_negative_empty_key_and_invalid_utf8_payloads() {
        let invalid_payloads = [
            Bytes::from_static(&[0]),
            Bytes::from_static(&[0, 1]),
            Bytes::from_static(&[0, 1, b'k', 0, 0, 0]),
            Bytes::from_static(&[0, 1, b'k', 0xff, 0xff, 0xff, 0xff]),
            Bytes::from_static(&[0, 0, 0, 0, 0, 1, b'v']),
            Bytes::from_static(&[0, 1, 0xff, 0, 0, 0, 1, b'v']),
            Bytes::from_static(&[0, 1, b'k', 0, 0, 0, 1, 0xff]),
        ];

        for payload in invalid_payloads {
            assert!(BinaryHeaderFields::new(payload).is_err());
        }
    }
}
