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

use std::fmt;
use std::mem::size_of;

use bytes::Bytes;
use cheetah_string::CheetahString;
use memchr::memchr;
use serde::de::DeserializeSeed;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;

use super::private::FieldSourceSealed;
use super::HeaderFieldSource;
use crate::HeaderMap;

const FIELD_LENGTH_BYTES: usize = 4;
const ESTIMATED_FIELD_BYTES: usize = 32;
const MIN_INITIAL_PAYLOAD_CAPACITY: usize = 256;
const MAX_INITIAL_MAP_CAPACITY: usize = 1024;

/// A compact, immutable JSON extension-field payload.
///
/// Serde writes decoded key/value text into one length-prefixed byte buffer.
/// This preserves JSON empty strings and duplicate input order without
/// allocating one owned string or hash-table entry per field.
#[derive(Clone)]
pub(crate) struct JsonHeaderFields {
    payload: Bytes,
    entry_count: usize,
    encoding: JsonFieldEncoding,
}

#[derive(Clone, Copy)]
enum JsonFieldEncoding {
    LengthPrefixed,
    CanonicalUnescapedObject,
    UnescapedObject,
}

impl JsonHeaderFields {
    pub(crate) fn from_length_prefixed(payload: Vec<u8>, entry_count: usize) -> Self {
        Self {
            payload: Bytes::from(payload),
            entry_count,
            encoding: JsonFieldEncoding::LengthPrefixed,
        }
    }

    pub(crate) fn from_unescaped_object(payload: Bytes, entry_count: usize) -> Self {
        Self {
            payload,
            entry_count,
            encoding: JsonFieldEncoding::UnescapedObject,
        }
    }

    pub(crate) fn from_canonical_unescaped_object(payload: Bytes, entry_count: usize) -> Self {
        Self {
            payload,
            entry_count,
            encoding: JsonFieldEncoding::CanonicalUnescapedObject,
        }
    }

    pub(crate) fn materialize(&self) -> HeaderMap {
        let mut map = HeaderMap::with_capacity(self.entry_count.min(MAX_INITIAL_MAP_CAPACITY));
        for (key, value) in self.iter() {
            map.insert(CheetahString::from_slice(key), CheetahString::from_slice(value));
        }
        map
    }

    #[inline]
    fn iter(&self) -> JsonHeaderFieldIter<'_> {
        JsonHeaderFieldIter {
            payload: &self.payload,
            cursor: 0,
            encoding: self.encoding,
        }
    }
}

impl<'de> Deserialize<'de> for JsonHeaderFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(JsonHeaderFieldsVisitor)
    }
}

struct JsonHeaderFieldsVisitor;

impl<'de> Visitor<'de> for JsonHeaderFieldsVisitor {
    type Value = JsonHeaderFields;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object whose extension-field values are strings")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let initial_capacity = map
            .size_hint()
            .unwrap_or_default()
            .min(MAX_INITIAL_MAP_CAPACITY)
            .saturating_mul(ESTIMATED_FIELD_BYTES)
            .max(MIN_INITIAL_PAYLOAD_CAPACITY);
        let mut payload = Vec::with_capacity(initial_capacity);
        let mut entry_count = 0usize;

        while map
            .next_key_seed(JsonFieldTextSeed { payload: &mut payload })?
            .is_some()
        {
            map.next_value_seed(JsonFieldTextSeed { payload: &mut payload })?;
            entry_count = entry_count.saturating_add(1);
        }

        Ok(JsonHeaderFields {
            payload: Bytes::from(payload),
            entry_count,
            encoding: JsonFieldEncoding::LengthPrefixed,
        })
    }
}

struct JsonFieldTextSeed<'a> {
    payload: &'a mut Vec<u8>,
}

impl<'de> DeserializeSeed<'de> for JsonFieldTextSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(JsonFieldTextVisitor { payload: self.payload })
    }
}

struct JsonFieldTextVisitor<'a> {
    payload: &'a mut Vec<u8>,
}

impl JsonFieldTextVisitor<'_> {
    fn append<E>(self, value: &str) -> Result<(), E>
    where
        E: serde::de::Error,
    {
        let length = u32::try_from(value.len()).map_err(|_| E::custom("JSON extension-field text exceeds u32"))?;
        let additional = FIELD_LENGTH_BYTES
            .checked_add(value.len())
            .ok_or_else(|| E::custom("JSON extension-field length overflow"))?;
        self.payload.reserve(additional);
        self.payload.extend_from_slice(&length.to_be_bytes());
        self.payload.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

impl<'de> Visitor<'de> for JsonFieldTextVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON extension-field string")
    }

    #[inline]
    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.append(value)
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.append(value)
    }

    #[inline]
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.append(&value)
    }
}

impl FieldSourceSealed for JsonHeaderFields {}

impl HeaderFieldSource for JsonHeaderFields {
    #[inline]
    fn visit_fields_while<'a>(&'a self, visitor: &mut dyn FnMut(&'a str, &'a str) -> bool) {
        let mut iter = self.iter();
        match self.encoding {
            JsonFieldEncoding::CanonicalUnescapedObject => {
                while let Some((key, value)) = iter.next_canonical_unescaped_object() {
                    if !visitor(key, value) {
                        break;
                    }
                }
            }
            JsonFieldEncoding::UnescapedObject => {
                while let Some((key, value)) = iter.next_unescaped_object() {
                    if !visitor(key, value) {
                        break;
                    }
                }
            }
            JsonFieldEncoding::LengthPrefixed => {
                while let Some((key, value)) = iter.next_length_prefixed() {
                    if !visitor(key, value) {
                        break;
                    }
                }
            }
        }
    }

    #[inline]
    fn to_header_map(&self) -> HeaderMap {
        self.materialize()
    }
}

struct JsonHeaderFieldIter<'a> {
    payload: &'a [u8],
    cursor: usize,
    encoding: JsonFieldEncoding,
}

impl<'a> JsonHeaderFieldIter<'a> {
    #[inline]
    fn take(&mut self, length: usize) -> &'a [u8] {
        let start = self.cursor;
        let end = start + length;
        debug_assert!(end <= self.payload.len());
        self.cursor = end;
        // SAFETY: construction validates every length-prefixed field boundary,
        // and the immutable payload cannot change while this iterator is alive.
        unsafe { self.payload.get_unchecked(start..end) }
    }

    #[inline]
    fn read_u32(&mut self) -> usize {
        let bytes = self.take(FIELD_LENGTH_BYTES);
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
    }

    #[inline]
    fn read_utf8(&mut self, length: usize) -> &'a str {
        let bytes = self.take(length);
        // SAFETY: `JsonHeaderFields` is private and its payload is populated only
        // from strings accepted by Serde. The iterator preserves byte boundaries.
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    #[inline]
    fn skip_json_whitespace(&mut self) {
        while matches!(self.payload.get(self.cursor), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.cursor += 1;
        }
    }

    #[inline]
    fn read_unescaped_json_string(&mut self) -> &'a str {
        self.skip_json_whitespace();
        debug_assert_eq!(self.payload[self.cursor], b'"');
        self.cursor += 1;
        let start = self.cursor;
        // SAFETY: construction validates every string terminator and the
        // immutable payload cannot change while this iterator is alive.
        let relative_end = unsafe { memchr(b'"', &self.payload[self.cursor..]).unwrap_unchecked() };
        self.cursor += relative_end;
        let end = self.cursor;
        self.cursor += 1;
        // SAFETY: the fast JSON parser validates the full header as UTF-8 and
        // accepts this representation only when strings contain no escapes.
        unsafe { std::str::from_utf8_unchecked(&self.payload[start..end]) }
    }

    #[inline]
    fn next_unescaped_object(&mut self) -> Option<(&'a str, &'a str)> {
        self.skip_json_whitespace();
        if self.cursor >= self.payload.len() {
            return None;
        }
        if self.payload[self.cursor] == b',' {
            self.cursor += 1;
        }
        let key = self.read_unescaped_json_string();
        self.skip_json_whitespace();
        debug_assert_eq!(self.payload[self.cursor], b':');
        self.cursor += 1;
        let value = self.read_unescaped_json_string();
        self.skip_json_whitespace();
        Some((key, value))
    }

    #[inline]
    fn next_canonical_unescaped_object(&mut self) -> Option<(&'a str, &'a str)> {
        if self.cursor >= self.payload.len() {
            return None;
        }
        if self.cursor != 0 {
            debug_assert_eq!(self.payload[self.cursor], b',');
            self.cursor += 1;
        }
        debug_assert_eq!(self.payload[self.cursor], b'"');
        self.cursor += 1;
        let key_start = self.cursor;
        let key_length = self.canonical_string_length();
        self.cursor += key_length;
        let key_end = self.cursor;
        debug_assert_eq!(self.payload[self.cursor + 1], b':');
        debug_assert_eq!(self.payload[self.cursor + 2], b'"');
        self.cursor += 3;
        let value_start = self.cursor;
        let value_length = self.canonical_string_length();
        self.cursor += value_length;
        let value_end = self.cursor;
        self.cursor += 1;
        // SAFETY: the canonical fast parser validates both retained ranges as
        // UTF-8 and rejects escaped strings before constructing this source.
        Some(unsafe {
            (
                std::str::from_utf8_unchecked(self.payload.get_unchecked(key_start..key_end)),
                std::str::from_utf8_unchecked(self.payload.get_unchecked(value_start..value_end)),
            )
        })
    }

    #[inline(always)]
    fn canonical_string_length(&self) -> usize {
        let mut length = 0usize;
        let remaining = self.payload.len() - self.cursor;
        while remaining - length >= size_of::<u64>() {
            // SAFETY: the loop condition keeps the unaligned word read inside
            // the immutable payload.
            let word = unsafe {
                (self.payload.as_ptr().add(self.cursor + length) as *const u64)
                    .read_unaligned()
                    .to_le()
            };
            let quote_bytes = word ^ 0x2222_2222_2222_2222;
            let matches = quote_bytes.wrapping_sub(0x0101_0101_0101_0101) & !quote_bytes & 0x8080_8080_8080_8080;
            if matches != 0 {
                return length + matches.trailing_zeros() as usize / 8;
            }
            length += size_of::<u64>();
        }
        loop {
            // SAFETY: the canonical parser validates every retained string
            // terminator before constructing this immutable source.
            if unsafe { *self.payload.get_unchecked(self.cursor + length) } == b'"' {
                return length;
            }
            length += 1;
        }
    }

    #[inline]
    fn next_length_prefixed(&mut self) -> Option<(&'a str, &'a str)> {
        if self.cursor >= self.payload.len() {
            return None;
        }
        let key_length = self.read_u32();
        let key = self.read_utf8(key_length);
        let value_length = self.read_u32();
        let value = self.read_utf8(value_length);
        Some((key, value))
    }
}

impl<'a> Iterator for JsonHeaderFieldIter<'a> {
    type Item = (&'a str, &'a str);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.encoding {
            JsonFieldEncoding::CanonicalUnescapedObject => return self.next_canonical_unescaped_object(),
            JsonFieldEncoding::UnescapedObject => return self.next_unescaped_object(),
            JsonFieldEncoding::LengthPrefixed => {}
        }
        self.next_length_prefixed()
    }
}
