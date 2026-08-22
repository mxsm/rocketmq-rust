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
    pub(crate) const fn len(&self) -> usize {
        self.entry_count
    }

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
    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let start = self.cursor;
        let end = start.checked_add(length)?;
        let bytes = self.payload.get(start..end)?;
        self.cursor = end;
        Some(bytes)
    }

    #[inline]
    fn read_u32(&mut self) -> Option<usize> {
        let bytes: [u8; FIELD_LENGTH_BYTES] = self.take(FIELD_LENGTH_BYTES)?.try_into().ok()?;
        Some(u32::from_be_bytes(bytes) as usize)
    }

    #[inline]
    fn read_utf8(&mut self, length: usize) -> Option<&'a str> {
        std::str::from_utf8(self.take(length)?).ok()
    }

    #[inline]
    fn skip_json_whitespace(&mut self) {
        while matches!(self.payload.get(self.cursor), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.cursor += 1;
        }
    }

    #[inline]
    fn read_unescaped_json_string(&mut self) -> Option<&'a str> {
        self.skip_json_whitespace();
        if self.payload.get(self.cursor).copied()? != b'"' {
            return None;
        }
        self.cursor += 1;
        let start = self.cursor;
        let relative_end = memchr(b'"', self.payload.get(start..)?)?;
        let end = start.checked_add(relative_end)?;
        let value = std::str::from_utf8(self.payload.get(start..end)?).ok()?;
        self.cursor = end.checked_add(1)?;
        Some(value)
    }

    #[inline]
    fn next_unescaped_object(&mut self) -> Option<(&'a str, &'a str)> {
        self.skip_json_whitespace();
        if self.cursor >= self.payload.len() {
            return None;
        }
        if self.payload.get(self.cursor) == Some(&b',') {
            self.cursor += 1;
        }
        let key = self.read_unescaped_json_string()?;
        self.skip_json_whitespace();
        if self.payload.get(self.cursor).copied()? != b':' {
            return None;
        }
        self.cursor += 1;
        let value = self.read_unescaped_json_string()?;
        self.skip_json_whitespace();
        Some((key, value))
    }

    #[inline]
    fn next_canonical_unescaped_object(&mut self) -> Option<(&'a str, &'a str)> {
        if self.cursor >= self.payload.len() {
            return None;
        }
        if self.cursor != 0 {
            if self.payload.get(self.cursor).copied()? != b',' {
                return None;
            }
            self.cursor += 1;
        }
        if self.payload.get(self.cursor).copied()? != b'"' {
            return None;
        }
        self.cursor += 1;
        let key_start = self.cursor;
        let key_length = self.canonical_string_length()?;
        self.cursor = self.cursor.checked_add(key_length)?;
        let key_end = self.cursor;
        let value_prefix_end = self.cursor.checked_add(3)?;
        if self.payload.get(self.cursor..value_prefix_end)? != b"\":\"" {
            return None;
        }
        self.cursor = value_prefix_end;
        let value_start = self.cursor;
        let value_length = self.canonical_string_length()?;
        self.cursor = self.cursor.checked_add(value_length)?;
        let value_end = self.cursor;
        self.cursor = self.cursor.checked_add(1)?;
        Some((
            std::str::from_utf8(self.payload.get(key_start..key_end)?).ok()?,
            std::str::from_utf8(self.payload.get(value_start..value_end)?).ok()?,
        ))
    }

    #[inline(always)]
    fn canonical_string_length(&self) -> Option<usize> {
        memchr(b'"', self.payload.get(self.cursor..)?)
    }

    #[inline]
    fn next_length_prefixed(&mut self) -> Option<(&'a str, &'a str)> {
        if self.cursor >= self.payload.len() {
            return None;
        }
        let key_length = self.read_u32()?;
        let key = self.read_utf8(key_length)?;
        let value_length = self.read_u32()?;
        let value = self.read_utf8(value_length)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterates_all_valid_internal_encodings() {
        let mut length_prefixed = Vec::new();
        length_prefixed.extend_from_slice(&3u32.to_be_bytes());
        length_prefixed.extend_from_slice(b"key");
        length_prefixed.extend_from_slice(&5u32.to_be_bytes());
        length_prefixed.extend_from_slice(b"value");
        let fields = [
            JsonHeaderFields::from_length_prefixed(length_prefixed, 1),
            JsonHeaderFields::from_unescaped_object(Bytes::from_static(b" \"key\" : \"value\" "), 1),
            JsonHeaderFields::from_canonical_unescaped_object(Bytes::from_static(b"\"key\":\"value\""), 1),
        ];

        for fields in fields {
            assert_eq!(fields.iter().collect::<Vec<_>>(), vec![("key", "value")]);
        }
    }

    #[test]
    fn iterators_fail_closed_on_truncated_invalid_utf8_and_missing_terminators() {
        let fields = [
            JsonHeaderFields::from_length_prefixed(vec![0, 0, 0, 4, b'k'], 1),
            JsonHeaderFields::from_length_prefixed(vec![0, 0, 0, 1, 0xff, 0, 0, 0, 1, b'v'], 1),
            JsonHeaderFields::from_unescaped_object(Bytes::from_static(b"\"key\":\"value"), 1),
            JsonHeaderFields::from_unescaped_object(Bytes::from_static(b"\"key\"?\"value\""), 1),
            JsonHeaderFields::from_canonical_unescaped_object(Bytes::from_static(b"\"unterminated"), 1),
            JsonHeaderFields::from_canonical_unescaped_object(Bytes::from_static(b"\"key\"?\"value\""), 1),
        ];

        for fields in fields {
            assert!(fields.iter().next().is_none());
        }
    }
}
