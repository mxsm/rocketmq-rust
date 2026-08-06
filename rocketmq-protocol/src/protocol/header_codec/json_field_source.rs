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

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
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
}

impl JsonHeaderFields {
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
            .saturating_mul(ESTIMATED_FIELD_BYTES);
        let mut payload = BytesMut::with_capacity(initial_capacity);
        let mut entry_count = 0usize;

        while map
            .next_key_seed(JsonFieldTextSeed { payload: &mut payload })?
            .is_some()
        {
            map.next_value_seed(JsonFieldTextSeed { payload: &mut payload })?;
            entry_count = entry_count.saturating_add(1);
        }

        Ok(JsonHeaderFields {
            payload: payload.freeze(),
            entry_count,
        })
    }
}

struct JsonFieldTextSeed<'a> {
    payload: &'a mut BytesMut,
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
    payload: &'a mut BytesMut,
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
        let reserve = if self.payload.capacity() == 0 {
            additional.max(MIN_INITIAL_PAYLOAD_CAPACITY)
        } else {
            additional
        };
        self.payload.reserve(reserve);
        self.payload.put_u32(length);
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

struct JsonHeaderFieldIter<'a> {
    payload: &'a [u8],
    cursor: usize,
}

impl<'a> JsonHeaderFieldIter<'a> {
    #[inline]
    fn take(&mut self, length: usize) -> &'a [u8] {
        let end = self
            .cursor
            .checked_add(length)
            .expect("validated JSON header field offset overflowed");
        let bytes = self
            .payload
            .get(self.cursor..end)
            .expect("validated JSON header field boundary changed");
        self.cursor = end;
        bytes
    }

    #[inline]
    fn read_u32(&mut self) -> usize {
        let bytes = self.take(FIELD_LENGTH_BYTES);
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
    }

    #[inline]
    fn read_utf8(&mut self, length: usize) -> &'a str {
        std::str::from_utf8(self.take(length)).expect("validated JSON header field UTF-8 changed")
    }
}

impl<'a> Iterator for JsonHeaderFieldIter<'a> {
    type Item = (&'a str, &'a str);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
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
