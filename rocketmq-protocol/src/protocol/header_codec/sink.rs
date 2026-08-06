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

use bytes::BufMut;
use bytes::BytesMut;
use cheetah_string::CheetahString;

use super::private::Sealed;
use super::write_json_string;
use super::HeaderCodecError;
use super::HeaderFieldContext;
use super::HeaderValue;
use crate::protocol::command_custom_header::HeaderMap;

/// A statically dispatched destination for typed header fields.
///
/// This trait is sealed. Protocol-owned implementations preserve identical
/// field semantics while allowing LLVM to specialize map and future binary
/// writes without a trait object in the per-field path.
pub trait EncodeSink: Sealed {
    /// Writes one canonical field.
    ///
    /// The caller is responsible for header validation and Java range checks
    /// before this method. Sinks must not duplicate those policy decisions.
    ///
    /// # Errors
    ///
    /// Returns a classified codec error when the destination wire format cannot
    /// represent the key or value. Map writes are currently infallible.
    fn write<V: HeaderValue>(
        &mut self,
        key: &'static str,
        value: &V,
        context: HeaderFieldContext,
    ) -> Result<(), HeaderCodecError>;
}

/// An [`EncodeSink`] that appends typed fields directly to a [`HeaderMap`].
pub struct MapSink<'a> {
    out: &'a mut HeaderMap,
}

impl<'a> MapSink<'a> {
    /// Creates a map sink over an existing destination.
    #[inline]
    pub const fn new(out: &'a mut HeaderMap) -> Self {
        Self { out }
    }

    /// Returns the borrowed destination after the sink is no longer needed.
    #[inline]
    pub fn into_inner(self) -> &'a mut HeaderMap {
        self.out
    }
}

impl Sealed for MapSink<'_> {}

impl EncodeSink for MapSink<'_> {
    #[inline]
    fn write<V: HeaderValue>(
        &mut self,
        key: &'static str,
        value: &V,
        _context: HeaderFieldContext,
    ) -> Result<(), HeaderCodecError> {
        self.out
            .insert(CheetahString::from_static_str(key), value.to_map_value());
        Ok(())
    }
}

/// An [`EncodeSink`] that writes one JSON extension-field object directly.
///
/// All RocketMQ extension-field values remain JSON strings, including scalar
/// Rust fields. String escaping is performed directly into the destination.
pub struct JsonSink<'a> {
    out: &'a mut BytesMut,
    first: bool,
}

impl<'a> JsonSink<'a> {
    /// Starts a JSON object in `out`.
    #[inline]
    pub fn new(out: &'a mut BytesMut) -> Self {
        out.extend_from_slice(b"{");
        Self { out, first: true }
    }

    /// Completes the JSON object and returns the destination.
    #[inline]
    pub fn finish(self) -> &'a mut BytesMut {
        self.out.extend_from_slice(b"}");
        self.out
    }
}

impl Sealed for JsonSink<'_> {}

impl EncodeSink for JsonSink<'_> {
    #[inline]
    fn write<V: HeaderValue>(
        &mut self,
        key: &'static str,
        value: &V,
        _context: HeaderFieldContext,
    ) -> Result<(), HeaderCodecError> {
        if self.first {
            self.first = false;
        } else {
            self.out.extend_from_slice(b",");
        }
        write_json_string(self.out, key);
        self.out.extend_from_slice(b":");
        value.write_json_string(self.out);
        Ok(())
    }
}

/// An [`EncodeSink`] that writes canonical extension fields directly to a
/// ROCKETMQ binary payload.
///
/// Each field uses a big-endian `u16` key length followed by a big-endian
/// signed Java `int` value length. Scalar values are appended through
/// [`HeaderValue::write_ascii`] without allocating an intermediate string.
pub struct BinarySink<'a> {
    out: &'a mut BytesMut,
}

impl<'a> BinarySink<'a> {
    /// Creates a binary sink over an existing destination.
    #[inline]
    pub fn new(out: &'a mut BytesMut) -> Self {
        Self { out }
    }

    /// Returns the borrowed destination after the sink is no longer needed.
    #[inline]
    pub fn into_inner(self) -> &'a mut BytesMut {
        self.out
    }
}

impl Sealed for BinarySink<'_> {}

impl EncodeSink for BinarySink<'_> {
    #[inline]
    fn write<V: HeaderValue>(
        &mut self,
        key: &'static str,
        value: &V,
        context: HeaderFieldContext,
    ) -> Result<(), HeaderCodecError> {
        let key_len = u16::try_from(key.len()).map_err(|_| HeaderCodecError::KeyLengthOverflow {
            header: context.header,
            key: context.key,
        })?;
        let value_len_hint = value.encoded_len();
        if value_len_hint > i32::MAX as usize {
            return Err(HeaderCodecError::ValueLengthOverflow {
                header: context.header,
                key: context.key,
            });
        }

        let pair_len = 2usize
            .checked_add(key.len())
            .and_then(|len| len.checked_add(4))
            .and_then(|len| len.checked_add(value_len_hint))
            .ok_or(HeaderCodecError::ValueLengthOverflow {
                header: context.header,
                key: context.key,
            })?;
        let pair_start = self.out.len();
        self.out.reserve(pair_len);
        self.out.put_u16(key_len);
        self.out.extend_from_slice(key.as_bytes());

        let value_len_offset = self.out.len();
        self.out.put_i32(0);
        let value_offset = self.out.len();
        value.write_ascii(self.out);
        let actual_value_len = self.out.len() - value_offset;
        debug_assert_eq!(actual_value_len, value_len_hint);
        let actual_value_len = i32::try_from(actual_value_len).map_err(|_| {
            self.out.truncate(pair_start);
            HeaderCodecError::ValueLengthOverflow {
                header: context.header,
                key: context.key,
            }
        })?;
        self.out[value_len_offset..value_len_offset + 4].copy_from_slice(&actual_value_len.to_be_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::header_codec::HeaderValueKind;

    const CONTEXT: HeaderFieldContext =
        HeaderFieldContext::new("ExampleHeader", "topic", HeaderValueKind::String, None);

    #[test]
    fn writes_into_existing_map_without_an_intermediate_map() {
        let mut fields = HeaderMap::with_capacity(2);
        fields.insert(
            CheetahString::from_static_str("existing"),
            CheetahString::from_static_str("value"),
        );

        let mut sink = MapSink::new(&mut fields);
        sink.write("topic", &CheetahString::from("测试-topic"), CONTEXT)
            .unwrap();
        let fields = sink.into_inner();

        assert_eq!(fields.len(), 2);
        assert_eq!(fields.get("topic").map(CheetahString::as_str), Some("测试-topic"));
        assert_eq!(fields.get("existing").map(CheetahString::as_str), Some("value"));
    }

    #[test]
    fn canonical_write_replaces_an_existing_value() {
        let mut fields = HeaderMap::new();
        fields.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("old"),
        );

        MapSink::new(&mut fields)
            .write("topic", &String::from("new"), CONTEXT)
            .unwrap();

        assert_eq!(fields.get("topic").map(CheetahString::as_str), Some("new"));
    }

    #[test]
    fn scalar_write_uses_the_header_value_canonical_form() {
        let mut fields = HeaderMap::new();
        let mut sink = MapSink::new(&mut fields);

        sink.write(
            "queueOffset",
            &u64::MAX,
            HeaderFieldContext::new("ExampleHeader", "queueOffset", HeaderValueKind::U64, None),
        )
        .unwrap();

        assert_eq!(
            fields.get("queueOffset").map(CheetahString::as_str),
            Some("18446744073709551615")
        );
    }

    #[test]
    fn binary_sink_appends_canonical_pairs_without_replacing_existing_bytes() {
        let mut out = BytesMut::from(&b"prefix"[..]);
        let mut sink = BinarySink::new(&mut out);
        sink.write("queueOffset", &-42_i64, CONTEXT).unwrap();
        let out = sink.into_inner();

        assert_eq!(out.len() - 6, 2 + 11 + 4 + 3);
        assert_eq!(&out[..6], b"prefix");
        assert_eq!(u16::from_be_bytes(out[6..8].try_into().unwrap()), 11);
        assert_eq!(&out[8..19], b"queueOffset");
        assert_eq!(i32::from_be_bytes(out[19..23].try_into().unwrap()), 3);
        assert_eq!(&out[23..], b"-42");
    }

    #[test]
    fn binary_sink_rejects_an_oversized_key_without_mutating_the_destination() {
        let key = Box::leak("k".repeat(u16::MAX as usize + 1).into_boxed_str());
        let mut out = BytesMut::from(&b"prefix"[..]);
        let error = BinarySink::new(&mut out).write(key, &true, CONTEXT).unwrap_err();

        assert!(matches!(error, HeaderCodecError::KeyLengthOverflow { .. }));
        assert_eq!(out.as_ref(), b"prefix");
    }

    #[test]
    fn json_sink_writes_string_scalars_and_escapes_text_without_allocating_values() {
        let mut out = BytesMut::new();
        let mut sink = JsonSink::new(&mut out);
        sink.write("topic", &CheetahString::from("主题\"\\\n"), CONTEXT)
            .unwrap();
        sink.write(
            "queueOffset",
            &-42_i64,
            HeaderFieldContext::new("ExampleHeader", "queueOffset", HeaderValueKind::I64, None),
        )
        .unwrap();
        sink.write(
            "enabled",
            &true,
            HeaderFieldContext::new("ExampleHeader", "enabled", HeaderValueKind::Bool, None),
        )
        .unwrap();
        sink.finish();

        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["topic"], "主题\"\\\n");
        assert_eq!(value["queueOffset"], "-42");
        assert_eq!(value["enabled"], "true");
    }
}
