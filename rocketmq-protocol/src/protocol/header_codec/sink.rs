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
use super::HeaderFieldContext;
use super::HeaderValue;
use super::ProtocolContractViolation;
use crate::protocol::command_custom_header::HeaderMap;

/// A canonical wire key together with the encodings a sink would otherwise
/// rebuild on every write.
///
/// Derive output creates one constant per field. Both encodings are checked
/// against the key when that constant is evaluated, so a sink can append them
/// without inspecting the key again.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeaderFieldKey {
    name: &'static str,
    binary: &'static [u8],
    json: &'static [u8],
}

impl HeaderFieldKey {
    /// Creates a key from its prepared encodings.
    ///
    /// `binary` is the big-endian `u16` key length followed by the key bytes.
    /// `json` is the quoted key followed by `:` when the key needs no JSON
    /// escaping, and empty otherwise.
    ///
    /// # Panics
    ///
    /// Panics when an encoding does not describe `name`. Generated codecs
    /// evaluate this in a constant, which turns a mismatch into a compile error.
    #[doc(hidden)]
    pub const fn new(name: &'static str, binary: &'static [u8], json: &'static [u8]) -> Self {
        let key = name.as_bytes();
        assert!(
            key.len() <= u16::MAX as usize,
            "wire key does not fit the ROCKETMQ key length"
        );
        assert!(binary.len() == key.len() + 2, "binary key has the wrong length");
        let length = (key.len() as u16).to_be_bytes();
        assert!(
            binary[0] == length[0] && binary[1] == length[1],
            "binary key has the wrong length prefix"
        );
        let mut index = 0;
        while index < key.len() {
            assert!(binary[index + 2] == key[index], "binary key does not contain the key");
            index += 1;
        }
        if !json.is_empty() {
            assert!(json.len() == key.len() + 3, "JSON key has the wrong length");
            assert!(
                json[0] == b'"' && json[key.len() + 1] == b'"' && json[key.len() + 2] == b':',
                "JSON key is not a quoted member name"
            );
            let mut index = 0;
            while index < key.len() {
                let byte = key[index];
                assert!(
                    byte >= 0x20 && byte != b'"' && byte != b'\\',
                    "JSON key requires escaping"
                );
                assert!(json[index + 1] == byte, "JSON key does not contain the key");
                index += 1;
            }
        }
        Self { name, binary, json }
    }

    /// Returns the canonical wire key.
    #[inline]
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

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
    ) -> Result<(), ProtocolContractViolation>;

    /// Writes one canonical field identified by a prepared key.
    ///
    /// The output is identical to [`Self::write`] with [`HeaderFieldKey::name`].
    /// Sinks override this method to append the prepared encoding directly.
    ///
    /// # Errors
    ///
    /// Returns the same classified errors as [`Self::write`].
    #[doc(hidden)]
    #[inline]
    fn write_field<V: HeaderValue>(
        &mut self,
        key: HeaderFieldKey,
        value: &V,
        context: HeaderFieldContext,
    ) -> Result<(), ProtocolContractViolation> {
        self.write(key.name, value, context)
    }
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
    ) -> Result<(), ProtocolContractViolation> {
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
    ) -> Result<(), ProtocolContractViolation> {
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

    #[inline]
    fn write_field<V: HeaderValue>(
        &mut self,
        key: HeaderFieldKey,
        value: &V,
        context: HeaderFieldContext,
    ) -> Result<(), ProtocolContractViolation> {
        if key.json.is_empty() {
            return self.write(key.name, value, context);
        }
        if self.first {
            self.first = false;
        } else {
            self.out.extend_from_slice(b",");
        }
        self.out.extend_from_slice(key.json);
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
    ) -> Result<(), ProtocolContractViolation> {
        let key_len = u16::try_from(key.len()).map_err(|_| ProtocolContractViolation::KeyLengthOverflow {
            header: context.header,
            key: context.key,
        })?;
        if !V::ALWAYS_FITS_WIRE_LENGTH && value.encoded_len() > i32::MAX as usize {
            return Err(ProtocolContractViolation::ValueLengthOverflow {
                header: context.header,
                key: context.key,
            });
        }

        let pair_start = self.out.len();
        self.out.put_u16(key_len);
        self.out.extend_from_slice(key.as_bytes());

        let value_len_offset = self.out.len();
        self.out.put_i32(0);
        let value_offset = self.out.len();
        value.write_ascii(self.out);
        let actual_value_len = self.out.len() - value_offset;
        let actual_value_len = i32::try_from(actual_value_len).map_err(|_| {
            self.out.truncate(pair_start);
            ProtocolContractViolation::ValueLengthOverflow {
                header: context.header,
                key: context.key,
            }
        })?;
        self.out[value_len_offset..value_len_offset + 4].copy_from_slice(&actual_value_len.to_be_bytes());
        Ok(())
    }

    #[inline]
    fn write_field<V: HeaderValue>(
        &mut self,
        key: HeaderFieldKey,
        value: &V,
        context: HeaderFieldContext,
    ) -> Result<(), ProtocolContractViolation> {
        if value.write_binary_pair(self.out, key.binary) {
            Ok(())
        } else {
            Err(ProtocolContractViolation::ValueLengthOverflow {
                header: context.header,
                key: context.key,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_model::boundary_type::BoundaryType;

    use super::*;
    use crate::protocol::header_codec::HeaderValueKind;

    const CONTEXT: HeaderFieldContext =
        HeaderFieldContext::new("ExampleHeader", "topic", HeaderValueKind::String, None);
    const TOPIC: HeaderFieldKey = HeaderFieldKey::new("topic", b"\x00\x05topic", b"\"topic\":");

    fn json_fields(write: impl FnOnce(&mut JsonSink<'_>)) -> BytesMut {
        let mut out = BytesMut::new();
        let mut sink = JsonSink::new(&mut out);
        write(&mut sink);
        sink.finish();
        out
    }

    fn assert_prepared_key_matches_plain_key<V: HeaderValue>(value: &V) {
        let mut plain = BytesMut::from(&b"prefix"[..]);
        BinarySink::new(&mut plain).write("topic", value, CONTEXT).unwrap();
        let mut prepared = BytesMut::from(&b"prefix"[..]);
        BinarySink::new(&mut prepared)
            .write_field(TOPIC, value, CONTEXT)
            .unwrap();
        assert_eq!(prepared, plain);

        assert_eq!(
            json_fields(|sink| sink.write_field(TOPIC, value, CONTEXT).unwrap()),
            json_fields(|sink| sink.write("topic", value, CONTEXT).unwrap())
        );
        assert_eq!(
            json_fields(|sink| {
                sink.write("first", &true, CONTEXT).unwrap();
                sink.write_field(TOPIC, value, CONTEXT).unwrap();
            }),
            json_fields(|sink| {
                sink.write("first", &true, CONTEXT).unwrap();
                sink.write("topic", value, CONTEXT).unwrap();
            })
        );

        let mut plain = HeaderMap::new();
        MapSink::new(&mut plain).write("topic", value, CONTEXT).unwrap();
        let mut prepared = HeaderMap::new();
        MapSink::new(&mut prepared).write_field(TOPIC, value, CONTEXT).unwrap();
        assert_eq!(prepared, plain);
    }

    #[test]
    fn prepared_keys_write_the_same_output_as_plain_keys() {
        assert_prepared_key_matches_plain_key(&CheetahString::from("主题\"\\\n"));
        assert_prepared_key_matches_plain_key(&String::new());
        assert_prepared_key_matches_plain_key(&i32::MIN);
        assert_prepared_key_matches_plain_key(&i64::MAX);
        assert_prepared_key_matches_plain_key(&0_u32);
        assert_prepared_key_matches_plain_key(&u64::MAX);
        assert_prepared_key_matches_plain_key(&false);
        assert_prepared_key_matches_plain_key(&BoundaryType::Upper);
    }

    #[test]
    fn prepared_key_that_needs_json_escaping_is_escaped_by_the_sink() {
        const QUOTED: HeaderFieldKey = HeaderFieldKey::new("a\"b", b"\x00\x03a\"b", b"");

        assert_eq!(QUOTED.name(), "a\"b");
        assert_eq!(
            json_fields(|sink| sink.write_field(QUOTED, &7_i32, CONTEXT).unwrap()),
            json_fields(|sink| sink.write("a\"b", &7_i32, CONTEXT).unwrap())
        );
    }

    #[test]
    #[should_panic(expected = "binary key has the wrong length prefix")]
    fn prepared_key_rejects_a_binary_encoding_with_another_length() {
        let _ = HeaderFieldKey::new("topic", b"\x00\x04topic", b"");
    }

    #[test]
    #[should_panic(expected = "JSON key requires escaping")]
    fn prepared_key_rejects_an_unescaped_json_member_name() {
        let _ = HeaderFieldKey::new("a\"b", b"\x00\x03a\"b", b"\"a\"b\":");
    }

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

        assert!(matches!(error, ProtocolContractViolation::KeyLengthOverflow { .. }));
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
