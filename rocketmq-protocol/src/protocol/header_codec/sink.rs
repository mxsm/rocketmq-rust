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

use cheetah_string::CheetahString;

use super::private::Sealed;
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
}
