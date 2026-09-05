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
use cheetah_string::CheetahString;
use rocketmq_model::boundary_type::BoundaryType;

use super::private::Sealed;
use super::write_json_string;
use super::ProtocolContractViolation;

/// Protocol-reviewed value categories supported by typed request headers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderValueKind {
    /// UTF-8 text.
    String,
    /// Canonical ASCII `true` or `false`.
    Bool,
    /// Signed 32-bit decimal integer.
    I32,
    /// Signed 64-bit decimal integer.
    I64,
    /// Unsigned 32-bit decimal integer.
    U32,
    /// Unsigned 64-bit decimal integer.
    U64,
    /// RocketMQ offset boundary selector.
    BoundaryType,
}

/// A signed Java numeric range applied to an unsigned Rust field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderRange {
    /// Java `int`/`Integer`: `0..=i32::MAX` for an unsigned Rust field.
    I32,
    /// Java `long`/`Long`: `0..=i64::MAX` for an unsigned Rust field.
    I64,
}

/// Static field metadata used for typed conversion and diagnostics.
///
/// Derive output creates one constant context per field, so successful codec
/// paths never allocate diagnostic metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeaderFieldContext {
    /// Stable header type identifier.
    pub header: &'static str,
    /// Canonical wire key.
    pub key: &'static str,
    /// Expected value category.
    pub kind: HeaderValueKind,
    /// Optional Java signed-range constraint.
    pub range: Option<HeaderRange>,
}

impl HeaderFieldContext {
    /// Creates static metadata for one header field.
    pub const fn new(
        header: &'static str,
        key: &'static str,
        kind: HeaderValueKind,
        range: Option<HeaderRange>,
    ) -> Self {
        Self {
            header,
            key,
            kind,
            range,
        }
    }
}

/// A protocol-approved request-header wire value.
///
/// Implementations must keep `encoded_len` and `write_ascii` byte-for-byte
/// consistent. This trait is sealed: new wire types require an explicit
/// implementation and compatibility review in `rocketmq-protocol`.
pub trait HeaderValue: Sealed + Sized {
    /// Static value category used by generated schemas and diagnostics.
    const KIND: HeaderValueKind;

    /// Whether every value of this type fits Java's signed 32-bit wire length.
    const ALWAYS_FITS_WIRE_LENGTH: bool = false;

    /// Produces the owned value required by a [`crate::HeaderMap`].
    fn to_map_value(&self) -> CheetahString;

    /// Returns the exact number of bytes written by [`Self::write_ascii`].
    fn encoded_len(&self) -> usize;

    /// Appends the canonical UTF-8/ASCII wire representation without a
    /// temporary `String` for scalar values.
    fn write_ascii(&self, out: &mut BytesMut);

    /// Appends the canonical extension-field value as a JSON string.
    ///
    /// Scalar implementations use their allocation-free ASCII form. Text
    /// implementations override this method to apply JSON escaping.
    #[inline]
    fn write_json_string(&self, out: &mut BytesMut) {
        out.extend_from_slice(b"\"");
        self.write_ascii(out);
        out.extend_from_slice(b"\"");
    }

    /// Decodes a complete map value according to the field context.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolContractViolation::InvalidValue`] for malformed or overflowing
    /// scalar values, or [`ProtocolContractViolation::JavaRange`] when an unsigned value
    /// exceeds its field's declared signed Java range. Errors never retain
    /// `raw`.
    fn decode(raw: &str, context: HeaderFieldContext) -> Result<Self, ProtocolContractViolation>;
}

/// Validates an unsigned value against its field-level signed Java range.
///
/// A context without a range is a Rust extension and remains unrestricted.
/// Derive output calls this helper once before sink dispatch; unsigned decoders
/// call it once after successful parsing.
///
/// # Errors
///
/// Returns [`ProtocolContractViolation::JavaRange`] when `value` exceeds the declared
/// range. The error contains only static field metadata.
#[inline]
pub fn validate_unsigned_java_range(value: u64, context: HeaderFieldContext) -> Result<(), ProtocolContractViolation> {
    let in_range = match context.range {
        None => true,
        Some(HeaderRange::I32) => value <= i32::MAX as u64,
        Some(HeaderRange::I64) => value <= i64::MAX as u64,
    };
    if in_range {
        Ok(())
    } else {
        Err(ProtocolContractViolation::JavaRange {
            header: context.header,
            key: context.key,
        })
    }
}

#[inline]
const fn invalid_value(context: HeaderFieldContext) -> ProtocolContractViolation {
    ProtocolContractViolation::InvalidValue {
        header: context.header,
        key: context.key,
        expected: context.kind,
    }
}

#[inline]
fn signed_decimal_len(value: i64) -> usize {
    unsigned_decimal_len(value.unsigned_abs()) + usize::from(value < 0)
}

#[inline]
fn unsigned_decimal_len(value: u64) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

impl Sealed for CheetahString {}

impl HeaderValue for CheetahString {
    const KIND: HeaderValueKind = HeaderValueKind::String;
    const ALWAYS_FITS_WIRE_LENGTH: bool = false;

    #[inline]
    fn to_map_value(&self) -> CheetahString {
        self.clone()
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn write_ascii(&self, out: &mut BytesMut) {
        out.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn write_json_string(&self, out: &mut BytesMut) {
        write_json_string(out, self.as_str());
    }

    #[inline]
    fn decode(raw: &str, _context: HeaderFieldContext) -> Result<Self, ProtocolContractViolation> {
        Ok(CheetahString::from_slice(raw))
    }
}

impl Sealed for String {}

impl HeaderValue for String {
    const KIND: HeaderValueKind = HeaderValueKind::String;
    const ALWAYS_FITS_WIRE_LENGTH: bool = false;

    #[inline]
    fn to_map_value(&self) -> CheetahString {
        CheetahString::from_slice(self)
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn write_ascii(&self, out: &mut BytesMut) {
        out.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn write_json_string(&self, out: &mut BytesMut) {
        write_json_string(out, self);
    }

    #[inline]
    fn decode(raw: &str, _context: HeaderFieldContext) -> Result<Self, ProtocolContractViolation> {
        Ok(raw.to_owned())
    }
}

impl Sealed for bool {}

impl HeaderValue for bool {
    const KIND: HeaderValueKind = HeaderValueKind::Bool;
    const ALWAYS_FITS_WIRE_LENGTH: bool = true;

    #[inline]
    fn to_map_value(&self) -> CheetahString {
        CheetahString::from_static_str(if *self { "true" } else { "false" })
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        if *self {
            4
        } else {
            5
        }
    }

    #[inline]
    fn write_ascii(&self, out: &mut BytesMut) {
        out.extend_from_slice(if *self { b"true" } else { b"false" });
    }

    #[inline]
    fn decode(raw: &str, context: HeaderFieldContext) -> Result<Self, ProtocolContractViolation> {
        if raw.eq_ignore_ascii_case("true") {
            Ok(true)
        } else if raw.eq_ignore_ascii_case("false") {
            Ok(false)
        } else {
            Err(invalid_value(context))
        }
    }
}

macro_rules! impl_signed_header_value {
    ($ty:ty, $kind:ident) => {
        impl Sealed for $ty {}

        impl HeaderValue for $ty {
            const KIND: HeaderValueKind = HeaderValueKind::$kind;
            const ALWAYS_FITS_WIRE_LENGTH: bool = true;

            #[inline]
            fn to_map_value(&self) -> CheetahString {
                let mut buffer = itoa::Buffer::new();
                CheetahString::from_slice(buffer.format(*self))
            }

            #[inline]
            fn encoded_len(&self) -> usize {
                signed_decimal_len(*self as i64)
            }

            #[inline]
            fn write_ascii(&self, out: &mut BytesMut) {
                let mut buffer = itoa::Buffer::new();
                out.extend_from_slice(buffer.format(*self).as_bytes());
            }

            #[inline]
            fn decode(raw: &str, context: HeaderFieldContext) -> Result<Self, ProtocolContractViolation> {
                raw.parse::<$ty>().map_err(|_| invalid_value(context))
            }
        }
    };
}

macro_rules! impl_unsigned_header_value {
    ($ty:ty, $kind:ident) => {
        impl Sealed for $ty {}

        impl HeaderValue for $ty {
            const KIND: HeaderValueKind = HeaderValueKind::$kind;
            const ALWAYS_FITS_WIRE_LENGTH: bool = true;

            #[inline]
            fn to_map_value(&self) -> CheetahString {
                let mut buffer = itoa::Buffer::new();
                CheetahString::from_slice(buffer.format(*self))
            }

            #[inline]
            fn encoded_len(&self) -> usize {
                unsigned_decimal_len(*self as u64)
            }

            #[inline]
            fn write_ascii(&self, out: &mut BytesMut) {
                let mut buffer = itoa::Buffer::new();
                out.extend_from_slice(buffer.format(*self).as_bytes());
            }

            #[inline]
            fn decode(raw: &str, context: HeaderFieldContext) -> Result<Self, ProtocolContractViolation> {
                let value = raw.parse::<$ty>().map_err(|_| invalid_value(context))?;
                validate_unsigned_java_range(value as u64, context)?;
                Ok(value)
            }
        }
    };
}

impl_signed_header_value!(i32, I32);
impl_signed_header_value!(i64, I64);
impl_unsigned_header_value!(u32, U32);
impl_unsigned_header_value!(u64, U64);

impl Sealed for BoundaryType {}

impl HeaderValue for BoundaryType {
    const KIND: HeaderValueKind = HeaderValueKind::BoundaryType;
    const ALWAYS_FITS_WIRE_LENGTH: bool = true;

    #[inline]
    fn to_map_value(&self) -> CheetahString {
        CheetahString::from_static_str(match self {
            BoundaryType::Lower => "LOWER",
            BoundaryType::Upper => "UPPER",
        })
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        match self {
            BoundaryType::Lower => 5,
            BoundaryType::Upper => 5,
        }
    }

    #[inline]
    fn write_ascii(&self, out: &mut BytesMut) {
        out.extend_from_slice(match self {
            BoundaryType::Lower => b"LOWER",
            BoundaryType::Upper => b"UPPER",
        });
    }

    #[inline]
    fn decode(raw: &str, _context: HeaderFieldContext) -> Result<Self, ProtocolContractViolation> {
        Ok(BoundaryType::get_type(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn context(kind: HeaderValueKind, range: Option<HeaderRange>) -> HeaderFieldContext {
        HeaderFieldContext::new("ExampleHeader", "value", kind, range)
    }

    fn assert_encoding<T>(value: T, expected: &str)
    where
        T: HeaderValue,
    {
        let mut out = BytesMut::new();
        value.write_ascii(&mut out);
        assert_eq!(value.encoded_len(), expected.len());
        assert_eq!(out.as_ref(), expected.as_bytes());
        assert_eq!(value.to_map_value().as_str(), expected);
    }

    #[test]
    fn strings_preserve_empty_unicode_and_exact_lengths() {
        for raw in ["", "topic", "主题-🚀"] {
            let cheetah = CheetahString::decode(raw, context(HeaderValueKind::String, None)).unwrap();
            assert_encoding(cheetah, raw);

            let owned = String::decode(raw, context(HeaderValueKind::String, None)).unwrap();
            assert_encoding(owned, raw);
        }
    }

    #[test]
    fn signed_integers_cover_extrema_zero_and_overflow() {
        for value in [i32::MIN, -1, 0, 1, i32::MAX] {
            let text = value.to_string();
            assert_eq!(i32::decode(&text, context(HeaderValueKind::I32, None)).unwrap(), value);
            assert_encoding(value, &text);
        }
        for value in [i64::MIN, -1, 0, 1, i64::MAX] {
            let text = value.to_string();
            assert_eq!(i64::decode(&text, context(HeaderValueKind::I64, None)).unwrap(), value);
            assert_encoding(value, &text);
        }

        assert_eq!(i32::decode("+1", context(HeaderValueKind::I32, None)).unwrap(), 1);
        for raw in ["2147483648", "-2147483649", " 1", "1 "] {
            assert!(matches!(
                i32::decode(raw, context(HeaderValueKind::I32, None)),
                Err(ProtocolContractViolation::InvalidValue { .. })
            ));
        }
        for raw in ["9223372036854775808", "-9223372036854775809"] {
            assert!(matches!(
                i64::decode(raw, context(HeaderValueKind::I64, None)),
                Err(ProtocolContractViolation::InvalidValue { .. })
            ));
        }
    }

    #[test]
    fn unsigned_integers_cover_extrema_overflow_and_java_ranges() {
        for value in [0, 1, u32::MAX] {
            let text = value.to_string();
            assert_eq!(u32::decode(&text, context(HeaderValueKind::U32, None)).unwrap(), value);
            assert_encoding(value, &text);
        }
        for value in [0, 1, u64::MAX] {
            let text = value.to_string();
            assert_eq!(u64::decode(&text, context(HeaderValueKind::U64, None)).unwrap(), value);
            assert_encoding(value, &text);
        }

        assert!(matches!(
            u32::decode("4294967296", context(HeaderValueKind::U32, None)),
            Err(ProtocolContractViolation::InvalidValue { .. })
        ));
        assert!(matches!(
            u64::decode("18446744073709551616", context(HeaderValueKind::U64, None)),
            Err(ProtocolContractViolation::InvalidValue { .. })
        ));
        assert!(matches!(
            u64::decode("-1", context(HeaderValueKind::U64, None)),
            Err(ProtocolContractViolation::InvalidValue { .. })
        ));

        assert_eq!(
            u32::decode(
                &i32::MAX.to_string(),
                context(HeaderValueKind::U32, Some(HeaderRange::I32)),
            )
            .unwrap(),
            i32::MAX as u32
        );
        assert!(matches!(
            u32::decode(
                &(i32::MAX as u32 + 1).to_string(),
                context(HeaderValueKind::U32, Some(HeaderRange::I32)),
            ),
            Err(ProtocolContractViolation::JavaRange { .. })
        ));
        assert_eq!(
            u64::decode(
                &i64::MAX.to_string(),
                context(HeaderValueKind::U64, Some(HeaderRange::I64)),
            )
            .unwrap(),
            i64::MAX as u64
        );
        assert!(matches!(
            u64::decode(
                &(i64::MAX as u64 + 1).to_string(),
                context(HeaderValueKind::U64, Some(HeaderRange::I64)),
            ),
            Err(ProtocolContractViolation::JavaRange { .. })
        ));
    }

    #[test]
    fn encode_range_helper_keeps_rust_extensions_unrestricted() {
        let unrestricted = context(HeaderValueKind::U64, None);
        assert!(validate_unsigned_java_range(u64::MAX, unrestricted).is_ok());

        let java_long = context(HeaderValueKind::U32, Some(HeaderRange::I64));
        assert!(validate_unsigned_java_range(u32::MAX as u64, java_long).is_ok());

        let java_int = context(HeaderValueKind::U32, Some(HeaderRange::I32));
        let error = validate_unsigned_java_range(i32::MAX as u64 + 1, java_int).unwrap_err();
        assert!(matches!(error, ProtocolContractViolation::JavaRange { .. }));
    }

    #[test]
    fn booleans_decode_case_insensitively_but_encode_canonically() {
        for raw in ["true", "TRUE", "TrUe"] {
            assert!(bool::decode(raw, context(HeaderValueKind::Bool, None)).unwrap());
        }
        for raw in ["false", "FALSE", "FaLsE"] {
            assert!(!bool::decode(raw, context(HeaderValueKind::Bool, None)).unwrap());
        }
        for raw in ["", "1", "yes", " true"] {
            assert!(matches!(
                bool::decode(raw, context(HeaderValueKind::Bool, None)),
                Err(ProtocolContractViolation::InvalidValue { .. })
            ));
        }
        assert_encoding(true, "true");
        assert_encoding(false, "false");
    }

    #[test]
    fn boundary_type_preserves_java_fallback_and_canonical_encoding() {
        assert_eq!(
            BoundaryType::decode("upper", context(HeaderValueKind::BoundaryType, None)).unwrap(),
            BoundaryType::Upper
        );
        for raw in ["lower", "unknown", "", "上界"] {
            assert_eq!(
                BoundaryType::decode(raw, context(HeaderValueKind::BoundaryType, None)).unwrap(),
                BoundaryType::Lower
            );
        }
        assert_encoding(BoundaryType::Lower, "LOWER");
        assert_encoding(BoundaryType::Upper, "UPPER");
    }

    #[test]
    fn errors_do_not_echo_malformed_values() {
        let raw = "secret-invalid-value";
        let error = bool::decode(raw, context(HeaderValueKind::Bool, None)).unwrap_err();
        let rendered = error.to_string();

        assert!(!rendered.contains(raw));
        assert_eq!(rendered, "invalid header field ExampleHeader.value: expected Bool");
    }
}
