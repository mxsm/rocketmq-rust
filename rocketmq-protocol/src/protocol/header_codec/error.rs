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

use super::HeaderValueKind;

/// A classified request-header encoding, decoding, or validation failure.
///
/// Variants intentionally carry only static schema metadata. In particular,
/// malformed wire values are never retained or rendered by this error type.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HeaderCodecError {
    /// A field declared as required was absent.
    #[error("missing required header field {header}.{key}")]
    Missing {
        /// Stable header type identifier.
        header: &'static str,
        /// Canonical wire key.
        key: &'static str,
    },

    /// A present field could not be decoded as its declared wire type.
    #[error("invalid header field {header}.{key}: expected {expected:?}")]
    InvalidValue {
        /// Stable header type identifier.
        header: &'static str,
        /// Canonical wire key.
        key: &'static str,
        /// Expected protocol value category.
        expected: HeaderValueKind,
    },

    /// Canonical and alias keys carried different values.
    #[error("conflicting canonical and alias values for {header}.{key}")]
    Conflict {
        /// Stable header type identifier.
        header: &'static str,
        /// Canonical wire key.
        key: &'static str,
    },

    /// A typed value and dynamic extension field disagreed.
    #[error("typed header and dynamic field conflict for {header}.{key}")]
    DynamicFieldConflict {
        /// Stable header type identifier.
        header: &'static str,
        /// Canonical wire key.
        key: &'static str,
    },

    /// A legacy [`crate::CommandCustomHeader`] validation hook failed.
    #[error("legacy header validation failed for {header}")]
    LegacyValidation {
        /// Static Rust type name for the legacy header.
        header: &'static str,
    },

    /// A legacy header could not provide its compatibility map.
    #[error("legacy header map conversion failed for {header}")]
    LegacyMapConversionFailed {
        /// Static Rust type name for the legacy header.
        header: &'static str,
    },

    /// A header-specific validation rule failed.
    #[error("header validation failed for {header}: {rule}")]
    Validation {
        /// Stable header type identifier.
        header: &'static str,
        /// Static, non-sensitive validation rule identifier.
        rule: &'static str,
    },

    /// An unsigned Rust value exceeded its declared signed Java range.
    #[error("header value is outside Java range for {header}.{key}")]
    JavaRange {
        /// Stable header type identifier.
        header: &'static str,
        /// Canonical wire key.
        key: &'static str,
    },

    /// A wire key cannot be represented by the ROCKETMQ binary format.
    #[error("header key length exceeds the ROCKETMQ limit for {header}.{key}")]
    KeyLengthOverflow {
        /// Stable header type identifier.
        header: &'static str,
        /// Canonical wire key.
        key: &'static str,
    },

    /// A wire value cannot be represented by the ROCKETMQ binary format.
    #[error("header value length exceeds the ROCKETMQ limit for {header}.{key}")]
    ValueLengthOverflow {
        /// Stable header type identifier.
        header: &'static str,
        /// Canonical wire key.
        key: &'static str,
    },

    /// The complete extension-field payload exceeded its signed length field.
    #[error("ROCKETMQ extension-field payload exceeds the signed 32-bit wire limit")]
    ExtensionFieldsLengthOverflow,

    /// A dynamic extension-field key exceeded Java's signed 16-bit length.
    #[error("dynamic header key length exceeds the ROCKETMQ wire limit")]
    DynamicKeyLengthOverflow,

    /// A dynamic extension-field value exceeded its signed 32-bit length.
    #[error("dynamic header value length exceeds the ROCKETMQ wire limit")]
    DynamicValueLengthOverflow,

    /// Direct binary encoding was requested for a header without that capability.
    #[error("direct binary codec is unavailable for {header}")]
    FastCodecUnavailable {
        /// Stable header type identifier.
        header: &'static str,
    },
}

/// Adapts a classified codec error to the legacy remoting error boundary.
#[doc(hidden)]
#[cold]
#[inline(never)]
pub fn into_rocketmq_error(error: HeaderCodecError) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::request_header_error(error.to_string())
}
