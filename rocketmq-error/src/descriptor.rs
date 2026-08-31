// Copyright 2023 The RocketMQ Rust Authors
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

use crate::field::FieldSchema;
use crate::projection::ProjectionSpec;
use crate::CanonicalCondition;
use crate::ErrorSeverity;
use crate::RecoveryHint;

/// Stable machine-readable error code.
///
/// `ErrorCode` values are intentionally separate from display messages.
/// Protocol mapping, retry policy, and observability should use the code, not
/// formatted error text.
///
/// [`Self::try_new`] accepts only reviewed catalog identity using the lowercase
/// dotted grammar. Source display text and runtime values must not become
/// catalog identity. [`Self::new`] remains available for transitional static
/// codes, including the existing uppercase [`crate::ErrorKind`] values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorCode(&'static str);

impl ErrorCode {
    /// Creates a stable error code without catalog-code validation.
    ///
    /// This constructor preserves the current static `ErrorKind` code set,
    /// whose uppercase underscore-delimited values predate the canonical
    /// catalog grammar. New catalog descriptors should use [`Self::try_new`].
    #[inline]
    pub const fn new(value: &'static str) -> Self {
        Self(value)
    }

    /// Attempts to create a canonical catalog code.
    ///
    /// Canonical codes have at least three dot-separated segments. Every
    /// segment starts with a lowercase ASCII letter and continues with
    /// lowercase ASCII letters, ASCII digits, or underscores.
    ///
    /// Returns [`None`] when `value` does not satisfy that grammar. The
    /// `'static` input prevents arbitrary runtime strings from becoming catalog
    /// identity; catalog entries must use reviewed static descriptors.
    #[inline]
    pub const fn try_new(value: &'static str) -> Option<Self> {
        if is_valid_catalog_code(value) {
            Some(Self(value))
        } else {
            None
        }
    }

    /// Returns the stable code string.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// Immutable catalog metadata for one canonical error identity.
///
/// A descriptor owns the stable code, protocol-independent condition, fixed
/// public message, operational severity, recovery hint, and explicit boundary
/// projections for an error. Its ordered field schemas define the only context
/// accepted by later descriptor-aware views. Catalog consumers can inspect
/// this metadata but cannot construct or modify descriptors outside this crate.
///
/// ```compile_fail
/// use rocketmq_error::{
///     CanonicalCondition, ErrorCode, ErrorDescriptor, ErrorSeverity, RecoveryHint,
/// };
///
/// let descriptor = ErrorDescriptor {
///     code: ErrorCode::try_new("example.operation.failed").unwrap(),
///     condition: CanonicalCondition::Internal,
///     public_message: "The operation failed",
///     severity: ErrorSeverity::Error,
///     recovery_hint: RecoveryHint::OperatorAction,
///     projection: todo!(),
///     fields: &[],
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorDescriptor {
    code: ErrorCode,
    condition: CanonicalCondition,
    public_message: &'static str,
    severity: ErrorSeverity,
    recovery_hint: RecoveryHint,
    projection: ProjectionSpec,
    fields: &'static [FieldSchema],
}

impl ErrorDescriptor {
    #[inline]
    pub(crate) const fn try_new(
        code: ErrorCode,
        condition: CanonicalCondition,
        public_message: &'static str,
        severity: ErrorSeverity,
        recovery_hint: RecoveryHint,
        projection: ProjectionSpec,
        fields: &'static [FieldSchema],
    ) -> Option<Self> {
        if !valid_descriptor_fields(fields) {
            return None;
        }
        Some(Self {
            code,
            condition,
            public_message,
            severity,
            recovery_hint,
            projection,
            fields,
        })
    }

    /// Returns the stable dotted catalog code.
    #[inline]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the protocol-independent canonical condition.
    #[inline]
    pub const fn condition(&self) -> CanonicalCondition {
        self.condition
    }

    /// Returns the fixed, boundary-safe public message.
    #[inline]
    pub const fn public_message(&self) -> &'static str {
        self.public_message
    }

    /// Returns the operational severity.
    #[inline]
    pub const fn severity(&self) -> ErrorSeverity {
        self.severity
    }

    /// Returns the catalog-owned recovery advice.
    #[inline]
    pub const fn recovery_hint(&self) -> RecoveryHint {
        self.recovery_hint
    }

    /// Returns the explicit boundary projections.
    #[inline]
    pub const fn projection(&self) -> ProjectionSpec {
        self.projection
    }

    /// Returns allowed context schemas in catalog declaration order.
    #[inline]
    pub const fn fields(&self) -> &'static [FieldSchema] {
        self.fields
    }
}

const fn valid_descriptor_fields(fields: &[FieldSchema]) -> bool {
    if fields.len() > 16 {
        return false;
    }
    let mut index = 0;
    while index < fields.len() {
        let mut other = index + 1;
        while other < fields.len() {
            if const_str_eq(fields[index].name(), fields[other].name()) {
                return false;
            }
            other += 1;
        }
        index += 1;
    }
    true
}

const fn const_str_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

const fn is_valid_catalog_code(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    let mut segment_count = 0;
    let mut starts_segment = true;

    while index < bytes.len() {
        let byte = bytes[index];

        if starts_segment {
            if !byte.is_ascii_lowercase() {
                return false;
            }
            starts_segment = false;
            segment_count += 1;
        } else if byte == b'.' {
            starts_segment = true;
        } else if !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && byte != b'_' {
            return false;
        }

        index += 1;
    }

    !starts_segment && segment_count >= 3
}

#[cfg(test)]
mod tests {
    use crate::fields;

    use super::*;

    #[test]
    fn descriptor_field_lists_reject_duplicate_names_and_capacity_overflow() {
        let duplicate = [fields::OPERATION.schema(), fields::OPERATION_DIAGNOSTIC.schema()];
        assert!(!valid_descriptor_fields(&duplicate));

        let too_many = [fields::TOPIC.schema(); 17];
        assert!(!valid_descriptor_fields(&too_many));
    }
}
