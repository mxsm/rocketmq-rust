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
