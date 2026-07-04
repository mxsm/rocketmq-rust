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

pub const REDACTED: &str = "<redacted>";

/// Wrapper for values that must never be formatted directly.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sensitive<T> {
    inner: T,
}

impl<T> Sensitive<T> {
    #[inline]
    pub const fn new(inner: T) -> Self {
        Self { inner }
    }

    #[inline]
    pub const fn expose_secret(&self) -> &T {
        &self.inner
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> From<T> for Sensitive<T> {
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> fmt::Display for Sensitive<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl<T> fmt::Debug for Sensitive<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Sensitive(<redacted>)")
    }
}

/// Redaction policy for one context field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RedactionKind {
    Public,
    Sensitive,
}

/// One structured error context field.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorContextField {
    pub key: &'static str,
    pub value: String,
    pub redaction: RedactionKind,
}

/// Redaction-aware structured context for errors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ErrorContext {
    fields: Vec<ErrorContextField>,
}

impl ErrorContext {
    #[inline]
    pub const fn new() -> Self {
        Self { fields: Vec::new() }
    }

    #[inline]
    pub fn with_field(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.push_field(key, value);
        self
    }

    #[inline]
    pub fn with_sensitive<T>(mut self, key: &'static str, _value: Sensitive<T>) -> Self {
        self.fields.push(ErrorContextField {
            key,
            value: REDACTED.to_string(),
            redaction: RedactionKind::Sensitive,
        });
        self
    }

    #[inline]
    pub fn push_field(&mut self, key: &'static str, value: impl Into<String>) {
        self.fields.push(ErrorContextField {
            key,
            value: value.into(),
            redaction: RedactionKind::Public,
        });
    }

    #[inline]
    pub fn push_sensitive<T>(&mut self, key: &'static str, value: Sensitive<T>) {
        self.fields.push(ErrorContextField {
            key,
            value: value.to_string(),
            redaction: RedactionKind::Sensitive,
        });
    }

    #[inline]
    pub fn fields(&self) -> &[ErrorContextField] {
        &self.fields
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.fields.len()
    }
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, field) in self.fields.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}={}", field.key, field.value)?;
        }
        Ok(())
    }
}
