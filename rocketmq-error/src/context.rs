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

use crate::field::BoolField;
use crate::field::ContextVisibility;
use crate::field::FieldKey;
use crate::field::FieldSchema;
use crate::field::I64Field;
use crate::field::SecretPresenceField;
use crate::field::TextField;
use crate::field::U64Field;
use crate::kind::ErrorKind;

/// The redacted constant.
pub const REDACTED: &str = "<redacted>";

/// Wrapper for values that must never be formatted directly.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sensitive<T> {
    inner: T,
}

impl<T> Sensitive<T> {
    #[inline]
    /// Creates a new `Sensitive`.
    pub const fn new(inner: T) -> Self {
        Self { inner }
    }

    #[inline]
    /// Returns the expose secret.
    pub const fn expose_secret(&self) -> &T {
        &self.inner
    }

    #[inline]
    /// Converts this value into inner.
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

/// Spec-level redaction policy for external error surfaces.
///
/// `Public` means the stable public message and explicitly public context fields
/// are safe for API/CLI/log surfaces. `RedactSensitive` means external adapters
/// must prefer `ErrorSpec::public_message` and redaction-aware context over raw
/// `Display` or `Debug` output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RedactionPolicy {
    /// Represents the public case.
    Public,
    /// Represents the redact sensitive case.
    RedactSensitive,
}

impl RedactionPolicy {
    /// Return the default external-surface redaction policy for an error kind.
    #[inline]
    pub const fn for_kind(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::Network
            | ErrorKind::Serialization
            | ErrorKind::Protocol
            | ErrorKind::Rpc
            | ErrorKind::Authentication
            | ErrorKind::BrokerRegistrationFailed
            | ErrorKind::BrokerOperationFailed
            | ErrorKind::MessageValidationFailed
            | ErrorKind::BrokerPermissionDenied
            | ErrorKind::NotMasterBroker
            | ErrorKind::TopicSendingForbidden
            | ErrorKind::BrokerAsyncTaskFailed
            | ErrorKind::RequestBodyInvalid
            | ErrorKind::RequestHeaderError
            | ErrorKind::ResponseProcessFailed
            | ErrorKind::Filter
            | ErrorKind::StorageReadFailed
            | ErrorKind::StorageWriteFailed
            | ErrorKind::StorageCorrupted
            | ErrorKind::StorageOutOfSpace
            | ErrorKind::StorageLockFailed
            | ErrorKind::ConfigParseFailed
            | ErrorKind::ConfigMissing
            | ErrorKind::ConfigInvalidValue
            | ErrorKind::AuthConfigInvalid
            | ErrorKind::AuthHotReloadFailed
            | ErrorKind::ObservabilityConfigInvalid
            | ErrorKind::ObservabilityMetricsInitFailed
            | ErrorKind::ObservabilityTracesInitFailed
            | ErrorKind::ObservabilityLogsInitFailed
            | ErrorKind::ObservabilityLoggingInitFailed
            | ErrorKind::ObservabilityLogFilterInvalid
            | ErrorKind::ObservabilityMetricsShutdownFailed
            | ErrorKind::ObservabilityTracesShutdownFailed
            | ErrorKind::ObservabilityLogsShutdownFailed
            | ErrorKind::Controller
            | ErrorKind::ControllerRaftError
            | ErrorKind::ControllerConsensusTimeout
            | ErrorKind::ControllerSnapshotFailed
            | ErrorKind::Io
            | ErrorKind::IllegalArgument
            | ErrorKind::Internal
            | ErrorKind::Service
            | ErrorKind::NotInitialized
            | ErrorKind::Tools => Self::RedactSensitive,
            _ => Self::Public,
        }
    }
}

/// Borrowed value of a structured context field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FieldValueRef<'a> {
    /// Bounded, normalized text.
    Text(&'a str),
    /// A signed integer.
    I64(i64),
    /// An unsigned integer.
    U64(u64),
    /// A Boolean value.
    Bool(bool),
    /// A value-free presence marker.
    Presence,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum FieldValue {
    Text(Box<str>),
    I64(i64),
    U64(u64),
    Bool(bool),
    Presence,
}

impl FieldValue {
    fn as_ref(&self) -> FieldValueRef<'_> {
        match self {
            Self::Text(value) => FieldValueRef::Text(value),
            Self::I64(value) => FieldValueRef::I64(*value),
            Self::U64(value) => FieldValueRef::U64(*value),
            Self::Bool(value) => FieldValueRef::Bool(*value),
            Self::Presence => FieldValueRef::Presence,
        }
    }
}

/// One immutable structured error context field.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorContextField {
    schema: FieldSchema,
    value: FieldValue,
}

impl ErrorContextField {
    /// Returns the catalog-owned field schema.
    #[inline]
    pub const fn schema(&self) -> FieldSchema {
        self.schema
    }

    /// Returns the external field name.
    #[inline]
    pub const fn name(&self) -> &'static str {
        self.schema.name()
    }

    /// Returns the field visibility.
    #[inline]
    pub const fn visibility(&self) -> ContextVisibility {
        self.schema.visibility()
    }

    /// Returns the field value.
    ///
    /// Public callers can obtain fields only through
    /// [`ErrorContext::public_fields`], which excludes diagnostic and
    /// secret-presence entries.
    #[inline]
    pub fn value(&self) -> FieldValueRef<'_> {
        self.value.as_ref()
    }
}

impl fmt::Debug for ErrorContextField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ErrorContextField")
            .field("name", &self.name())
            .field("visibility", &self.visibility())
            .field("value", &RedactedValue(self))
            .finish()
    }
}

/// Redaction-aware structured context for errors.
///
/// Context retains at most 16 unique catalog fields. The first value for a
/// name wins and insertion order is preserved. Duplicate and over-capacity
/// inserts are ignored and recorded by [`Self::is_truncated`].
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ErrorContext {
    fields: Vec<ErrorContextField>,
    truncated: bool,
}

impl ErrorContext {
    const MAX_FIELDS: usize = 16;

    #[inline]
    /// Creates an empty context.
    pub const fn new() -> Self {
        Self {
            fields: Vec::new(),
            truncated: false,
        }
    }

    /// Adds bounded text under a catalog-owned text key.
    ///
    /// Every Unicode control character is replaced with one space before the
    /// value is truncated at a UTF-8 boundary to the schema's byte limit.
    pub fn with_text(mut self, key: FieldKey<TextField>, value: impl AsRef<str>) -> Self {
        let schema = key.schema();
        if !self.reserve_name(schema) {
            return self;
        }
        let limit = schema.text_byte_limit().unwrap_or(crate::field::MAX_TEXT_FIELD_BYTES);
        let (value, truncated) = normalize_text(value.as_ref(), limit);
        self.truncated |= truncated;
        self.fields.push(ErrorContextField {
            schema,
            value: FieldValue::Text(value),
        });
        self
    }

    /// Adds a signed integer under a catalog-owned key.
    pub fn with_i64(mut self, key: FieldKey<I64Field>, value: i64) -> Self {
        self.insert_scalar(key.schema(), FieldValue::I64(value));
        self
    }

    /// Adds an unsigned integer under a catalog-owned key.
    pub fn with_u64(mut self, key: FieldKey<U64Field>, value: u64) -> Self {
        self.insert_scalar(key.schema(), FieldValue::U64(value));
        self
    }

    /// Adds a Boolean under a catalog-owned key.
    pub fn with_bool(mut self, key: FieldKey<BoolField>, value: bool) -> Self {
        self.insert_scalar(key.schema(), FieldValue::Bool(value));
        self
    }

    /// Records the presence of secret-bearing input without accepting a value.
    pub fn with_secret_presence(mut self, key: FieldKey<SecretPresenceField>) -> Self {
        self.insert_scalar(key.schema(), FieldValue::Presence);
        self
    }

    /// Iterates fields approved for public projections in insertion order.
    pub fn public_fields(&self) -> impl Iterator<Item = &ErrorContextField> {
        self.fields()
            .iter()
            .filter(|field| matches!(field.visibility(), ContextVisibility::Public))
    }

    pub(crate) fn fields(&self) -> &[ErrorContextField] {
        &self.fields
    }

    #[inline]
    /// Returns whether the context contains no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    #[inline]
    /// Returns the number of retained fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns whether text, a duplicate field, or an over-capacity insert was truncated.
    #[inline]
    pub const fn is_truncated(&self) -> bool {
        self.truncated
    }

    fn insert_scalar(&mut self, schema: FieldSchema, value: FieldValue) {
        if self.reserve_name(schema) {
            self.fields.push(ErrorContextField { schema, value });
        }
    }

    fn reserve_name(&mut self, schema: FieldSchema) -> bool {
        if self.fields.iter().any(|field| field.name() == schema.name()) || self.fields.len() >= Self::MAX_FIELDS {
            self.truncated = true;
            return false;
        }
        true
    }
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, field) in self.fields().iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}={}", field.name(), DisplayValue(field))?;
        }
        Ok(())
    }
}

impl fmt::Debug for ErrorContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ErrorContext")
            .field("fields", &self.fields)
            .field("truncated", &self.truncated)
            .finish()
    }
}

struct DisplayValue<'a>(&'a ErrorContextField);

impl fmt::Display for DisplayValue<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !matches!(self.0.visibility(), ContextVisibility::Public) {
            return formatter.write_str(REDACTED);
        }
        match self.0.value() {
            FieldValueRef::Text(value) => formatter.write_str(value),
            FieldValueRef::I64(value) => value.fmt(formatter),
            FieldValueRef::U64(value) => value.fmt(formatter),
            FieldValueRef::Bool(value) => value.fmt(formatter),
            FieldValueRef::Presence => formatter.write_str(REDACTED),
        }
    }
}

struct RedactedValue<'a>(&'a ErrorContextField);

impl fmt::Debug for RedactedValue<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", DisplayValue(self.0))
    }
}

fn normalize_text(value: &str, limit: usize) -> (Box<str>, bool) {
    let mut normalized = String::with_capacity(limit);
    for character in value.chars() {
        let character = if character.is_control() { ' ' } else { character };
        if normalized.len() + character.len_utf8() > limit {
            return (normalized.into_boxed_str(), true);
        }
        normalized.push(character);
    }
    (normalized.into_boxed_str(), false)
}
