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

use std::marker::PhantomData;

/// Maximum number of bytes retained for a text context field.
pub(crate) const MAX_TEXT_FIELD_BYTES: usize = 256;

/// Controls which error projections may observe a context field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ContextVisibility {
    /// The field is safe for public error projections.
    Public,
    /// The field is available only to controlled diagnostic projections.
    Diagnostic,
    /// Only the presence of secret-bearing input is recorded.
    SecretPresenceOnly,
}

/// Closed vocabulary of values supported by structured error context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FieldValueKind {
    /// Bounded, normalized UTF-8 text.
    Text,
    /// A signed 64-bit integer.
    I64,
    /// An unsigned 64-bit integer.
    U64,
    /// A Boolean value.
    Bool,
    /// A value-free presence marker.
    Presence,
}

/// Immutable schema for one catalog-owned context field.
///
/// Schemas define the external name, value shape, visibility, and text bound.
/// Their fields and constructor are private so callers can inspect catalog
/// policy but cannot create or alter it.
///
/// ```compile_fail,E0451
/// use rocketmq_error::{ContextVisibility, FieldSchema, FieldValueKind};
///
/// let _ = FieldSchema {
///     name: "external_name",
///     visibility: ContextVisibility::Public,
///     value_kind: FieldValueKind::Text,
///     text_byte_limit: Some(64),
/// };
/// ```
///
/// ```compile_fail,E0624
/// use rocketmq_error::{ContextVisibility, FieldSchema, FieldValueKind};
///
/// let _ = FieldSchema::try_new(
///     "external_name",
///     ContextVisibility::Public,
///     FieldValueKind::Text,
///     Some(64),
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldSchema {
    name: &'static str,
    visibility: ContextVisibility,
    value_kind: FieldValueKind,
    text_byte_limit: Option<usize>,
}

impl FieldSchema {
    pub(crate) const fn try_new(
        name: &'static str,
        visibility: ContextVisibility,
        value_kind: FieldValueKind,
        text_byte_limit: Option<usize>,
    ) -> Option<Self> {
        if !is_valid_field_name(name) || !is_valid_text_limit(value_kind, text_byte_limit) {
            return None;
        }
        if matches!(visibility, ContextVisibility::SecretPresenceOnly)
            && !matches!(value_kind, FieldValueKind::Presence)
        {
            return None;
        }
        Some(Self {
            name,
            visibility,
            value_kind,
            text_byte_limit,
        })
    }

    /// Returns the lower-snake-case external field name.
    #[inline]
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// Returns the visibility policy for this field.
    #[inline]
    pub const fn visibility(self) -> ContextVisibility {
        self.visibility
    }

    /// Returns the field's closed value shape.
    #[inline]
    pub const fn value_kind(self) -> FieldValueKind {
        self.value_kind
    }

    /// Returns the maximum retained text size in bytes.
    ///
    /// Non-text fields always return [`None`].
    #[inline]
    pub const fn text_byte_limit(self) -> Option<usize> {
        self.text_byte_limit
    }
}

/// Opaque catalog key for a context field of marker type `T`.
///
/// Keys are copyable handles to immutable schemas. They can be obtained only
/// from [`fields`], preventing callers from inventing names or changing field
/// visibility.
///
/// ```compile_fail,E0451
/// use std::marker::PhantomData;
///
/// use rocketmq_error::{fields, FieldKey, TextField};
///
/// let _: FieldKey<TextField> = FieldKey {
///     schema: fields::TOPIC.schema(),
///     marker: PhantomData,
/// };
/// ```
///
/// ```compile_fail,E0624
/// use rocketmq_error::{fields, FieldKey, TextField};
///
/// let _: FieldKey<TextField> = FieldKey::new(fields::TOPIC.schema());
/// ```
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldKey<T> {
    schema: FieldSchema,
    marker: PhantomData<fn() -> T>,
}

impl<T> Copy for FieldKey<T> {}

impl<T> Clone for FieldKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> std::fmt::Debug for FieldKey<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("FieldKey").field(&self.schema).finish()
    }
}

impl<T> FieldKey<T> {
    const fn new(schema: FieldSchema) -> Self {
        Self {
            schema,
            marker: PhantomData,
        }
    }

    /// Returns the immutable schema carried by this key.
    #[inline]
    pub const fn schema(self) -> FieldSchema {
        self.schema
    }
}

mod marker_seal {
    pub trait Sealed {}
}

/// Marker for bounded text fields.
#[derive(Debug)]
pub struct TextField {
    _private: (),
}

/// Marker for signed integer fields.
#[derive(Debug)]
pub struct I64Field {
    _private: (),
}

/// Marker for unsigned integer fields.
#[derive(Debug)]
pub struct U64Field {
    _private: (),
}

/// Marker for Boolean fields.
#[derive(Debug)]
pub struct BoolField {
    _private: (),
}

/// Marker for value-free secret-presence fields.
#[derive(Debug)]
pub struct SecretPresenceField {
    _private: (),
}

impl marker_seal::Sealed for TextField {}
impl marker_seal::Sealed for I64Field {}
impl marker_seal::Sealed for U64Field {}
impl marker_seal::Sealed for BoolField {}
impl marker_seal::Sealed for SecretPresenceField {}

trait FieldMarker: marker_seal::Sealed {
    const KIND: FieldValueKind;
}

impl FieldMarker for TextField {
    const KIND: FieldValueKind = FieldValueKind::Text;
}

impl FieldMarker for I64Field {
    const KIND: FieldValueKind = FieldValueKind::I64;
}

impl FieldMarker for U64Field {
    const KIND: FieldValueKind = FieldValueKind::U64;
}

impl FieldMarker for BoolField {
    const KIND: FieldValueKind = FieldValueKind::Bool;
}

impl FieldMarker for SecretPresenceField {
    const KIND: FieldValueKind = FieldValueKind::Presence;
}

const fn checked_key<T: FieldMarker>(schema: FieldSchema) -> FieldKey<T> {
    if !same_value_kind(schema.value_kind(), T::KIND) {
        panic!("field marker does not match schema value kind");
    }
    FieldKey::new(schema)
}

const fn same_value_kind(left: FieldValueKind, right: FieldValueKind) -> bool {
    matches!(
        (left, right),
        (FieldValueKind::Text, FieldValueKind::Text)
            | (FieldValueKind::I64, FieldValueKind::I64)
            | (FieldValueKind::U64, FieldValueKind::U64)
            | (FieldValueKind::Bool, FieldValueKind::Bool)
            | (FieldValueKind::Presence, FieldValueKind::Presence)
    )
}

const fn is_valid_text_limit(value_kind: FieldValueKind, limit: Option<usize>) -> bool {
    match (value_kind, limit) {
        (FieldValueKind::Text, Some(limit)) => limit > 0 && limit <= MAX_TEXT_FIELD_BYTES,
        (FieldValueKind::Text, None) => false,
        (_, None) => true,
        (_, Some(_)) => false,
    }
}

const fn is_valid_field_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return false;
    }

    let mut index = 0;
    let mut starts_segment = true;
    while index < bytes.len() {
        let byte = bytes[index];
        if starts_segment {
            if !byte.is_ascii_lowercase() {
                return false;
            }
            starts_segment = false;
        } else if byte == b'_' {
            starts_segment = true;
        } else if !byte.is_ascii_lowercase() && !byte.is_ascii_digit() {
            return false;
        }
        index += 1;
    }
    !starts_segment
}

macro_rules! define_field {
    ($name:ident, $marker:ty, $external_name:literal, $visibility:path, $kind:path, $limit:expr, $docs:literal) => {
        #[doc = $docs]
        pub const $name: FieldKey<$marker> = {
            const SCHEMA: FieldSchema = match FieldSchema::try_new($external_name, $visibility, $kind, $limit) {
                Some(schema) => schema,
                None => panic!("invalid context field schema"),
            };
            checked_key(SCHEMA)
        };
    };
}

/// Catalog-owned context field keys.
///
/// Constants with different Rust names may intentionally share one external
/// name when separate domains require different immutable schemas. Context
/// uniqueness is still determined by the external name.
pub mod fields {
    use super::*;

    define_field!(
        ACTUAL_STATE,
        TextField,
        "actual",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Actual client state."
    );
    define_field!(
        ACTUAL_U64,
        U64Field,
        "actual",
        ContextVisibility::Diagnostic,
        FieldValueKind::U64,
        None,
        "Actual numeric version."
    );
    define_field!(
        ACTUAL_BYTES,
        U64Field,
        "actual_bytes",
        ContextVisibility::Public,
        FieldValueKind::U64,
        None,
        "Actual message size in bytes."
    );
    define_field!(
        ADDR,
        TextField,
        "addr",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(256),
        "Network address used by a failed operation."
    );
    define_field!(
        ALLOWED,
        TextField,
        "allowed",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Allowed values for a validation failure."
    );
    define_field!(
        ATTEMPTED,
        BoolField,
        "attempted",
        ContextVisibility::Diagnostic,
        FieldValueKind::Bool,
        None,
        "Whether installation was attempted."
    );
    define_field!(
        BROKER,
        TextField,
        "broker",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(127),
        "Broker identity involved in an operation."
    );
    define_field!(
        BROKER_ADDR,
        TextField,
        "broker_addr",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(256),
        "Broker address involved in an operation."
    );
    define_field!(
        BROKER_CODE,
        I64Field,
        "broker_code",
        ContextVisibility::Diagnostic,
        FieldValueKind::I64,
        None,
        "Broker response code."
    );
    define_field!(
        CLUSTER,
        TextField,
        "cluster",
        ContextVisibility::Public,
        FieldValueKind::Text,
        Some(127),
        "Cluster identity."
    );
    define_field!(
        CLIENT_ROLE,
        TextField,
        "client_role",
        ContextVisibility::Public,
        FieldValueKind::Text,
        Some(32),
        "Client component role."
    );
    define_field!(
        COMPONENT_NAME,
        TextField,
        "component",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Component involved in a lifecycle failure."
    );
    define_field!(
        CONSUMER,
        TextField,
        "consumer",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(127),
        "Consumer identity."
    );
    define_field!(
        CURRENT,
        I64Field,
        "current",
        ContextVisibility::Public,
        FieldValueKind::I64,
        None,
        "Current numeric value."
    );
    define_field!(
        DURATION_MS,
        U64Field,
        "duration_ms",
        ContextVisibility::Public,
        FieldValueKind::U64,
        None,
        "Operation duration in milliseconds."
    );
    define_field!(
        EXPECTED_STATE,
        TextField,
        "expected",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Expected client state."
    );
    define_field!(
        EXPECTED_U64,
        U64Field,
        "expected",
        ContextVisibility::Diagnostic,
        FieldValueKind::U64,
        None,
        "Expected numeric version."
    );
    define_field!(
        FEATURE,
        TextField,
        "feature",
        ContextVisibility::Public,
        FieldValueKind::Text,
        Some(64),
        "Disabled feature name."
    );
    define_field!(
        FILTER_KIND,
        TextField,
        "filter_kind",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Filter failure classification."
    );
    define_field!(
        FORMAT,
        TextField,
        "format",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Serialization format classification."
    );
    define_field!(
        FIELD,
        TextField,
        "field",
        ContextVisibility::Public,
        FieldValueKind::Text,
        Some(64),
        "Input field name."
    );
    define_field!(
        FILTER_COMPILE_KIND,
        TextField,
        "filter_compile_kind",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Filter compilation failure kind."
    );
    define_field!(
        FILTER_COMPILE_POSITION,
        U64Field,
        "filter_compile_position",
        ContextVisibility::Diagnostic,
        FieldValueKind::U64,
        None,
        "Filter compilation byte position."
    );
    define_field!(
        FILTER_COMPILE_SOURCE,
        TextField,
        "filter_compile_source",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Filter compiler source classification."
    );
    define_field!(
        FILTER_COMPILE_STAGE,
        TextField,
        "filter_compile_stage",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Filter compilation stage."
    );
    define_field!(
        GROUP,
        TextField,
        "group",
        ContextVisibility::Public,
        FieldValueKind::Text,
        Some(127),
        "Consumer group identity."
    );
    define_field!(
        INSTALLED,
        BoolField,
        "installed",
        ContextVisibility::Diagnostic,
        FieldValueKind::Bool,
        None,
        "Whether a subscriber was installed."
    );
    define_field!(
        INVARIANT,
        TextField,
        "invariant",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(128),
        "Violated invariant classification."
    );
    define_field!(
        KEY,
        TextField,
        "key",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Configuration key name."
    );
    define_field!(
        LEADER_ID,
        U64Field,
        "leader_id",
        ContextVisibility::Diagnostic,
        FieldValueKind::U64,
        None,
        "Controller leader identifier."
    );
    define_field!(
        LIMIT,
        U64Field,
        "limit",
        ContextVisibility::Diagnostic,
        FieldValueKind::U64,
        None,
        "Capacity limit associated with a failure."
    );
    define_field!(
        LIMIT_BYTES,
        U64Field,
        "limit_bytes",
        ContextVisibility::Public,
        FieldValueKind::U64,
        None,
        "Configured byte limit."
    );
    define_field!(
        MASTER_ADDRESS,
        TextField,
        "master_address",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(256),
        "Current master broker address."
    );
    define_field!(
        MAX,
        I64Field,
        "max",
        ContextVisibility::Public,
        FieldValueKind::I64,
        None,
        "Maximum numeric value."
    );
    define_field!(
        MAX_QUEUE_ID,
        I64Field,
        "max_queue_id",
        ContextVisibility::Public,
        FieldValueKind::I64,
        None,
        "Maximum queue identifier."
    );
    define_field!(
        OFFSET,
        I64Field,
        "offset",
        ContextVisibility::Diagnostic,
        FieldValueKind::I64,
        None,
        "Storage or message offset."
    );
    define_field!(
        OPERATION,
        TextField,
        "operation",
        ContextVisibility::Public,
        FieldValueKind::Text,
        Some(64),
        "Public operation classification."
    );
    define_field!(
        OPERATION_DIAGNOSTIC,
        TextField,
        "operation",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Diagnostic operation classification."
    );
    define_field!(
        OBSERVABILITY_SIGNAL,
        TextField,
        "observability_signal",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(32),
        "Observability signal classification."
    );
    define_field!(
        ORDINAL,
        U64Field,
        "ordinal",
        ContextVisibility::Public,
        FieldValueKind::U64,
        None,
        "Protocol version ordinal."
    );
    define_field!(
        PERMISSION_VALUE,
        I64Field,
        "value",
        ContextVisibility::Public,
        FieldValueKind::I64,
        None,
        "Invalid permission value."
    );
    define_field!(
        PROPERTY,
        TextField,
        "property",
        ContextVisibility::Public,
        FieldValueKind::Text,
        Some(127),
        "Message property name."
    );
    define_field!(
        PHASE,
        TextField,
        "phase",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(32),
        "Operation phase at which the failure occurred."
    );
    define_field!(
        POSITION,
        U64Field,
        "position",
        ContextVisibility::Diagnostic,
        FieldValueKind::U64,
        None,
        "Input position associated with a failure."
    );
    define_field!(
        QUEUE_ID,
        I64Field,
        "queue_id",
        ContextVisibility::Public,
        FieldValueKind::I64,
        None,
        "Message queue identifier."
    );
    define_field!(
        REQUEST_CODE,
        I64Field,
        "request_code",
        ContextVisibility::Public,
        FieldValueKind::I64,
        None,
        "RocketMQ request code."
    );
    define_field!(
        REMOTE_ADDR,
        TextField,
        "remote_addr",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(256),
        "Remote transport address."
    );
    define_field!(
        REMOTE_CODE,
        I64Field,
        "remote_code",
        ContextVisibility::Diagnostic,
        FieldValueKind::I64,
        None,
        "Remote response code."
    );
    define_field!(
        RESOURCE,
        TextField,
        "resource",
        ContextVisibility::Public,
        FieldValueKind::Text,
        Some(127),
        "Resource identity."
    );
    define_field!(
        STORE_COMPONENT,
        TextField,
        "store_component",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Storage component classification."
    );
    define_field!(
        STORE_OPERATION,
        TextField,
        "store_operation",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Storage operation classification."
    );
    define_field!(
        SERIALIZATION_TYPE,
        U64Field,
        "serialization_type",
        ContextVisibility::Public,
        FieldValueKind::U64,
        None,
        "Protocol serialization type identifier."
    );
    define_field!(
        TASK,
        TextField,
        "task",
        ContextVisibility::Diagnostic,
        FieldValueKind::Text,
        Some(64),
        "Background task classification."
    );
    define_field!(
        TIMEOUT_MS,
        U64Field,
        "timeout_ms",
        ContextVisibility::Public,
        FieldValueKind::U64,
        None,
        "Timeout duration in milliseconds."
    );
    define_field!(
        TOPIC,
        TextField,
        "topic",
        ContextVisibility::Public,
        FieldValueKind::Text,
        Some(127),
        "Topic identity."
    );
    define_field!(
        DECLARED_SIZE,
        I64Field,
        "declared_size",
        ContextVisibility::Diagnostic,
        FieldValueKind::I64,
        None,
        "Declared record size."
    );

    define_field!(
        AUTH_ERROR_PRESENT,
        SecretPresenceField,
        "auth_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered authentication error detail."
    );
    define_field!(
        CONTEXT_PRESENT,
        SecretPresenceField,
        "context",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of task context."
    );
    define_field!(
        DETAIL_PRESENT,
        SecretPresenceField,
        "detail",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of arbitrary diagnostic detail."
    );
    define_field!(
        CONTROLLER_ERROR_PRESENT,
        SecretPresenceField,
        "controller_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered controller error detail."
    );
    define_field!(
        CREDENTIALS_PRESENT,
        SecretPresenceField,
        "credentials_present",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of authentication credentials."
    );
    define_field!(
        DOMAIN_ERROR_PRESENT,
        SecretPresenceField,
        "domain_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered domain error detail."
    );
    define_field!(
        ERROR_PRESENT,
        SecretPresenceField,
        "error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered error detail."
    );
    define_field!(
        FILTER_PRESENT,
        SecretPresenceField,
        "filter",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of filter text."
    );
    define_field!(
        FILTER_ERROR_PRESENT,
        SecretPresenceField,
        "filter_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered filter error detail."
    );
    define_field!(
        INTERNAL_ERROR_PRESENT,
        SecretPresenceField,
        "internal_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered internal error detail."
    );
    define_field!(
        HOST_PRESENT,
        SecretPresenceField,
        "host",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of a network host value."
    );
    define_field!(
        INVALID_VALUE_PRESENT,
        SecretPresenceField,
        "invalid_value_present",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of an invalid secret-bearing value."
    );
    define_field!(
        IO_ERROR_PRESENT,
        SecretPresenceField,
        "io_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered I/O error detail."
    );
    define_field!(
        MESSAGE_PRESENT,
        SecretPresenceField,
        "message",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of arbitrary error message detail."
    );
    define_field!(
        PATH_PRESENT,
        SecretPresenceField,
        "path",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of a storage or configuration path."
    );
    define_field!(
        PROTOCOL_ERROR_PRESENT,
        SecretPresenceField,
        "protocol_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered protocol error detail."
    );
    define_field!(
        REMOTE_ADDR_PRESENT,
        SecretPresenceField,
        "remote_addr",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of a remote transport address."
    );
    define_field!(
        REASON_PRESENT,
        SecretPresenceField,
        "reason",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of arbitrary failure detail."
    );
    define_field!(
        RPC_ERROR_PRESENT,
        SecretPresenceField,
        "rpc_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered RPC error detail."
    );
    define_field!(
        SERIALIZATION_ERROR_PRESENT,
        SecretPresenceField,
        "serialization_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered serialization error detail."
    );
    define_field!(
        SERVICE_ERROR_PRESENT,
        SecretPresenceField,
        "service_error",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered service error detail."
    );
    define_field!(
        SOURCE_PRESENT,
        SecretPresenceField,
        "source_present",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of a typed source error."
    );
    define_field!(
        SOURCE_DETAIL_PRESENT,
        SecretPresenceField,
        "source",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of rendered source error detail."
    );
    define_field!(
        STORE_DETAIL_PRESENT,
        SecretPresenceField,
        "store_detail",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of storage diagnostic detail."
    );
    define_field!(
        VALUE_PRESENT,
        SecretPresenceField,
        "value",
        ContextVisibility::SecretPresenceOnly,
        FieldValueKind::Presence,
        None,
        "Presence of a configuration value."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_schemas_are_rejected() {
        for name in ["", "Upper", "two__parts", "trailing_", "9leading"] {
            assert_eq!(
                FieldSchema::try_new(name, ContextVisibility::Public, FieldValueKind::Text, Some(64)),
                None
            );
        }
        assert_eq!(
            FieldSchema::try_new("text", ContextVisibility::Public, FieldValueKind::Text, None),
            None
        );
        assert_eq!(
            FieldSchema::try_new("text", ContextVisibility::Public, FieldValueKind::Text, Some(0)),
            None
        );
        assert_eq!(
            FieldSchema::try_new("text", ContextVisibility::Public, FieldValueKind::Text, Some(257)),
            None
        );
        assert_eq!(
            FieldSchema::try_new("number", ContextVisibility::Public, FieldValueKind::U64, Some(8)),
            None
        );
        assert_eq!(
            FieldSchema::try_new(
                "secret",
                ContextVisibility::SecretPresenceOnly,
                FieldValueKind::Text,
                Some(64)
            ),
            None
        );
    }

    #[test]
    fn same_external_name_can_have_distinct_immutable_schemas() {
        assert_eq!(
            fields::OPERATION.schema().name(),
            fields::OPERATION_DIAGNOSTIC.schema().name()
        );
        assert_ne!(fields::OPERATION.schema(), fields::OPERATION_DIAGNOSTIC.schema());
    }
}
