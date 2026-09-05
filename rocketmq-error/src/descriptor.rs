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
use crate::RecoveryHint;

/// Broad classification used for stable diagnostic policy.
///
/// Values are closed to the associated constants exposed by this crate. The
/// catalog owns the class for every descriptor; error instances and domain
/// facades cannot override it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorClass(&'static str);

impl ErrorClass {
    /// Deterministic input or contract validation failure.
    pub const VALIDATION: Self = Self("validation");
    /// Authentication failure.
    pub const AUTHENTICATION: Self = Self("authentication");
    /// Authorization failure.
    pub const AUTHORIZATION: Self = Self("authorization");
    /// Routing or leadership failure.
    pub const ROUTING: Self = Self("routing");
    /// Resource-capacity failure.
    pub const CAPACITY: Self = Self("capacity");
    /// Deadline or timeout failure.
    pub const TIMEOUT: Self = Self("timeout");
    /// Temporarily unavailable service or resource.
    pub const UNAVAILABLE: Self = Self("unavailable");
    /// Input/output failure.
    pub const IO: Self = Self("io");
    /// Corrupted or irrecoverable data.
    pub const DATA_CORRUPTION: Self = Self("data_corruption");
    /// Unsupported operation or capability.
    pub const UNSUPPORTED: Self = Self("unsupported");
    /// Internal operational failure.
    pub const INTERNAL: Self = Self("internal");
    /// Violation of an implementation invariant.
    pub const BUG: Self = Self("bug");

    /// Returns the stable class name.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Catalog owner for a canonical descriptor.
///
/// Domain-specific operation and subcomponent values remain typed context and
/// do not replace this catalog-level owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ComponentId(&'static str);

impl ComponentId {
    /// Canonical error core.
    pub const CORE: Self = Self("core");
    /// Protocol encoding and validation.
    pub const PROTOCOL: Self = Self("protocol");
    /// Route discovery and leadership.
    pub const ROUTE: Self = Self("route");
    /// Authentication and authorization.
    pub const AUTH: Self = Self("auth");
    /// Controller services.
    pub const CONTROLLER: Self = Self("controller");
    /// Storage services.
    pub const STORAGE: Self = Self("storage");
    /// Runtime services.
    pub const RUNTIME: Self = Self("runtime");
    /// Transport services.
    pub const TRANSPORT: Self = Self("transport");
    /// Broker services.
    pub const BROKER: Self = Self("broker");
    /// Client services.
    pub const CLIENT: Self = Self("client");
    /// Administrative tools.
    pub const TOOLS: Self = Self("tools");
    /// Observability services.
    pub const OBSERVABILITY: Self = Self("observability");

    /// Returns the stable component name.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for ComponentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Party or resource attributed with a canonical failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultAttribution {
    /// The caller supplied invalid input or violated a contract.
    Caller,
    /// A remote peer failed or rejected an operation.
    RemotePeer,
    /// A local resource failed or became exhausted.
    LocalResource,
    /// A required dependency failed.
    Dependency,
    /// Configuration caused the failure.
    Configuration,
    /// An implementation invariant was violated.
    Bug,
    /// The failure cannot be attributed safely.
    Unknown,
}

/// Public-context exposure policy for a descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Exposure {
    /// The fixed message and descriptor-approved public fields may be exposed.
    Public,
    /// Only the fixed descriptor message may be exposed.
    Generic,
}

/// Catalog-controlled backtrace capture policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BacktracePolicy {
    /// Never capture a backtrace.
    Never,
    /// Capture only when the standard library's backtrace environment enables it.
    OnDemand,
}

/// Default severity for logs, metrics, traces, and alert routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorSeverity {
    /// Diagnostic-only failure.
    Debug,
    /// Informational failure.
    Info,
    /// Warning-level failure.
    Warn,
    /// Error-level failure.
    Error,
    /// Critical failure requiring immediate attention.
    Critical,
}

/// Stable machine-readable error code.
///
/// `ErrorCode` values are intentionally separate from display messages.
/// Protocol mapping, retry policy, and observability should use the code, not
/// formatted error text.
///
/// [`Self::try_new`] accepts only reviewed catalog identity using the lowercase
/// dotted grammar. Source display text and runtime values must not become
/// catalog identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorCode(&'static str);

impl ErrorCode {
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
/// A descriptor owns stable identity, classification, fault attribution,
/// component ownership, the fixed public message, operational policy, public
/// exposure, backtrace capture, and explicit boundary projections. Its ordered
/// field schemas define the only context accepted by descriptor-aware views.
/// Catalog consumers can inspect this metadata but cannot construct or modify
/// descriptors outside this crate.
///
/// ```compile_fail
/// use rocketmq_error::{
///     BacktracePolicy, CanonicalCondition, ComponentId, ErrorClass, ErrorCode,
///     ErrorDescriptor, ErrorSeverity, Exposure, FaultAttribution, RecoveryHint,
/// };
///
/// let descriptor = ErrorDescriptor {
///     code: ErrorCode::try_new("example.operation.failed").unwrap(),
///     class: ErrorClass::INTERNAL,
///     condition: CanonicalCondition::Internal,
///     fault: FaultAttribution::Unknown,
///     component: ComponentId::CORE,
///     public_message: "The operation failed",
///     severity: ErrorSeverity::Error,
///     recovery_hint: RecoveryHint::OperatorAction,
///     backtrace: BacktracePolicy::OnDemand,
///     exposure: Exposure::Generic,
///     projection: todo!(),
///     fields: &[],
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorDescriptor {
    code: ErrorCode,
    class: ErrorClass,
    condition: CanonicalCondition,
    fault: FaultAttribution,
    component: ComponentId,
    public_message: &'static str,
    severity: ErrorSeverity,
    recovery_hint: RecoveryHint,
    backtrace: BacktracePolicy,
    exposure: Exposure,
    projection: ProjectionSpec,
    fields: &'static [FieldSchema],
}

impl ErrorDescriptor {
    #[inline]
    pub(crate) const fn try_new(
        code: ErrorCode,
        class: ErrorClass,
        condition: CanonicalCondition,
        fault: FaultAttribution,
        component: ComponentId,
        public_message: &'static str,
        severity: ErrorSeverity,
        recovery_hint: RecoveryHint,
        backtrace: BacktracePolicy,
        exposure: Exposure,
        projection: ProjectionSpec,
        fields: &'static [FieldSchema],
    ) -> Option<Self> {
        if !valid_descriptor_fields(fields) {
            return None;
        }
        Some(Self {
            code,
            class,
            condition,
            fault,
            component,
            public_message,
            severity,
            recovery_hint,
            backtrace,
            exposure,
            projection,
            fields,
        })
    }

    /// Returns the stable dotted catalog code.
    #[inline]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the broad catalog-owned class.
    #[inline]
    pub const fn class(&self) -> ErrorClass {
        self.class
    }

    /// Returns the protocol-independent canonical condition.
    #[inline]
    pub const fn condition(&self) -> CanonicalCondition {
        self.condition
    }

    /// Returns the catalog-owned fault attribution.
    #[inline]
    pub const fn fault(&self) -> FaultAttribution {
        self.fault
    }

    /// Returns the catalog component that owns this descriptor.
    #[inline]
    pub const fn component(&self) -> ComponentId {
        self.component
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

    /// Returns the catalog-controlled backtrace policy.
    #[inline]
    pub const fn backtrace_policy(&self) -> BacktracePolicy {
        self.backtrace
    }

    /// Returns the public-context exposure policy.
    #[inline]
    pub const fn exposure(&self) -> Exposure {
        self.exposure
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
