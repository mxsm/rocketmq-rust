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

use rocketmq_error::CanonicalCondition;

use crate::protocol::header_codec::HeaderValueKind;
use crate::protocol::remoting_command_defaults::RemotingCommandDefaultsConflict;
use crate::protocol::remoting_command_facade::InvalidRemotingSerializeType;

/// A deterministic RocketMQ protocol contract violation.
///
/// Header variants contain only static schema metadata and never retain raw
/// wire values. Typed configuration sources remain available through
/// [`std::error::Error::source`], while their text is excluded from this
/// error's fixed display messages.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolContractViolation {
    /// A checkpoint uses an unsupported manifest schema version.
    #[error("checkpoint schema version {actual} does not match {expected}")]
    CheckpointSchemaVersion {
        /// Supported schema version.
        expected: u16,
        /// Version carried by the manifest.
        actual: u16,
    },
    /// A checkpoint field violates a fixed validation rule.
    #[error("invalid checkpoint field {field}: {reason}")]
    InvalidCheckpointField {
        /// Stable field identifier.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Checkpoint offsets violate a fixed ordering rule.
    #[error("invalid checkpoint offsets: {reason}")]
    InvalidCheckpointOffsets {
        /// Stable offset-ordering reason.
        reason: &'static str,
    },
    /// A checkpoint set does not contain a Store member.
    #[error("checkpoint set has no Store member")]
    MissingStoreMembers,
    /// A checkpoint set repeats a Store member.
    #[error("checkpoint set repeats a Store member")]
    DuplicateStoreMember,
    /// A checkpoint artifact does not match its set barrier.
    #[error("checkpoint member does not match the set barrier")]
    CheckpointSetBindingMismatch {
        /// Static checkpoint member category.
        member: &'static str,
    },
    /// A checkpoint permits destructive rollback behavior.
    #[error("checkpoint permits destructive WAL or persistent-volume replacement")]
    DestructiveRollback,
    /// Restore verification does not prove every required invariant.
    #[error("checkpoint restore verification is incomplete")]
    RestoreVerificationIncomplete,
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
    /// A legacy custom-header validation hook failed.
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
    /// Process remoting configuration selects an unsupported serialization type.
    #[error("invalid remoting serialization type")]
    InvalidSerializeType(
        #[from]
        #[source]
        InvalidRemotingSerializeType,
    ),
    /// Process remoting defaults conflict with an initialized value.
    #[error("remoting command defaults conflict with the initialized value")]
    RemotingDefaultsConflict(
        #[from]
        #[source]
        RemotingCommandDefaultsConflict,
    ),
    /// A subscription-group name is blank.
    #[error("The specified group is blank.")]
    BlankSubscriptionGroup,
    /// A subscription-group name exceeds the supported length.
    #[error("The specified group is longer than group max length: {max_length}")]
    SubscriptionGroupTooLong {
        /// Maximum accepted UTF-8 byte length.
        max_length: usize,
    },
    /// A subscription-group name contains unsupported characters.
    #[error("The specified group contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$")]
    SubscriptionGroupIllegalCharacters,
    /// A batch contains the same subscription-group name more than once.
    #[error("The specified group list contains a duplicate group.")]
    DuplicateSubscriptionGroup,
}

impl ProtocolContractViolation {
    /// Returns the protocol-independent condition for this violation.
    #[must_use]
    pub const fn condition(&self) -> CanonicalCondition {
        match self {
            Self::DestructiveRollback | Self::RestoreVerificationIncomplete | Self::RemotingDefaultsConflict(_) => {
                CanonicalCondition::FailedPrecondition
            }
            Self::CheckpointSchemaVersion { .. }
            | Self::InvalidCheckpointField { .. }
            | Self::InvalidCheckpointOffsets { .. }
            | Self::MissingStoreMembers
            | Self::DuplicateStoreMember
            | Self::CheckpointSetBindingMismatch { .. }
            | Self::Missing { .. }
            | Self::InvalidValue { .. }
            | Self::Conflict { .. }
            | Self::DynamicFieldConflict { .. }
            | Self::LegacyValidation { .. }
            | Self::LegacyMapConversionFailed { .. }
            | Self::Validation { .. }
            | Self::JavaRange { .. }
            | Self::KeyLengthOverflow { .. }
            | Self::ValueLengthOverflow { .. }
            | Self::ExtensionFieldsLengthOverflow
            | Self::DynamicKeyLengthOverflow
            | Self::DynamicValueLengthOverflow
            | Self::FastCodecUnavailable { .. }
            | Self::InvalidSerializeType(_)
            | Self::BlankSubscriptionGroup
            | Self::SubscriptionGroupTooLong { .. }
            | Self::SubscriptionGroupIllegalCharacters
            | Self::DuplicateSubscriptionGroup => CanonicalCondition::InvalidArgument,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;
    use crate::protocol::remoting_command::SERIALIZE_TYPE_PROPERTY;
    use crate::protocol::remoting_command_facade::resolve_remoting_serialize_type;

    #[test]
    fn invalid_configuration_keeps_a_typed_source_behind_fixed_text() {
        const SENTINEL: &str = "unsupported-serialize-type-secret";

        let error = resolve_remoting_serialize_type(Some(SENTINEL), None).unwrap_err();

        assert_eq!(error.condition(), CanonicalCondition::InvalidArgument);
        assert_eq!(error.to_string(), "invalid remoting serialization type");
        assert!(!format!("{error:?}").contains(SENTINEL));
        let source = error
            .source()
            .and_then(|source| source.downcast_ref::<InvalidRemotingSerializeType>())
            .expect("typed remoting serialization source");
        assert_eq!(source.key(), SERIALIZE_TYPE_PROPERTY);
        assert_eq!(source.value(), SENTINEL);
    }

    #[test]
    fn rollback_contracts_are_failed_preconditions() {
        assert_eq!(
            ProtocolContractViolation::DestructiveRollback.condition(),
            CanonicalCondition::FailedPrecondition
        );
        assert_eq!(
            ProtocolContractViolation::RestoreVerificationIncomplete.condition(),
            CanonicalCondition::FailedPrecondition
        );
    }
}
