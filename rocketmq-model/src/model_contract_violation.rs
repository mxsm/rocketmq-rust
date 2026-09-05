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
use thiserror::Error;

/// Deterministic validation failure at the runtime-neutral model boundary.
///
/// The public messages are fixed. Variant fields retain only static schema
/// identifiers and numeric admission evidence.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ModelContractViolation {
    /// A topic-creation attribute operation did not use the add form.
    #[error("topic creation attribute operation must use the add form")]
    AttributeCreateRequiresAdd,
    /// An attribute delete operation named a key that is not present.
    #[error("attribute delete operation targets a missing key")]
    AttributeDeleteTargetsMissingKey,
    /// An attribute operation key did not have a supported form.
    #[error("attribute operation key has an unsupported form")]
    AttributeOperationKeyHasUnsupportedForm,
    /// An attribute operation set contains the same key more than once.
    #[error("attribute operation set contains a duplicate key")]
    AttributeOperationSetContainsDuplicateKey,
    /// An attribute operation key is structurally invalid.
    #[error("attribute operation key is invalid")]
    AttributeOperationKeyIsInvalid,
    /// An attribute operation named an unsupported key.
    #[error("attribute operation targets an unsupported key")]
    AttributeOperationTargetsUnsupportedKey,
    /// An attribute update targeted an immutable attribute.
    #[error("attribute update targets an immutable attribute")]
    AttributeUpdateTargetsImmutableAttribute,
    /// An attribute value does not satisfy the selected attribute's rules.
    #[error("attribute value does not satisfy the attribute rules")]
    AttributeValueDoesNotSatisfyRules,
    /// A cleanup-policy parser input did not name a supported policy.
    #[error("cleanup policy is invalid")]
    InvalidCleanupPolicy,
    /// A consume-queue type parser input did not name a supported type.
    #[error("consume queue type is invalid")]
    InvalidCqType,
    /// A timer request did not specify a supported delivery property.
    #[error("timer request does not contain a supported delivery property")]
    MissingTimerDeliveryProperty,
    /// A timer property does not contain an unsigned integer.
    #[error("timer property has an invalid unsigned integer value")]
    InvalidTimerProperty {
        /// Name of the selected timer property.
        property: &'static str,
    },
    /// A timer precision is not supported by the Java-compatible timer engine.
    #[error("timer precision is unsupported")]
    UnsupportedTimerPrecision {
        /// Requested timer precision in milliseconds.
        precision_ms: u64,
    },
    /// Checked timer-delivery arithmetic overflowed.
    #[error("timer delivery time arithmetic overflowed")]
    TimerDeliveryArithmeticOverflow,
    /// A timer delivery timestamp is not later than the caller's clock sample.
    #[error("timer delivery time is not in the future")]
    TimerDeliveryTimeIsNotInFuture {
        /// Requested delivery timestamp in milliseconds.
        deliver_ms: u64,
        /// Caller-supplied current timestamp in milliseconds.
        now_ms: u64,
    },
    /// A timer delay exceeds the configured admission horizon.
    #[error("timer delay exceeds the configured maximum")]
    TimerDelayExceedsMaximum {
        /// Requested delay in milliseconds.
        delay_ms: u64,
        /// Configured maximum delay in milliseconds.
        max_delay_ms: u64,
    },
    /// A POP retry policy does not describe a supported migration state.
    #[error("POP retry policy does not describe a supported migration state")]
    InvalidPopRetryPolicyState,
}

impl ModelContractViolation {
    /// Returns the canonical condition for every model contract rejection.
    #[inline]
    pub const fn condition(&self) -> CanonicalCondition {
        CanonicalCondition::InvalidArgument
    }
}
