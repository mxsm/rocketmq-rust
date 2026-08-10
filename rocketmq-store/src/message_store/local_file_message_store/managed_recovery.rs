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

use rocketmq_store_local::mapped_file::prepare_managed_lifecycle_activation;
use rocketmq_store_local::mapped_file::LockedManagedLifecycleInspection;
use rocketmq_store_local::mapped_file::ManagedLifecycleActivationError;
use rocketmq_store_local::mapped_file::ManagedLifecycleActivationErrorKind;
use rocketmq_store_local::mapped_file::ManagedReconciliationDisposition;
use rocketmq_store_local::mapped_file::ManagedReconciliationError;
use rocketmq_store_local::mapped_file::ManagedReconciliationErrorKind;
use rocketmq_store_local::mapped_file::ManagedReconciliationLimits;
use rocketmq_store_local::mapped_file::ManagedRecoverySession;
use rocketmq_store_local::mapped_file::PreparedManagedLifecycleActivation;

use super::root_lock::StoreRootLease;
use crate::store_error::StoreComponent;
use crate::store_error::StoreError;
use crate::store_error::StoreErrorKind;
use crate::store_error::StoreOperation;

/// Result of the complete read-only Wave-A proof chain.
#[derive(Debug)]
pub(super) enum ManagedReadOnlyDisposition {
    Ready(PreparedManagedLifecycleActivation),
    RecoveryRequired(ManagedRecoverySession),
}

/// Replays and reconciles managed lifecycle evidence before any persistent Store component exists.
///
/// This function deliberately returns data rather than a publication capability. Wave-B must bind
/// the reconciled session to staged queue generations, the retirement registry, and the owned
/// reaper before the constructor may proceed beyond this boundary.
pub(super) fn inspect_and_reconcile_managed_root(
    store_root_lease: &StoreRootLease,
) -> Result<ManagedReadOnlyDisposition, StoreError> {
    let inspection = store_root_lease.inspect_managed_lifecycle(StoreOperation::Load)?;
    let LockedManagedLifecycleInspection::Managed(session) = inspection else {
        return Err(StoreError::new(StoreErrorKind::Corruption, StoreOperation::Load)
            .in_component(StoreComponent::MappedFile)
            .with_detail("managed lifecycle evidence disappeared between classification and reconciliation"));
    };

    match session
        .reconcile(ManagedReconciliationLimits::default())
        .map_err(managed_reconciliation_error)?
    {
        ManagedReconciliationDisposition::Ready(reconciled) => prepare_managed_lifecycle_activation(reconciled)
            .map(ManagedReadOnlyDisposition::Ready)
            .map_err(managed_activation_error),
        ManagedReconciliationDisposition::RecoveryRequired(recovery) => {
            Ok(ManagedReadOnlyDisposition::RecoveryRequired(recovery))
        }
    }
}

/// Converts a successful read-only proof into the deliberate Wave-B activation fence.
pub(super) fn wave_b_activation_fence(disposition: ManagedReadOnlyDisposition) -> StoreError {
    let detail = match disposition {
        ManagedReadOnlyDisposition::Ready(prepared) => format!(
            "managed lifecycle reconciled {} active segments and rebuilt {} pending retirements, but Wave-B queue, registry, and reaper activation is not yet enabled",
            prepared.unclaimed_active_count(),
            prepared.recovered_retirement_count(),
        ),
        ManagedReadOnlyDisposition::RecoveryRequired(recovery) => format!(
            "managed lifecycle requires {} durable recovery actions, but Wave-B lifecycle writes are not yet enabled",
            recovery.required_action_count(),
        ),
    };
    StoreError::new(StoreErrorKind::Unsupported, StoreOperation::Load)
        .in_component(StoreComponent::MappedFile)
        .with_detail(detail)
}

fn managed_activation_error(error: ManagedLifecycleActivationError) -> StoreError {
    let kind = match error.kind() {
        ManagedLifecycleActivationErrorKind::QueueLoad | ManagedLifecycleActivationErrorKind::SegmentClaim => {
            StoreErrorKind::Unavailable
        }
        ManagedLifecycleActivationErrorKind::Registry
        | ManagedLifecycleActivationErrorKind::DuplicateQueue
        | ManagedLifecycleActivationErrorKind::StagingFailed
        | ManagedLifecycleActivationErrorKind::UnclaimedSegments
        | ManagedLifecycleActivationErrorKind::FleetFenceMismatch => StoreErrorKind::Corruption,
        ManagedLifecycleActivationErrorKind::Writer | ManagedLifecycleActivationErrorKind::Namespace => {
            StoreErrorKind::Storage
        }
    };
    StoreError::new(kind, StoreOperation::Load)
        .in_component(StoreComponent::MappedFile)
        .with_detail("managed lifecycle activation staging failed before Store construction")
        .with_source(error)
}

fn managed_reconciliation_error(error: ManagedReconciliationError) -> StoreError {
    let kind = match error.kind() {
        ManagedReconciliationErrorKind::Inventory => StoreErrorKind::Unavailable,
        ManagedReconciliationErrorKind::ReplayRecoveryRequired
        | ManagedReconciliationErrorKind::MissingStoreUuid
        | ManagedReconciliationErrorKind::State => StoreErrorKind::Corruption,
    };
    StoreError::new(kind, StoreOperation::Load)
        .in_component(StoreComponent::MappedFile)
        .with_detail("managed replay and namespace reconciliation failed before Store construction")
        .with_source(error)
}
