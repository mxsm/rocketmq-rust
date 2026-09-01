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

use rocketmq_security_api::MaintenanceAuthorizationGrant;

use crate::checkpoint::CheckpointManifest;
use crate::checkpoint::CheckpointRequest;
use crate::checkpoint::CheckpointRestoreVerification;
use crate::StoreError;

/// Result of attempting to create a release checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "the public checkpoint outcome contract requires an inline manifest payload"
)]
pub enum ReleaseCheckpointCreateOutcome {
    /// The checkpoint was created successfully.
    Created(CheckpointManifest),
    /// The request was rejected without an operational storage failure.
    Rejected(ReleaseCheckpointCreateRejection),
}

/// Expected rejection from release-checkpoint creation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseCheckpointCreateRejection {
    /// The authorization grant expired before the operation began.
    AuthorizationExpired,
    /// The grant does not authorize release-checkpoint creation.
    CapabilityNotGranted,
    /// The requested checkpoint already exists.
    AlreadyExists,
    /// The checkpoint exceeded its authorized resource budget.
    CapacityExceeded {
        /// Bytes observed before the operation was rejected.
        actual_bytes: u64,
        /// Maximum bytes allowed for this checkpoint.
        maximum_bytes: u64,
    },
}

/// Result of verifying a release checkpoint for restoration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseCheckpointRestoreOutcome {
    /// The checkpoint was verified successfully.
    Verified(CheckpointRestoreVerification),
    /// The request was rejected without an operational storage failure.
    Rejected(ReleaseCheckpointRestoreRejection),
}

/// Expected rejection from release-checkpoint restore verification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseCheckpointRestoreRejection {
    /// The authorization grant expired before the operation began.
    AuthorizationExpired,
    /// The grant does not authorize release-checkpoint verification.
    CapabilityNotGranted,
}

/// Authorized, deadline-bounded Store checkpoint capability.
///
/// The non-forgeable [`MaintenanceAuthorizationGrant`] keeps this API separate
/// from ordinary administrative storage operations.
#[allow(async_fn_in_trait)]
pub trait ReleaseCheckpointStore: Send + Sync {
    /// Flushes the Store barrier and creates a checksummed checkpoint artifact.
    ///
    /// # Errors
    ///
    /// Returns an expected rejection for authorization, conflicts, or resource
    /// limits. Operational failures are returned as [`StoreError`] with
    /// [`crate::StoreOperation::Flush`].
    async fn create_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: CheckpointRequest,
    ) -> Result<ReleaseCheckpointCreateOutcome, StoreError>;

    /// Verifies that a checkpoint can be restored without replacing its WAL or
    /// persistent volume identity.
    ///
    /// # Errors
    ///
    /// Returns an expected rejection for authorization decisions. Operational
    /// verification failures are returned as [`StoreError`] with
    /// [`crate::StoreOperation::Read`].
    async fn restore_verify_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &CheckpointManifest,
    ) -> Result<ReleaseCheckpointRestoreOutcome, StoreError>;
}
