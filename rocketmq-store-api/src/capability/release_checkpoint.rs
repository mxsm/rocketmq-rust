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

use std::error::Error as StdError;

use rocketmq_auth::MaintenanceAuthorizationGrant;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointRestoreVerification;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointRequest;

/// Authorized, deadline-bounded Store checkpoint capability.
///
/// The non-forgeable [`MaintenanceAuthorizationGrant`] keeps this API separate
/// from ordinary administrative storage operations.
#[allow(async_fn_in_trait)]
pub trait ReleaseCheckpointStore: Send + Sync {
    type Error: StdError + Send + Sync + 'static;

    /// Flushes the Store barrier and creates a checksummed checkpoint artifact.
    ///
    /// # Errors
    ///
    /// Returns a typed backend error when authorization has expired, the flush
    /// barrier cannot be completed, the artifact exceeds its resource budget,
    /// or checkpoint persistence fails.
    async fn create_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: StoreReleaseCheckpointRequest,
    ) -> Result<StoreReleaseCheckpointManifest, Self::Error>;

    /// Verifies that a checkpoint can be restored without replacing its WAL or
    /// persistent volume identity.
    ///
    /// # Errors
    ///
    /// Returns a typed backend error when authorization has expired or artifact
    /// integrity, generation, offsets, WAL, or storage identity cannot be proven.
    async fn restore_verify_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &StoreReleaseCheckpointManifest,
    ) -> Result<ReleaseCheckpointRestoreVerification, Self::Error>;
}
