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

//! Deterministic checkpoint-set assembly and restore-proof validation.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use rocketmq_error::Sensitive;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::MaintenanceCapabilitiesResponse;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointRestoreVerification;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointSetManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::RELEASE_CHECKPOINT_SCHEMA_VERSION;

use crate::core::AdminError;
use crate::core::AdminResult;

const REQUIRED_COMMON_OPERATIONS: [&str; 3] = ["capabilities", "verify_checkpoint", "restore_verify"];

/// Validated policy capabilities used to constrain set assembly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedMaintenanceCapabilities {
    response: MaintenanceCapabilitiesResponse,
}

impl ValidatedMaintenanceCapabilities {
    /// Validates the capability response returned by a maintenance endpoint.
    pub fn try_from_response(response: MaintenanceCapabilitiesResponse) -> AdminResult<Self> {
        if response.schema_version != RELEASE_CHECKPOINT_SCHEMA_VERSION
            || response.policy_id.trim().is_empty()
            || response.policy_version == 0
            || response.max_checkpoint_bytes == 0
            || response.max_store_members == 0
            || response.max_concurrent_operations == 0
        {
            return Err(AdminError::invalid_argument(
                "maintenanceCapabilities",
                "schema, policy identity, and resource budgets must be complete",
            ));
        }
        let operations = response.operations.iter().map(String::as_str).collect::<BTreeSet<_>>();
        if REQUIRED_COMMON_OPERATIONS
            .iter()
            .any(|operation| !operations.contains(operation))
        {
            return Err(AdminError::invalid_argument(
                "maintenanceCapabilities.operations",
                "required release-checkpoint operations are missing",
            ));
        }
        let create_operation = if response.store.is_some() {
            "create_store_checkpoint"
        } else {
            "create_controller_snapshot"
        };
        if !operations.contains(create_operation) {
            return Err(AdminError::invalid_argument(
                "maintenanceCapabilities.operations",
                format!("required operation '{create_operation}' is missing"),
            ));
        }
        if let Some(store) = response.store.as_ref() {
            if store.member_id.trim().is_empty()
                || store.storage_identity.volume_id.trim().is_empty()
                || store.storage_identity.wal_generation == 0
            {
                return Err(AdminError::invalid_argument(
                    "maintenanceCapabilities.store",
                    "Store member and persistent storage identity must be complete",
                ));
            }
        }
        Ok(Self { response })
    }

    pub const fn policy_version(&self) -> u64 {
        self.response.policy_version
    }

    pub const fn max_store_members(&self) -> u32 {
        self.response.max_store_members
    }

    pub const fn response(&self) -> &MaintenanceCapabilitiesResponse {
        &self.response
    }
}

/// Constructs a complete set from independently checksummed Controller and
/// Store artifacts captured under one barrier.
#[derive(Clone, Eq, PartialEq)]
pub struct ReleaseCheckpointSetBuilder {
    release_id: String,
    policy_version: u64,
    fencing_token: u64,
    max_store_members: u32,
}

impl fmt::Debug for ReleaseCheckpointSetBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReleaseCheckpointSetBuilder")
            .field("release_id", &self.release_id)
            .field("policy_version", &self.policy_version)
            .field("fencing_token", &Sensitive::new(self.fencing_token))
            .field("max_store_members", &self.max_store_members)
            .finish()
    }
}

impl ReleaseCheckpointSetBuilder {
    pub fn new(
        release_id: impl Into<String>,
        policy_version: u64,
        fencing_token: u64,
        max_store_members: u32,
    ) -> AdminResult<Self> {
        let release_id = release_id.into();
        if release_id.trim().is_empty() || policy_version == 0 || fencing_token == 0 || max_store_members == 0 {
            return Err(AdminError::invalid_argument(
                "releaseCheckpointSet",
                "release ID, policy version, fencing token, and Store budget are required",
            ));
        }
        Ok(Self {
            release_id,
            policy_version,
            fencing_token,
            max_store_members,
        })
    }

    /// Binds all artifacts to the Controller barrier and validates the complete set.
    pub fn build(
        self,
        controller: ControllerReleaseSnapshotManifest,
        stores: Vec<StoreReleaseCheckpointManifest>,
        created_at_unix_millis: u64,
    ) -> AdminResult<ReleaseCheckpointSetManifest> {
        if stores.len() > self.max_store_members as usize {
            return Err(AdminError::invalid_argument(
                "stores",
                format!(
                    "{} Store members exceed policy maximum {}",
                    stores.len(),
                    self.max_store_members
                ),
            ));
        }
        let artifact = &controller.artifact;
        let manifest = ReleaseCheckpointSetManifest {
            schema_version: RELEASE_CHECKPOINT_SCHEMA_VERSION,
            checkpoint_set_id: artifact.checkpoint_set_id.clone(),
            release_id: self.release_id,
            generation: artifact.generation,
            barrier_id: artifact.barrier_id.clone(),
            policy_version: self.policy_version,
            fencing_token: self.fencing_token,
            created_at_unix_millis,
            controller,
            stores,
        };
        verify_checkpoint_set(&manifest)?;
        Ok(manifest)
    }
}

/// Verifies all intrinsic and cross-artifact checkpoint-set invariants.
pub fn verify_checkpoint_set(manifest: &ReleaseCheckpointSetManifest) -> AdminResult<()> {
    manifest
        .validate()
        .map_err(|error| AdminError::invalid_argument("checkpointSet", error.to_string()))
}

/// Verifies that every Controller/Store member produced exactly one complete
/// restore proof for the same generation.
pub fn verify_checkpoint_set_restore(
    manifest: &ReleaseCheckpointSetManifest,
    proofs: &[ReleaseCheckpointRestoreVerification],
) -> AdminResult<()> {
    verify_checkpoint_set(manifest)?;
    let mut by_checkpoint = BTreeMap::new();
    for proof in proofs {
        proof
            .validate()
            .map_err(|error| AdminError::invalid_argument("restoreProof", error.to_string()))?;
        if proof.generation != manifest.generation {
            return Err(AdminError::invalid_argument(
                "restoreProof.generation",
                "does not match the checkpoint set",
            ));
        }
        if by_checkpoint.insert(proof.checkpoint_id.as_str(), proof).is_some() {
            return Err(AdminError::invalid_argument(
                "restoreProof.checkpointId",
                "duplicate restore proof",
            ));
        }
    }

    let required = std::iter::once(manifest.controller.artifact.checkpoint_id.as_str())
        .chain(
            manifest
                .stores
                .iter()
                .map(|store| store.artifact.checkpoint_id.as_str()),
        )
        .collect::<BTreeSet<_>>();
    let provided = by_checkpoint.keys().copied().collect::<BTreeSet<_>>();
    if required != provided {
        return Err(AdminError::invalid_argument(
            "restoreProofs",
            "must contain exactly one proof for the Controller and every Store member",
        ));
    }
    Ok(())
}

pub fn decode_checkpoint_set(bytes: &[u8]) -> AdminResult<ReleaseCheckpointSetManifest> {
    let manifest = serde_json::from_slice(bytes)
        .map_err(|error| AdminError::invalid_argument("checkpointSet", error.to_string()))?;
    verify_checkpoint_set(&manifest)?;
    Ok(manifest)
}

pub fn encode_checkpoint_set(manifest: &ReleaseCheckpointSetManifest) -> AdminResult<Vec<u8>> {
    verify_checkpoint_set(manifest)?;
    serde_json::to_vec_pretty(manifest).map_err(|error| AdminError::backend("encode checkpoint set", error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointArtifact;
    use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointBackend;
    use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointOffsets;
    use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointStorageIdentity;

    fn artifact(checkpoint_id: &str) -> ReleaseCheckpointArtifact {
        ReleaseCheckpointArtifact {
            schema_version: 1,
            checkpoint_id: checkpoint_id.to_string(),
            checkpoint_set_id: "set-7".to_string(),
            generation: 7,
            barrier_id: "barrier-42".to_string(),
            created_at_unix_millis: 1_800_000_000_000,
            length_bytes: 512,
            sha256: "a".repeat(64),
            uri: format!("file:///checkpoints/{checkpoint_id}"),
        }
    }

    fn controller() -> ControllerReleaseSnapshotManifest {
        ControllerReleaseSnapshotManifest {
            artifact: artifact("controller-7"),
            snapshot_id: "snapshot-99".to_string(),
            last_applied_index: 99,
            last_applied_term: 3,
            voter_ids: vec![1, 2, 3],
        }
    }

    fn store(member: &str) -> StoreReleaseCheckpointManifest {
        StoreReleaseCheckpointManifest {
            artifact: artifact(&format!("store-{member}")),
            member_id: member.to_string(),
            backend: ReleaseCheckpointBackend::Local,
            offsets: ReleaseCheckpointOffsets {
                appended_offset: 120,
                durable_offset: 120,
                consume_queue_offset: 100,
                index_offset: 100,
            },
            storage_identity: ReleaseCheckpointStorageIdentity {
                volume_id: format!("pvc-{member}"),
                wal_generation: 7,
            },
            wal_retained: true,
            persistent_volume_retained: true,
        }
    }

    fn proof(checkpoint_id: &str) -> ReleaseCheckpointRestoreVerification {
        ReleaseCheckpointRestoreVerification {
            checkpoint_id: checkpoint_id.to_string(),
            generation: 7,
            verified_at_unix_millis: 1_800_000_001_000,
            checksum_verified: true,
            offsets_verified: true,
            storage_identity_verified: true,
            wal_retained: true,
            persistent_volume_retained: true,
        }
    }

    #[test]
    fn maintenance_capabilities_distinguish_controller_and_store_endpoints() {
        let common = MaintenanceCapabilitiesResponse {
            schema_version: 1,
            policy_id: "release-policy".to_string(),
            policy_version: 7,
            operations: vec![
                "capabilities".to_string(),
                "create_controller_snapshot".to_string(),
                "verify_checkpoint".to_string(),
                "restore_verify".to_string(),
            ],
            max_checkpoint_bytes: 1024,
            max_store_members: 4,
            max_concurrent_operations: 1,
            store: None,
        };
        ValidatedMaintenanceCapabilities::try_from_response(common.clone()).expect("Controller capabilities");

        let mut store = common;
        store.operations[1] = "create_store_checkpoint".to_string();
        store.store = Some(
            rocketmq_protocol::protocol::body::release_checkpoint::MaintenanceStoreCapabilities {
                member_id: "broker-a".to_string(),
                backend: ReleaseCheckpointBackend::Local,
                storage_identity: ReleaseCheckpointStorageIdentity {
                    volume_id: "volume-a".to_string(),
                    wal_generation: 7,
                },
            },
        );
        ValidatedMaintenanceCapabilities::try_from_response(store).expect("Store capabilities");
    }

    #[test]
    fn release_checkpoint_set_binds_barrier_and_requires_every_restore_proof() {
        let manifest = ReleaseCheckpointSetBuilder::new("release-7", 7, 42, 8)
            .expect("builder")
            .build(
                controller(),
                vec![store("broker-a"), store("broker-b")],
                1_800_000_000_000,
            )
            .expect("complete set");
        let bytes = encode_checkpoint_set(&manifest).expect("encode");
        assert_eq!(decode_checkpoint_set(&bytes).expect("decode"), manifest);

        let complete = vec![proof("controller-7"), proof("store-broker-a"), proof("store-broker-b")];
        verify_checkpoint_set_restore(&manifest, &complete).expect("complete restore proof set");
        assert!(verify_checkpoint_set_restore(&manifest, &complete[..2]).is_err());
    }

    #[test]
    fn release_checkpoint_set_rejects_barrier_drift_and_store_budget_overrun() {
        let mut drifted = store("broker-a");
        drifted.artifact.barrier_id = "other-barrier".to_string();
        assert!(ReleaseCheckpointSetBuilder::new("release-7", 7, 42, 8)
            .expect("builder")
            .build(controller(), vec![drifted], 1_800_000_000_000)
            .is_err());
        assert!(ReleaseCheckpointSetBuilder::new("release-7", 7, 42, 1)
            .expect("builder")
            .build(
                controller(),
                vec![store("broker-a"), store("broker-b")],
                1_800_000_000_000
            )
            .is_err());
    }
}
