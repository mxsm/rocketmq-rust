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

//! Integrity binding and non-destructive verification for Controller release snapshots.

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::CONTROLLER_CONSENSUS_FAILED;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointRestoreVerification;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingKind;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceCapability;
use sha2::Digest;
use sha2::Sha256;

use crate::openraft::inspect_snapshot_payload;

static NEXT_PARTIAL_ARTIFACT_ID: AtomicU64 = AtomicU64::new(1);

/// Snapshot payload and the production manifest that identifies it.
///
/// The remoting maintenance API returns the manifest and retains the payload in
/// Controller storage. Tests and offline tooling may use the payload to prove
/// that restore verification rejects drift before installation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerReleaseSnapshot {
    /// Checksummed release artifact metadata.
    pub manifest: ControllerReleaseSnapshotManifest,
    /// Exact OpenRaft snapshot payload represented by the manifest.
    pub payload: Vec<u8>,
}

/// Persistent, content-addressed repository for Controller release snapshots.
///
/// OpenRaft retains only its current replication snapshot. Release snapshots
/// therefore need an independent immutable artifact boundary so a later Raft
/// snapshot cannot invalidate an authorized rollback checkpoint.
pub(crate) struct ControllerReleaseSnapshotRepository {
    checkpoint_root: PathBuf,
    node_id: u64,
    storage_io: BlockingExecutor,
    cpu_crypto: BlockingExecutor,
}

impl ControllerReleaseSnapshotRepository {
    pub(crate) fn new(checkpoint_root: PathBuf, node_id: u64, service_context: &ChildServiceContext) -> Self {
        Self {
            checkpoint_root,
            node_id,
            storage_io: service_context.storage_io().clone(),
            cpu_crypto: service_context.cpu_crypto().clone(),
        }
    }

    /// Durably publishes a release snapshot as an immutable content-addressed object.
    pub(crate) async fn publish(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        snapshot: &ControllerReleaseSnapshot,
    ) -> RocketMQResult<()> {
        let deadline = authorization_deadline(authorization)?;
        validate_repository_request(
            authorization,
            &snapshot.manifest,
            self.node_id,
            snapshot.payload.len() as u64,
        )?;
        let object_path = snapshot_object_path(&self.checkpoint_root, &snapshot.manifest);
        let payload = snapshot.payload.clone();
        self.storage_io
            .spawn_io_until("controller.publish-release-snapshot", deadline, move || {
                publish_snapshot_object(&object_path, &payload)
            })
            .await
            .map_err(controller_snapshot_error_by)?
    }

    /// Loads and verifies an immutable release snapshot without installing it.
    pub(crate) async fn verify(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &ControllerReleaseSnapshotManifest,
    ) -> RocketMQResult<ReleaseCheckpointRestoreVerification> {
        let deadline = authorization_deadline(authorization)?;
        validate_repository_request(authorization, manifest, self.node_id, manifest.artifact.length_bytes)?;
        let object_path = snapshot_object_path(&self.checkpoint_root, manifest);
        let expected_length = manifest.artifact.length_bytes;
        let payload = self
            .storage_io
            .spawn_io_until("controller.read-release-snapshot", deadline, move || {
                read_snapshot_object(&object_path, expected_length)
            })
            .await
            .map_err(controller_snapshot_error_by)??;
        let manifest = manifest.clone();
        self.cpu_crypto
            .spawn_until(
                "controller.verify-release-snapshot",
                BlockingKind::CpuBound,
                deadline,
                move || verify_controller_release_snapshot(&payload, &manifest),
            )
            .await
            .map_err(controller_snapshot_error_by)?
    }
}

/// Verifies a Controller snapshot without mutating the running state machine.
///
/// # Errors
///
/// Returns a typed snapshot error when the manifest, outer SHA-256, embedded
/// snapshot checksum, Raft position, membership, or snapshot identity differs.
pub fn verify_controller_release_snapshot(
    payload: &[u8],
    manifest: &ControllerReleaseSnapshotManifest,
) -> RocketMQResult<ReleaseCheckpointRestoreVerification> {
    manifest.validate().map_err(controller_snapshot_error_by)?;
    if payload.len() as u64 != manifest.artifact.length_bytes {
        return Err(controller_snapshot_error());
    }
    let actual_sha256 = hex::encode(Sha256::digest(payload));
    if actual_sha256 != manifest.artifact.sha256 {
        return Err(controller_snapshot_error());
    }

    let identity = inspect_snapshot_payload(payload).map_err(controller_snapshot_error_by)?;
    let Some(last_applied) = identity.last_applied else {
        return Err(controller_snapshot_error());
    };
    if identity.snapshot_id != manifest.snapshot_id
        || last_applied.index != manifest.last_applied_index
        || last_applied.leader_id.term != manifest.last_applied_term
        || identity.voter_ids != manifest.voter_ids
    {
        return Err(controller_snapshot_error());
    }
    if !manifest.artifact.uri.ends_with(&format!("/{}", manifest.snapshot_id)) {
        return Err(controller_snapshot_error());
    }

    Ok(ReleaseCheckpointRestoreVerification {
        checkpoint_id: manifest.artifact.checkpoint_id.clone(),
        generation: manifest.artifact.generation,
        verified_at_unix_millis: current_millis(),
        checksum_verified: true,
        offsets_verified: true,
        storage_identity_verified: true,
        wal_retained: true,
        persistent_volume_retained: true,
    })
}

pub(crate) fn controller_snapshot_error() -> RocketMQError {
    RocketMQError::Shared(Arc::new(
        Error::new(&CONTROLLER_CONSENSUS_FAILED).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, "operate on controller release snapshot")
                .with_text(fields::PHASE, "snapshot")
                .with_secret_presence(fields::REASON_PRESENT),
        ),
    ))
}

pub(crate) fn controller_snapshot_error_by(source: impl std::error::Error + Send + Sync + 'static) -> RocketMQError {
    RocketMQError::Shared(Arc::new(
        Error::caused_by(&CONTROLLER_CONSENSUS_FAILED, source).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, "operate on controller release snapshot")
                .with_text(fields::PHASE, "snapshot")
                .with_secret_presence(fields::SOURCE_PRESENT),
        ),
    ))
}

fn authorization_deadline(authorization: &MaintenanceAuthorizationGrant) -> RocketMQResult<ShutdownDeadline> {
    if authorization.capability() != MaintenanceCapability::ReleaseCheckpoint {
        return Err(RocketMQError::authentication_failed(
            "maintenance grant does not authorize release checkpoints",
        ));
    }
    let remaining = authorization
        .deadline_unix_millis()
        .checked_sub(current_millis())
        .filter(|remaining| *remaining > 0)
        .ok_or_else(controller_snapshot_error)?;
    Ok(ShutdownDeadline::after(Duration::from_millis(remaining)))
}

fn validate_repository_request(
    authorization: &MaintenanceAuthorizationGrant,
    manifest: &ControllerReleaseSnapshotManifest,
    node_id: u64,
    payload_length: u64,
) -> RocketMQResult<()> {
    manifest.validate().map_err(controller_snapshot_error_by)?;
    if payload_length != manifest.artifact.length_bytes {
        return Err(controller_snapshot_error());
    }
    let max_snapshot_bytes =
        (crate::openraft::SNAPSHOT_MAX_BYTES as u64).min(authorization.resource_budget().max_checkpoint_bytes);
    if payload_length > max_snapshot_bytes || manifest.artifact.length_bytes > max_snapshot_bytes {
        return Err(RocketMQError::MessageTooLarge {
            actual: usize::try_from(payload_length.max(manifest.artifact.length_bytes)).unwrap_or(usize::MAX),
            limit: usize::try_from(max_snapshot_bytes).unwrap_or(usize::MAX),
        });
    }
    let expected_uri = format!(
        "controller://node-{node_id}/objects/{}/{}",
        manifest.artifact.sha256, manifest.snapshot_id
    );
    if manifest.artifact.uri != expected_uri {
        return Err(controller_snapshot_error());
    }
    Ok(())
}

fn snapshot_object_path(root: &Path, manifest: &ControllerReleaseSnapshotManifest) -> PathBuf {
    root.join("objects")
        .join(format!("{}.snapshot", manifest.artifact.sha256))
}

fn publish_snapshot_object(path: &Path, payload: &[u8]) -> RocketMQResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(controller_snapshot_error_by)?;
    }
    if path.exists() {
        return verify_existing_snapshot_object(path, payload);
    }

    let partial_id = NEXT_PARTIAL_ARTIFACT_ID.fetch_add(1, Ordering::Relaxed);
    let partial_path = path.with_extension(format!("snapshot.partial-{}-{partial_id}", std::process::id()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partial_path)
            .map_err(controller_snapshot_error_by)?;
        file.write_all(payload)
            .and_then(|()| file.sync_all())
            .map_err(controller_snapshot_error_by)?;
        match fs::rename(&partial_path, path) {
            Ok(()) => Ok(()),
            Err(_error) if path.exists() => verify_existing_snapshot_object(path, payload),
            Err(error) => Err(controller_snapshot_error_by(error)),
        }
    })();
    if result.is_err() || partial_path.exists() {
        let _ = fs::remove_file(&partial_path);
    }
    result
}

fn verify_existing_snapshot_object(path: &Path, payload: &[u8]) -> RocketMQResult<()> {
    let existing = fs::read(path).map_err(controller_snapshot_error_by)?;
    if existing != payload {
        return Err(controller_snapshot_error());
    }
    Ok(())
}

fn read_snapshot_object(path: &Path, expected_length: u64) -> RocketMQResult<Vec<u8>> {
    let metadata = fs::metadata(path).map_err(controller_snapshot_error_by)?;
    if !metadata.is_file() || metadata.len() != expected_length {
        return Err(controller_snapshot_error());
    }
    fs::read(path).map_err(controller_snapshot_error_by)
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn snapshot_errors_preserve_consensus_identity_r2015_and_typed_source() {
        let error = controller_snapshot_error_by(std::io::Error::other("private snapshot detail"));

        assert_eq!(error.descriptor(), &CONTROLLER_CONSENSUS_FAILED);
        assert_eq!(error.descriptor().projection().remoting().code.as_i32(), 2015);
        let RocketMQError::Shared(error) = error else {
            panic!("snapshot errors must use the canonical carrier");
        };
        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
        assert_eq!(
            error.public_view().expect("schema-valid view").message(),
            "Controller consensus operation failed"
        );
    }

    #[test]
    fn immutable_snapshot_object_is_idempotent_and_rejects_content_drift() {
        let directory = tempfile::tempdir().expect("temporary snapshot repository");
        let path = directory.path().join("objects").join("artifact.snapshot");

        publish_snapshot_object(&path, b"controller-snapshot").expect("publish object");
        publish_snapshot_object(&path, b"controller-snapshot").expect("idempotent publication");
        assert_eq!(
            read_snapshot_object(&path, 19).expect("read object"),
            b"controller-snapshot"
        );
        assert!(publish_snapshot_object(&path, b"different").is_err());
    }

    #[test]
    fn snapshot_reader_rejects_manifest_length_drift() {
        let directory = tempfile::tempdir().expect("temporary snapshot repository");
        let path = directory.path().join("objects").join("artifact.snapshot");
        publish_snapshot_object(&path, b"payload").expect("publish object");

        assert!(read_snapshot_object(&path, 8).is_err());
    }
}
