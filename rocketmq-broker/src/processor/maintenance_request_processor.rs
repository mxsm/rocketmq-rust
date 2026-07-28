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

//! Privileged Broker maintenance protocol, separate from ordinary Admin APIs.

use std::sync::Arc;

use rocketmq_auth::AuthRuntime;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::release_checkpoint::MaintenanceCapabilitiesResponse;
use rocketmq_protocol::protocol::body::release_checkpoint::MaintenanceStoreCapabilities;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointArtifact;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointBackend;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointOffsets;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointRestoreVerification;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointStorageIdentity;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointRequest;
use rocketmq_protocol::protocol::body::release_checkpoint::RELEASE_CHECKPOINT_SCHEMA_VERSION;
use rocketmq_protocol::protocol::header::maintenance_request_header::MaintenanceRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::MaintenanceAuthorizationContext;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceAuthorizer;
use rocketmq_security_api::MaintenanceCapability;
use rocketmq_security_api::MaintenanceRequestClass;
use rocketmq_store::StoreReleaseCheckpointService;
use rocketmq_store_api::checkpoint::CheckpointArtifact;
use rocketmq_store_api::checkpoint::CheckpointBackend;
use rocketmq_store_api::checkpoint::CheckpointManifest;
use rocketmq_store_api::checkpoint::CheckpointOffsets;
use rocketmq_store_api::checkpoint::CheckpointRequest;
use rocketmq_store_api::checkpoint::CheckpointRestoreVerification;
use rocketmq_store_api::checkpoint::CheckpointStorageIdentity;
use rocketmq_store_api::ReleaseCheckpointStore;
use rocketmq_transport::request_code_not_supported_with_remark;
use rocketmq_transport::Channel;
use rocketmq_transport::ConnectionHandlerContext;
use rocketmq_transport::RemotingRequestProcessor as RequestProcessor;

use crate::config::broker_config::BrokerConfig;

/// Broker endpoint for release-checkpoint operations.
pub struct MaintenanceRequestProcessor {
    broker_config: Arc<BrokerConfig>,
    auth_runtime: Arc<AuthRuntime>,
    authorizer: Arc<MaintenanceAuthorizer>,
    checkpoint_service: Arc<StoreReleaseCheckpointService>,
}

impl MaintenanceRequestProcessor {
    pub(crate) fn new(
        broker_config: Arc<BrokerConfig>,
        auth_runtime: Arc<AuthRuntime>,
        authorizer: Arc<MaintenanceAuthorizer>,
        checkpoint_service: Arc<StoreReleaseCheckpointService>,
    ) -> Self {
        Self {
            broker_config,
            auth_runtime,
            authorizer,
            checkpoint_service,
        }
    }

    pub(crate) async fn process_request_shared(
        &self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let grant = self.authorize(&channel, request).await?;
        let response = match RequestCode::from(request.code()) {
            RequestCode::MaintenanceGetCapabilities => self.capabilities(&grant).await?,
            RequestCode::MaintenanceCreateStoreCheckpoint => self.create_store_checkpoint(&grant, request).await?,
            RequestCode::MaintenanceVerifyCheckpoint => self.verify_store_checkpoint(request)?,
            RequestCode::MaintenanceRestoreVerify => self.restore_verify(&grant, request).await?,
            _ => {
                return Ok(Some(request_code_not_supported_with_remark(
                    request.code(),
                    format!("request type {} is not a Broker maintenance operation", request.code()),
                )));
            }
        };
        Ok(Some(response.set_opaque(request.opaque())))
    }

    async fn authorize(
        &self,
        channel: &Channel,
        request: &RemotingCommand,
    ) -> RocketMQResult<MaintenanceAuthorizationGrant> {
        if !self.broker_config.maintenance_enabled {
            return Err(RocketMQError::authentication_failed(
                "Broker maintenance API is disabled",
            ));
        }
        let principal = self
            .auth_runtime
            .authenticate_maintenance_principal(request, Some(channel.channel_id()))
            .await?;
        let header = request
            .decode_command_custom_header::<MaintenanceRequestHeader>()
            .map_err(|source| RocketMQError::request_header_source("decode privileged maintenance header", source))?;
        header.validate().map_err(RocketMQError::request_header_error)?;
        if header.policy_version != self.authorizer.policy().policy_version {
            return Err(RocketMQError::authentication_failed(format!(
                "maintenance policy version {} does not match loaded version {}",
                header.policy_version,
                self.authorizer.policy().policy_version
            )));
        }
        self.authorizer
            .authorize(
                Some(&MaintenanceAuthorizationContext {
                    authentication_enabled: self.broker_config.authentication_enabled,
                    authorization_enabled: self.broker_config.authorization_enabled,
                    principal: Some(principal.to_string()),
                    request_class: MaintenanceRequestClass::PrivilegedMaintenance,
                    capability: MaintenanceCapability::ReleaseCheckpoint,
                    deadline_unix_millis: header.deadline_unix_millis,
                    fencing_token: Some(header.fencing_token),
                }),
                rocketmq_runtime::common::time_utils::current_millis(),
            )
            .map_err(|source| RocketMQError::authentication_source("authorize privileged maintenance request", source))
    }

    async fn capabilities(&self, grant: &MaintenanceAuthorizationGrant) -> RocketMQResult<RemotingCommand> {
        let storage_identity = self
            .checkpoint_service
            .storage_identity(grant)
            .await
            .map_err(checkpoint_error)?;
        let backend = self.checkpoint_service.backend().map_err(checkpoint_error)?;
        let response = MaintenanceCapabilitiesResponse {
            schema_version: RELEASE_CHECKPOINT_SCHEMA_VERSION,
            policy_id: self.authorizer.policy().policy_id.clone(),
            policy_version: grant.policy_version(),
            operations: vec![
                "capabilities".to_string(),
                "create_store_checkpoint".to_string(),
                "verify_checkpoint".to_string(),
                "restore_verify".to_string(),
            ],
            max_checkpoint_bytes: grant.resource_budget().max_checkpoint_bytes,
            max_store_members: grant.resource_budget().max_store_members,
            max_concurrent_operations: grant.resource_budget().max_concurrent_operations,
            store: Some(MaintenanceStoreCapabilities {
                member_id: self.broker_config.broker_identity.get_canonical_name(),
                backend: checkpoint_backend_to_wire(backend),
                storage_identity: checkpoint_identity_to_wire(storage_identity),
            }),
        };
        encode_success(&response, "encode Broker maintenance capabilities")
    }

    async fn create_store_checkpoint(
        &self,
        grant: &MaintenanceAuthorizationGrant,
        request: &RemotingCommand,
    ) -> RocketMQResult<RemotingCommand> {
        let body = required_body(request, "MAINTENANCE_CREATE_STORE_CHECKPOINT")?;
        let checkpoint_request: StoreReleaseCheckpointRequest = serde_json::from_slice(body)
            .map_err(|source| RocketMQError::request_body_source("MAINTENANCE_CREATE_STORE_CHECKPOINT", source))?;
        let manifest = self
            .checkpoint_service
            .create_release_checkpoint(grant, checkpoint_request_from_wire(checkpoint_request))
            .await
            .map_err(checkpoint_error)?;
        encode_success(
            &checkpoint_manifest_to_wire(manifest),
            "encode Store release-checkpoint manifest",
        )
    }

    fn verify_store_checkpoint(&self, request: &RemotingCommand) -> RocketMQResult<RemotingCommand> {
        let body = required_body(request, "MAINTENANCE_VERIFY_CHECKPOINT")?;
        let manifest: StoreReleaseCheckpointManifest = serde_json::from_slice(body)
            .map_err(|source| RocketMQError::request_body_source("MAINTENANCE_VERIFY_CHECKPOINT", source))?;
        manifest
            .validate()
            .map_err(|source| RocketMQError::request_body_source("MAINTENANCE_VERIFY_CHECKPOINT", source))?;
        encode_success(&manifest, "encode verified Store release-checkpoint manifest")
    }

    async fn restore_verify(
        &self,
        grant: &MaintenanceAuthorizationGrant,
        request: &RemotingCommand,
    ) -> RocketMQResult<RemotingCommand> {
        let body = required_body(request, "MAINTENANCE_RESTORE_VERIFY")?;
        let manifest: StoreReleaseCheckpointManifest = serde_json::from_slice(body)
            .map_err(|source| RocketMQError::request_body_source("MAINTENANCE_RESTORE_VERIFY", source))?;
        let verification = self
            .checkpoint_service
            .restore_verify_release_checkpoint(grant, &checkpoint_manifest_from_wire(manifest))
            .await
            .map_err(checkpoint_error)?;
        encode_success(
            &checkpoint_verification_to_wire(verification),
            "encode Store restore-verification proof",
        )
    }
}

impl RequestProcessor for MaintenanceRequestProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.process_request_shared(channel, ctx, request).await
    }
}

fn required_body<'a>(request: &'a RemotingCommand, operation: &'static str) -> RocketMQResult<&'a [u8]> {
    request
        .body()
        .map(bytes::Bytes::as_ref)
        .ok_or_else(|| RocketMQError::request_body_invalid(operation, "request body is empty"))
}

fn encode_success<T: serde::Serialize>(value: &T, operation: &'static str) -> RocketMQResult<RemotingCommand> {
    let body = serde_json::to_vec(value).map_err(|error| RocketMQError::internal(operation, error))?;
    Ok(RemotingCommand::create_response_command()
        .set_code(ResponseCode::Success)
        .set_body(body))
}

fn checkpoint_error(error: impl std::error::Error + Send + Sync + 'static) -> RocketMQError {
    RocketMQError::internal("release-checkpoint", error)
}

fn checkpoint_request_from_wire(request: StoreReleaseCheckpointRequest) -> CheckpointRequest {
    CheckpointRequest {
        checkpoint_id: request.checkpoint_id,
        checkpoint_set_id: request.checkpoint_set_id,
        generation: request.generation,
        barrier_id: request.barrier_id,
        member_id: request.member_id,
        offsets: checkpoint_offsets_from_wire(request.offsets),
        storage_identity: checkpoint_identity_from_wire(request.storage_identity),
    }
}

fn checkpoint_manifest_from_wire(manifest: StoreReleaseCheckpointManifest) -> CheckpointManifest {
    CheckpointManifest {
        artifact: checkpoint_artifact_from_wire(manifest.artifact),
        member_id: manifest.member_id,
        backend: checkpoint_backend_from_wire(manifest.backend),
        offsets: checkpoint_offsets_from_wire(manifest.offsets),
        storage_identity: checkpoint_identity_from_wire(manifest.storage_identity),
        wal_retained: manifest.wal_retained,
        persistent_volume_retained: manifest.persistent_volume_retained,
    }
}

fn checkpoint_manifest_to_wire(manifest: CheckpointManifest) -> StoreReleaseCheckpointManifest {
    StoreReleaseCheckpointManifest {
        artifact: checkpoint_artifact_to_wire(manifest.artifact),
        member_id: manifest.member_id,
        backend: checkpoint_backend_to_wire(manifest.backend),
        offsets: checkpoint_offsets_to_wire(manifest.offsets),
        storage_identity: checkpoint_identity_to_wire(manifest.storage_identity),
        wal_retained: manifest.wal_retained,
        persistent_volume_retained: manifest.persistent_volume_retained,
    }
}

fn checkpoint_artifact_from_wire(artifact: ReleaseCheckpointArtifact) -> CheckpointArtifact {
    CheckpointArtifact {
        schema_version: artifact.schema_version,
        checkpoint_id: artifact.checkpoint_id,
        checkpoint_set_id: artifact.checkpoint_set_id,
        generation: artifact.generation,
        barrier_id: artifact.barrier_id,
        created_at_unix_millis: artifact.created_at_unix_millis,
        length_bytes: artifact.length_bytes,
        sha256: artifact.sha256,
        uri: artifact.uri,
    }
}

fn checkpoint_artifact_to_wire(artifact: CheckpointArtifact) -> ReleaseCheckpointArtifact {
    ReleaseCheckpointArtifact {
        schema_version: artifact.schema_version,
        checkpoint_id: artifact.checkpoint_id,
        checkpoint_set_id: artifact.checkpoint_set_id,
        generation: artifact.generation,
        barrier_id: artifact.barrier_id,
        created_at_unix_millis: artifact.created_at_unix_millis,
        length_bytes: artifact.length_bytes,
        sha256: artifact.sha256,
        uri: artifact.uri,
    }
}

const fn checkpoint_backend_from_wire(backend: ReleaseCheckpointBackend) -> CheckpointBackend {
    match backend {
        ReleaseCheckpointBackend::Local => CheckpointBackend::Local,
        ReleaseCheckpointBackend::RocksDb => CheckpointBackend::RocksDb,
    }
}

const fn checkpoint_backend_to_wire(backend: CheckpointBackend) -> ReleaseCheckpointBackend {
    match backend {
        CheckpointBackend::Local => ReleaseCheckpointBackend::Local,
        CheckpointBackend::RocksDb => ReleaseCheckpointBackend::RocksDb,
    }
}

const fn checkpoint_offsets_from_wire(offsets: ReleaseCheckpointOffsets) -> CheckpointOffsets {
    CheckpointOffsets {
        appended_offset: offsets.appended_offset,
        durable_offset: offsets.durable_offset,
        consume_queue_offset: offsets.consume_queue_offset,
        index_offset: offsets.index_offset,
    }
}

const fn checkpoint_offsets_to_wire(offsets: CheckpointOffsets) -> ReleaseCheckpointOffsets {
    ReleaseCheckpointOffsets {
        appended_offset: offsets.appended_offset,
        durable_offset: offsets.durable_offset,
        consume_queue_offset: offsets.consume_queue_offset,
        index_offset: offsets.index_offset,
    }
}

fn checkpoint_identity_from_wire(identity: ReleaseCheckpointStorageIdentity) -> CheckpointStorageIdentity {
    CheckpointStorageIdentity {
        volume_id: identity.volume_id,
        wal_generation: identity.wal_generation,
    }
}

fn checkpoint_identity_to_wire(identity: CheckpointStorageIdentity) -> ReleaseCheckpointStorageIdentity {
    ReleaseCheckpointStorageIdentity {
        volume_id: identity.volume_id,
        wal_generation: identity.wal_generation,
    }
}

fn checkpoint_verification_to_wire(
    verification: CheckpointRestoreVerification,
) -> ReleaseCheckpointRestoreVerification {
    ReleaseCheckpointRestoreVerification {
        checkpoint_id: verification.checkpoint_id,
        generation: verification.generation,
        verified_at_unix_millis: verification.verified_at_unix_millis,
        checksum_verified: verification.checksum_verified,
        offsets_verified: verification.offsets_verified,
        storage_identity_verified: verification.storage_identity_verified,
        wal_retained: verification.wal_retained,
        persistent_volume_retained: verification.persistent_volume_retained,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire_manifest(backend: ReleaseCheckpointBackend) -> StoreReleaseCheckpointManifest {
        StoreReleaseCheckpointManifest {
            artifact: ReleaseCheckpointArtifact {
                schema_version: RELEASE_CHECKPOINT_SCHEMA_VERSION,
                checkpoint_id: "checkpoint-7".to_string(),
                checkpoint_set_id: "set-4".to_string(),
                generation: 9,
                barrier_id: "barrier-3".to_string(),
                created_at_unix_millis: 123_456,
                length_bytes: 4_096,
                sha256: "a".repeat(64),
                uri: "file:///checkpoints/checkpoint-7".to_string(),
            },
            member_id: "broker-a".to_string(),
            backend,
            offsets: ReleaseCheckpointOffsets {
                appended_offset: 100,
                durable_offset: 90,
                consume_queue_offset: 80,
                index_offset: 70,
            },
            storage_identity: ReleaseCheckpointStorageIdentity {
                volume_id: "volume-a".to_string(),
                wal_generation: 5,
            },
            wal_retained: true,
            persistent_volume_retained: true,
        }
    }

    #[test]
    fn wire_domain_manifest_mapping_is_lossless_for_every_backend() {
        for backend in [ReleaseCheckpointBackend::Local, ReleaseCheckpointBackend::RocksDb] {
            let wire = wire_manifest(backend);

            let round_trip = checkpoint_manifest_to_wire(checkpoint_manifest_from_wire(wire.clone()));

            assert_eq!(wire, round_trip);
        }
    }

    #[test]
    fn checkpoint_wire_json_field_names_remain_compatible() {
        let wire = wire_manifest(ReleaseCheckpointBackend::Local);

        assert_eq!(
            serde_json::to_value(wire).expect("serialize wire manifest"),
            serde_json::json!({
                "artifact": {
                    "schemaVersion": 1,
                    "checkpointId": "checkpoint-7",
                    "checkpointSetId": "set-4",
                    "generation": 9,
                    "barrierId": "barrier-3",
                    "createdAtUnixMillis": 123456,
                    "lengthBytes": 4096,
                    "sha256": "a".repeat(64),
                    "uri": "file:///checkpoints/checkpoint-7"
                },
                "memberId": "broker-a",
                "backend": "local",
                "offsets": {
                    "appendedOffset": 100,
                    "durableOffset": 90,
                    "consumeQueueOffset": 80,
                    "indexOffset": 70
                },
                "storageIdentity": {
                    "volumeId": "volume-a",
                    "walGeneration": 5
                },
                "walRetained": true,
                "persistentVolumeRetained": true
            })
        );
    }
}
