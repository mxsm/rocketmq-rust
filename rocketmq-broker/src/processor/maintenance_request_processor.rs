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
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
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
use rocketmq_transport::api::v1::request_code_not_supported_with_factory_and_remark;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestProcessorV2;

use crate::config::broker_config::BrokerConfig;
use crate::processor::response_plan::immediate_outcome_from_command_result;

/// Broker endpoint for release-checkpoint operations.
#[derive(Clone)]
pub struct MaintenanceRequestProcessor {
    command_factory: RemotingCommandFactory,
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
        Self::new_with_factory(
            broker_config,
            auth_runtime,
            authorizer,
            checkpoint_service,
            application_remoting_command_factory(),
        )
    }

    pub(crate) fn new_with_factory(
        broker_config: Arc<BrokerConfig>,
        auth_runtime: Arc<AuthRuntime>,
        authorizer: Arc<MaintenanceAuthorizer>,
        checkpoint_service: Arc<StoreReleaseCheckpointService>,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        Self {
            command_factory,
            broker_config,
            auth_runtime,
            authorizer,
            checkpoint_service,
        }
    }

    pub(crate) fn legacy_adapter(&self) -> LegacyMaintenanceRequestProcessor {
        LegacyMaintenanceRequestProcessor {
            processor: self.clone(),
        }
    }

    async fn process_authorized(
        &self,
        grant: &MaintenanceAuthorizationGrant,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<RemotingCommand> {
        let response = match RequestCode::from(request.code()) {
            RequestCode::MaintenanceGetCapabilities => self.capabilities(grant).await?,
            RequestCode::MaintenanceCreateStoreCheckpoint => self.create_store_checkpoint(grant, request).await?,
            RequestCode::MaintenanceVerifyCheckpoint => self.verify_store_checkpoint(request)?,
            RequestCode::MaintenanceRestoreVerify => self.restore_verify(grant, request).await?,
            _ => {
                return Ok(request_code_not_supported_with_factory_and_remark(
                    &self.command_factory,
                    request.code(),
                    format!("request type {} is not a Broker maintenance operation", request.code()),
                ));
            }
        };
        Ok(response.set_opaque(request.opaque()))
    }

    fn authorize_v2(&self, request: &RemotingRequest) -> RocketMQResult<MaintenanceAuthorizationGrant> {
        if !self.broker_config.maintenance_enabled {
            return Err(RocketMQError::authentication_failed(
                "Broker maintenance API is disabled",
            ));
        }
        let principal = request
            .authentication()
            .principal()
            .ok_or_else(|| RocketMQError::authentication_failed("maintenance request is anonymous"))?;
        self.authorize_principal(principal.id(), request.command())
    }

    fn authorize_principal(
        &self,
        principal: &str,
        request: &RemotingCommand,
    ) -> RocketMQResult<MaintenanceAuthorizationGrant> {
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
        encode_success(
            &self.command_factory,
            &response,
            "encode Broker maintenance capabilities",
        )
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
            &self.command_factory,
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
        encode_success(
            &self.command_factory,
            &manifest,
            "encode verified Store release-checkpoint manifest",
        )
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
            &self.command_factory,
            &checkpoint_verification_to_wire(verification),
            "encode Store restore-verification proof",
        )
    }
}

pub(crate) struct LegacyMaintenanceRequestProcessor {
    processor: MaintenanceRequestProcessor,
}

impl LegacyMaintenanceRequestProcessor {
    pub(crate) async fn process_legacy(
        &self,
        channel: &Channel,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        if !self.processor.broker_config.maintenance_enabled {
            return Err(RocketMQError::authentication_failed(
                "Broker maintenance API is disabled",
            ));
        }
        let principal = self
            .processor
            .auth_runtime
            .authenticate_maintenance_principal(request, Some(channel.channel_id()))
            .await?;
        let grant = self.processor.authorize_principal(principal.as_str(), request)?;
        self.processor.process_authorized(&grant, request).await.map(Some)
    }
}

impl RequestProcessorV2 for MaintenanceRequestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        self.process_v2_shared(request).await
    }
}

impl MaintenanceRequestProcessor {
    pub(crate) async fn process_v2_shared(&self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        let original_opaque = request.original_identity().original_opaque();
        let result = match self.authorize_v2(request) {
            Ok(grant) => self.process_authorized(&grant, request.command_mut()).await.map(Some),
            Err(error) => Err(error),
        };
        immediate_outcome_from_command_result(
            &self.command_factory,
            result,
            original_opaque,
            "maintenance processor returned no response",
        )
    }
}

fn required_body<'a>(request: &'a RemotingCommand, operation: &'static str) -> RocketMQResult<&'a [u8]> {
    request
        .body()
        .map(bytes::Bytes::as_ref)
        .ok_or_else(|| RocketMQError::request_body_invalid(operation, "request body is empty"))
}

fn encode_success<T: serde::Serialize>(
    command_factory: &RemotingCommandFactory,
    value: &T,
    operation: &'static str,
) -> RocketMQResult<RemotingCommand> {
    let body = serde_json::to_vec(value).map_err(|error| RocketMQError::internal(operation, error))?;
    Ok(command_factory.create_success_response_command().set_body(body))
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
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::sync::Weak;

    use rocketmq_auth::AuthConfig;
    use rocketmq_auth::AuthRuntimeBuilder;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_security_api::AuthenticatedRequestContext;
    use rocketmq_security_api::Decision;
    use rocketmq_security_api::MaintenancePolicy;
    use rocketmq_security_api::MaintenancePrincipalBinding;
    use rocketmq_security_api::MaintenanceResourceBudget;
    use rocketmq_security_api::MaintenanceRole;
    use rocketmq_security_api::MaintenanceRoleGrant;
    use rocketmq_security_api::Principal;
    use rocketmq_security_api::RequestPolicy;
    use rocketmq_security_api::MAINTENANCE_POLICY_SCHEMA_VERSION;
    use rocketmq_store::StorePorts;
    use rocketmq_transport::api::v1::AdmissionController;
    use rocketmq_transport::api::v1::AdmissionLimits;
    use rocketmq_transport::api::v1::TransportSecurity;
    use rocketmq_transport::api::v2::AuthorizedCommandDispatcherV2;
    use rocketmq_transport::api::v2::EmbeddedDispatchOutcome;
    use rocketmq_transport::test_support::EmbeddedRequestHarnessV2;

    use super::*;

    struct AllowEmbeddedPolicy;

    impl RequestPolicy for AllowEmbeddedPolicy {
        fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
            Decision::Allow
        }
    }

    fn maintenance_policy() -> MaintenancePolicy {
        MaintenancePolicy {
            schema_version: MAINTENANCE_POLICY_SCHEMA_VERSION,
            policy_id: "broker-maintenance-v2-test".to_string(),
            policy_version: 7,
            require_authentication: true,
            require_authorization: true,
            require_fencing_token: true,
            max_request_lifetime_millis: 30_000,
            resource_budget: MaintenanceResourceBudget {
                max_checkpoint_bytes: 4_096,
                max_store_members: 1,
                max_concurrent_operations: 1,
            },
            principal_bindings: vec![MaintenancePrincipalBinding {
                principal: "release-operator".to_string(),
                roles: BTreeSet::from([MaintenanceRole::ReleaseOperator]),
            }],
            role_grants: vec![MaintenanceRoleGrant {
                role: MaintenanceRole::ReleaseOperator,
                capabilities: BTreeSet::from([MaintenanceCapability::ReleaseCheckpoint]),
            }],
        }
    }

    async fn maintenance_processor_for_test() -> MaintenanceRequestProcessor {
        let service_context = crate::test_service_context("maintenance-v2");
        let auth_runtime = Arc::new(
            AuthRuntimeBuilder::new(AuthConfig::default(), service_context.component("auth"))
                .build()
                .await
                .expect("build maintenance auth runtime"),
        );
        let authorizer = Arc::new(MaintenanceAuthorizer::new(
            maintenance_policy().into_validated().expect("valid maintenance policy"),
        ));
        let checkpoint_service = Arc::new(StoreReleaseCheckpointService::new(
            Weak::<StorePorts>::new(),
            PathBuf::from("maintenance-v2-unused"),
            service_context.component("checkpoint"),
        ));
        MaintenanceRequestProcessor::new(
            Arc::new(BrokerConfig {
                maintenance_enabled: true,
                authentication_enabled: true,
                authorization_enabled: true,
                ..BrokerConfig::default()
            }),
            auth_runtime,
            authorizer,
            checkpoint_service,
        )
    }

    async fn dispatch_v2(
        processor: MaintenanceRequestProcessor,
        principal: &'static str,
        command: RemotingCommand,
    ) -> Result<EmbeddedDispatchOutcome, rocketmq_transport::api::v2::EmbeddedDispatchError> {
        let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
            processor,
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        EmbeddedRequestHarnessV2::new(
            dispatcher,
            crate::test_task_group("maintenance-request-v2"),
            Principal::new(principal),
        )
        .dispatch(None, command)
        .await
    }

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

    fn verify_request(opaque: i32) -> RemotingCommand {
        let deadline_unix_millis = rocketmq_runtime::common::time_utils::current_millis() + 20_000;
        RemotingCommand::create_request_command(
            RequestCode::MaintenanceVerifyCheckpoint,
            MaintenanceRequestHeader {
                operation_id: "verify-checkpoint-7".into(),
                policy_version: 7,
                deadline_unix_millis,
                fencing_token: 42,
            },
        )
        .set_body(
            serde_json::to_vec(&wire_manifest(ReleaseCheckpointBackend::Local))
                .expect("serialize maintenance manifest"),
        )
        .set_opaque(opaque)
    }

    #[tokio::test]
    async fn maintenance_v2_uses_authenticated_principal_for_authorization() {
        let processor = maintenance_processor_for_test().await;

        let EmbeddedDispatchOutcome::Reply(plan) =
            dispatch_v2(processor.clone(), "release-operator", verify_request(5_505))
                .await
                .expect("bound maintenance principal should be authorized")
        else {
            panic!("authorized maintenance V2 must return an inline response plan");
        };
        assert_eq!(ResponseCode::from(plan.response_code()), ResponseCode::Success);
        assert!(plan.body_len() > 0);

        let EmbeddedDispatchOutcome::Reply(denied) = dispatch_v2(processor, "ordinary-admin", verify_request(5_506))
            .await
            .expect("authorization failures should become typed response plans")
        else {
            panic!("denied maintenance V2 must return an inline response plan");
        };
        assert_eq!(ResponseCode::from(denied.response_code()), ResponseCode::NoPermission);
        assert_eq!(denied.body_len(), 0);
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
