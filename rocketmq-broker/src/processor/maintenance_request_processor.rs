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
use rocketmq_auth::RemotingAuthContext;
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
use rocketmq_store_api::ReleaseCheckpointCreateOutcome;
use rocketmq_store_api::ReleaseCheckpointCreateRejection;
use rocketmq_store_api::ReleaseCheckpointRestoreOutcome;
use rocketmq_store_api::ReleaseCheckpointRestoreRejection;
use rocketmq_store_api::ReleaseCheckpointStore;
use rocketmq_transport::api::internal_error_with_factory_and_opaque;
use rocketmq_transport::api::request_code_not_supported_with_factory_and_remark;
use rocketmq_transport::api::EmbeddedCaller;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;

use crate::config::broker_config::BrokerConfig;
use crate::processor::response_assembly::immediate_outcome_from_command_result;

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

    async fn process_authorized(
        &self,
        grant: &MaintenanceAuthorizationGrant,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<RemotingCommand> {
        let response = match request_code {
            RequestCode::MaintenanceGetCapabilities => self.capabilities(grant).await?,
            RequestCode::MaintenanceCreateStoreCheckpoint => self.create_store_checkpoint(grant, request).await?,
            RequestCode::MaintenanceVerifyCheckpoint => self.verify_store_checkpoint(request)?,
            RequestCode::MaintenanceRestoreVerify => self.restore_verify(grant, request).await?,
            _ => {
                return Ok(request_code_not_supported_with_factory_and_remark(
                    &self.command_factory,
                    request_code.to_i32(),
                    format!(
                        "request type {} is not a Broker maintenance operation",
                        request_code.to_i32()
                    ),
                ));
            }
        };
        Ok(response.set_opaque(request.opaque()))
    }

    async fn authorize_request(
        &self,
        request: &RemotingRequest,
        original_code: i32,
    ) -> RocketMQResult<MaintenanceAuthorizationGrant> {
        if !self.broker_config.maintenance_enabled {
            return Err(RocketMQError::authentication_failed(
                "Broker maintenance API is disabled",
            ));
        }
        let mut authoritative_command = request.command().clone();
        authoritative_command.set_code_mut(original_code);
        let principal = match request.origin() {
            RequestOrigin::Network { .. } => {
                let auth_context = RemotingAuthContext::from_request(request)?;
                self.auth_runtime
                    .authenticate_maintenance_principal(&authoritative_command, auth_context.channel_id())
                    .await?
            }
            RequestOrigin::Embedded {
                caller: EmbeddedCaller::BrokerProxy,
            } => request
                .authentication()
                .principal()
                .map(|principal| principal.id().into())
                .ok_or_else(|| RocketMQError::authentication_failed("maintenance request is anonymous"))?,
            _ => {
                return Err(RocketMQError::authentication_failed(
                    "maintenance request origin is not authorized",
                ));
            }
        };
        self.authorize_principal(principal.as_str(), &authoritative_command)
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
        let outcome = self
            .checkpoint_service
            .create_release_checkpoint(grant, checkpoint_request_from_wire(checkpoint_request))
            .await
            .map_err(checkpoint_error)?;
        match outcome {
            ReleaseCheckpointCreateOutcome::Created(manifest) => encode_success(
                &self.command_factory,
                &checkpoint_manifest_to_wire(manifest),
                "encode Store release-checkpoint manifest",
            ),
            ReleaseCheckpointCreateOutcome::Rejected(rejection) => {
                Ok(checkpoint_create_rejection_response(&self.command_factory, rejection))
            }
        }
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
        let outcome = self
            .checkpoint_service
            .restore_verify_release_checkpoint(grant, &checkpoint_manifest_from_wire(manifest))
            .await
            .map_err(checkpoint_error)?;
        match outcome {
            ReleaseCheckpointRestoreOutcome::Verified(verification) => encode_success(
                &self.command_factory,
                &checkpoint_verification_to_wire(verification),
                "encode Store restore-verification proof",
            ),
            ReleaseCheckpointRestoreOutcome::Rejected(rejection) => {
                Ok(checkpoint_restore_rejection_response(&self.command_factory, rejection))
            }
        }
    }
}

impl RequestProcessor for MaintenanceRequestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }
}

impl MaintenanceRequestProcessor {
    pub(crate) async fn process_shared(&self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        let original_opaque = request.original_identity().original_opaque();
        let original_code = request.original_identity().original_code();
        let result = match self.authorize_request(request, original_code).await {
            Ok(grant) => self
                .process_authorized(&grant, RequestCode::from(original_code), request.command_mut())
                .await
                .map(Some),
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

fn checkpoint_create_rejection_response(
    command_factory: &RemotingCommandFactory,
    rejection: ReleaseCheckpointCreateRejection,
) -> RemotingCommand {
    let remark = match rejection {
        ReleaseCheckpointCreateRejection::AuthorizationExpired => "Release checkpoint authorization expired",
        ReleaseCheckpointCreateRejection::CapabilityNotGranted => "Release checkpoint capability was not granted",
        ReleaseCheckpointCreateRejection::AlreadyExists => "Release checkpoint already exists",
        ReleaseCheckpointCreateRejection::CapacityExceeded { .. } => "Release checkpoint capacity was exceeded",
    };
    internal_error_with_factory_and_opaque(command_factory, 0, remark)
}

fn checkpoint_restore_rejection_response(
    command_factory: &RemotingCommandFactory,
    rejection: ReleaseCheckpointRestoreRejection,
) -> RemotingCommand {
    let remark = match rejection {
        ReleaseCheckpointRestoreRejection::AuthorizationExpired => "Release checkpoint authorization expired",
        ReleaseCheckpointRestoreRejection::CapabilityNotGranted => "Release checkpoint capability was not granted",
    };
    internal_error_with_factory_and_opaque(command_factory, 0, remark)
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
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Weak;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_auth::cal_signature;
    use rocketmq_auth::AuthConfig;
    use rocketmq_auth::AuthRuntimeBuilder;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
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
    use rocketmq_store::LocalFileMessageStore;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_store::StorePorts;
    use rocketmq_store::StoreRuntimeConfig;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::api::AuthorizedCommandDispatcher;
    use rocketmq_transport::api::EmbeddedDispatchOutcome;
    use rocketmq_transport::api::RPCHook;
    use rocketmq_transport::api::ServerConfig;
    use rocketmq_transport::api::TransportSecurity;
    use rocketmq_transport::api::TransportServer;
    use rocketmq_transport::test_support::Connection;
    use rocketmq_transport::test_support::EmbeddedRequestHarness;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::*;

    struct AllowEmbeddedPolicy;

    impl RequestPolicy for AllowEmbeddedPolicy {
        fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
            Decision::Allow
        }
    }

    struct MutateMaintenanceCodeHook;

    impl RPCHook for MutateMaintenanceCodeHook {
        fn do_before_request(&self, _remote_addr: SocketAddr, request: &mut RemotingCommand) -> RocketMQResult<()> {
            request.set_code_mut(RequestCode::MaintenanceCreateStoreCheckpoint.to_i32());
            Ok(())
        }

        fn do_after_response(
            &self,
            _remote_addr: SocketAddr,
            _request: &RemotingCommand,
            _response: &mut RemotingCommand,
        ) -> RocketMQResult<()> {
            Ok(())
        }
    }

    fn maintenance_policy() -> MaintenancePolicy {
        MaintenancePolicy {
            schema_version: MAINTENANCE_POLICY_SCHEMA_VERSION,
            policy_id: "broker-maintenance-test".to_string(),
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

    fn maintenance_auth_config() -> AuthConfig {
        AuthConfig {
            authentication_enabled: true,
            authorization_enabled: true,
            init_authentication_user: CheetahString::from_static_str("release-operator:secret"),
            ..AuthConfig::default()
        }
    }

    async fn maintenance_processor_for_test() -> MaintenanceRequestProcessor {
        let service_context = crate::test_service_context("maintenance");
        let auth_runtime = Arc::new(
            AuthRuntimeBuilder::new(maintenance_auth_config(), service_context.component("auth"))
                .build()
                .await
                .expect("build maintenance auth runtime"),
        );
        let authorizer = Arc::new(MaintenanceAuthorizer::new(
            maintenance_policy().into_validated().expect("valid maintenance policy"),
        ));
        let checkpoint_service = Arc::new(StoreReleaseCheckpointService::new(
            Weak::<StorePorts>::new(),
            PathBuf::from("maintenance-unused"),
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

    async fn maintenance_processor_with_store_for_test(
    ) -> (MaintenanceRequestProcessor, Arc<StorePorts>, tempfile::TempDir) {
        let service_context = crate::test_service_context("maintenance-original-code");
        let auth_runtime = Arc::new(
            AuthRuntimeBuilder::new(maintenance_auth_config(), service_context.component("auth"))
                .build()
                .await
                .expect("build maintenance auth runtime"),
        );
        let authorizer = Arc::new(MaintenanceAuthorizer::new(
            maintenance_policy().into_validated().expect("valid maintenance policy"),
        ));
        let root = tempfile::tempdir().expect("maintenance checkpoint root");
        let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
        let store = Arc::new(StorePorts::local_file(LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
                timer_wheel_enable: false,
                ..MessageStoreConfig::default()
            }),
            rocketmq_store::MicroBatchPolicy::disabled(1).expect("valid test policy"),
            Arc::new(StoreRuntimeConfig::default()),
            topic_table,
            None,
            false,
            service_context.component("store"),
        )));
        let checkpoint_service = Arc::new(StoreReleaseCheckpointService::new(
            Arc::downgrade(&store),
            root.path().join("checkpoints"),
            service_context.component("checkpoint"),
        ));
        let processor = MaintenanceRequestProcessor::new(
            Arc::new(BrokerConfig {
                maintenance_enabled: true,
                authentication_enabled: true,
                authorization_enabled: true,
                ..BrokerConfig::default()
            }),
            auth_runtime,
            authorizer,
            checkpoint_service,
        );
        (processor, store, root)
    }

    async fn dispatch_request(
        processor: MaintenanceRequestProcessor,
        principal: &'static str,
        command: RemotingCommand,
    ) -> Result<EmbeddedDispatchOutcome, rocketmq_transport::api::EmbeddedDispatchError> {
        dispatch_request_with_hooks(processor, principal, command, Vec::new()).await
    }

    async fn dispatch_request_with_hooks(
        processor: MaintenanceRequestProcessor,
        principal: &'static str,
        command: RemotingCommand,
        hooks: Vec<Arc<dyn RPCHook>>,
    ) -> Result<EmbeddedDispatchOutcome, rocketmq_transport::api::EmbeddedDispatchError> {
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            processor,
            hooks,
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        EmbeddedRequestHarness::new(
            dispatcher,
            crate::test_task_group("maintenance-request"),
            Principal::new(principal),
        )
        .dispatch(None, command)
        .await
    }

    fn capabilities_request(opaque: i32) -> RemotingCommand {
        RemotingCommand::create_request_command(
            RequestCode::MaintenanceGetCapabilities,
            MaintenanceRequestHeader {
                operation_id: "get-capabilities".into(),
                policy_version: 7,
                deadline_unix_millis: rocketmq_runtime::common::time_utils::current_millis() + 20_000,
                fencing_token: 42,
            },
        )
        .set_opaque(opaque)
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

    fn sign_maintenance_request(mut command: RemotingCommand, access_key: &str, secret: &str) -> RemotingCommand {
        command.make_custom_header_to_net();
        command.ensure_ext_fields_initialized();
        command.add_ext_field("AccessKey", access_key);
        let mut fields = command
            .ext_fields()
            .cloned()
            .expect("maintenance request fields")
            .into_iter()
            .filter(|(key, _)| key.as_str() != "Signature")
            .collect::<Vec<_>>();
        fields.sort_by(|left, right| left.0.cmp(&right.0));
        let mut content = Vec::new();
        for (_, value) in fields {
            content.extend_from_slice(value.as_bytes());
        }
        if let Some(body) = command.body() {
            content.extend_from_slice(body);
        }
        let signature = cal_signature(content.as_slice(), secret).expect("maintenance signature");
        command.add_ext_field("Signature", signature);
        command
    }

    #[tokio::test]
    async fn maintenance_uses_authenticated_principal_for_authorization() {
        let processor = maintenance_processor_for_test().await;

        let EmbeddedDispatchOutcome::Reply(plan) =
            dispatch_request(processor.clone(), "release-operator", verify_request(5_505))
                .await
                .expect("bound maintenance principal should be authorized")
        else {
            panic!("authorized maintenance must return an inline remoting response");
        };
        assert_eq!(ResponseCode::from(plan.response_code()), ResponseCode::Success);
        assert!(plan.body_len() > 0);

        let EmbeddedDispatchOutcome::Reply(denied) =
            dispatch_request(processor, "ordinary-admin", verify_request(5_506))
                .await
                .expect("authorization failures should become typed remoting responses")
        else {
            panic!("denied maintenance must return an inline remoting response");
        };
        assert_eq!(ResponseCode::from(denied.response_code()), ResponseCode::NoPermission);
        assert_eq!(denied.body_len(), 0);
    }

    #[tokio::test]
    async fn network_maintenance_uses_verified_credentials_not_bootstrap_authentication_state() {
        const AUTHENTICATED_OPAQUE: i32 = 5_508;
        const FORGED_OPAQUE: i32 = 5_509;
        let owner = RuntimeOwner::new(RuntimeConfig::server_default("maintenance-network-auth"))
            .expect("maintenance network test runtime");
        let server_context = owner.root_context().component("maintenance-network-auth.server");
        let runner_context = owner.root_context().component("maintenance-network-auth.runner");
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            maintenance_processor_for_test().await,
            Vec::new(),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let server = TransportServer::new_with_authorized_dispatcher(
            Arc::new(ServerConfig {
                bind_address: "127.0.0.1".to_owned(),
                listen_port: 0,
                ..ServerConfig::default()
            }),
            server_context,
            dispatcher,
        );
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (startup_sender, startup_receiver) = oneshot::channel();
        let (result_sender, result_receiver) = oneshot::channel();
        runner_context
            .spawn_service("maintenance-network-server", async move {
                let result = server
                    .try_run_with_shutdown_report_and_startup(
                        async move {
                            let _ = shutdown_receiver.await;
                        },
                        startup_sender,
                    )
                    .await;
                let _ = result_sender.send(result);
            })
            .expect("spawn maintenance network server");

        let address = startup_receiver
            .await
            .expect("maintenance startup channel")
            .expect("maintenance server startup");
        let mut client = Connection::new(TcpStream::connect(address).await.expect("connect maintenance client"));

        client
            .send_command(sign_maintenance_request(
                verify_request(AUTHENTICATED_OPAQUE),
                "release-operator",
                "secret",
            ))
            .await
            .expect("send authenticated maintenance request");
        let authenticated = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
            .await
            .expect("authenticated maintenance response deadline")
            .expect("authenticated maintenance connection remains open")
            .expect("authenticated maintenance response frame");
        assert_eq!(authenticated.opaque(), AUTHENTICATED_OPAQUE);
        assert_eq!(ResponseCode::from(authenticated.code()), ResponseCode::Success);

        let mut forged = verify_request(FORGED_OPAQUE);
        forged.ensure_ext_fields_initialized();
        forged.add_ext_field("principal", "release-operator");
        let forged = sign_maintenance_request(forged, "release-operator", "wrong-secret");
        client
            .send_command(forged)
            .await
            .expect("send forged maintenance principal");
        let denied = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
            .await
            .expect("forged maintenance response deadline")
            .expect("forged maintenance connection remains open")
            .expect("forged maintenance response frame");
        assert_eq!(denied.opaque(), FORGED_OPAQUE);
        assert_eq!(ResponseCode::from(denied.code()), ResponseCode::NoPermission);

        client.shutdown().await.expect("shutdown maintenance client");
        let _ = shutdown_sender.send(());
        let report = tokio::time::timeout(Duration::from_secs(2), result_receiver)
            .await
            .expect("maintenance shutdown deadline")
            .expect("maintenance shutdown result channel")
            .expect("maintenance shutdown report");
        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(owner.shutdown_tasks().await.is_healthy());
    }

    #[tokio::test]
    async fn maintenance_before_hook_cannot_replace_the_original_operation() {
        let (processor, _store, _root) = maintenance_processor_with_store_for_test().await;

        let EmbeddedDispatchOutcome::Reply(plan) = dispatch_request_with_hooks(
            processor,
            "release-operator",
            capabilities_request(5_507),
            vec![Arc::new(MutateMaintenanceCodeHook)],
        )
        .await
        .expect("original maintenance capability request should remain authoritative") else {
            panic!("maintenance capability request must return an inline remoting response");
        };

        assert_eq!(ResponseCode::from(plan.response_code()), ResponseCode::Success);
        assert!(plan.body_len() > 0);
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

    #[test]
    fn checkpoint_outcome_rejections_use_fixed_redaction_safe_broker_responses() {
        let factory = application_remoting_command_factory();
        let create_cases = [
            (
                ReleaseCheckpointCreateRejection::AuthorizationExpired,
                "Release checkpoint authorization expired",
            ),
            (
                ReleaseCheckpointCreateRejection::CapabilityNotGranted,
                "Release checkpoint capability was not granted",
            ),
            (
                ReleaseCheckpointCreateRejection::AlreadyExists,
                "Release checkpoint already exists",
            ),
            (
                ReleaseCheckpointCreateRejection::CapacityExceeded {
                    actual_bytes: 987_654,
                    maximum_bytes: 123_456,
                },
                "Release checkpoint capacity was exceeded",
            ),
        ];
        for (rejection, expected_remark) in create_cases {
            let response = checkpoint_create_rejection_response(&factory, rejection);
            assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
            assert_eq!(response.remark().map(|remark| remark.as_str()), Some(expected_remark));
            assert!(!response.remark().is_some_and(|remark| remark.contains("987654")));
            assert!(!response.remark().is_some_and(|remark| remark.contains("123456")));
        }

        for (rejection, expected_remark) in [
            (
                ReleaseCheckpointRestoreRejection::AuthorizationExpired,
                "Release checkpoint authorization expired",
            ),
            (
                ReleaseCheckpointRestoreRejection::CapabilityNotGranted,
                "Release checkpoint capability was not granted",
            ),
        ] {
            let response = checkpoint_restore_rejection_response(&factory, rejection);
            assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
            assert_eq!(response.remark().map(|remark| remark.as_str()), Some(expected_remark));
        }
    }
}
