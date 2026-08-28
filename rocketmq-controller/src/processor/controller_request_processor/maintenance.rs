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

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotRequest;
use rocketmq_protocol::protocol::body::release_checkpoint::MaintenanceCapabilitiesResponse;
use rocketmq_protocol::protocol::header::maintenance_request_header::MaintenanceRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::MaintenanceAuthorizationContext;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceCapability;
use rocketmq_security_api::MaintenanceRequestClass;

use super::ControllerRequestProcessor;

impl ControllerRequestProcessor {
    async fn authorize_maintenance_request(
        &self,
        channel_identity: &str,
        request: &RemotingCommand,
    ) -> RocketMQResult<(MaintenanceRequestHeader, MaintenanceAuthorizationGrant)> {
        let controller_manager = self.controller_manager()?;
        let config = controller_manager.controller_config();
        if !config.maintenance_enabled {
            return Err(RocketMQError::authentication_failed(
                "Controller maintenance API is disabled",
            ));
        }
        let security = controller_manager
            .security()
            .ok_or_else(|| RocketMQError::authentication_failed("Controller security adapter is unavailable"))?;
        let principal = security
            .authenticator()
            .authenticate_maintenance_principal(request, Some(channel_identity))
            .await?;
        let header = request
            .decode_command_custom_header::<MaintenanceRequestHeader>()
            .map_err(|error| RocketMQError::request_header_source("decode privileged maintenance header", error))?;
        header
            .validate()
            .map_err(|reason| RocketMQError::request_header_error(reason.to_string()))?;
        let authorizer = security
            .maintenance_authorizer()
            .ok_or_else(|| RocketMQError::authentication_failed("Controller maintenance policy is unavailable"))?;
        if header.policy_version != authorizer.policy().policy_version {
            return Err(RocketMQError::authentication_failed(format!(
                "maintenance policy version {} does not match loaded version {}",
                header.policy_version,
                authorizer.policy().policy_version
            )));
        }
        let grant = authorizer
            .authorize(
                Some(&MaintenanceAuthorizationContext {
                    authentication_enabled: config.authentication_enabled,
                    authorization_enabled: config.authorization_enabled,
                    principal: Some(principal),
                    request_class: MaintenanceRequestClass::PrivilegedMaintenance,
                    capability: MaintenanceCapability::ReleaseCheckpoint,
                    deadline_unix_millis: header.deadline_unix_millis,
                    fencing_token: Some(header.fencing_token),
                }),
                rocketmq_runtime::common::time_utils::current_millis(),
            )
            .map_err(|error| RocketMQError::authentication_source("authorize Controller maintenance request", error))?;
        Ok((header, grant))
    }

    pub(super) async fn handle_maintenance_capabilities(
        &self,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let (_header, grant) = self.authorize_maintenance_request(channel_identity, request).await?;
        let controller_manager = self.controller_manager()?;
        let policy = controller_manager
            .security()
            .and_then(|security| security.maintenance_authorizer())
            .ok_or_else(|| RocketMQError::authentication_failed("Controller maintenance policy is unavailable"))?
            .policy();
        let response = MaintenanceCapabilitiesResponse {
            schema_version: 1,
            policy_id: policy.policy_id.clone(),
            policy_version: grant.policy_version(),
            operations: vec![
                "capabilities".to_string(),
                "create_controller_snapshot".to_string(),
                "verify_checkpoint".to_string(),
                "restore_verify".to_string(),
            ],
            max_checkpoint_bytes: grant.resource_budget().max_checkpoint_bytes,
            max_store_members: grant.resource_budget().max_store_members,
            max_concurrent_operations: grant.resource_budget().max_concurrent_operations,
            store: None,
        };
        let body = serde_json::to_vec(&response)
            .map_err(|error| RocketMQError::internal("encode maintenance capabilities", error))?;
        Ok(Some(
            self.command_factory.create_success_response_command().set_body(body),
        ))
    }

    pub(super) async fn handle_create_release_snapshot(
        &self,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let (_header, grant) = self.authorize_maintenance_request(channel_identity, request).await?;
        let request_body = request.body().ok_or_else(|| {
            RocketMQError::request_body_invalid("MAINTENANCE_CREATE_CONTROLLER_SNAPSHOT", "request body is empty")
        })?;
        let snapshot_request: ControllerReleaseSnapshotRequest = serde_json::from_slice(request_body)
            .map_err(|error| RocketMQError::request_body_source("MAINTENANCE_CREATE_CONTROLLER_SNAPSHOT", error))?;
        let snapshot = self
            .controller_manager()?
            .controller()
            .create_release_snapshot(&grant, snapshot_request)
            .await?;
        let body = serde_json::to_vec(&snapshot.manifest)
            .map_err(|error| RocketMQError::internal("encode Controller release snapshot manifest", error))?;
        Ok(Some(
            self.command_factory.create_success_response_command().set_body(body),
        ))
    }

    pub(super) async fn handle_verify_release_snapshot(
        &self,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let (_header, grant) = self.authorize_maintenance_request(channel_identity, request).await?;
        let manifest = decode_controller_release_snapshot_manifest(request, "MAINTENANCE_VERIFY_CHECKPOINT")?;
        self.controller_manager()?
            .controller()
            .verify_release_snapshot(&grant, &manifest)
            .await?;
        let body = serde_json::to_vec(&manifest)
            .map_err(|error| RocketMQError::internal("encode verified Controller snapshot manifest", error))?;
        Ok(Some(
            self.command_factory.create_success_response_command().set_body(body),
        ))
    }

    pub(super) async fn handle_restore_verify(
        &self,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let (_header, grant) = self.authorize_maintenance_request(channel_identity, request).await?;
        let manifest = decode_controller_release_snapshot_manifest(request, "MAINTENANCE_RESTORE_VERIFY")?;
        let verification = self
            .controller_manager()?
            .controller()
            .verify_release_snapshot(&grant, &manifest)
            .await?;
        let body = serde_json::to_vec(&verification)
            .map_err(|error| RocketMQError::internal("encode Controller restore-verification proof", error))?;
        Ok(Some(
            self.command_factory.create_success_response_command().set_body(body),
        ))
    }
}

fn decode_controller_release_snapshot_manifest(
    request: &RemotingCommand,
    operation: &'static str,
) -> RocketMQResult<ControllerReleaseSnapshotManifest> {
    let body = request
        .body()
        .ok_or_else(|| RocketMQError::request_body_invalid(operation, "request body is empty"))?;
    serde_json::from_slice(body).map_err(|error| RocketMQError::request_body_source(operation, error))
}

#[cfg(test)]
mod tests {
    use rocketmq_error::ErrorKind;
    use rocketmq_protocol::code::request_code::RequestCode;

    use super::*;

    #[test]
    fn malformed_release_snapshot_manifest_preserves_serde_source() {
        let request =
            RemotingCommand::create_remoting_command(RequestCode::MaintenanceVerifyCheckpoint).set_body(b"{".to_vec());

        let error = decode_controller_release_snapshot_manifest(&request, "MAINTENANCE_VERIFY_CHECKPOINT")
            .expect_err("malformed JSON must be rejected");

        assert_eq!(error.kind(), ErrorKind::RequestBodyInvalid);
        let source = std::error::Error::source(&error).expect("serde source must be retained");
        assert!(source.downcast_ref::<serde_json::Error>().is_some());
        assert!(!error.boundary_view().context().to_string().contains("expected ident"));
    }
}
