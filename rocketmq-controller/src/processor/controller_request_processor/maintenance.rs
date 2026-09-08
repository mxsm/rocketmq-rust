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

use crate::ControllerResult;
use rocketmq_error::Error;
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
    ) -> ControllerResult<(MaintenanceRequestHeader, MaintenanceAuthorizationGrant)> {
        let controller_manager = self.controller_manager()?;
        let config = controller_manager.controller_config();
        if !config.maintenance_enabled {
            return Err(maintenance_permission_denied());
        }
        let security = controller_manager
            .security()
            .ok_or_else(maintenance_authorizer_unavailable)?;
        let principal = security
            .authenticator()
            .authenticate_maintenance_principal(request, Some(channel_identity))
            .await?;
        let header = request
            .decode_command_custom_header::<MaintenanceRequestHeader>()
            .map_err(|error| crate::error::request_header_invalid_by("decode privileged maintenance header", error))?;
        header
            .validate()
            .map_err(|_reason| crate::error::request_header_invalid("validate privileged maintenance header"))?;
        let authorizer = security
            .maintenance_authorizer()
            .ok_or_else(maintenance_authorizer_unavailable)?;
        if header.policy_version != authorizer.policy().policy_version {
            return Err(maintenance_permission_denied());
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
            .map_err(|_denial| maintenance_permission_denied())?;
        Ok((header, grant))
    }

    pub(super) async fn handle_maintenance_capabilities(
        &self,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> ControllerResult<Option<RemotingCommand>> {
        let (_header, grant) = self.authorize_maintenance_request(channel_identity, request).await?;
        let controller_manager = self.controller_manager()?;
        let policy = controller_manager
            .security()
            .and_then(|security| security.maintenance_authorizer())
            .ok_or_else(maintenance_authorizer_unavailable)?
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
            .map_err(|error| crate::error::serialization_failed("encode maintenance capabilities", "json", error))?;
        Ok(Some(
            self.command_factory.create_success_response_command().set_body(body),
        ))
    }

    pub(super) async fn handle_create_release_snapshot(
        &self,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> ControllerResult<Option<RemotingCommand>> {
        let (_header, grant) = self.authorize_maintenance_request(channel_identity, request).await?;
        let request_body = request
            .body()
            .ok_or_else(|| crate::error::request_body_invalid("MAINTENANCE_CREATE_CONTROLLER_SNAPSHOT"))?;
        let snapshot_request: ControllerReleaseSnapshotRequest = serde_json::from_slice(request_body)
            .map_err(|error| crate::error::request_body_invalid_by("MAINTENANCE_CREATE_CONTROLLER_SNAPSHOT", error))?;
        let snapshot = self
            .controller_manager()?
            .controller()
            .create_release_snapshot(&grant, snapshot_request)
            .await?;
        let body = serde_json::to_vec(&snapshot.manifest).map_err(|error| {
            crate::error::serialization_failed("encode Controller release snapshot manifest", "json", error)
        })?;
        Ok(Some(
            self.command_factory.create_success_response_command().set_body(body),
        ))
    }

    pub(super) async fn handle_verify_release_snapshot(
        &self,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> ControllerResult<Option<RemotingCommand>> {
        let (_header, grant) = self.authorize_maintenance_request(channel_identity, request).await?;
        let manifest = decode_controller_release_snapshot_manifest(request, "MAINTENANCE_VERIFY_CHECKPOINT")?;
        self.controller_manager()?
            .controller()
            .verify_release_snapshot(&grant, &manifest)
            .await?;
        let body = serde_json::to_vec(&manifest).map_err(|error| {
            crate::error::serialization_failed("encode verified Controller snapshot manifest", "json", error)
        })?;
        Ok(Some(
            self.command_factory.create_success_response_command().set_body(body),
        ))
    }

    pub(super) async fn handle_restore_verify(
        &self,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> ControllerResult<Option<RemotingCommand>> {
        let (_header, grant) = self.authorize_maintenance_request(channel_identity, request).await?;
        let manifest = decode_controller_release_snapshot_manifest(request, "MAINTENANCE_RESTORE_VERIFY")?;
        let verification = self
            .controller_manager()?
            .controller()
            .verify_release_snapshot(&grant, &manifest)
            .await?;
        let body = serde_json::to_vec(&verification).map_err(|error| {
            crate::error::serialization_failed("encode Controller restore-verification proof", "json", error)
        })?;
        Ok(Some(
            self.command_factory.create_success_response_command().set_body(body),
        ))
    }
}

fn maintenance_permission_denied() -> Error {
    crate::error::permission_denied("privileged maintenance")
}

fn maintenance_authorizer_unavailable() -> Error {
    crate::error::auth_operation_failed("load-maintenance-authorizer")
}

fn decode_controller_release_snapshot_manifest(
    request: &RemotingCommand,
    operation: &'static str,
) -> ControllerResult<ControllerReleaseSnapshotManifest> {
    let body = request
        .body()
        .ok_or_else(|| crate::error::request_body_invalid(operation))?;
    serde_json::from_slice(body).map_err(|error| crate::error::request_body_invalid_by(operation, error))
}

#[cfg(test)]
mod tests {
    use rocketmq_protocol::code::request_code::RequestCode;

    use super::*;

    #[test]
    fn maintenance_boundary_distinguishes_denial_from_unavailable_authorizer() {
        assert_eq!(
            maintenance_permission_denied().descriptor(),
            &rocketmq_error::AUTH_PERMISSION_DENIED
        );
        assert_eq!(
            maintenance_authorizer_unavailable().descriptor(),
            &rocketmq_error::AUTH_OPERATION_FAILED
        );
        assert_eq!(
            maintenance_permission_denied().descriptor().projection().remoting(),
            maintenance_authorizer_unavailable()
                .descriptor()
                .projection()
                .remoting()
        );
    }

    #[test]
    fn malformed_release_snapshot_manifest_preserves_serde_source() {
        let request =
            RemotingCommand::create_remoting_command(RequestCode::MaintenanceVerifyCheckpoint).set_body(b"{".to_vec());

        let error = decode_controller_release_snapshot_manifest(&request, "MAINTENANCE_VERIFY_CHECKPOINT")
            .expect_err("malformed JSON must be rejected");

        assert_eq!(error.descriptor(), &rocketmq_error::PROTOCOL_BODY_INVALID);
        let source = std::error::Error::source(&error).expect("serde source must be retained");
        assert!(source.downcast_ref::<serde_json::Error>().is_some());
        let context = error.context();
        let public = rocketmq_error::PublicErrorView::try_new(error.descriptor(), context).expect("valid public view");
        assert!(!format!("{public:?}").contains("expected ident"));
    }
}
