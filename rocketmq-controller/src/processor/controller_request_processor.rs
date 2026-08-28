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

//! # Controller Request Processor
//!
//! Main request processor for the RocketMQ Controller module. This processor handles all RPC
//! requests from brokers including:
//! - Sync state management (ALTER_SYNC_STATE_SET)
//! - Master election (ELECT_MASTER)
//! - Broker registration and heartbeats
//! - Metadata and replica information queries
//! - Configuration management
//! - Broker ID allocation
//!
//! ## Request Flow
//!
//! ```text
//! ┌─────────┐      ┌────────────────────┐      ┌────────────┐
//! │ Broker  │─────>│ Request Processor  │─────>│ Controller │
//! └─────────┘      └────────────────────┘      └────────────┘
//!      │                    │                         │
//!      │    1. Decode       │                         │
//!      │       Headers      │                         │
//!      │                    │                         │
//!      │                    │    2. Forward to        │
//!      │                    │       Controller        │
//!      │                    │       (via Raft)        │
//!      │                    │                         │
//!      │    3. Response     │    4. Consensus         │
//!      │<───────────────────│<────────────────────────│
//! ```
//!
//! ## Thread Safety
//!
//! This processor is designed to be used concurrently. All state modifications go through
//! the Controller which ensures consistency via Raft consensus.
//!
//! Protocol codes, headers, bodies, and serialization are owned by `rocketmq-protocol`.
//! Transport request/session facts are borrowed through the V2 request aggregate;
//! controller orchestration and metrics remain local to this crate.

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use crate::controller::broker_heartbeat_manager::BrokerSession;
use crate::controller::broker_heartbeat_manager::BrokerSessionHeartbeatManager;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::manager::ControllerManager;
use crate::metrics::RequestHandleStatus;
use crate::metrics::RequestType as MetricsRequestType;
use crate::Controller;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotRequest;
use rocketmq_protocol::protocol::body::release_checkpoint::MaintenanceCapabilitiesResponse;
use rocketmq_protocol::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_protocol::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_protocol::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_protocol::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_protocol::protocol::header::maintenance_request_header::MaintenanceRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_security_api::MaintenanceAuthorizationContext;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceCapability;
use rocketmq_security_api::MaintenanceRequestClass;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::SessionView;
use tracing::info;
use tracing::warn;
/// Timeout for controller operations (in seconds)
const WAIT_TIMEOUT_SECONDS: u64 = 5;

/// Controller request processor implementing NettyRequestProcessor equivalent
///
/// This processor handles all incoming requests from brokers and routes them to the
/// appropriate Controller methods. It provides:
/// - Request routing based on request code
/// - Metrics collection and reporting
/// - Error handling and response generation
/// - Configuration blacklist validation
#[derive(Clone)]
pub struct ControllerRequestProcessor {
    /// Reference to the controller manager
    controller_manager: Weak<ControllerManager>,

    /// Immutable wire defaults captured from the owning Controller manager.
    command_factory: RemotingCommandFactory,

    /// Reference to the heartbeat manager
    heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,

    /// Configuration blacklist - configs that cannot be dynamically updated
    config_blacklist: Arc<HashSet<String>>,
}

impl ControllerRequestProcessor {
    /// Create a new controller request processor
    ///
    /// # Arguments
    ///
    /// * `controller_manager` - Reference to the controller manager
    ///
    /// The caller remains responsible for owning the manager lifecycle. The
    /// processor keeps only a weak manager reference so the remoting task cannot
    /// retain the complete service graph after shutdown.
    ///
    /// # Returns
    ///
    /// A new instance of `ControllerRequestProcessor`
    pub fn new(controller_manager: Arc<ControllerManager>) -> Self {
        let heartbeat_manager = controller_manager.heartbeat_manager().clone();
        let config_blacklist = Arc::new(Self::init_config_blacklist(&controller_manager));
        let command_factory = controller_manager.remoting_command_factory();

        Self {
            controller_manager: Arc::downgrade(&controller_manager),
            command_factory,
            heartbeat_manager,
            config_blacklist,
        }
    }

    /// Initialize configuration blacklist
    ///
    /// # Arguments
    ///
    /// * `controller_manager` - Reference to the controller manager
    ///
    /// # Returns
    ///
    /// A HashSet containing all blacklisted configuration keys
    fn init_config_blacklist(controller_manager: &ControllerManager) -> HashSet<String> {
        let mut blacklist = HashSet::new();

        // Default blacklisted configs
        blacklist.insert("configBlackList".to_string());
        blacklist.insert("configStorePath".to_string());
        blacklist.insert("rocketmqHome".to_string());

        let config = controller_manager.controller_config();
        let config_black_list = config.config_black_list.as_str();
        if !config_black_list.is_empty() {
            for item in config_black_list.split(';') {
                let trimmed: &str = item.trim();
                if !trimmed.is_empty() {
                    blacklist.insert(trimmed.to_string());
                }
            }
        }

        blacklist
    }

    fn controller_manager(&self) -> RocketMQResult<Arc<ControllerManager>> {
        self.controller_manager
            .upgrade()
            .ok_or_else(|| RocketMQError::not_initialized("controller manager is no longer available"))
    }

    /// Handle incoming request and route to appropriate handler
    ///
    /// # Arguments
    ///
    /// * `session` - Stable session identity and read-only lifecycle capability
    /// * `channel_identity` - Compatibility identity used by maintenance authentication
    /// * `request` - The incoming remoting command
    ///
    /// # Returns
    ///
    /// Result containing the response command or error
    pub(crate) async fn handle_request(
        &self,
        session: BrokerSession,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());

        match request_code {
            RequestCode::ControllerAlterSyncStateSet => self.handle_alter_sync_state_set(request).await,
            RequestCode::ControllerElectMaster => self.handle_elect_master(request).await,
            RequestCode::ControllerGetReplicaInfo => self.handle_get_replica_info(request).await,
            RequestCode::ControllerGetMetadataInfo => self.handle_get_metadata_info(request).await,
            RequestCode::BrokerHeartbeat => self.handle_broker_heartbeat(session, request).await,
            RequestCode::ControllerGetSyncStateData => self.handle_get_sync_state_data(request).await,
            RequestCode::UpdateControllerConfig => self.handle_update_controller_config(request).await,
            RequestCode::GetControllerConfig => self.handle_get_controller_config().await,
            RequestCode::CleanBrokerData => self.handle_clean_broker_data(request).await,
            RequestCode::ControllerGetNextBrokerId => self.handle_get_next_broker_id(request).await,
            RequestCode::ControllerApplyBrokerId => self.handle_apply_broker_id(request).await,
            RequestCode::ControllerRegisterBroker => self.handle_register_broker(request).await,
            RequestCode::MaintenanceGetCapabilities => {
                self.handle_maintenance_capabilities(channel_identity, request).await
            }
            RequestCode::MaintenanceCreateControllerSnapshot => {
                self.handle_create_release_snapshot(channel_identity, request).await
            }
            RequestCode::MaintenanceVerifyCheckpoint => {
                self.handle_verify_release_snapshot(channel_identity, request).await
            }
            RequestCode::MaintenanceRestoreVerify => self.handle_restore_verify(channel_identity, request).await,
            _ => {
                let error_msg = format!("request type {} not supported", request.code());
                Ok(Some(self.command_factory.create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    error_msg,
                )))
            }
        }
    }

    // ==================== Request Handlers ====================

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

    async fn handle_maintenance_capabilities(
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

    async fn handle_create_release_snapshot(
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
        let controller_manager = self.controller_manager()?;
        let snapshot = controller_manager
            .controller()
            .create_release_snapshot(&grant, snapshot_request)
            .await?;
        let body = serde_json::to_vec(&snapshot.manifest)
            .map_err(|error| RocketMQError::internal("encode Controller release snapshot manifest", error))?;
        Ok(Some(
            self.command_factory.create_success_response_command().set_body(body),
        ))
    }

    async fn handle_verify_release_snapshot(
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

    async fn handle_restore_verify(
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

    /// Handle ALTER_SYNC_STATE_SET request
    ///
    /// This changes the in-sync replica set for a broker group.
    ///
    /// # Request Flow
    ///
    /// 1. Decode AlterSyncStateSetRequestHeader from request
    /// 2. Decode SyncStateSet from request body
    /// 3. Forward to controller.alter_sync_state_set()
    /// 4. Wait for response with WAIT_TIMEOUT_SECONDS timeout
    /// 5. Return response command
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel (unused, for compatibility)
    /// * `ctx` - Connection context (unused, for compatibility)
    /// * `request` - Request command containing header and sync state set
    ///
    /// # Returns
    ///
    /// Result containing response command
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Request decoding fails
    /// - Controller operation times out
    /// - Controller returns error response
    ///
    /// # Implementation Note
    ///
    /// The actual logic is delegated to the Raft controller layer, which handles:
    /// - Leader state validation
    /// - Raft consensus (proposal submission)
    /// - State machine application via ReplicasInfoManager
    async fn handle_alter_sync_state_set(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        use rocketmq_error::RocketMQError;
        use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
        use rocketmq_protocol::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;

        // Decode request header
        let request_header = request
            .decode_command_custom_header::<AlterSyncStateSetRequestHeader>()
            .map_err(|e| {
                RocketMQError::request_header_error(format!("Failed to decode AlterSyncStateSetRequestHeader: {:?}", e))
            })?;

        // Decode request body (SyncStateSet)
        let sync_state_set = if let Some(body) = request.body() {
            SyncStateSet::decode(body)?
        } else {
            return Err(RocketMQError::request_body_invalid(
                "ALTER_SYNC_STATE_SET",
                "Request body is empty",
            ));
        };

        let controller_manager = self.controller_manager()?;
        controller_manager
            .controller()
            .alter_sync_state_set(&request_header, sync_state_set)
            .await
    }

    /// Handle ELECT_MASTER request
    ///
    /// Triggers master election for a broker group.
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command containing election parameters
    ///
    /// # Returns
    ///
    /// Result containing response command with new master information
    async fn handle_elect_master(&self, request: &mut RemotingCommand) -> RocketMQResult<Option<RemotingCommand>> {
        // Decode request header
        let request_header = request
            .decode_command_custom_header::<ElectMasterRequestHeader>()
            .map_err(|e| {
                RocketMQError::request_header_error(format!("Failed to decode ElectMasterRequestHeader: {:?}", e))
            })?;

        let controller_manager = self.controller_manager()?;
        let config = controller_manager.controller_config();

        // Forward to Controller
        let response = controller_manager.controller().elect_master(&request_header).await?;

        if let Some(response_command) = response.as_ref() {
            if response_command.code() == ResponseCode::Success as i32 && config.notify_broker_role_changed {
                if let Err(error) = controller_manager
                    .notify_broker_role_changed(response_command.clone())
                    .await
                {
                    warn!("Failed to notify brokers after elect-master request: {}", error);
                }
            }
        }

        Ok(response)
    }

    /// Handle GET_REPLICA_INFO request
    ///
    /// Returns replica information for a broker group.
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command with broker name
    ///
    /// # Returns
    ///
    /// Result containing replica information
    async fn handle_get_replica_info(&self, _request: &mut RemotingCommand) -> RocketMQResult<Option<RemotingCommand>> {
        use rocketmq_protocol::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;

        let request_header = _request
            .decode_command_custom_header::<GetReplicaInfoRequestHeader>()
            .map_err(|e| {
                RocketMQError::request_header_error(format!("Failed to decode GetReplicaInfoRequestHeader: {:?}", e))
            })?;

        let controller_manager = self.controller_manager()?;
        controller_manager.controller().get_replica_info(&request_header).await
    }

    /// Handle GET_METADATA_INFO request
    ///
    /// Returns controller metadata (e.g., leader info, sync state data).
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command
    ///
    /// # Returns
    ///
    /// Result containing controller metadata
    async fn handle_get_metadata_info(
        &self,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let controller_manager = self.controller_manager()?;
        controller_manager.controller().get_controller_metadata().await
    }

    /// Handle BROKER_HEARTBEAT request
    ///
    /// Records broker heartbeat to track liveness and state.
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel from the broker
    /// * `ctx` - Connection context
    /// * `request` - Request command with heartbeat data
    ///
    /// # Returns
    ///
    /// Result containing acknowledgment
    async fn handle_broker_heartbeat(
        &self,
        session: BrokerSession,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header_fast::<BrokerHeartbeatRequestHeader>()?;

        if let Some(broker_id) = &request_header.broker_id {
            let heartbeat_timeout_mills = request_header.heartbeat_timeout_mills.ok_or_else(|| {
                RocketMQError::request_header_error("BrokerHeartbeatRequestHeader.heartbeat_timeout_mills is missing")
            })?;
            let heartbeat_timeout_mills = u64::try_from(heartbeat_timeout_mills).map_err(|_| {
                RocketMQError::request_header_error(
                    "BrokerHeartbeatRequestHeader.heartbeat_timeout_mills must be non-negative",
                )
            })?;
            self.heartbeat_manager.on_broker_session_heartbeat(
                &request_header.cluster_name,
                &request_header.broker_name,
                &request_header.broker_addr,
                *broker_id,
                Some(heartbeat_timeout_mills),
                session,
                request_header.epoch,
                request_header.max_offset,
                request_header.confirm_offset,
                request_header.election_priority,
            );
            let controller_manager = self.controller_manager()?;
            controller_manager
                .controller()
                .record_broker_heartbeat(&request_header)
                .await
        } else {
            Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "Heart beat with empty brokerId",
            )))
        }
    }

    /// Handle GET_SYNC_STATE_DATA request
    ///
    /// Returns sync state data for specified broker groups.
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command with broker names list
    ///
    /// # Returns
    ///
    /// Result containing sync state data
    async fn handle_get_sync_state_data(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        if let Some(body) = request.body() {
            let broker_names: Vec<CheetahString> = serde_json::from_slice(body).unwrap_or_default();
            if !broker_names.is_empty() {
                let controller_manager = self.controller_manager()?;
                return controller_manager.controller().get_sync_state_data(&broker_names).await;
            }
        }
        Ok(Some(self.command_factory.create_success_response_command()))
    }

    /// Handle UPDATE_CONTROLLER_CONFIG request
    ///
    /// Updates controller configuration dynamically (respects blacklist).
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command with configuration properties
    ///
    /// # Returns
    ///
    /// Result containing success or error response
    async fn handle_update_controller_config(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // Parse request body as properties
        let properties = if let Some(body) = request.body() {
            // Convert body to properties map
            // Format: "key1=value1\nkey2=value2"
            Self::parse_properties_from_string(body).await?
        } else {
            return Err(RocketMQError::request_body_invalid(
                "UPDATE_CONTROLLER_CONFIG",
                "request body not exist",
            ));
        };
        if properties.is_empty() {
            return Err(RocketMQError::request_body_invalid(
                "UPDATE_CONTROLLER_CONFIG",
                "update config found empty config",
            ));
        }
        // Validate against blacklist
        if self.validate_blacklist_config_exist(&properties) {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::NoPermission,
                "Cannot update blacklisted configuration".to_string(),
            )));
        }

        // Apply configuration updates
        let controller_manager = self.controller_manager()?;
        controller_manager.update_config(properties).await?;

        // Return success
        Ok(Some(self.command_factory.create_success_response_command()))
    }
    // Helper function to parse properties
    async fn parse_properties_from_string(body: &[u8]) -> RocketMQResult<HashMap<String, String>> {
        let content = String::from_utf8(body.to_vec()).map_err(|error| {
            RocketMQError::request_body_invalid(
                "UPDATE_CONTROLLER_CONFIG",
                format!("parse property string failed: {error}"),
            )
        })?;
        let mut properties = HashMap::new();

        for line in content.lines() {
            if let Some((key, value)) = line.split_once('=') {
                properties.insert(key.trim().to_string(), value.trim().to_string());
            }
        }

        Ok(properties)
    }
    /// Handle GET_CONTROLLER_CONFIG request
    ///
    /// Returns all controller configurations as formatted string.
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command
    ///
    /// # Returns
    ///
    /// Result containing configuration string
    async fn handle_get_controller_config(&self) -> RocketMQResult<Option<RemotingCommand>> {
        let controller_config = self.controller_manager()?.controller_config();
        let config_string = controller_config.to_properties_string();

        let response = self
            .command_factory
            .create_success_response_command()
            .set_body(config_string.into_bytes());
        Ok(Some(response))
    }

    /// Handle CLEAN_BROKER_DATA request
    ///
    /// Cleans broker data from controller (e.g., after broker offline).
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command with clean parameters
    ///
    /// # Returns
    ///
    /// Result containing success or error response
    async fn handle_clean_broker_data(&self, request: &mut RemotingCommand) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<CleanBrokerDataRequestHeader>()
            .map_err(|e| {
                warn!("Failed to decode CleanBrokerDataRequestHeader: {:?}", e);
                RocketMQError::request_header_error(format!("Failed to decode CleanBrokerDataRequestHeader: {:?}", e))
            })?;

        if request_header.broker_name.is_empty() {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "broker_name cannot be empty",
            )));
        }

        let controller_manager = self.controller_manager()?;
        controller_manager.controller().clean_broker_data(&request_header).await
    }

    /// Handle GET_NEXT_BROKER_ID request
    ///
    /// Allocates the next available broker ID for a cluster/broker name.
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command with cluster and broker name
    ///
    /// # Returns
    ///
    /// Result containing the allocated broker ID
    async fn handle_get_next_broker_id(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // Decode the request header
        let request_header = request
            .decode_command_custom_header::<GetNextBrokerIdRequestHeader>()
            .map_err(|e| {
                warn!("Failed to decode GetNextBrokerIdRequestHeader: {:?}", e);
                RocketMQError::request_header_error(format!("Failed to decode GetNextBrokerIdRequestHeader: {:?}", e))
            })?;

        info!(
            "Received GetNextBrokerId request: cluster={}, broker={}",
            request_header.cluster_name, request_header.broker_name
        );

        // Validate cluster_name
        if request_header.cluster_name.is_empty() {
            warn!("GetNextBrokerId request rejected: cluster_name is empty");
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "cluster_name cannot be empty".to_string(),
            )));
        }

        // Validate broker_name
        if request_header.broker_name.is_empty() {
            warn!("GetNextBrokerId request rejected: broker_name is empty");
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "broker_name cannot be empty".to_string(),
            )));
        }

        // Forward to controller to allocate next broker ID
        let controller_manager = self.controller_manager()?;
        let response: Option<RemotingCommand> = controller_manager
            .controller()
            .get_next_broker_id(&request_header)
            .await?;

        // Log result
        if let Some(_res) = &response {
            info!(
                "Allocated broker_id response created for cluster={}, broker={}",
                request_header.cluster_name, request_header.broker_name
            );
        } else {
            warn!(
                "Failed to allocate broker_id for cluster={}, broker={}",
                request_header.cluster_name, request_header.broker_name
            );
        }

        // Return the controller's response
        Ok(response)
    }

    /// Handle APPLY_BROKER_ID request
    ///
    /// Applies for a specific broker ID (for broker restart or migration).
    /// This operation requires Raft consensus to reserve/allocate the ID.
    ///
    /// # Request Flow
    ///
    /// 1. Decode ApplyBrokerIdRequestHeader from request
    /// 2. Validate requested broker ID (must be non-negative)
    /// 3. Validate cluster_name and broker_name are not empty
    /// 4. Forward to controller.apply_broker_id() for Raft consensus
    /// 5. Return response indicating success or rejection
    ///
    /// # Use Cases
    ///
    /// - Broker restart: Broker reclaims its previous ID
    /// - Broker migration: New instance takes over old broker's ID
    /// - Disaster recovery: Restoring broker from backup with known ID
    /// - Pre-planned topology: Admin assigns specific IDs
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command with desired broker ID
    ///
    /// # Returns
    ///
    /// Result containing approval or rejection
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Request header decoding fails
    /// - Invalid broker ID (negative value)
    /// - Empty cluster_name or broker_name
    /// - ID is already in use by another active broker
    /// - Controller is not the leader
    /// - Raft consensus fails
    async fn handle_apply_broker_id(&self, request: &mut RemotingCommand) -> RocketMQResult<Option<RemotingCommand>> {
        // Decode request header
        let request_header = request
            .decode_command_custom_header::<ApplyBrokerIdRequestHeader>()
            .map_err(|e| {
                warn!("Failed to decode ApplyBrokerIdRequestHeader: {:?}", e);
                RocketMQError::request_header_error(format!("Failed to decode ApplyBrokerIdRequestHeader: {:?}", e))
            })?;

        info!(
            "Received ApplyBrokerId request: cluster={}, broker={}, broker_id={}",
            request_header.cluster_name, request_header.broker_name, request_header.applied_broker_id
        );

        // Validate cluster_name is not empty
        if request_header.cluster_name.is_empty() {
            warn!("ApplyBrokerId request rejected: cluster_name is empty");
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "cluster_name cannot be empty".to_string(),
            )));
        }

        // Validate broker_name is not empty
        if request_header.broker_name.is_empty() {
            warn!("ApplyBrokerId request rejected: broker_name is empty");
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "broker_name cannot be empty".to_string(),
            )));
        }

        // Validate requested broker ID is non-negative
        if request_header.applied_broker_id < 0 {
            warn!(
                "ApplyBrokerId request rejected: invalid broker_id={} for broker={}",
                request_header.applied_broker_id, request_header.broker_name
            );
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerBrokerIdInvalid,
                format!(
                    "Invalid broker ID: {}. Broker ID must be non-negative.",
                    request_header.applied_broker_id
                ),
            )));
        }

        // Forward to controller for Raft consensus
        let controller_manager = self.controller_manager()?;
        let result = controller_manager.controller().apply_broker_id(&request_header).await;

        match &result {
            Ok(Some(response)) => {
                if response.code() == ResponseCode::Success as i32 {
                    info!(
                        "ApplyBrokerId succeeded: broker={} applied broker_id={}",
                        request_header.broker_name, request_header.applied_broker_id
                    );
                } else {
                    warn!(
                        "ApplyBrokerId failed: broker={}, broker_id={}, code={}, remark={:?}",
                        request_header.broker_name,
                        request_header.applied_broker_id,
                        response.code(),
                        response.remark()
                    );
                }
            }
            Ok(None) => {
                warn!(
                    "ApplyBrokerId returned no response for broker={}",
                    request_header.broker_name
                );
            }
            Err(e) => {
                warn!("ApplyBrokerId error for broker={}: {:?}", request_header.broker_name, e);
            }
        }

        result
    }

    /// Handle REGISTER_BROKER request
    ///
    /// Registers a broker with the controller.
    ///
    /// # Arguments
    ///
    /// * `channel` - Network channel
    /// * `ctx` - Connection context
    /// * `request` - Request command with broker registration data
    ///
    /// # Returns
    ///
    /// Result containing registration response
    async fn handle_register_broker(&self, request: &mut RemotingCommand) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<RegisterBrokerToControllerRequestHeader>()
            .map_err(|e| {
                warn!("Failed to decode RegisterBrokerToControllerRequestHeader: {:?}", e);
                RocketMQError::request_header_error(format!(
                    "Failed to decode RegisterBrokerToControllerRequestHeader: {:?}",
                    e
                ))
            })?;

        let broker_name = request_header.broker_name.clone().unwrap_or_default();
        let cluster_name = request_header.cluster_name.clone().unwrap_or_default();
        let broker_address = request_header.broker_address.clone().unwrap_or_default();

        if broker_name.is_empty() || cluster_name.is_empty() || broker_address.is_empty() {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "cluster_name, broker_name and broker_address cannot be empty",
            )));
        }

        let controller_manager = self.controller_manager()?;
        controller_manager.controller().register_broker(&request_header).await
    }

    // ==================== Helper Methods ====================

    /// Validate if any property key exists in the configuration blacklist
    ///
    /// # Arguments
    ///
    /// * `properties` - Properties to validate
    ///
    /// # Returns
    ///
    /// true if any blacklisted config exists, false otherwise
    fn validate_blacklist_config_exist(&self, properties: &std::collections::HashMap<String, String>) -> bool {
        for black_config in self.config_blacklist.iter() {
            if properties.contains_key(black_config) {
                return true;
            }
        }
        false
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

impl ControllerRequestProcessor {
    fn record_request_metrics(&self, request_name: &str, status: RequestHandleStatus, latency_us: u64) {
        #[cfg(feature = "metrics")]
        if let Some(controller_manager) = self.controller_manager.upgrade() {
            controller_manager
                .metrics_manager()
                .inc_request_total(request_name, status);
            controller_manager
                .metrics_manager()
                .record_request_latency(request_name, latency_us);
        }

        #[cfg(not(feature = "metrics"))]
        let _ = (request_name, status, latency_us);
    }

    pub(crate) async fn complete_request<F>(
        &self,
        request_name: Option<&'static str>,
        dispatch: F,
    ) -> RocketMQResult<RemotingCommand>
    where
        F: Future<Output = RocketMQResult<Option<RemotingCommand>>>,
    {
        let start = Instant::now();

        let result = tokio::time::timeout(Duration::from_secs(WAIT_TIMEOUT_SECONDS), dispatch).await;

        let latency_us = start.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
        match result {
            Ok(Ok(response)) => {
                let response = response.unwrap_or_else(|| {
                    self.command_factory.create_response_command_with_code_remark(
                        ResponseCode::SystemError,
                        "Controller request completed without a response",
                    )
                });
                if let Some(name) = request_name {
                    let status = if response.code() == ResponseCode::Success as i32 {
                        RequestHandleStatus::Success
                    } else {
                        RequestHandleStatus::Failed
                    };
                    self.record_request_metrics(name, status, latency_us);
                }
                Ok(response)
            }
            Ok(Err(error)) => {
                if let Some(name) = request_name {
                    self.record_request_metrics(name, RequestHandleStatus::Failed, latency_us);
                }
                Err(error)
            }
            Err(_) => {
                if let Some(name) = request_name {
                    self.record_request_metrics(name, RequestHandleStatus::Timeout, latency_us);
                }
                Ok(self.command_factory.create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    "Controller request timed out",
                ))
            }
        }
    }

    fn response_outcome(mut response: RemotingCommand) -> RocketMQResult<HandlerOutcome> {
        let body = response.take_body();
        let plan = match body {
            Some(body) => ResponsePlan::bytes(response, body),
            None => ResponsePlan::command(response),
        }
        .map_err(|error| RocketMQError::response_process_failed("controller.response_plan", error.to_string()))?;
        Ok(HandlerOutcome::Reply(plan))
    }
}

// ==================== RequestProcessorV2 Implementation ====================

impl RequestProcessorV2 for ControllerRequestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        let owner_id = request.original_identity().request_id().owner_id();
        let channel_identity = match request.session() {
            SessionView::Network { .. } => format!("transport-session-{owner_id}"),
            SessionView::Embedded { .. } => format!("embedded-proxy-{owner_id}"),
            _ => format!("transport-session-{owner_id}"),
        };
        let session = BrokerSession::new(request.session().id(), request.session().state().clone());
        let request_name = RequestCode::from(request.command().code()).get_controller_request_name();
        let dispatch = self.handle_request(session, &channel_identity, request.command_mut());
        let response = self.complete_request(request_name, dispatch).await?;
        Self::response_outcome(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_error::ErrorKind;

    #[tokio::test]
    async fn parse_properties_from_string_rejects_invalid_utf8_as_request_body() {
        let error = ControllerRequestProcessor::parse_properties_from_string(&[0xff])
            .await
            .expect_err("invalid utf8 should be rejected");

        assert_eq!(error.kind(), ErrorKind::RequestBodyInvalid);
        assert!(error.to_string().contains("UPDATE_CONTROLLER_CONFIG"));
    }

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
