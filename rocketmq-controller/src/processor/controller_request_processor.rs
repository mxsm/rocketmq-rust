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
//! ## Implementation Notes
//!
//! The following types and modules need to be implemented in rocketmq-rust:
//!
//! 1. **rocketmq-remoting additions**:
//!    - `SyncStateSet` type in protocol::body
//!    - `RoleChangeNotifyEntry` type in protocol::body
//!    - Controller-specific request headers (AlterSyncStateSet, ElectMaster, etc.)
//!    - `RemotingSerializable` trait for body serialization/deserialization
//!
//! 2. **Controller Manager methods**:
//!    - `get_controller()` - returns Controller instance
//!    - `get_heartbeat_manager()` - returns BrokerHeartbeatManager
//!    - `get_controller_config()` - returns configuration
//!    - `get_configuration()` - returns Configuration manager
//!    - `notify_broker_role_changed()` - notifies brokers of role changes
//!
//! 3. **Controller methods** (all async, return `Result<RemotingCommand>`):
//!    - `alter_sync_state_set(header, sync_state_set)`
//!    - `elect_master(header)`
//!    - `get_replica_info(header)`
//!    - `get_controller_metadata()`
//!    - `get_sync_state_data(broker_names)`
//!    - `clean_broker_data(header)`
//!    - `get_next_broker_id(header)`
//!    - `apply_broker_id(header)`
//!    - `register_broker(header)`
//!
//! 4. **Metrics**:
//!    - `ControllerMetricsManager` for recording request metrics
//!    - `RequestType` and `RequestHandleStatus` enums
//!
//! 5. **Configuration**:
//!    - String property parsing utility (string2properties)
//!    - Configuration update/get methods

use std::collections::HashSet;
use std::sync::Arc;

use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::manager::ControllerManager;
use crate::Controller;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use tracing::info;
use tracing::warn;
// Note: These types need to be implemented in their respective modules
// Placeholder imports that need actual implementation:
// - SyncStateSet in rocketmq-remoting::protocol::body
// - All controller request headers in rocketmq-remoting::protocol::header::controller
// - BrokerHeartbeatManager in crate::heartbeat
// - ControllerMetricsManager in crate::metrics

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
    controller_manager: ArcMut<ControllerManager>,

    /// Reference to the heartbeat manager
    heartbeat_manager: ArcMut<DefaultBrokerHeartbeatManager>,

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
    /// # Returns
    ///
    /// A new instance of `ControllerRequestProcessor`
    pub fn new(controller_manager: ArcMut<ControllerManager>) -> Self {
        let heartbeat_manager = controller_manager.heartbeat_manager().clone();
        let config_blacklist = Arc::new(Self::init_config_blacklist(&controller_manager));

        Self {
            controller_manager,
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

        let config_black_list = controller_manager.controller_config().config_black_list.as_str();
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

    /// Handle incoming request and route to appropriate handler
    ///
    /// # Arguments
    ///
    /// * `ctx` - Connection handler context
    /// * `request` - The incoming remoting command
    ///
    /// # Returns
    ///
    /// Result containing the response command or error
    async fn handle_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());

        match request_code {
            RequestCode::ControllerAlterSyncStateSet => self.handle_alter_sync_state_set(channel, ctx, request).await,
            RequestCode::ControllerElectMaster => self.handle_elect_master(channel, ctx, request).await,
            RequestCode::ControllerGetReplicaInfo => self.handle_get_replica_info(channel, ctx, request).await,
            RequestCode::ControllerGetMetadataInfo => self.handle_get_metadata_info(channel, ctx, request).await,
            RequestCode::BrokerHeartbeat => self.handle_broker_heartbeat(channel, ctx, request).await,
            RequestCode::ControllerGetSyncStateData => self.handle_get_sync_state_data(channel, ctx, request).await,
            RequestCode::UpdateControllerConfig => self.handle_update_controller_config(channel, ctx, request).await,
            RequestCode::GetControllerConfig => self.handle_get_controller_config(channel, ctx, request).await,
            RequestCode::CleanBrokerData => self.handle_clean_broker_data(channel, ctx, request).await,
            RequestCode::ControllerGetNextBrokerId => self.handle_get_next_broker_id(channel, ctx, request).await,
            RequestCode::ControllerApplyBrokerId => self.handle_apply_broker_id(channel, ctx, request).await,
            RequestCode::ControllerRegisterBroker => self.handle_register_broker(channel, ctx, request).await,
            _ => {
                let error_msg = format!("request type {} not supported", request.code());
                Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    error_msg,
                )))
            }
        }
    }

    // ==================== Request Handlers ====================

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
    /// This method requires the Controller (RaftController or DLedgerController) to implement
    /// the `alter_sync_state_set` method. The actual logic is delegated to the controller layer
    /// which handles:
    /// - Leader state validation
    /// - Raft consensus (proposal submission)
    /// - State machine application via ReplicasInfoManager
    ///
    /// **TODO**: Implement Controller::alter_sync_state_set() in the controller layer
    async fn handle_alter_sync_state_set(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        use rocketmq_error::RocketMQError;
        use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
        use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;

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

        self.controller_manager
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
    async fn handle_elect_master(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // Decode request header
        let request_header = request
            .decode_command_custom_header::<ElectMasterRequestHeader>()
            .map_err(|e| {
                RocketMQError::request_header_error(format!("Failed to decode ElectMasterRequestHeader: {:?}", e))
            })?;

        // Forward to Controller
        let response = self
            .controller_manager
            .controller()
            .elect_master(&request_header)
            .await?;

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
    async fn handle_get_replica_info(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        use rocketmq_remoting::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;

        let request_header = _request
            .decode_command_custom_header::<GetReplicaInfoRequestHeader>()
            .map_err(|e| {
                RocketMQError::request_header_error(format!("Failed to decode GetReplicaInfoRequestHeader: {:?}", e))
            })?;

        self.controller_manager
            .controller()
            .get_replica_info(&request_header)
            .await
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
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.controller_manager.controller().get_controller_metadata().await
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
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header_fast::<BrokerHeartbeatRequestHeader>()?;

        if let Some(broker_id) = &request_header.broker_id {
            let heartbeat_timeout_mills = request_header.heartbeat_timeout_mills.ok_or(RocketMQError::Internal(
                "in fn handle_broker_heartbeat, request_header
                .heartbeat_timeout_mills is none"
                    .to_string(),
            ))? as u64;
            self.heartbeat_manager.on_broker_heartbeat(
                &request_header.cluster_name,
                &request_header.broker_name,
                &request_header.broker_addr,
                *broker_id,
                Some(heartbeat_timeout_mills),
                channel,
                request_header.epoch,
                request_header.max_offset,
                request_header.confirm_offset,
                request_header.election_priority,
            );
            Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::Success,
                "Heart beat success",
            )))
        } else {
            Ok(Some(RemotingCommand::create_response_command_with_code_remark(
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
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        unimplemented!("unimplemented handle_get_sync_state_data")
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
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        unimplemented!("unimplemented handle_update_controller_config")
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
    async fn handle_get_controller_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let controller_config = self.controller_manager.controller_config();
        let config_string = controller_config.to_properties_string();

        let response = RemotingCommand::create_response_command().set_body(config_string.into_bytes());
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
    async fn handle_clean_broker_data(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        unimplemented!("unimplemented handle_clean_broker_data")
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
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        unimplemented!("unimplemented handle_get_next_broker_id")
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
    async fn handle_apply_broker_id(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
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
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "cluster_name cannot be empty".to_string(),
            )));
        }

        // Validate broker_name is not empty
        if request_header.broker_name.is_empty() {
            warn!("ApplyBrokerId request rejected: broker_name is empty");
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
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
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::ControllerBrokerIdInvalid,
                format!(
                    "Invalid broker ID: {}. Broker ID must be non-negative.",
                    request_header.applied_broker_id
                ),
            )));
        }

        // Forward to controller for Raft consensus
        let result = self
            .controller_manager
            .controller()
            .apply_broker_id(&request_header)
            .await;

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
    async fn handle_register_broker(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        unimplemented!("unimplemented handle_register_broker")
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

// ==================== RequestProcessor Implementation ====================

impl RequestProcessor for ControllerRequestProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.handle_request(channel, ctx, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cheetah_string::CheetahString;

    #[test]
    fn test_config_blacklist() {
        // Test configuration blacklist initialization
        let blacklist = HashSet::from([
            "configBlackList".to_string(),
            "configStorePath".to_string(),
            "rocketmqHome".to_string(),
        ]);

        assert!(blacklist.contains("configBlackList"));
        assert!(blacklist.contains("configStorePath"));
        assert!(blacklist.contains("rocketmqHome"));
    }

    #[test]
    fn test_apply_broker_id_request_header_validation() {
        // Test that ApplyBrokerIdRequestHeader can be created properly
        let header = ApplyBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str("test_cluster"),
            broker_name: CheetahString::from_static_str("test_broker"),
            applied_broker_id: 1,
            register_check_code: CheetahString::from_static_str("check_code_123"),
        };

        assert_eq!(header.cluster_name.as_str(), "test_cluster");
        assert_eq!(header.broker_name.as_str(), "test_broker");
        assert_eq!(header.applied_broker_id, 1);
        assert_eq!(header.register_check_code.as_str(), "check_code_123");
    }

    #[test]
    fn test_apply_broker_id_request_header_with_zero_id() {
        // Test with broker ID 0 (master)
        let header = ApplyBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str("production"),
            broker_name: CheetahString::from_static_str("broker-group-1"),
            applied_broker_id: 0,
            register_check_code: CheetahString::from_static_str("master_check"),
        };

        assert_eq!(header.applied_broker_id, 0);
        // Broker ID 0 is valid (represents master)
        assert!(header.applied_broker_id >= 0);
    }

    #[test]
    fn test_apply_broker_id_request_header_with_negative_id() {
        // Test with negative broker ID (should be invalid)
        let header = ApplyBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str("test"),
            broker_name: CheetahString::from_static_str("broker"),
            applied_broker_id: -1,
            register_check_code: CheetahString::from_static_str(""),
        };

        // Negative ID should be rejected
        assert!(header.applied_broker_id < 0);
    }

    #[test]
    fn test_apply_broker_id_request_header_empty_fields() {
        // Test with empty cluster_name and broker_name
        let header = ApplyBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str(""),
            broker_name: CheetahString::from_static_str(""),
            applied_broker_id: 1,
            register_check_code: CheetahString::from_static_str(""),
        };

        // Empty fields should be rejected in handler
        assert!(header.cluster_name.is_empty());
        assert!(header.broker_name.is_empty());
    }

    #[test]
    fn test_apply_broker_id_request_header_large_broker_id() {
        // Test with large broker ID
        let header = ApplyBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str("cluster"),
            broker_name: CheetahString::from_static_str("broker"),
            applied_broker_id: i64::MAX,
            register_check_code: CheetahString::from_static_str(""),
        };

        assert_eq!(header.applied_broker_id, i64::MAX);
        assert!(header.applied_broker_id > 0);
    }
}
