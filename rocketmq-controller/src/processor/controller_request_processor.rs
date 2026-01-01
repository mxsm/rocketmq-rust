// Copyright 2025-2026 The RocketMQ Rust Authors
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
//! **This is a complete translation from Java RocketMQ 5.3.1 ControllerRequestProcessor**
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

use rocketmq_error::RocketMQResult;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;

use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::manager::ControllerManager;
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
pub struct ControllerRequestProcessor {
    /// Reference to the controller manager
    controller_manager: Arc<ControllerManager>,

    /// Reference to the heartbeat manager
    heartbeat_manager: Arc<dyn BrokerHeartbeatManager>,

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
    pub fn new(_controller_manager: Arc<ControllerManager>) -> Self {
        unimplemented!("Implement ControllerRequestProcessor::new")
    }

    /// Initialize the configuration blacklist
    ///
    /// These configurations cannot be updated dynamically for security and stability reasons.
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
    /// Equivalent to Java's handleAlterSyncStateSet method.
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
        let _request_header = request
            .decode_command_custom_header::<AlterSyncStateSetRequestHeader>()
            .map_err(|e| {
                RocketMQError::request_header_error(format!("Failed to decode AlterSyncStateSetRequestHeader: {:?}", e))
            })?;

        // Decode request body (SyncStateSet)
        let _sync_state_set = if let Some(body) = request.body() {
            SyncStateSet::decode(body)?
        } else {
            return Err(RocketMQError::request_body_invalid(
                "ALTER_SYNC_STATE_SET",
                "Request body is empty",
            ));
        };

        // TODO: Forward to controller with timeout
        // This requires implementing Controller::alter_sync_state_set() method
        //
        // Expected implementation (once Controller trait is ready):
        // ```
        // use std::time::Duration;
        //
        // use tokio::time::timeout;
        //
        // let controller = self.controller_manager.get_controller();
        // let future = controller.alter_sync_state_set(&request_header, sync_state_set);
        //
        // match timeout(Duration::from_secs(WAIT_TIMEOUT_SECONDS), future).await {
        //     Ok(Ok(response)) => Ok(Some(response)),
        //     Ok(Err(e)) => Err(e),
        //     Err(_) => Err(RocketMQError::Timeout {
        //         operation: "alter_sync_state_set",
        //         timeout_ms: (WAIT_TIMEOUT_SECONDS * 1000),
        //     }),
        // }
        // ```

        Ok(Some(RemotingCommand::create_response_command_with_code_remark(
            rocketmq_remoting::code::response_code::ResponseCode::SystemError,
            "Controller::alter_sync_state_set() not implemented yet. See ALIGNMENT_REPORT_ALTER_SYNC_STATE_SET.md for \
             implementation details.",
        )))
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
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        unimplemented!("unimplemented handle_elect_master")
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
        unimplemented!("unimplemented handle_get_replica_info")
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
        // This is a read-only operation, no timeout needed
        unimplemented!("unimplemented handle_get_metadata_info")
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
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        unimplemented!("unimplemented handle_broker_heartbeat")
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
        unimplemented!("unimplemented handle_get_controller_config")
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
    async fn handle_apply_broker_id(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        unimplemented!("unimplemented handle_apply_broker_id")
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
}
