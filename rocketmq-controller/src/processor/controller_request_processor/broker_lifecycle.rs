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

use crate::controller::broker_heartbeat_manager::BrokerSession;
use crate::controller::broker_heartbeat_manager::BrokerSessionHeartbeatManager;
use crate::Controller;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_protocol::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_protocol::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use tracing::info;
use tracing::warn;

use super::ControllerRequestProcessor;

impl ControllerRequestProcessor {
    pub(super) async fn handle_broker_heartbeat(
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
            self.controller_manager()?
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

    pub(super) async fn handle_clean_broker_data(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<CleanBrokerDataRequestHeader>()
            .map_err(|error| {
                warn!("Failed to decode CleanBrokerDataRequestHeader: {:?}", error);
                RocketMQError::request_header_error(format!(
                    "Failed to decode CleanBrokerDataRequestHeader: {:?}",
                    error
                ))
            })?;

        if request_header.broker_name.is_empty() {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "broker_name cannot be empty",
            )));
        }

        self.controller_manager()?
            .controller()
            .clean_broker_data(&request_header)
            .await
    }

    pub(super) async fn handle_get_next_broker_id(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<GetNextBrokerIdRequestHeader>()
            .map_err(|error| {
                warn!("Failed to decode GetNextBrokerIdRequestHeader: {:?}", error);
                RocketMQError::request_header_error(format!(
                    "Failed to decode GetNextBrokerIdRequestHeader: {:?}",
                    error
                ))
            })?;

        info!(
            "Received GetNextBrokerId request: cluster={}, broker={}",
            request_header.cluster_name, request_header.broker_name
        );

        if request_header.cluster_name.is_empty() {
            warn!("GetNextBrokerId request rejected: cluster_name is empty");
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "cluster_name cannot be empty".to_string(),
            )));
        }

        if request_header.broker_name.is_empty() {
            warn!("GetNextBrokerId request rejected: broker_name is empty");
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "broker_name cannot be empty".to_string(),
            )));
        }

        let response = self
            .controller_manager()?
            .controller()
            .get_next_broker_id(&request_header)
            .await?;

        if response.is_some() {
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

        Ok(response)
    }

    pub(super) async fn handle_apply_broker_id(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<ApplyBrokerIdRequestHeader>()
            .map_err(|error| {
                warn!("Failed to decode ApplyBrokerIdRequestHeader: {:?}", error);
                RocketMQError::request_header_error(format!("Failed to decode ApplyBrokerIdRequestHeader: {:?}", error))
            })?;

        info!(
            "Received ApplyBrokerId request: cluster={}, broker={}, broker_id={}",
            request_header.cluster_name, request_header.broker_name, request_header.applied_broker_id
        );

        if request_header.cluster_name.is_empty() {
            warn!("ApplyBrokerId request rejected: cluster_name is empty");
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "cluster_name cannot be empty".to_string(),
            )));
        }

        if request_header.broker_name.is_empty() {
            warn!("ApplyBrokerId request rejected: broker_name is empty");
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::ControllerInvalidRequest,
                "broker_name cannot be empty".to_string(),
            )));
        }

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

        let result = self
            .controller_manager()?
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
            Err(error) => {
                warn!(
                    "ApplyBrokerId error for broker={}: {:?}",
                    request_header.broker_name, error
                );
            }
        }

        result
    }

    pub(super) async fn handle_register_broker(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<RegisterBrokerToControllerRequestHeader>()
            .map_err(|error| {
                warn!("Failed to decode RegisterBrokerToControllerRequestHeader: {:?}", error);
                RocketMQError::request_header_error(format!(
                    "Failed to decode RegisterBrokerToControllerRequestHeader: {:?}",
                    error
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

        self.controller_manager()?
            .controller()
            .register_broker(&request_header)
            .await
    }
}
