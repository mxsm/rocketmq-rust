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

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_protocol::protocol::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_store::BrokerAdminStore;
use tracing::info;
use tracing::warn;

use super::broker_config_request_handler::BrokerConfigRequestHandler;
use super::AdminRequestMetadata;

#[derive(Clone)]
pub struct NotifyBrokerRoleChangeHandler;

impl NotifyBrokerRoleChangeHandler {
    pub fn new() -> Self {
        Self
    }

    pub async fn notify_broker_role_changed<MS: BrokerAdminStore>(
        &self,
        broker_config_request_handler: &BrokerConfigRequestHandler<MS>,
        metadata: &AdminRequestMetadata,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_success_response_command();
        let Some(controller_leader_address) = metadata.network_remote_addr() else {
            warn!("Reject embedded notifyBrokerRoleChanged because the controller network peer is unavailable");
            return Ok(Some(response.set_code(ResponseCode::NoPermission).set_remark(
                "notify broker role change requires a trusted network controller peer",
            )));
        };

        let request_header = match request.decode_command_custom_header::<NotifyBrokerRoleChangedRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("invalid role-change header: {error}"),
                )));
            }
        };

        let Some(body) = request.get_body() else {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "notify broker role change is missing the sync-state-set body",
            )));
        };
        let sync_state_set_info = match SyncStateSet::decode(body) {
            Ok(sync_state_set) => sync_state_set,
            Err(error) => {
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("invalid sync-state-set body: {error}"),
                )));
            }
        };

        info!(
            "Receive notifyBrokerRoleChanged request, try to change brokerRole, request:{}",
            request_header
        );

        if broker_config_request_handler
            .broker_runtime_inner()
            .replicas_manager()
            .is_none()
        {
            warn!("Ignore notifyBrokerRoleChanged because controller mode is not initialized");
            return Ok(Some(response.set_code(ResponseCode::Success)));
        }

        let sync_state_set = sync_state_set_info.get_sync_state_set().cloned().unwrap_or_default();
        let controller_leader_address = controller_leader_address.to_string().into();

        if let Err(error) = broker_config_request_handler
            .apply_controller_role_change(
                Some(controller_leader_address),
                request_header.master_broker_id,
                request_header.master_address,
                request_header.master_epoch,
                request_header.sync_state_set_epoch,
                sync_state_set,
            )
            .await
        {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                error.to_string(),
            )));
        }

        Ok(Some(response.set_code(ResponseCode::Success)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::config::broker_config::BrokerConfig;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
    use rocketmq_protocol::protocol::header::notify_broker_role_change_request_header::NotifyBrokerRoleChangedRequestHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_store::MessageStoreConfig;

    use crate::broker_runtime::BrokerRuntime;
    use crate::processor::admin_broker_processor::trusted_admin_metadata;
    use crate::processor::admin_broker_processor::AdminOriginFact;
    use crate::processor::admin_broker_processor::AdminSessionFact;

    use super::AdminRequestMetadata;
    use super::BrokerConfigRequestHandler;
    use super::NotifyBrokerRoleChangeHandler;

    #[tokio::test]
    async fn uninitialized_controller_fails_closed_without_retaining_runtime_owner() {
        let runtime = BrokerRuntime::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        );
        let admin = runtime.admin_runtime_for_test();
        let broker_config_request_handler = BrokerConfigRequestHandler::new(admin);
        let handler = NotifyBrokerRoleChangeHandler::new();

        let peer = "127.0.0.1:10911".parse().expect("test peer");
        let header = NotifyBrokerRoleChangedRequestHeader {
            master_address: Some(CheetahString::from_static_str("127.0.0.1:10911")),
            master_epoch: Some(1),
            sync_state_set_epoch: Some(1),
            master_broker_id: Some(0),
        };
        let body = SyncStateSet::with_values(HashSet::from([0]), 1);
        let mut request = RemotingCommand::create_request_command(RequestCode::NotifyBrokerRoleChanged, header)
            .set_body(Bytes::from(
                serde_json::to_vec(&body).expect("serialize sync-state set"),
            ));

        let response = handler
            .notify_broker_role_changed(
                &broker_config_request_handler,
                &AdminRequestMetadata::network_for_test(peer),
                RequestCode::NotifyBrokerRoleChanged,
                &mut request,
            )
            .await
            .expect("uninitialized controller notification should return a response")
            .expect("notification should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
    }

    #[tokio::test]
    async fn embedded_proxy_cannot_supply_a_controller_network_endpoint() {
        let runtime = BrokerRuntime::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        );
        let admin = runtime.admin_runtime_for_test();
        let broker_config_request_handler = BrokerConfigRequestHandler::new(admin);
        let handler = NotifyBrokerRoleChangeHandler::new();
        let header = NotifyBrokerRoleChangedRequestHeader {
            master_address: Some(CheetahString::from_static_str("127.0.0.1:10911")),
            master_epoch: Some(1),
            sync_state_set_epoch: Some(1),
            master_broker_id: Some(0),
        };
        let body = SyncStateSet::with_values(HashSet::from([0]), 1);
        let mut request = RemotingCommand::create_request_command(RequestCode::NotifyBrokerRoleChanged, header)
            .set_body(Bytes::from(
                serde_json::to_vec(&body).expect("serialize sync-state set"),
            ));
        let metadata = trusted_admin_metadata(AdminOriginFact::BrokerProxy, AdminSessionFact::Embedded)
            .expect("trusted embedded Broker Proxy metadata");

        let response = handler
            .notify_broker_role_changed(
                &broker_config_request_handler,
                &metadata,
                RequestCode::NotifyBrokerRoleChanged,
                &mut request,
            )
            .await
            .expect("embedded notification should return a response")
            .expect("embedded notification should fail closed with a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("notify broker role change requires a trusted network controller peer")
        );
    }
}
