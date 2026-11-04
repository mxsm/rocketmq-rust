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
use rocketmq_protocol::protocol::body::epoch_entry_cache::EpochEntry;
use rocketmq_protocol::protocol::body::epoch_entry_cache::EpochEntryCache;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_store::BrokerAdminStore;
use rocketmq_store::HAService;

use crate::broker::broker_admin_runtime::BrokerAdminRuntime;

pub struct BrokerEpochCacheHandler;

impl BrokerEpochCacheHandler {
    pub const fn new() -> Self {
        Self
    }

    pub async fn get_broker_epoch_cache<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> crate::broker_error::BrokerResult<Option<RemotingCommand>> {
        let broker_config = broker_runtime_inner.broker_config();
        let response = RemotingCommand::create_java_default_error_response_command();

        if !broker_config.enable_controller_mode {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("this request only for controllerMode"),
            ));
        }

        if broker_runtime_inner.replicas_manager().is_none() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("replicas manager is not initialized"),
            ));
        }

        let broker_identity = &broker_config.broker_identity;

        let message_store = match broker_runtime_inner.message_store() {
            Some(message_store) => message_store,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is not available"),
                ));
            }
        };

        // Empty until this Broker first became master, which a replica waiting for it can tell.
        let epoch_list = message_store
            .get_ha_service()
            .map(HAService::epoch_log)
            .unwrap_or_default()
            .entries()
            .iter()
            .map(|entry| EpochEntry::new(entry.epoch, entry.start_offset))
            .collect();
        let entry_code = EpochEntryCache::new(
            &broker_identity.broker_cluster_name,
            broker_config.broker_name(),
            broker_identity.broker_id,
            epoch_list,
            message_store.get_max_phy_offset() as u64,
        );

        let cache = entry_code.encode().unwrap_or_default();
        Ok(Some(RemotingCommand::create_success_response_command().set_body(cache)))
    }
}
