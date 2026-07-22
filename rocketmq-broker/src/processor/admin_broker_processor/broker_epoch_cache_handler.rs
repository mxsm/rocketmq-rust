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

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::epoch_entry_cache::EpochEntryCache;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker::broker_admin_runtime::BrokerAdminRuntime;

pub struct BrokerEpochCacheHandler;

impl BrokerEpochCacheHandler {
    pub const fn new() -> Self {
        Self
    }

    pub async fn get_broker_epoch_cache<MS: MessageStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let broker_config = broker_runtime_inner.broker_config();
        let response = RemotingCommand::create_response_command();

        if !broker_config.enable_controller_mode {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("this request only for controllerMode"),
            ));
        }

        let replicas_manage = match broker_runtime_inner.replicas_manager() {
            Some(replicas_manage) => replicas_manage,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("replicas manager is not initialized"),
                ));
            }
        };

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

        let entry_code = EpochEntryCache::new(
            &broker_identity.broker_cluster_name,
            broker_config.broker_name(),
            broker_identity.broker_id,
            replicas_manage.get_epoch_entries(),
            message_store.get_max_phy_offset() as u64,
        );

        let cache = entry_code.encode().unwrap_or_default();
        Ok(Some(response.set_body(cache).set_code(ResponseCode::Success)))
    }
}
