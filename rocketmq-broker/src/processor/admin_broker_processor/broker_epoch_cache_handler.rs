/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::epoch_entry_cache::EpochEntryCache;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct BrokerEpochCacheHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> BrokerEpochCacheHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
        }
    }

    pub async fn get_broker_epoch_cache(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> Option<RemotingCommand> {
        let broker_runtime_inner = self.broker_runtime_inner.as_mut();

        let replicas_manage = if let Some(replicas_manage) = broker_runtime_inner.replicas_manager()
        {
            replicas_manage
        } else {
            panic!("`replicas_manage` object is empty")
        };

        let broker_config = broker_runtime_inner.broker_config();
        let response = RemotingCommand::create_response_command();

        if !broker_config.enable_controller_mode {
            return Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("this request only for controllerMode"),
            );
        }

        let broker_identity = &broker_config.broker_identity;
        let broker_cluster_name = &broker_identity.broker_cluster_name;
        let broker_name = broker_config.broker_name();
        let broker_id = broker_identity.broker_id;

        let epoch_list = replicas_manage.get_epoch_entries();

        let message_store = broker_runtime_inner.message_store().as_ref().unwrap();
        let max_offset = message_store.get_max_phy_offset().cast_unsigned();

        let entry_code = EpochEntryCache::new(
            broker_cluster_name,
            broker_name,
            broker_id,
            epoch_list,
            max_offset,
        );
        let cache = match entry_code.encode() {
            Ok(cache) => cache,
            Err(_) => Vec::new(),
        };
        Some(response.set_body(cache).set_code(ResponseCode::Success))
    }
}
