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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_rust::RocketMQTokioMutex;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
#[derive(Clone)]
pub struct NotifyMinBrokerChangeIdHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    lock: Arc<RocketMQTokioMutex<()>>,
}

impl<MS: MessageStore> NotifyMinBrokerChangeIdHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner: broker_runtime_inner,
            lock: Arc::new(RocketMQTokioMutex::new(())),
        }
    }

    pub async fn notify_min_broker_id_change(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let change_header = request
            .decode_command_custom_header::<NotifyMinBrokerIdChangeRequestHeader>()
            .unwrap();

        let broker_config = self.broker_runtime_inner.broker_config();

        let latest_broker_id = change_header
            .min_broker_id
            .expect("min broker id not must be present");

        warn!(
            "min broker id changed, prev {}, new {}",
            broker_config.broker_identity.broker_id, latest_broker_id
        );

        self.update_min_broker(change_header).await;

        let mut response = RemotingCommand::default();
        response.set_code_ref(ResponseCode::Success);
        Some(response)
    }

    async fn update_min_broker(&self, change_header: NotifyMinBrokerIdChangeRequestHeader) {
        let broker_config = self.broker_runtime_inner.broker_config();

        if broker_config.enable_slave_acting_master
            && broker_config.broker_identity.broker_id != MASTER_ID
        {
            let lock = self
                .lock
                .try_lock_timeout(Duration::from_millis(3000))
                .await;

            if let Some(_) = lock {
                if let Some(min_broker_id) = change_header.min_broker_id {
                    if min_broker_id != self.broker_runtime_inner.get_min_broker_id_in_group() {
                        // on min broker change
                        self.on_min_broker_change(&change_header);
                    }
                }
            } else {
                error!("Update min broker failed");
            }
        }
    }

    fn on_min_broker_change(&self, _change_header: &NotifyMinBrokerIdChangeRequestHeader) {
        // data update specific logic
    }
}
