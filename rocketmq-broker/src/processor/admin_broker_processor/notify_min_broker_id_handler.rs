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

use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_rust::RocketMQTokioRwLock;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::slave::slave_synchronize::SlaveSynchronize;

#[derive(Clone)]
pub struct NotifyMinBrokerChangeIdHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    lock: Arc<RocketMQTokioRwLock<MinBrokerIngroup>>,
}

#[derive(Clone)]
struct MinBrokerIngroup {
    min_broker_id_in_group: Option<u64>,
    min_broker_addr_in_group: CheetahString,
}

impl MinBrokerIngroup {
    fn new() -> Self {
        Self {
            min_broker_id_in_group: None,
            min_broker_addr_in_group: CheetahString::empty(),
        }
    }
}

impl<MS: MessageStore> NotifyMinBrokerChangeIdHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
            lock: Arc::new(RocketMQTokioRwLock::new(MinBrokerIngroup::new())),
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

    async fn update_min_broker(&mut self, change_header: NotifyMinBrokerIdChangeRequestHeader) {
        let broker_config = self.broker_runtime_inner.broker_config();

        if broker_config.enable_slave_acting_master
            && broker_config.broker_identity.broker_id != MASTER_ID
        {
            if let Some(_) = self
                .lock
                .try_write_timeout(Duration::from_millis(3000))
                .await
            {
                if let Some(min_broker_id) = change_header.min_broker_id {
                    if min_broker_id != self.broker_runtime_inner.get_min_broker_id_in_group() {
                        // on min broker change
                        self.on_min_broker_change(&change_header).await;
                    }
                }
            } else {
                error!("Update min broker failed");
            }
        }
    }

    async fn on_min_broker_change(&self, change_header: &NotifyMinBrokerIdChangeRequestHeader) {
        let min_broker_id = change_header.min_broker_id.unwrap();
        let min_broker_addr = change_header.min_broker_addr.as_deref().unwrap();

        info!(
            "Min broker changed, old: {}-{}, new {}-{}",
            self.broker_runtime_inner.get_min_broker_id_in_group(),
            self.broker_runtime_inner.get_broker_addr(),
            min_broker_id,
            min_broker_addr
        );

        self.lock.write().await.min_broker_id_in_group = Some(min_broker_id);
        self.lock.write().await.min_broker_addr_in_group =
            CheetahString::from_slice(min_broker_addr);

        let should_start = self.broker_runtime_inner.get_min_broker_id_in_group()
            == self.lock.read().await.min_broker_id_in_group.unwrap();

        self.change_special_service_status(should_start).await;

        // master offline
        if let Some(offline_broker_addr) = change_header.offline_broker_addr.as_deref() {
            if let Some(slave_sync) = self.broker_runtime_inner.slave_synchronize() {
                match slave_sync.master_addr() {
                    Some(master_addr) => {
                        if offline_broker_addr.eq(master_addr.deref()) {
                            self.on_master_offline().await;
                        }
                    }

                    None => {}
                }
            }
        }

        //master online
        if min_broker_id == MASTER_ID {
            self.on_master_on_line().await;
        }
    }

    async fn change_special_service_status(&self, _should_start: bool) {}

    async fn on_master_offline(&self) {
        let broker_runtime_inner = self.broker_runtime_inner.clone();

        let _slave_synchronize = SlaveSynchronize::new(broker_runtime_inner);

        self.lock.write().await.min_broker_id_in_group = Some(123);
    }

    async fn on_master_on_line(&self) {}

    pub async fn get_min_broker_id_in_group(&self) -> Option<u64> {
        self.lock.read().await.min_broker_id_in_group
    }

    pub async fn get_min_broker_addr_in_group(&self) -> CheetahString {
        self.lock.read().await.min_broker_addr_in_group.clone()
    }
}
