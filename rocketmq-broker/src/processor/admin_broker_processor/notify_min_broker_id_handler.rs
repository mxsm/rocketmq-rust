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

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::RocketMQTokioRwLock;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct NotifyMinBrokerChangeIdHandler {
    lock: Arc<RocketMQTokioRwLock<MinBrokerIngroup>>,
}

#[derive(Clone)]
struct MinBrokerIngroup {
    min_broker_id_in_group: Option<u64>,
    min_broker_addr_in_group: Arc<CheetahString>,
}

impl MinBrokerIngroup {
    fn new() -> Self {
        Self {
            min_broker_id_in_group: Some(MASTER_ID),
            min_broker_addr_in_group: Arc::new(CheetahString::empty()),
        }
    }
}

impl NotifyMinBrokerChangeIdHandler {
    pub fn new() -> Self {
        Self {
            lock: Arc::new(RocketMQTokioRwLock::new(MinBrokerIngroup::new())),
        }
    }

    pub async fn notify_min_broker_id_change<MS: MessageStore>(
        &mut self,
        broker_runtime_inner: &mut BrokerRuntimeInner<MS>,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let change_header = request
            .decode_command_custom_header::<NotifyMinBrokerIdChangeRequestHeader>()
            .unwrap();

        let broker_config = broker_runtime_inner.broker_config();

        let latest_broker_id = change_header.min_broker_id.expect("min broker id not must be present");

        warn!(
            "min broker id changed, prev {}, new {}",
            broker_config.broker_identity.broker_id, latest_broker_id
        );

        self.update_min_broker(broker_runtime_inner, change_header).await;

        let response = RemotingCommand::create_response_command();
        Ok(Some(response.set_code(ResponseCode::Success)))
    }

    async fn update_min_broker<MS: MessageStore>(
        &mut self,
        broker_runtime_inner: &mut BrokerRuntimeInner<MS>,
        change_header: NotifyMinBrokerIdChangeRequestHeader,
    ) {
        let broker_config = broker_runtime_inner.broker_config();

        if broker_config.enable_slave_acting_master && broker_config.broker_identity.broker_id != MASTER_ID {
            if self.lock.try_write_timeout(Duration::from_millis(3000)).await.is_some() {
                if let Some(min_broker_id) = change_header.min_broker_id {
                    if min_broker_id != broker_runtime_inner.get_min_broker_id_in_group() {
                        // on min broker change
                        let min_broker_addr = change_header.min_broker_addr.as_deref().unwrap();

                        self.on_min_broker_change(
                            broker_runtime_inner,
                            min_broker_id,
                            min_broker_addr,
                            &change_header.offline_broker_addr,
                            &change_header.ha_broker_addr,
                        )
                        .await;
                    }
                }
            } else {
                error!("Update min broker failed");
            }
        }
    }

    async fn on_min_broker_change<MS: MessageStore>(
        &self,
        broker_runtime_inner: &mut BrokerRuntimeInner<MS>,
        min_broker_id: u64,
        min_broker_addr: &str,
        offline_broker_addr: &Option<CheetahString>,
        master_ha_addr: &Option<CheetahString>,
    ) {
        info!(
            "Min broker changed, old: {}-{}, new {}-{}",
            broker_runtime_inner.get_min_broker_id_in_group(),
            broker_runtime_inner.get_broker_addr(),
            min_broker_id,
            min_broker_addr
        );

        {
            let mut lock_guard = self.lock.write().await;
            lock_guard.min_broker_id_in_group = Some(min_broker_id);
            lock_guard.min_broker_addr_in_group = Arc::new(CheetahString::from_slice(min_broker_addr));
        }

        let should_start = broker_runtime_inner.get_min_broker_id_in_group() == min_broker_id;

        Self::change_special_service_status(broker_runtime_inner, should_start).await;

        // master offline
        if let Some(offline_broker_addr) = offline_broker_addr {
            let master_is_offline = broker_runtime_inner
                .slave_synchronize()
                .and_then(|slave_sync| slave_sync.master_addr())
                .is_some_and(|master_addr| !master_addr.is_empty() && offline_broker_addr.eq(master_addr.deref()));
            if master_is_offline {
                Self::on_master_offline(broker_runtime_inner).await;
            }
        }

        //master online
        if min_broker_id == MASTER_ID && !min_broker_addr.is_empty() {
            Self::on_master_on_line(broker_runtime_inner, min_broker_addr, master_ha_addr).await;
        }

        if min_broker_id == MASTER_ID {
            let pull_request_hold_service = broker_runtime_inner.pull_request_hold_service();
            if let Some(pull_request_hold_service) = pull_request_hold_service {
                pull_request_hold_service.notify_master_online();
            } else {
                error!("pull_request_hold_service is empty");
            }
        }
    }

    async fn change_special_service_status<MS: MessageStore>(
        broker_runtime_inner: &mut BrokerRuntimeInner<MS>,
        should_start: bool,
    ) {
        broker_runtime_inner.change_special_service_status(should_start).await;
    }

    async fn on_master_offline<MS: MessageStore>(broker_runtime_inner: &mut BrokerRuntimeInner<MS>) {
        if let Some(slave_synchronize) = broker_runtime_inner.slave_synchronize() {
            if let Some(master_addr) = slave_synchronize.master_addr() {
                let vip_channel = mix_all::broker_vip_channel(true, master_addr.as_str());
                let addr_list = vec![master_addr.to_string(), vip_channel.to_string()];
                broker_runtime_inner.broker_outer_api().close_channel(addr_list);
            }
        }

        broker_runtime_inner.update_slave_master_addr(None);
        if let Some(message_store) = broker_runtime_inner.message_store() {
            message_store.update_master_address(&CheetahString::empty());
        }
    }

    async fn on_master_on_line<MS: MessageStore>(
        broker_runtime_inner: &BrokerRuntimeInner<MS>,
        master_addr: &str,
        master_ha_addr: &Option<CheetahString>,
    ) {
        let need_sync_master_flush_offset = if let Some(message_store) = broker_runtime_inner.message_store() {
            message_store.get_master_flushed_offset() == 0x0000
                && broker_runtime_inner
                    .message_store_config()
                    .sync_master_flush_offset_when_startup
        } else {
            false
        };

        if master_ha_addr.is_none() || need_sync_master_flush_offset {
            let broker_sync_info = broker_runtime_inner
                .broker_outer_api()
                .retrieve_broker_ha_info(Some(&CheetahString::from(master_addr)))
                .await;

            match broker_sync_info {
                Ok(broker_sync_info) => {
                    if let Some(message_store) = broker_runtime_inner.message_store() {
                        if need_sync_master_flush_offset {
                            info!(
                                "Set master flush offset in slave to {}",
                                broker_sync_info.master_flush_offset
                            );
                            message_store.set_master_flushed_offset(broker_sync_info.master_flush_offset);
                        }

                        if master_ha_addr.is_none() {
                            if let Some(master_hs_address) = broker_sync_info.master_ha_address {
                                message_store.update_ha_master_address(master_hs_address.as_str()).await;
                            }
                            if let Some(master_address) = broker_sync_info.master_address {
                                message_store.update_master_address(&master_address);
                            }
                        }
                    } else {
                        error!("message_store is empty");
                    }
                }
                Err(e) => {
                    error!("retrieve master ha info exception {}", e);
                }
            };
        }

        if let Some(message_store) = broker_runtime_inner.message_store() {
            if master_ha_addr.is_some() {
                if let Some(master_ha_addr) = master_ha_addr {
                    message_store.update_ha_master_address(master_ha_addr.as_str()).await;
                }
            }
            message_store.wakeup_ha_client();
        }
    }

    pub async fn get_min_broker_id_in_group(&self) -> Option<u64> {
        self.lock.read().await.min_broker_id_in_group
    }

    pub async fn get_min_broker_addr_in_group(&self) -> Arc<CheetahString> {
        self.lock.read().await.min_broker_addr_in_group.clone()
    }
}
