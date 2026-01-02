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

use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::exchange_ha_info_request_header::ExchangeHAInfoRequestHeader;
use rocketmq_remoting::protocol::header::exchange_ha_info_response_header::ExchangeHaInfoResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct UpdateBrokerHaHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> UpdateBrokerHaHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn update_broker_ha_info(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let exchange_request_header = request
            .decode_command_custom_header::<ExchangeHAInfoRequestHeader>()
            .unwrap();

        let mut response = RemotingCommand::default();

        if let Some(master_ha_addr) = exchange_request_header.master_ha_address.as_ref() {
            if !master_ha_addr.is_empty() {
                if let Some(message_store) = self.broker_runtime_inner.message_store() {
                    message_store.update_ha_master_address(master_ha_addr.as_str()).await;

                    let master_address = exchange_request_header.master_address.unwrap_or_default();
                    message_store.update_master_address(&master_address);

                    let should_sync_master_flush_offset_on_startup = self
                        .broker_runtime_inner
                        .message_store_config()
                        .sync_master_flush_offset_when_startup;
                    if message_store.get_master_flushed_offset() == 0x0000 && should_sync_master_flush_offset_on_startup
                    {
                        let master_flush_offset = exchange_request_header.master_flush_offset.unwrap();

                        info!("Set master flush offset in slave to {}", master_flush_offset);
                        message_store.set_master_flushed_offset(master_flush_offset);
                    }
                }
            } else if self.broker_runtime_inner.broker_config().broker_identity.broker_id == MASTER_ID {
                let response_header = response
                    .read_custom_header_mut::<ExchangeHaInfoResponseHeader>()
                    .unwrap();

                response_header.master_ha_address = Some(self.broker_runtime_inner.get_ha_server_addr());

                if let Some(message_store) = self.broker_runtime_inner.message_store() {
                    response_header.master_flush_offset = Some(message_store.get_broker_init_max_offset());
                }
                response_header.master_address = Some(self.broker_runtime_inner.get_broker_addr().clone());
            }
        }

        response.set_code_ref(ResponseCode::Success);
        Ok(Some(response))
    }
}
