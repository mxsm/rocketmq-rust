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
use rocketmq_remoting::protocol::body::broker_stats_item::BrokerStatsItem;
use rocketmq_remoting::protocol::header::view_broker_stats_data_request_header::ViewBrokerStatsDataRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub(super) struct BrokerStatsHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> BrokerStatsHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn view_broker_stats_data(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header::<ViewBrokerStatsDataRequestHeader>()?;

        let stats_name = request_header.stats_name.as_str();
        let stats_key = request_header.stats_key.as_str();

        let stats_item = self
            .broker_runtime_inner
            .broker_stats_manager()
            .get_stats_item(stats_name, stats_key);

        match stats_item {
            Some(item) => {
                let minute = item.get_stats_data_in_minute();
                let hour = item.get_stats_data_in_hour();
                let day = item.get_stats_data_in_day();

                let broker_stats_data = BrokerStatsData::new(
                    BrokerStatsItem::new(minute.get_sum(), minute.get_tps(), minute.get_avgpt()),
                    BrokerStatsItem::new(hour.get_sum(), hour.get_tps(), hour.get_avgpt()),
                    BrokerStatsItem::new(day.get_sum(), day.get_tps(), day.get_avgpt()),
                );

                let body = broker_stats_data.encode()?;
                response.set_body_mut_ref(body);
                Ok(Some(response))
            }
            None => Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                "No stats data for statsName={}, statsKey={}",
                stats_name, stats_key
            )))),
        }
    }
}
