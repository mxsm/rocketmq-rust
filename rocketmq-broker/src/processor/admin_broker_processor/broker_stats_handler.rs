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
use rocketmq_protocol::protocol::body::broker_stats_item::BrokerStatsItem;
use rocketmq_protocol::protocol::header::view_broker_stats_data_request_header::ViewBrokerStatsDataRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::subscription::broker_stats_data::BrokerStatsData;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_store::BrokerStatsManager;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct BrokerStatsHandler {
    broker_stats_manager: Arc<BrokerStatsManager>,
}

impl BrokerStatsHandler {
    pub fn new(broker_stats_manager: Arc<BrokerStatsManager>) -> Self {
        Self { broker_stats_manager }
    }

    pub async fn view_broker_stats_data(
        &self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
        let request_header = request.decode_command_custom_header::<ViewBrokerStatsDataRequestHeader>()?;

        let stats_name = request_header.stats_name.as_str();
        let stats_key = request_header.stats_key.as_str();

        let stats_item = self.broker_stats_manager.get_stats_item(stats_name, stats_key);

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
                Ok(Some(RemotingCommand::create_success_response_command().set_body(body)))
            }
            None => Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                "No stats data for statsName={}, statsKey={}",
                stats_name, stats_key
            )))),
        }
    }
}
