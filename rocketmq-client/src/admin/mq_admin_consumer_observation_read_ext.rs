// Copyright 2026 The RocketMQ Rust Authors
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

//! Exact-Broker, read-only Consumer observation capability.

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;

use super::default_mq_admin_ext::DefaultMQAdminExt;
use super::mq_admin_read_ext::MQAdminReadExt;
use super::mq_admin_read_ext::SubscriptionGroupConfigVersioned;

#[derive(Debug, Clone)]
pub enum ConsumerGroupConfigRead {
    Present(Box<SubscriptionGroupConfigVersioned>),
    Absent,
}

#[derive(Debug, Clone)]
pub enum ConsumerConnectionRead {
    Online(ConsumerConnection),
    Offline,
}

#[derive(Debug)]
pub enum ConsumerProgressRead {
    Observed(ConsumeStats),
    Absent,
}

#[allow(async_fn_in_trait)]
pub trait MQAdminConsumerObservationReadExt: Send {
    async fn consumer_group_config_at(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumerGroupConfigRead>;

    async fn consumer_connection_at(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnectionRead>;

    async fn consumer_progress_at(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumerProgressRead>;
}

impl MQAdminConsumerObservationReadExt for DefaultMQAdminExt {
    async fn consumer_group_config_at(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumerGroupConfigRead> {
        match MQAdminReadExt::subscription_group_config_with_version(self, broker_addr, consumer_group).await {
            Ok(config) => Ok(ConsumerGroupConfigRead::Present(Box::new(config))),
            Err(error) if broker_response_is(&error, ResponseCode::SubscriptionGroupNotExist) => {
                Ok(ConsumerGroupConfigRead::Absent)
            }
            Err(error) => Err(error),
        }
    }

    async fn consumer_connection_at(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnectionRead> {
        match MQAdminReadExt::observe_consumer_connection_at(self, consumer_group, broker_addr).await {
            Ok(connection) if connection.get_connection_set().is_empty() => Ok(ConsumerConnectionRead::Offline),
            Ok(connection) => Ok(ConsumerConnectionRead::Online(connection)),
            Err(error) if broker_response_is(&error, ResponseCode::ConsumerNotOnline) => {
                Ok(ConsumerConnectionRead::Offline)
            }
            Err(error) => Err(error),
        }
    }

    async fn consumer_progress_at(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumerProgressRead> {
        match MQAdminReadExt::examine_consume_stats(self, consumer_group, None, None, Some(broker_addr), None).await {
            Ok(stats) => Ok(ConsumerProgressRead::Observed(stats)),
            Err(error) if broker_response_is(&error, ResponseCode::SubscriptionGroupNotExist) => {
                Ok(ConsumerProgressRead::Absent)
            }
            Err(error) => Err(error),
        }
    }
}

fn broker_response_is(error: &RocketMQError, expected: ResponseCode) -> bool {
    matches!(
        error,
        RocketMQError::BrokerOperationFailed { code, .. } if *code == expected.to_i32()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_exact_broker_response_codes_are_closed_states() {
        let missing = RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            ResponseCode::SubscriptionGroupNotExist.to_i32(),
            "must not be inspected",
        );
        let offline = RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            ResponseCode::ConsumerNotOnline.to_i32(),
            "must not be inspected",
        );
        let unavailable = RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            ResponseCode::SystemError.to_i32(),
            "consumer not online",
        );

        assert!(broker_response_is(&missing, ResponseCode::SubscriptionGroupNotExist));
        assert!(broker_response_is(&offline, ResponseCode::ConsumerNotOnline));
        assert!(!broker_response_is(&unavailable, ResponseCode::ConsumerNotOnline));
    }
}
