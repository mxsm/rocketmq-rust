//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::local::LocalRequestHarness;
use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use tokio::time::timeout;

use crate::broker_runtime::BrokerRuntime;

const LOCAL_PROXY_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);

/// Compatibility types required by the extracted local Proxy adapter.
///
/// The adapter depends on this Broker-owned surface instead of taking direct
/// dependencies on Broker implementation crates and wire-protocol crates.
#[doc(hidden)]
pub mod proxy_adapter_compat {
    pub use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
    pub use rocketmq_common::common::boundary_type::BoundaryType;
    pub use rocketmq_common::common::broker::broker_config::BrokerConfig;
    pub use rocketmq_common::common::filter::expression_type::ExpressionType;
    pub use rocketmq_common::common::message::message_ext::MessageExt;
    pub use rocketmq_common::common::message::message_id::MessageId;
    pub use rocketmq_common::common::message::message_queue::MessageQueue;
    pub use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
    pub use rocketmq_common::common::message::message_single::Message;
    pub use rocketmq_common::common::message::MessageConst;
    pub use rocketmq_common::common::message::MessageTrait;
    pub use rocketmq_common::common::mix_all;
    pub use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
    pub use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
    pub use rocketmq_common::common::topic::TopicValidator;
    pub use rocketmq_common::MessageDecoder;
    pub use rocketmq_common::TimeUtils::current_millis;
    pub use rocketmq_remoting::code::request_code::RequestCode;
    pub use rocketmq_remoting::code::response_code::ResponseCode;
    pub use rocketmq_remoting::prelude::RemotingDeserializable;
    pub use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
    pub use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
    pub use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
    pub use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
    pub use rocketmq_remoting::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
    pub use rocketmq_remoting::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
    pub use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
    pub use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
    pub use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
    pub use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
    pub use rocketmq_remoting::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
    pub use rocketmq_remoting::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
    pub use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
    pub use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
    pub use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::TopicRequestHeader as OperationTopicRequestHeader;
    pub use rocketmq_remoting::protocol::header::pop_message_request_header::PopMessageRequestHeader;
    pub use rocketmq_remoting::protocol::header::pop_message_response_header::PopMessageResponseHeader;
    pub use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
    pub use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
    pub use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
    pub use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
    pub use rocketmq_remoting::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
    pub use rocketmq_remoting::protocol::header::recall_message_response_header::RecallMessageResponseHeader;
    pub use rocketmq_remoting::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
    pub use rocketmq_remoting::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
    pub use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
    pub use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
    pub use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    pub use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    pub use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
    pub use rocketmq_remoting::protocol::RemotingSerializable;
    pub use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
    pub use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
    pub use rocketmq_store::config::message_store_config::MessageStoreConfig;
}

pub struct ProxyBrokerFacade {
    runtime: BrokerRuntime,
}

impl ProxyBrokerFacade {
    pub fn new(mut broker_config: BrokerConfig, message_store_config: MessageStoreConfig) -> Self {
        broker_config.transfer_msg_by_heap = true;
        broker_config.broker_server_config.listen_port = broker_config.listen_port;
        Self {
            runtime: BrokerRuntime::new(Arc::new(broker_config), Arc::new(message_store_config)),
        }
    }

    pub async fn initialize(&mut self) -> bool {
        self.runtime.initialize().await
    }

    pub async fn start(&mut self) {
        self.runtime.start().await;
    }

    pub async fn shutdown(&mut self) {
        self.runtime.shutdown().await;
    }

    pub fn broker_config(&self) -> &BrokerConfig {
        self.runtime.broker_config()
    }

    pub fn query_route(&self, topic: &str) -> rocketmq_error::RocketMQResult<TopicRouteData> {
        let topic_name = CheetahString::from(topic);
        let topic_config =
            self.runtime
                .topic_config(&topic_name)
                .ok_or_else(|| rocketmq_error::RocketMQError::TopicNotExist {
                    topic: topic.to_owned(),
                })?;

        Ok(build_topic_route(self.runtime.broker_config(), topic_config.as_ref()))
    }

    pub fn query_topic_message_type(&self, topic: &str) -> rocketmq_error::RocketMQResult<TopicMessageType> {
        let topic_name = CheetahString::from(topic);
        let topic_config =
            self.runtime
                .topic_config(&topic_name)
                .ok_or_else(|| rocketmq_error::RocketMQError::TopicNotExist {
                    topic: topic.to_owned(),
                })?;
        Ok(topic_config.get_topic_message_type())
    }

    pub fn query_subscription_group(
        &self,
        group: &str,
    ) -> rocketmq_error::RocketMQResult<Option<Arc<SubscriptionGroupConfig>>> {
        Ok(self.runtime.subscription_group(&CheetahString::from(group)))
    }

    pub async fn process_request(
        &self,
        mut request: rocketmq_remoting::protocol::remoting_command::RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<rocketmq_remoting::protocol::remoting_command::RemotingCommand> {
        request.make_custom_header_to_net();
        let mut processor = self
            .runtime
            .proxy_request_processor()
            .ok_or_else(embedded_broker_request_processor_not_ready)?;

        let opaque = request.opaque();
        let mut harness = LocalRequestHarness::new().await?;
        if let Some(mut response) = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await?
        {
            response.make_custom_header_to_net();
            return Ok(response.set_opaque(opaque).mark_response_type());
        }

        let response = timeout(LOCAL_PROXY_RESPONSE_TIMEOUT, harness.receive_response())
            .await
            .map_err(|_| rocketmq_error::RocketMQError::Timeout {
                operation: "embedded_broker_response",
                timeout_ms: LOCAL_PROXY_RESPONSE_TIMEOUT.as_millis() as u64,
            })??;

        response.ok_or_else(|| {
            rocketmq_error::RocketMQError::response_process_failed(
                "embedded_broker_response",
                format!(
                    "embedded broker produced no response for request code {}",
                    request.code()
                ),
            )
        })
    }
}

fn embedded_broker_request_processor_not_ready() -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::not_initialized("embedded_broker_request_processor")
}

fn build_topic_route(broker_config: &BrokerConfig, topic_config: &TopicConfig) -> TopicRouteData {
    let broker_name = broker_config.broker_identity.broker_name.clone();
    let broker_addr = CheetahString::from(format!(
        "{}:{}",
        broker_config.broker_ip1, broker_config.broker_server_config.listen_port
    ));
    let mut broker_addrs = HashMap::new();
    broker_addrs.insert(mix_all::MASTER_ID, broker_addr);

    TopicRouteData {
        queue_datas: vec![QueueData::new(
            broker_name.clone(),
            topic_config.read_queue_nums,
            topic_config.write_queue_nums,
            topic_config.perm,
            topic_config.topic_sys_flag,
        )],
        broker_datas: vec![BrokerData::new(
            broker_config.broker_identity.broker_cluster_name.clone(),
            broker_name,
            broker_addrs,
            None,
        )],
        ..TopicRouteData::default()
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::ErrorKind;

    use super::*;

    #[test]
    fn embedded_broker_request_processor_not_ready_uses_not_initialized_kind() {
        let error = embedded_broker_request_processor_not_ready();

        assert_eq!(error.kind(), ErrorKind::NotInitialized);
    }
}
