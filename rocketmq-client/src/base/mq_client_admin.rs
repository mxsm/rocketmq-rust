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

use std::collections::HashMap;

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::body::group_list::GroupList;
use rocketmq_remoting::protocol::body::queue_time_span::QueueTimeSpan;
use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
use rocketmq_remoting::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_remoting::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_remoting::protocol::header::delete_subscription_group_request_header::DeleteSubscriptionGroupRequestHeader;
use rocketmq_remoting::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_remoting::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_remoting::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
use rocketmq_remoting::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_remoting::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
use rocketmq_remoting::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_remoting::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
use rocketmq_remoting::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_remoting::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

#[trait_variant::make(MqClientAdmin: Send)]
pub trait MqClientAdminInner: Sync {
    /// Queries messages based on the provided request header.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `unique_key_flag` - Flag indicating if the unique key should be used.
    /// * `decompress_body` - Flag indicating if the message body should be decompressed.
    /// * `request_header` - The request header containing query parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing a vector of `MessageExt` or an error.
    async fn query_message(
        &self,
        address: &str,
        unique_key_flag: bool,
        decompress_body: bool,
        request_header: QueryMessageRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageExt>>;

    /// Retrieves topic statistics information.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing query parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `TopicStatsTable` or an error.
    async fn get_topic_stats_info(
        &self,
        address: &str,
        request_header: GetTopicStatsInfoRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable>;

    /// Queries the consume time span for a topic.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing query parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing a vector of `QueueTimeSpan` or an error.
    async fn query_consume_time_span(
        &self,
        address: &str,
        request_header: QueryConsumeTimeSpanRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Vec<QueueTimeSpan>>;

    /// Updates or creates a topic.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing topic parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    async fn update_or_create_topic(
        &self,
        address: &str,
        request_header: CreateTopicRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Updates or creates a subscription group.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `config` - The configuration for the subscription group.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    async fn update_or_create_subscription_group(
        &self,
        address: &str,
        config: SubscriptionGroupConfig,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Deletes a topic in the broker.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing topic parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    async fn delete_topic_in_broker(
        &self,
        address: &str,
        request_header: DeleteTopicRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Deletes a topic in the nameserver.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the nameserver.
    /// * `request_header` - The request header containing topic parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    async fn delete_topic_in_nameserver(
        &self,
        address: &str,
        request_header: DeleteTopicFromNamesrvRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Deletes a key-value configuration.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the nameserver.
    /// * `request_header` - The request header containing key-value parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    async fn delete_kv_config(
        &self,
        address: &str,
        request_header: DeleteKVConfigRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Deletes a subscription group.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing subscription group parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    async fn delete_subscription_group(
        &self,
        address: &str,
        request_header: DeleteSubscriptionGroupRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Invokes the broker to reset the offset.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing reset offset parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing a hashmap of `MessageQueue` to offset or an error.
    async fn invoke_broker_to_reset_offset(
        &self,
        address: &str,
        request_header: ResetOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HashMap<MessageQueue, i64>>;

    /// Views a message.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing view message parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `MessageExt` or an error.
    async fn view_message(
        &self,
        address: &str,
        request_header: ViewMessageRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<MessageExt>;

    /// Retrieves broker cluster information.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `ClusterInfo` or an error.
    async fn get_broker_cluster_info(
        &self,
        address: &str,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ClusterInfo>;

    /// Retrieves the consumer connection list.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing consumer connection parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `ConsumerConnection` or an error.
    async fn get_consumer_connection_list(
        &self,
        address: &str,
        request_header: GetConsumerConnectionListRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnection>;

    /// Queries topics by consumer.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing query parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `TopicList` or an error.
    async fn query_topics_by_consumer(
        &self,
        address: &str,
        request_header: QueryTopicsByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicList>;

    /// Queries subscription by consumer.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing query parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `SubscriptionData` or an error.
    async fn query_subscription_by_consumer(
        &self,
        address: &str,
        request_header: QuerySubscriptionByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<SubscriptionData>;

    /// Retrieves consume statistics.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing query parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `ConsumeStats` or an error.
    async fn get_consume_stats(
        &self,
        address: &str,
        request_header: GetConsumeStatsRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ConsumeStats>;

    /// Queries which group consumes a topic.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing query parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `GroupList` or an error.
    async fn query_topic_consume_by_who(
        &self,
        address: &str,
        request_header: QueryTopicConsumeByWhoRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<GroupList>;

    /// Retrieves consumer running information.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing query parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `ConsumerRunningInfo` or an error.
    async fn get_consumer_running_info(
        &self,
        address: &str,
        request_header: GetConsumerRunningInfoRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ConsumerRunningInfo>;

    /// Consumes a message directly.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the broker.
    /// * `request_header` - The request header containing consume message parameters.
    /// * `timeout_millis` - The timeout in milliseconds for the operation.
    ///
    /// # Returns
    ///
    /// A result containing `ConsumeMessageDirectlyResult` or an error.
    async fn consume_message_directly(
        &self,
        address: &str,
        request_header: ConsumeMessageDirectlyResultRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult>;
}
