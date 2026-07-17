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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use cheetah_string::CheetahString;
use rocketmq_client_rust::base::client_config::ClientConfig as RocketmqClientConfig;
use rocketmq_client_rust::consumer::ack_result::AckResult;
use rocketmq_client_rust::consumer::ack_status::AckStatus;
use rocketmq_client_rust::consumer::pop_result::PopResult;
use rocketmq_client_rust::consumer::pop_status::PopStatus;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::proxy_adapter_compat::client_config_for_managed_domain;
use rocketmq_client_rust::proxy_adapter_compat::current_millis;
use rocketmq_client_rust::proxy_adapter_compat::rpc_hook_from_outbound_signer;
use rocketmq_client_rust::proxy_adapter_compat::BoundaryType;
use rocketmq_client_rust::proxy_adapter_compat::BrokerDataExt;
use rocketmq_client_rust::proxy_adapter_compat::ClientInstanceHandle;
use rocketmq_client_rust::proxy_adapter_compat::ClientRpcHook;
use rocketmq_client_rust::proxy_adapter_compat::ExpressionType;
use rocketmq_client_rust::proxy_adapter_compat::Message;
use rocketmq_client_rust::proxy_adapter_compat::MessageConst;
use rocketmq_client_rust::proxy_adapter_compat::MessageDecoder;
use rocketmq_client_rust::proxy_adapter_compat::MessageExt;
use rocketmq_client_rust::proxy_adapter_compat::MessageId;
use rocketmq_client_rust::proxy_adapter_compat::MessageQueue;
use rocketmq_client_rust::proxy_adapter_compat::MessageQueueAssignment;
use rocketmq_client_rust::proxy_adapter_compat::MessageSysFlag;
use rocketmq_client_rust::proxy_adapter_compat::MessageTrait;
use rocketmq_client_rust::proxy_adapter_compat::PullSysFlag;
use rocketmq_client_rust::proxy_adapter_compat::TopicMessageType;
use rocketmq_client_rust::proxy_adapter_compat::LOGICAL_QUEUE_MOCK_BROKER_PREFIX;
use rocketmq_client_rust::proxy_adapter_compat::MASTER_ID;
use rocketmq_error::RocketMQError;
use rocketmq_model::result::PullStatus;
use rocketmq_model::result::SendResult;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_protocol::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_protocol::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_protocol::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_protocol::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::TopicRequestHeader as PopTopicRequestHeader;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_protocol::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_proxy_core::proto::v2;
use rocketmq_proxy_core::status::ProxyStatusMapper;
use rocketmq_proxy_core::AckMessageEntry;
use rocketmq_proxy_core::AckMessageRequest;
use rocketmq_proxy_core::AckMessageResultEntry;
use rocketmq_proxy_core::ChangeInvisibleDurationPlan;
use rocketmq_proxy_core::ChangeInvisibleDurationRequest;
use rocketmq_proxy_core::EndTransactionPlan;
use rocketmq_proxy_core::EndTransactionRequest;
use rocketmq_proxy_core::ForwardMessageToDeadLetterQueuePlan;
use rocketmq_proxy_core::ForwardMessageToDeadLetterQueueRequest;
use rocketmq_proxy_core::GetOffsetPlan;
use rocketmq_proxy_core::GetOffsetRequest;
use rocketmq_proxy_core::MessageQueueTarget;
use rocketmq_proxy_core::ProxyContext;
use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyPayloadStatus;
use rocketmq_proxy_core::ProxyResult;
use rocketmq_proxy_core::ProxyTopicMessageType;
use rocketmq_proxy_core::PullMessagePlan;
use rocketmq_proxy_core::PullMessageRequest;
use rocketmq_proxy_core::QueryOffsetPlan;
use rocketmq_proxy_core::QueryOffsetPolicy;
use rocketmq_proxy_core::QueryOffsetRequest;
use rocketmq_proxy_core::RecallMessagePlan;
use rocketmq_proxy_core::RecallMessageRequest;
use rocketmq_proxy_core::ReceiveMessagePlan;
use rocketmq_proxy_core::ReceiveMessageRequest;
use rocketmq_proxy_core::ReceivedMessage;
use rocketmq_proxy_core::ResourceIdentity;
use rocketmq_proxy_core::SendMessageEntry;
use rocketmq_proxy_core::SendMessageRequest;
use rocketmq_proxy_core::SendMessageResultEntry;
use rocketmq_proxy_core::SubscriptionGroupMetadata;
use rocketmq_proxy_core::TransactionResolution;
use rocketmq_proxy_core::TransactionSource;
use rocketmq_proxy_core::UpdateOffsetPlan;
use rocketmq_proxy_core::UpdateOffsetRequest;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_security_api::OutboundSigner;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::config::ClusterConfig;
use crate::message::message_ext_to_core;
use crate::message::message_from_core;

const CLUSTER_COMMAND_CAPACITY: usize = 1024;

#[async_trait]
pub trait ClusterClient: Send + Sync {
    fn transaction_producer_group(&self, _context: &ProxyContext) -> Option<String> {
        None
    }

    async fn query_route(&self, topic: &ResourceIdentity) -> ProxyResult<TopicRouteData>;

    async fn query_assignment(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        client_id: &str,
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>>;

    async fn query_topic_message_type(&self, topic: &ResourceIdentity) -> ProxyResult<ProxyTopicMessageType>;

    async fn query_subscription_group(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>>;

    async fn query_user(&self, _username: &str) -> ProxyResult<Option<UserInfo>> {
        Ok(None)
    }

    async fn query_acl(&self, _subject: &str) -> ProxyResult<Option<AclInfo>> {
        Ok(None)
    }

    async fn send_message(
        &self,
        context: &ProxyContext,
        request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>>;

    async fn recall_message(
        &self,
        context: &ProxyContext,
        request: &RecallMessageRequest,
    ) -> ProxyResult<RecallMessagePlan>;

    async fn receive_message(
        &self,
        context: &ProxyContext,
        request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan>;

    async fn pull_message(&self, context: &ProxyContext, request: &PullMessageRequest) -> ProxyResult<PullMessagePlan>;

    async fn ack_message(
        &self,
        context: &ProxyContext,
        request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>>;

    async fn forward_message_to_dead_letter_queue(
        &self,
        context: &ProxyContext,
        request: &ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan>;

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan>;

    async fn update_offset(
        &self,
        context: &ProxyContext,
        request: &UpdateOffsetRequest,
    ) -> ProxyResult<UpdateOffsetPlan>;

    async fn get_offset(&self, context: &ProxyContext, request: &GetOffsetRequest) -> ProxyResult<GetOffsetPlan>;

    async fn query_offset(&self, context: &ProxyContext, request: &QueryOffsetRequest) -> ProxyResult<QueryOffsetPlan>;

    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan>;
}

#[async_trait]
trait ClusterClientIo: Send + Sync {
    async fn start(&self) -> Result<(), RocketMQError>;

    async fn shutdown(&self);

    async fn topic_route(&self, topic: &str, timeout_millis: u64) -> Result<Option<TopicRouteData>, RocketMQError>;

    async fn lock_batch_mq(
        &self,
        broker_addr: &str,
        request: LockBatchRequestBody,
        timeout_millis: u64,
    ) -> Result<std::collections::HashSet<MessageQueue>, RocketMQError>;

    async fn unlock_batch_mq(
        &self,
        broker_addr: &CheetahString,
        request: UnlockBatchRequestBody,
        timeout_millis: u64,
    ) -> Result<(), RocketMQError>;

    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the RocketMQ query-assignment wire contract"
    )]
    async fn query_assignment(
        &self,
        broker_addr: &CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        strategy_name: CheetahString,
        message_model: MessageModel,
        timeout_millis: u64,
    ) -> Result<Option<Vec<MessageQueueAssignment>>, RocketMQError>;

    async fn pop_message(
        &self,
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
        request: PopMessageRequestHeader,
        timeout_millis: u64,
    ) -> Result<PopResult, RocketMQError>;

    async fn ack_message(
        &self,
        broker_addr: &CheetahString,
        request: AckMessageRequestHeader,
        timeout_millis: u64,
    ) -> Result<AckResult, RocketMQError>;

    async fn change_invisible_time(
        &self,
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
        request: ChangeInvisibleTimeRequestHeader,
        timeout_millis: u64,
    ) -> Result<AckResult, RocketMQError>;

    async fn end_transaction(
        &self,
        broker_addr: &CheetahString,
        request: EndTransactionRequestHeader,
        remark: CheetahString,
        timeout_millis: u64,
    ) -> Result<(), RocketMQError>;

    async fn find_subscribe_broker_addr(
        &self,
        broker_name: &CheetahString,
        broker_id: u64,
        only_this_broker: bool,
    ) -> Option<CheetahString>;

    async fn refresh_topic_route(&self, topic: &CheetahString) -> bool;

    async fn broker_name_for_queue(&self, queue: &MessageQueue) -> CheetahString;

    async fn pull_outcome_from_broker(
        &self,
        broker_addr: &str,
        request: PullMessageRequestHeader,
        timeout_millis: u64,
    ) -> Result<rocketmq_model::result::PullOutcome<MessageExt>, RocketMQError>;

    #[allow(clippy::too_many_arguments, reason = "mirrors the RocketMQ send-back wire contract")]
    async fn consumer_send_message_back(
        &self,
        broker_addr: &str,
        broker_name: Option<&str>,
        message: &MessageExt,
        consumer_group: &str,
        delay_level: i32,
        timeout_millis: u64,
        max_consume_retry_times: i32,
    ) -> Result<(), RocketMQError>;

    async fn update_consumer_offset(
        &self,
        broker_addr: &CheetahString,
        request: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> Result<(), RocketMQError>;

    async fn query_consumer_offset(
        &self,
        broker_addr: &str,
        request: QueryConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> Result<i64, RocketMQError>;

    async fn min_offset(
        &self,
        broker_addr: &str,
        queue: &MessageQueue,
        timeout_millis: u64,
    ) -> Result<i64, RocketMQError>;

    async fn max_offset(
        &self,
        broker_addr: &str,
        queue: &MessageQueue,
        timeout_millis: u64,
    ) -> Result<i64, RocketMQError>;

    async fn search_offset(
        &self,
        broker_addr: &str,
        queue: &MessageQueue,
        timestamp: i64,
        boundary_type: BoundaryType,
        timeout_millis: u64,
    ) -> Result<i64, RocketMQError>;

    async fn topic_config(
        &self,
        broker_addr: &CheetahString,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> Result<rocketmq_model::topic::TopicConfig, RocketMQError>;

    async fn subscription_group_config(
        &self,
        broker_addr: &CheetahString,
        group: CheetahString,
        timeout_millis: u64,
    ) -> Result<SubscriptionGroupConfig, RocketMQError>;

    async fn broker_cluster_info(&self, timeout_millis: u64) -> Result<ClusterInfo, RocketMQError>;

    async fn user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        timeout_millis: u64,
    ) -> Result<Option<UserInfo>, RocketMQError>;

    async fn acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        timeout_millis: u64,
    ) -> Result<Option<AclInfo>, RocketMQError>;
}

trait ClusterClientFactory: Send + Sync {
    fn get_or_create(
        &self,
        domain_id: u64,
        client_config: RocketmqClientConfig,
        rpc_hook: Option<Arc<ClientRpcHook>>,
    ) -> Result<Arc<dyn ClusterClientIo>, RocketMQError>;
}

struct DefaultClusterClientFactory;

impl ClusterClientFactory for DefaultClusterClientFactory {
    fn get_or_create(
        &self,
        domain_id: u64,
        client_config: RocketmqClientConfig,
        rpc_hook: Option<Arc<ClientRpcHook>>,
    ) -> Result<Arc<dyn ClusterClientIo>, RocketMQError> {
        ClientInstanceHandle::get_or_create(domain_id, client_config, rpc_hook)
            .map(|client| Arc::new(client) as Arc<dyn ClusterClientIo>)
    }
}

#[cfg(test)]
struct StaticClusterClientFactory {
    client: Arc<dyn ClusterClientIo>,
}

#[cfg(test)]
impl ClusterClientFactory for StaticClusterClientFactory {
    fn get_or_create(
        &self,
        _domain_id: u64,
        _client_config: RocketmqClientConfig,
        _rpc_hook: Option<Arc<ClientRpcHook>>,
    ) -> Result<Arc<dyn ClusterClientIo>, RocketMQError> {
        Ok(self.client.clone())
    }
}

#[async_trait]
impl ClusterClientIo for ClientInstanceHandle {
    async fn start(&self) -> Result<(), RocketMQError> {
        ClientInstanceHandle::start(self).await
    }

    async fn shutdown(&self) {
        self.shutdown_owned().await;
    }

    async fn topic_route(&self, topic: &str, timeout_millis: u64) -> Result<Option<TopicRouteData>, RocketMQError> {
        self.topic_route(topic, timeout_millis).await
    }

    async fn lock_batch_mq(
        &self,
        broker_addr: &str,
        request: LockBatchRequestBody,
        timeout_millis: u64,
    ) -> Result<std::collections::HashSet<MessageQueue>, RocketMQError> {
        self.lock_batch_mq(broker_addr, request, timeout_millis).await
    }

    async fn unlock_batch_mq(
        &self,
        broker_addr: &CheetahString,
        request: UnlockBatchRequestBody,
        timeout_millis: u64,
    ) -> Result<(), RocketMQError> {
        self.unlock_batch_mq(broker_addr, request, timeout_millis).await
    }

    async fn query_assignment(
        &self,
        broker_addr: &CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        strategy_name: CheetahString,
        message_model: MessageModel,
        timeout_millis: u64,
    ) -> Result<Option<Vec<MessageQueueAssignment>>, RocketMQError> {
        self.query_assignment(
            broker_addr,
            topic,
            consumer_group,
            client_id,
            strategy_name,
            message_model,
            timeout_millis,
        )
        .await
    }

    async fn pop_message(
        &self,
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
        request: PopMessageRequestHeader,
        timeout_millis: u64,
    ) -> Result<PopResult, RocketMQError> {
        self.pop_message(broker_name, broker_addr, request, timeout_millis)
            .await
    }

    async fn ack_message(
        &self,
        broker_addr: &CheetahString,
        request: AckMessageRequestHeader,
        timeout_millis: u64,
    ) -> Result<AckResult, RocketMQError> {
        self.ack_message(broker_addr, request, timeout_millis).await
    }

    async fn change_invisible_time(
        &self,
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
        request: ChangeInvisibleTimeRequestHeader,
        timeout_millis: u64,
    ) -> Result<AckResult, RocketMQError> {
        self.change_invisible_time(broker_name, broker_addr, request, timeout_millis)
            .await
    }

    async fn end_transaction(
        &self,
        broker_addr: &CheetahString,
        request: EndTransactionRequestHeader,
        remark: CheetahString,
        timeout_millis: u64,
    ) -> Result<(), RocketMQError> {
        self.end_transaction(broker_addr, request, remark, timeout_millis).await
    }

    async fn find_subscribe_broker_addr(
        &self,
        broker_name: &CheetahString,
        broker_id: u64,
        only_this_broker: bool,
    ) -> Option<CheetahString> {
        self.find_subscribe_broker_addr(broker_name, broker_id, only_this_broker)
            .await
    }

    async fn refresh_topic_route(&self, topic: &CheetahString) -> bool {
        self.refresh_topic_route(topic).await
    }

    async fn broker_name_for_queue(&self, queue: &MessageQueue) -> CheetahString {
        self.broker_name_for_queue(queue).await
    }

    async fn pull_outcome_from_broker(
        &self,
        broker_addr: &str,
        request: PullMessageRequestHeader,
        timeout_millis: u64,
    ) -> Result<rocketmq_model::result::PullOutcome<MessageExt>, RocketMQError> {
        self.pull_outcome_from_broker(broker_addr, request, timeout_millis)
            .await
    }

    async fn consumer_send_message_back(
        &self,
        broker_addr: &str,
        broker_name: Option<&str>,
        message: &MessageExt,
        consumer_group: &str,
        delay_level: i32,
        timeout_millis: u64,
        max_consume_retry_times: i32,
    ) -> Result<(), RocketMQError> {
        self.consumer_send_message_back(
            broker_addr,
            broker_name,
            message,
            consumer_group,
            delay_level,
            timeout_millis,
            max_consume_retry_times,
        )
        .await
    }

    async fn update_consumer_offset(
        &self,
        broker_addr: &CheetahString,
        request: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> Result<(), RocketMQError> {
        self.update_consumer_offset(broker_addr, request, timeout_millis).await
    }

    async fn query_consumer_offset(
        &self,
        broker_addr: &str,
        request: QueryConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> Result<i64, RocketMQError> {
        self.query_consumer_offset(broker_addr, request, timeout_millis).await
    }

    async fn min_offset(
        &self,
        broker_addr: &str,
        queue: &MessageQueue,
        timeout_millis: u64,
    ) -> Result<i64, RocketMQError> {
        self.min_offset(broker_addr, queue, timeout_millis).await
    }

    async fn max_offset(
        &self,
        broker_addr: &str,
        queue: &MessageQueue,
        timeout_millis: u64,
    ) -> Result<i64, RocketMQError> {
        self.max_offset(broker_addr, queue, timeout_millis).await
    }

    async fn search_offset(
        &self,
        broker_addr: &str,
        queue: &MessageQueue,
        timestamp: i64,
        boundary_type: BoundaryType,
        timeout_millis: u64,
    ) -> Result<i64, RocketMQError> {
        self.search_offset(broker_addr, queue, timestamp, boundary_type, timeout_millis)
            .await
    }

    async fn topic_config(
        &self,
        broker_addr: &CheetahString,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> Result<rocketmq_model::topic::TopicConfig, RocketMQError> {
        self.topic_config(broker_addr, topic, timeout_millis).await
    }

    async fn subscription_group_config(
        &self,
        broker_addr: &CheetahString,
        group: CheetahString,
        timeout_millis: u64,
    ) -> Result<SubscriptionGroupConfig, RocketMQError> {
        self.subscription_group_config(broker_addr, group, timeout_millis).await
    }

    async fn broker_cluster_info(&self, timeout_millis: u64) -> Result<ClusterInfo, RocketMQError> {
        self.broker_cluster_info(timeout_millis).await
    }

    async fn user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        timeout_millis: u64,
    ) -> Result<Option<UserInfo>, RocketMQError> {
        self.user(broker_addr, username, timeout_millis).await
    }

    async fn acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        timeout_millis: u64,
    ) -> Result<Option<AclInfo>, RocketMQError> {
        self.acl(broker_addr, subject, timeout_millis).await
    }
}

#[async_trait]
trait ClusterProducerIo: Send {
    fn set_topics(&mut self, topics: Vec<CheetahString>);

    fn topics(&self) -> Vec<CheetahString>;

    fn set_send_timeout(&mut self, timeout_millis: u32);

    fn producer_group(&self) -> &str;

    async fn start(&mut self) -> Result<(), RocketMQError>;

    async fn shutdown(&mut self);

    async fn recall_message(
        &mut self,
        topic: CheetahString,
        recall_handle: CheetahString,
    ) -> Result<String, RocketMQError>;

    async fn fetch_publish_message_queues(&mut self, topic: &str) -> Result<Vec<MessageQueue>, RocketMQError>;

    async fn send(&mut self, message: Message, timeout_millis: u64) -> Result<Option<SendResult>, RocketMQError>;

    async fn send_to_queue(
        &mut self,
        message: Message,
        queue: MessageQueue,
        timeout_millis: u64,
    ) -> Result<Option<SendResult>, RocketMQError>;
}

#[async_trait]
impl ClusterProducerIo for DefaultMQProducer {
    fn set_topics(&mut self, topics: Vec<CheetahString>) {
        self.set_topics(topics);
    }

    fn topics(&self) -> Vec<CheetahString> {
        self.topics().clone()
    }

    fn set_send_timeout(&mut self, timeout_millis: u32) {
        self.set_send_msg_timeout(timeout_millis);
    }

    fn producer_group(&self) -> &str {
        self.producer_group()
    }

    async fn start(&mut self) -> Result<(), RocketMQError> {
        self.start().await
    }

    async fn shutdown(&mut self) {
        self.shutdown_for_shared_factory().await;
    }

    async fn recall_message(
        &mut self,
        topic: CheetahString,
        recall_handle: CheetahString,
    ) -> Result<String, RocketMQError> {
        self.recall_message(topic, recall_handle).await
    }

    async fn fetch_publish_message_queues(&mut self, topic: &str) -> Result<Vec<MessageQueue>, RocketMQError> {
        self.fetch_publish_message_queues(topic).await
    }

    async fn send(&mut self, message: Message, timeout_millis: u64) -> Result<Option<SendResult>, RocketMQError> {
        self.send_with_timeout(message, timeout_millis).await
    }

    async fn send_to_queue(
        &mut self,
        message: Message,
        queue: MessageQueue,
        timeout_millis: u64,
    ) -> Result<Option<SendResult>, RocketMQError> {
        self.send_to_queue_with_timeout(message, queue, timeout_millis).await
    }
}

trait ClusterProducerFactory: Send + Sync {
    fn create(
        &self,
        domain_id: u64,
        config: &ClusterConfig,
        producer_group: &str,
        timeout_millis: u64,
        rpc_hook: Option<Arc<ClientRpcHook>>,
    ) -> Box<dyn ClusterProducerIo>;
}

struct DefaultClusterProducerFactory;

impl ClusterProducerFactory for DefaultClusterProducerFactory {
    fn create(
        &self,
        domain_id: u64,
        config: &ClusterConfig,
        producer_group: &str,
        timeout_millis: u64,
        rpc_hook: Option<Arc<ClientRpcHook>>,
    ) -> Box<dyn ClusterProducerIo> {
        Box::new(build_send_producer(
            domain_id,
            config,
            producer_group,
            timeout_millis,
            rpc_hook,
        ))
    }
}

pub struct RocketmqClusterClient {
    executor: ClusterTaskExecutor,
    producer_group_prefix: String,
}

impl RocketmqClusterClient {
    pub fn new(config: ClusterConfig) -> Self {
        Self::with_outbound_signer(config, None)
    }

    pub fn with_outbound_signer(config: ClusterConfig, signer: Option<Arc<dyn OutboundSigner>>) -> Self {
        let executor = ClusterTaskExecutor::new(config.clone(), signer, None);
        Self {
            executor,
            producer_group_prefix: config.producer_group_prefix,
        }
    }

    /// Compatibility constructor for the legacy Proxy Client hook path.
    #[doc(hidden)]
    pub fn with_rpc_hook(config: ClusterConfig, rpc_hook: Option<Arc<ClientRpcHook>>) -> Self {
        let executor = ClusterTaskExecutor::new_with_rpc_hook(config.clone(), rpc_hook, None);
        Self {
            executor,
            producer_group_prefix: config.producer_group_prefix,
        }
    }

    pub fn with_service_context(
        config: ClusterConfig,
        signer: Option<Arc<dyn OutboundSigner>>,
        service_context: &ServiceContext,
    ) -> Self {
        let executor = ClusterTaskExecutor::new(config.clone(), signer, Some(service_context));
        Self {
            executor,
            producer_group_prefix: config.producer_group_prefix,
        }
    }

    pub(crate) async fn lock_batch_mq(&self, request: LockBatchRequestBody) -> ProxyResult<LockBatchResponseBody> {
        self.executor.lock_batch_mq(request).await
    }

    pub(crate) async fn unlock_batch_mq(&self, request: UnlockBatchRequestBody) -> ProxyResult<()> {
        self.executor.unlock_batch_mq(request).await
    }
}

impl Default for RocketmqClusterClient {
    fn default() -> Self {
        Self::new(ClusterConfig::default())
    }
}

#[async_trait]
impl ClusterClient for RocketmqClusterClient {
    fn transaction_producer_group(&self, context: &ProxyContext) -> Option<String> {
        let prefix = sanitize_group_component(self.producer_group_prefix.as_str());
        let identity = sanitize_group_component(context.client_id().unwrap_or(context.request_id()));
        Some(format!("{prefix}-{identity}"))
    }

    async fn query_route(&self, topic: &ResourceIdentity) -> ProxyResult<TopicRouteData> {
        self.executor.query_route(topic.clone()).await
    }

    async fn query_assignment(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        client_id: &str,
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        self.executor
            .query_assignment(topic.clone(), group.clone(), client_id.to_owned())
            .await
    }

    async fn query_topic_message_type(&self, topic: &ResourceIdentity) -> ProxyResult<ProxyTopicMessageType> {
        self.executor.query_topic_message_type(topic.clone()).await
    }

    async fn query_subscription_group(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        self.executor
            .query_subscription_group(topic.clone(), group.clone())
            .await
    }

    async fn query_user(&self, username: &str) -> ProxyResult<Option<UserInfo>> {
        self.executor.query_user(username.to_owned()).await
    }

    async fn query_acl(&self, subject: &str) -> ProxyResult<Option<AclInfo>> {
        self.executor.query_acl(subject.to_owned()).await
    }

    async fn send_message(
        &self,
        context: &ProxyContext,
        request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        self.executor
            .send_message(
                request.clone(),
                context.client_id().map(ToOwned::to_owned),
                context.request_id().to_owned(),
            )
            .await
    }

    async fn recall_message(
        &self,
        context: &ProxyContext,
        request: &RecallMessageRequest,
    ) -> ProxyResult<RecallMessagePlan> {
        self.executor
            .recall_message(
                request.clone(),
                context.client_id().map(ToOwned::to_owned),
                context.request_id().to_owned(),
            )
            .await
    }

    async fn receive_message(
        &self,
        context: &ProxyContext,
        request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan> {
        self.executor.receive_message(request.clone(), context.deadline()).await
    }

    async fn pull_message(&self, context: &ProxyContext, request: &PullMessageRequest) -> ProxyResult<PullMessagePlan> {
        self.executor.pull_message(request.clone(), context.deadline()).await
    }

    async fn ack_message(
        &self,
        context: &ProxyContext,
        request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        self.executor.ack_message(request.clone(), context.deadline()).await
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        context: &ProxyContext,
        request: &ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
        self.executor
            .forward_message_to_dead_letter_queue(request.clone(), context.deadline())
            .await
    }

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        self.executor
            .change_invisible_duration(request.clone(), context.deadline())
            .await
    }

    async fn update_offset(
        &self,
        context: &ProxyContext,
        request: &UpdateOffsetRequest,
    ) -> ProxyResult<UpdateOffsetPlan> {
        self.executor.update_offset(request.clone(), context.deadline()).await
    }

    async fn get_offset(&self, context: &ProxyContext, request: &GetOffsetRequest) -> ProxyResult<GetOffsetPlan> {
        self.executor.get_offset(request.clone(), context.deadline()).await
    }

    async fn query_offset(&self, context: &ProxyContext, request: &QueryOffsetRequest) -> ProxyResult<QueryOffsetPlan> {
        self.executor.query_offset(request.clone(), context.deadline()).await
    }

    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan> {
        self.executor
            .end_transaction(
                request.clone(),
                context.client_id().map(ToOwned::to_owned),
                context.request_id().to_owned(),
                context.deadline(),
            )
            .await
    }
}

#[derive(Clone)]
struct ClusterTaskExecutor {
    sender: mpsc::Sender<ClusterCommand>,
    startup_error: Option<Arc<str>>,
}

enum ClusterCommand {
    QueryRoute {
        topic: ResourceIdentity,
        reply: oneshot::Sender<ProxyResult<TopicRouteData>>,
    },
    QueryAssignment {
        topic: ResourceIdentity,
        group: ResourceIdentity,
        client_id: String,
        reply: oneshot::Sender<ProxyResult<Option<Vec<MessageQueueAssignment>>>>,
    },
    QueryTopicMessageType {
        topic: ResourceIdentity,
        reply: oneshot::Sender<ProxyResult<ProxyTopicMessageType>>,
    },
    QuerySubscriptionGroup {
        topic: ResourceIdentity,
        group: ResourceIdentity,
        reply: oneshot::Sender<ProxyResult<Option<SubscriptionGroupMetadata>>>,
    },
    QueryUser {
        username: String,
        reply: oneshot::Sender<ProxyResult<Option<UserInfo>>>,
    },
    QueryAcl {
        subject: String,
        reply: oneshot::Sender<ProxyResult<Option<AclInfo>>>,
    },
    SendMessage {
        request: SendMessageRequest,
        client_id: Option<String>,
        request_id: String,
        reply: oneshot::Sender<ProxyResult<Vec<SendMessageResultEntry>>>,
    },
    RecallMessage {
        request: RecallMessageRequest,
        client_id: Option<String>,
        request_id: String,
        reply: oneshot::Sender<ProxyResult<RecallMessagePlan>>,
    },
    ReceiveMessage {
        request: ReceiveMessageRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<ReceiveMessagePlan>>,
    },
    PullMessage {
        request: PullMessageRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<PullMessagePlan>>,
    },
    AckMessage {
        request: AckMessageRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<Vec<AckMessageResultEntry>>>,
    },
    ForwardMessageToDeadLetterQueue {
        request: ForwardMessageToDeadLetterQueueRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<ForwardMessageToDeadLetterQueuePlan>>,
    },
    ChangeInvisibleDuration {
        request: ChangeInvisibleDurationRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<ChangeInvisibleDurationPlan>>,
    },
    UpdateOffset {
        request: UpdateOffsetRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<UpdateOffsetPlan>>,
    },
    GetOffset {
        request: GetOffsetRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<GetOffsetPlan>>,
    },
    QueryOffset {
        request: QueryOffsetRequest,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<QueryOffsetPlan>>,
    },
    EndTransaction {
        request: EndTransactionRequest,
        client_id: Option<String>,
        request_id: String,
        deadline: Option<Duration>,
        reply: oneshot::Sender<ProxyResult<EndTransactionPlan>>,
    },
    LockBatchMq {
        request: LockBatchRequestBody,
        reply: oneshot::Sender<ProxyResult<LockBatchResponseBody>>,
    },
    UnlockBatchMq {
        request: UnlockBatchRequestBody,
        reply: oneshot::Sender<ProxyResult<()>>,
    },
}

#[derive(Clone)]
struct CachedValue<T> {
    value: T,
    expires_at: Instant,
}

impl<T> CachedValue<T> {
    fn new(value: T, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

struct ClusterWorkerState {
    domain_id: u64,
    rpc_hook: Option<Arc<ClientRpcHook>>,
    client: Option<Arc<dyn ClusterClientIo>>,
    client_factory: Arc<dyn ClusterClientFactory>,
    producer_factory: Arc<dyn ClusterProducerFactory>,
    send_producers: HashMap<String, Box<dyn ClusterProducerIo>>,
    route_cache: HashMap<String, CachedValue<TopicRouteData>>,
    topic_message_type_cache: HashMap<String, CachedValue<ProxyTopicMessageType>>,
    subscription_group_cache: HashMap<(String, String), CachedValue<Option<SubscriptionGroupMetadata>>>,
    user_cache: HashMap<String, CachedValue<Option<UserInfo>>>,
    acl_cache: HashMap<String, CachedValue<Option<AclInfo>>>,
}

impl ClusterWorkerState {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_rpc_hook(0, None)
    }

    fn with_rpc_hook(domain_id: u64, rpc_hook: Option<Arc<ClientRpcHook>>) -> Self {
        Self {
            domain_id,
            rpc_hook,
            client: None,
            client_factory: Arc::new(DefaultClusterClientFactory),
            producer_factory: Arc::new(DefaultClusterProducerFactory),
            send_producers: HashMap::new(),
            route_cache: HashMap::new(),
            topic_message_type_cache: HashMap::new(),
            subscription_group_cache: HashMap::new(),
            user_cache: HashMap::new(),
            acl_cache: HashMap::new(),
        }
    }

    #[cfg(test)]
    fn with_test_runtime(client: Arc<dyn ClusterClientIo>, producer_factory: Arc<dyn ClusterProducerFactory>) -> Self {
        Self::with_test_factories(
            0,
            None,
            Arc::new(StaticClusterClientFactory { client }),
            producer_factory,
        )
    }

    #[cfg(test)]
    fn with_test_factories(
        domain_id: u64,
        rpc_hook: Option<Arc<ClientRpcHook>>,
        client_factory: Arc<dyn ClusterClientFactory>,
        producer_factory: Arc<dyn ClusterProducerFactory>,
    ) -> Self {
        Self {
            domain_id,
            rpc_hook,
            client: None,
            client_factory,
            producer_factory,
            send_producers: HashMap::new(),
            route_cache: HashMap::new(),
            topic_message_type_cache: HashMap::new(),
            subscription_group_cache: HashMap::new(),
            user_cache: HashMap::new(),
            acl_cache: HashMap::new(),
        }
    }

    fn rpc_hook(&self) -> Option<Arc<ClientRpcHook>> {
        self.rpc_hook.clone()
    }

    async fn client(&mut self, config: &ClusterConfig) -> ProxyResult<Arc<dyn ClusterClientIo>> {
        if self.client.is_none() {
            let client_config = cluster_client_config(config);
            let client = self
                .client_factory
                .get_or_create(self.domain_id, client_config, self.rpc_hook())?;
            self.client = Some(client.clone());
            let start_result = self
                .client
                .as_ref()
                .ok_or_else(|| ProxyError::Transport {
                    message: "proxy cluster client ownership was lost during initialization".to_owned(),
                })?
                .start()
                .await;
            if let Err(error) = start_result {
                if let Some(client) = self.client.as_ref() {
                    client.shutdown().await;
                }
                self.client.take();
                return Err(error.into());
            }
        }
        self.client.as_ref().cloned().ok_or_else(|| ProxyError::Transport {
            message: "proxy cluster client was not initialized".to_owned(),
        })
    }

    fn cached_route(&mut self, topic: &str) -> Option<TopicRouteData> {
        cached_value(&mut self.route_cache, &topic.to_owned())
    }

    fn cache_route(&mut self, topic: impl Into<String>, route: TopicRouteData, ttl: Duration) {
        cache_value(&mut self.route_cache, topic.into(), route, ttl);
    }

    fn cached_topic_message_type(&mut self, topic: &str) -> Option<ProxyTopicMessageType> {
        cached_value(&mut self.topic_message_type_cache, &topic.to_owned())
    }

    fn cache_topic_message_type(
        &mut self,
        topic: impl Into<String>,
        message_type: ProxyTopicMessageType,
        ttl: Duration,
    ) {
        cache_value(&mut self.topic_message_type_cache, topic.into(), message_type, ttl);
    }

    fn cached_subscription_group(&mut self, topic: &str, group: &str) -> Option<Option<SubscriptionGroupMetadata>> {
        cached_value(
            &mut self.subscription_group_cache,
            &(topic.to_owned(), group.to_owned()),
        )
    }

    fn cache_subscription_group(
        &mut self,
        topic: impl Into<String>,
        group: impl Into<String>,
        metadata: Option<SubscriptionGroupMetadata>,
        ttl: Duration,
    ) {
        cache_value(
            &mut self.subscription_group_cache,
            (topic.into(), group.into()),
            metadata,
            ttl,
        );
    }

    fn cached_user(&mut self, username: &str) -> Option<Option<UserInfo>> {
        cached_value(&mut self.user_cache, &username.to_owned())
    }

    fn cache_user(&mut self, username: impl Into<String>, user: Option<UserInfo>, ttl: Duration) {
        cache_value(&mut self.user_cache, username.into(), user, ttl);
    }

    fn cached_acl(&mut self, subject: &str) -> Option<Option<AclInfo>> {
        cached_value(&mut self.acl_cache, &subject.to_owned())
    }

    fn cache_acl(&mut self, subject: impl Into<String>, acl: Option<AclInfo>, ttl: Duration) {
        cache_value(&mut self.acl_cache, subject.into(), acl, ttl);
    }
}

impl ClusterTaskExecutor {
    fn new(
        config: ClusterConfig,
        signer: Option<Arc<dyn OutboundSigner>>,
        service_context: Option<&ServiceContext>,
    ) -> Self {
        let rpc_hook = signer.map(rpc_hook_from_outbound_signer);
        Self::new_with_rpc_hook(config, rpc_hook, service_context)
    }

    fn new_with_rpc_hook(
        config: ClusterConfig,
        rpc_hook: Option<Arc<ClientRpcHook>>,
        service_context: Option<&ServiceContext>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(CLUSTER_COMMAND_CAPACITY);
        let startup_error = if let Some(service_context) = service_context {
            // Each adapter owns an isolated Client manager domain even when a
            // caller reuses the same parent ServiceContext for several
            // adapters. The child group also makes that ownership visible in
            // the runtime lifecycle tree.
            let worker_context = service_context.child("proxy.cluster.adapter");
            let cancellation = worker_context.task_group().cancellation_token();
            let domain_id = worker_context.task_group().id().as_u64();
            worker_context
                .spawn_service("proxy.cluster.worker", async move {
                    run_cluster_worker(config, domain_id, rpc_hook, receiver, cancellation).await;
                })
                .err()
                .map(|error| Arc::<str>::from(format!("failed to spawn proxy cluster worker: {error}")))
        } else {
            Some(Arc::<str>::from(
                "proxy cluster worker requires an injected ServiceContext",
            ))
        };
        Self { sender, startup_error }
    }

    async fn query_route(&self, topic: ResourceIdentity) -> ProxyResult<TopicRouteData> {
        self.execute(|reply| ClusterCommand::QueryRoute { topic, reply }).await
    }

    async fn query_assignment(
        &self,
        topic: ResourceIdentity,
        group: ResourceIdentity,
        client_id: String,
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        self.execute(|reply| ClusterCommand::QueryAssignment {
            topic,
            group,
            client_id,
            reply,
        })
        .await
    }

    async fn query_topic_message_type(&self, topic: ResourceIdentity) -> ProxyResult<ProxyTopicMessageType> {
        self.execute(|reply| ClusterCommand::QueryTopicMessageType { topic, reply })
            .await
    }

    async fn query_subscription_group(
        &self,
        topic: ResourceIdentity,
        group: ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        self.execute(|reply| ClusterCommand::QuerySubscriptionGroup { topic, group, reply })
            .await
    }

    async fn query_user(&self, username: String) -> ProxyResult<Option<UserInfo>> {
        self.execute(|reply| ClusterCommand::QueryUser { username, reply })
            .await
    }

    async fn query_acl(&self, subject: String) -> ProxyResult<Option<AclInfo>> {
        self.execute(|reply| ClusterCommand::QueryAcl { subject, reply }).await
    }

    async fn send_message(
        &self,
        request: SendMessageRequest,
        client_id: Option<String>,
        request_id: String,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        self.execute(|reply| ClusterCommand::SendMessage {
            request,
            client_id,
            request_id,
            reply,
        })
        .await
    }

    async fn recall_message(
        &self,
        request: RecallMessageRequest,
        client_id: Option<String>,
        request_id: String,
    ) -> ProxyResult<RecallMessagePlan> {
        self.execute(|reply| ClusterCommand::RecallMessage {
            request,
            client_id,
            request_id,
            reply,
        })
        .await
    }

    async fn receive_message(
        &self,
        request: ReceiveMessageRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<ReceiveMessagePlan> {
        self.execute(|reply| ClusterCommand::ReceiveMessage {
            request,
            deadline,
            reply,
        })
        .await
    }

    async fn pull_message(
        &self,
        request: PullMessageRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<PullMessagePlan> {
        self.execute(|reply| ClusterCommand::PullMessage {
            request,
            deadline,
            reply,
        })
        .await
    }

    async fn ack_message(
        &self,
        request: AckMessageRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        self.execute(|reply| ClusterCommand::AckMessage {
            request,
            deadline,
            reply,
        })
        .await
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        request: ForwardMessageToDeadLetterQueueRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
        self.execute(|reply| ClusterCommand::ForwardMessageToDeadLetterQueue {
            request,
            deadline,
            reply,
        })
        .await
    }

    async fn change_invisible_duration(
        &self,
        request: ChangeInvisibleDurationRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        self.execute(|reply| ClusterCommand::ChangeInvisibleDuration {
            request,
            deadline,
            reply,
        })
        .await
    }

    async fn update_offset(
        &self,
        request: UpdateOffsetRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<UpdateOffsetPlan> {
        self.execute(|reply| ClusterCommand::UpdateOffset {
            request,
            deadline,
            reply,
        })
        .await
    }

    async fn get_offset(&self, request: GetOffsetRequest, deadline: Option<Duration>) -> ProxyResult<GetOffsetPlan> {
        self.execute(|reply| ClusterCommand::GetOffset {
            request,
            deadline,
            reply,
        })
        .await
    }

    async fn query_offset(
        &self,
        request: QueryOffsetRequest,
        deadline: Option<Duration>,
    ) -> ProxyResult<QueryOffsetPlan> {
        self.execute(|reply| ClusterCommand::QueryOffset {
            request,
            deadline,
            reply,
        })
        .await
    }

    async fn end_transaction(
        &self,
        request: EndTransactionRequest,
        client_id: Option<String>,
        request_id: String,
        deadline: Option<Duration>,
    ) -> ProxyResult<EndTransactionPlan> {
        self.execute(|reply| ClusterCommand::EndTransaction {
            request,
            client_id,
            request_id,
            deadline,
            reply,
        })
        .await
    }

    async fn lock_batch_mq(&self, request: LockBatchRequestBody) -> ProxyResult<LockBatchResponseBody> {
        self.execute(|reply| ClusterCommand::LockBatchMq { request, reply })
            .await
    }

    async fn unlock_batch_mq(&self, request: UnlockBatchRequestBody) -> ProxyResult<()> {
        self.execute(|reply| ClusterCommand::UnlockBatchMq { request, reply })
            .await
    }

    async fn execute<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<ProxyResult<T>>) -> ClusterCommand,
    ) -> ProxyResult<T>
    where
        T: Send + 'static,
    {
        if let Some(message) = &self.startup_error {
            return Err(ProxyError::Transport {
                message: message.to_string(),
            });
        }
        let (reply, receiver) = oneshot::channel();
        self.sender
            .send(command(reply))
            .await
            .map_err(|_| ProxyError::Transport {
                message: "proxy cluster worker task is unavailable".to_owned(),
            })?;
        receiver.await.map_err(|_| ProxyError::Transport {
            message: "proxy cluster worker dropped response".to_owned(),
        })?
    }
}

async fn run_cluster_worker(
    config: ClusterConfig,
    domain_id: u64,
    rpc_hook: Option<Arc<ClientRpcHook>>,
    receiver: mpsc::Receiver<ClusterCommand>,
    cancellation: tokio_util::sync::CancellationToken,
) {
    let state = ClusterWorkerState::with_rpc_hook(domain_id, rpc_hook);
    run_cluster_worker_with_state(config, state, receiver, cancellation).await;
}

async fn run_cluster_worker_with_state(
    config: ClusterConfig,
    mut state: ClusterWorkerState,
    mut receiver: mpsc::Receiver<ClusterCommand>,
    cancellation: tokio_util::sync::CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            command = receiver.recv() => match command {
                Some(command) => {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => break,
                        () = handle_cluster_command(&config, &mut state, command) => {}
                    }
                }
                None => break,
            },
        }
    }
    let shutdown_deadline = ShutdownDeadline::after(config.shutdown_timeout());
    for (producer_group, producer) in &mut state.send_producers {
        if tokio::time::timeout(shutdown_deadline.remaining(), producer.shutdown())
            .await
            .is_err()
        {
            tracing::warn!(
                producer_group,
                "proxy cluster producer shutdown exceeded the shared deadline"
            );
        }
    }
    if let Some(client) = state.client.take() {
        if tokio::time::timeout(shutdown_deadline.remaining(), client.shutdown())
            .await
            .is_err()
        {
            tracing::warn!("proxy cluster Client shutdown exceeded the shared deadline");
        }
    }
}

fn cluster_client_config(config: &ClusterConfig) -> RocketmqClientConfig {
    let mut client_config = RocketmqClientConfig::default();
    client_config.set_instance_name(CheetahString::from(config.instance_name.as_str()));
    client_config.set_mq_client_api_timeout(config.mq_client_api_timeout_ms);
    if let Some(namesrv_addr) = config.namesrv_addr.as_deref() {
        client_config.set_namesrv_addr(CheetahString::from(namesrv_addr));
    }
    client_config
}

async fn handle_cluster_command(config: &ClusterConfig, state: &mut ClusterWorkerState, command: ClusterCommand) {
    match command {
        ClusterCommand::QueryRoute { topic, reply } => {
            let _ = reply.send(query_route_inner(config, state, topic).await);
        }
        ClusterCommand::QueryAssignment {
            topic,
            group,
            client_id,
            reply,
        } => {
            let _ = reply.send(query_assignment_inner(config, state, topic, group, client_id).await);
        }
        ClusterCommand::QueryTopicMessageType { topic, reply } => {
            let _ = reply.send(query_topic_message_type_inner(config, state, topic).await);
        }
        ClusterCommand::QuerySubscriptionGroup { topic, group, reply } => {
            let _ = reply.send(query_subscription_group_inner(config, state, topic, group).await);
        }
        ClusterCommand::QueryUser { username, reply } => {
            let _ = reply.send(query_user_inner(config, state, username).await);
        }
        ClusterCommand::QueryAcl { subject, reply } => {
            let _ = reply.send(query_acl_inner(config, state, subject).await);
        }
        ClusterCommand::SendMessage {
            request,
            client_id,
            request_id,
            reply,
        } => {
            let _ = reply.send(send_message_inner(config, state, request, client_id, request_id).await);
        }
        ClusterCommand::RecallMessage {
            request,
            client_id,
            request_id,
            reply,
        } => {
            let _ = reply.send(recall_message_inner(config, state, request, client_id, request_id).await);
        }
        ClusterCommand::ReceiveMessage {
            request,
            deadline,
            reply,
        } => {
            let _ = reply.send(receive_message_inner(config, state, request, deadline).await);
        }
        ClusterCommand::PullMessage {
            request,
            deadline,
            reply,
        } => {
            let _ = reply.send(pull_message_inner(config, state, request, deadline).await);
        }
        ClusterCommand::AckMessage {
            request,
            deadline,
            reply,
        } => {
            let _ = reply.send(ack_message_inner(config, state, request, deadline).await);
        }
        ClusterCommand::ForwardMessageToDeadLetterQueue {
            request,
            deadline,
            reply,
        } => {
            let _ = reply.send(forward_message_to_dead_letter_queue_inner(config, state, request, deadline).await);
        }
        ClusterCommand::ChangeInvisibleDuration {
            request,
            deadline,
            reply,
        } => {
            let _ = reply.send(change_invisible_duration_request_inner(config, state, request, deadline).await);
        }
        ClusterCommand::UpdateOffset {
            request,
            deadline,
            reply,
        } => {
            let _ = reply.send(update_offset_request_inner(config, state, request, deadline).await);
        }
        ClusterCommand::GetOffset {
            request,
            deadline,
            reply,
        } => {
            let _ = reply.send(get_offset_request_inner(config, state, request, deadline).await);
        }
        ClusterCommand::QueryOffset {
            request,
            deadline,
            reply,
        } => {
            let _ = reply.send(query_offset_request_inner(config, state, request, deadline).await);
        }
        ClusterCommand::EndTransaction {
            request,
            client_id,
            request_id,
            deadline,
            reply,
        } => {
            let _ = reply
                .send(end_transaction_request_inner(config, state, request, client_id, request_id, deadline).await);
        }
        ClusterCommand::LockBatchMq { request, reply } => {
            let _ = reply.send(lock_batch_mq_inner(config, state, request).await);
        }
        ClusterCommand::UnlockBatchMq { request, reply } => {
            let _ = reply.send(unlock_batch_mq_inner(config, state, request).await);
        }
    }
}

async fn lock_batch_mq_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: LockBatchRequestBody,
) -> ProxyResult<LockBatchResponseBody> {
    let mut response = LockBatchResponseBody::default();
    if request.mq_set.is_empty() {
        return Ok(response);
    }
    let (broker_name, topic_name) = single_broker_and_topic(&request.mq_set)?;
    let client = state.client(config).await?;
    let broker_addr = resolve_remoting_broker_addr(
        client.as_ref(),
        broker_name.as_str(),
        topic_name.as_str(),
        request.only_this_broker,
    )
    .await?;
    response.lock_ok_mq_set = client
        .lock_batch_mq(broker_addr.as_str(), request, config.mq_client_api_timeout_ms)
        .await?;
    Ok(response)
}

async fn unlock_batch_mq_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: UnlockBatchRequestBody,
) -> ProxyResult<()> {
    if request.mq_set.is_empty() {
        return Ok(());
    }
    let (broker_name, topic_name) = single_broker_and_topic(&request.mq_set)?;
    let client = state.client(config).await?;
    let broker_addr = resolve_remoting_broker_addr(
        client.as_ref(),
        broker_name.as_str(),
        topic_name.as_str(),
        request.only_this_broker,
    )
    .await?;
    client
        .unlock_batch_mq(&broker_addr, request, config.mq_client_api_timeout_ms)
        .await?;
    Ok(())
}

fn single_broker_and_topic(
    mq_set: &std::collections::HashSet<MessageQueue>,
) -> ProxyResult<(CheetahString, CheetahString)> {
    let mut broker_name: Option<CheetahString> = None;
    let mut topic_name: Option<CheetahString> = None;
    for queue in mq_set {
        if broker_name
            .as_deref()
            .is_some_and(|existing| existing != queue.broker_name())
        {
            return Err(ProxyError::invalid_metadata(
                "lock/unlock batch request must target a single broker",
            ));
        }
        if topic_name.as_deref().is_some_and(|existing| existing != queue.topic()) {
            return Err(ProxyError::invalid_metadata(
                "lock/unlock batch request must target a single topic",
            ));
        }
        broker_name.get_or_insert_with(|| CheetahString::from(queue.broker_name()));
        topic_name.get_or_insert_with(|| CheetahString::from(queue.topic()));
    }
    Ok((
        broker_name.ok_or_else(|| ProxyError::invalid_metadata("message queue set must not be empty"))?,
        topic_name.ok_or_else(|| ProxyError::invalid_metadata("message queue set must not be empty"))?,
    ))
}

async fn resolve_remoting_broker_addr(
    client: &dyn ClusterClientIo,
    broker_name: &str,
    topic: &str,
    only_this_broker: bool,
) -> ProxyResult<CheetahString> {
    let broker_name = CheetahString::from(broker_name);
    let mut result = client
        .find_subscribe_broker_addr(&broker_name, 0, only_this_broker)
        .await;
    if result.is_none() {
        client.refresh_topic_route(&CheetahString::from(topic)).await;
        result = client
            .find_subscribe_broker_addr(&broker_name, 0, only_this_broker)
            .await;
    }
    result
        .ok_or_else(|| RocketMQError::BrokerNotFound {
            name: broker_name.to_string(),
        })
        .map_err(Into::into)
}

async fn query_route_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    topic: ResourceIdentity,
) -> ProxyResult<TopicRouteData> {
    let topic_name = topic.to_string();
    let client = state.client(config).await?;
    fetch_topic_route(client.as_ref(), config, state, topic_name.as_str()).await
}

async fn query_assignment_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    topic: ResourceIdentity,
    group: ResourceIdentity,
    client_id: String,
) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
    let topic_name = topic.to_string();
    let group_name = group.to_string();
    let client = state.client(config).await?;
    let route = fetch_topic_route(client.as_ref(), config, state, topic_name.as_str()).await?;
    let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
        name: topic_name.clone(),
    })?;
    client
        .query_assignment(
            &broker_addr,
            CheetahString::from(topic_name),
            CheetahString::from(group_name),
            CheetahString::from(client_id),
            CheetahString::from(config.query_assignment_strategy_name.as_str()),
            MessageModel::Clustering,
            config.mq_client_api_timeout_ms,
        )
        .await
        .map_err(Into::into)
}

async fn query_topic_message_type_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    topic: ResourceIdentity,
) -> ProxyResult<ProxyTopicMessageType> {
    let topic_name = topic.to_string();
    if let Some(message_type) = state.cached_topic_message_type(topic_name.as_str()) {
        return Ok(message_type);
    }

    let client = state.client(config).await?;
    let route = fetch_topic_route(client.as_ref(), config, state, topic_name.as_str()).await?;
    let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
        name: topic_name.clone(),
    })?;
    let topic_config = client
        .topic_config(
            &broker_addr,
            CheetahString::from(topic_name.as_str()),
            config.mq_client_api_timeout_ms,
        )
        .await?;

    let message_type = convert_topic_message_type(topic_config.get_topic_message_type());
    state.cache_topic_message_type(topic_name, message_type, config.metadata_cache_ttl());
    Ok(message_type)
}

async fn query_subscription_group_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    topic: ResourceIdentity,
    group: ResourceIdentity,
) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
    let topic_name = topic.to_string();
    let group_name = group.to_string();
    if let Some(metadata) = state.cached_subscription_group(topic_name.as_str(), group_name.as_str()) {
        return Ok(metadata);
    }

    let client = state.client(config).await?;
    let route = fetch_topic_route(client.as_ref(), config, state, topic_name.as_str()).await?;
    let Some(broker_addr) = select_master_broker_addr(&route) else {
        state.cache_subscription_group(topic_name, group_name, None, config.metadata_cache_ttl());
        return Ok(None);
    };
    let group_config = client
        .subscription_group_config(
            &broker_addr,
            CheetahString::from(group_name.as_str()),
            config.mq_client_api_timeout_ms,
        )
        .await?;

    let metadata = Some(convert_subscription_group(group_config));
    state.cache_subscription_group(topic_name, group_name, metadata.clone(), config.metadata_cache_ttl());
    Ok(metadata)
}

async fn query_user_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    username: String,
) -> ProxyResult<Option<UserInfo>> {
    if let Some(user) = state.cached_user(username.as_str()) {
        return Ok(user);
    }

    let client = state.client(config).await?;
    let broker_addr = fetch_auth_metadata_broker_addr(client.as_ref(), config).await?;
    let user = client
        .user(
            broker_addr,
            CheetahString::from(username.as_str()),
            config.mq_client_api_timeout_ms,
        )
        .await?;

    state.cache_user(username, user.clone(), config.metadata_cache_ttl());
    Ok(user)
}

async fn query_acl_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    subject: String,
) -> ProxyResult<Option<AclInfo>> {
    if let Some(acl) = state.cached_acl(subject.as_str()) {
        return Ok(acl);
    }

    let client = state.client(config).await?;
    let broker_addr = fetch_auth_metadata_broker_addr(client.as_ref(), config).await?;
    let acl = client
        .acl(
            broker_addr,
            CheetahString::from(subject.as_str()),
            config.mq_client_api_timeout_ms,
        )
        .await?;

    state.cache_acl(subject, acl.clone(), config.metadata_cache_ttl());
    Ok(acl)
}

async fn send_message_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: SendMessageRequest,
    client_id: Option<String>,
    request_id: String,
) -> ProxyResult<Vec<SendMessageResultEntry>> {
    let timeout = effective_send_timeout_ms(config, request.timeout);
    let producer_group = build_proxy_producer_group(config, client_id.as_deref(), request_id.as_str());
    let topics = collect_send_topics(&request);
    let producer = match acquire_send_producer(config, state, producer_group.as_str(), topics, timeout).await {
        Ok(producer) => producer,
        Err(error) => {
            return Ok(request
                .messages
                .iter()
                .map(|_| failure_send_result_entry(&error))
                .collect());
        }
    };
    let mut results = Vec::with_capacity(request.messages.len());
    for entry in request.messages {
        results.push(send_message_entry(producer, entry, timeout).await);
    }
    Ok(results)
}

async fn recall_message_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: RecallMessageRequest,
    client_id: Option<String>,
    request_id: String,
) -> ProxyResult<RecallMessagePlan> {
    let producer_group = build_proxy_producer_group(config, client_id.as_deref(), request_id.as_str());
    let topic = CheetahString::from(request.topic.to_string());
    let producer = acquire_send_producer(
        config,
        state,
        producer_group.as_str(),
        vec![topic.clone()],
        config.send_message_timeout_ms.max(1),
    )
    .await?;
    let message_id = producer
        .recall_message(topic, CheetahString::from(request.recall_handle))
        .await
        .map_err(ProxyError::from)?;

    Ok(RecallMessagePlan {
        status: ProxyStatusMapper::ok_payload(),
        message_id,
    })
}

async fn acquire_send_producer<'a>(
    config: &ClusterConfig,
    state: &'a mut ClusterWorkerState,
    producer_group: &str,
    topics: Vec<CheetahString>,
    timeout_ms: u64,
) -> Result<&'a mut dyn ClusterProducerIo, ProxyError> {
    if !state.send_producers.contains_key(producer_group) {
        // Every producer is registered with the worker-owned Client instance so
        // shutdown has one factory owner and one manager entry to remove.
        let _ = state.client(config).await?;
        let mut producer =
            state
                .producer_factory
                .create(state.domain_id, config, producer_group, timeout_ms, state.rpc_hook());
        producer.set_topics(topics.clone());
        state.send_producers.insert(producer_group.to_owned(), producer);
        let start_result = state
            .send_producers
            .get_mut(producer_group)
            .ok_or_else(|| ProxyError::Transport {
                message: "send producer was not retained before initialization".to_owned(),
            })?
            .start()
            .await;
        if let Err(error) = start_result {
            if let Some(producer) = state.send_producers.get_mut(producer_group) {
                producer.shutdown().await;
            }
            state.send_producers.remove(producer_group);
            return Err(ProxyError::from(error));
        }
    }

    let producer = state
        .send_producers
        .get_mut(producer_group)
        .ok_or_else(|| ProxyError::Transport {
            message: "send producer was not retained after initialization".to_owned(),
        })?;
    refresh_send_producer(producer.as_mut(), topics, timeout_ms);
    Ok(producer.as_mut())
}

fn refresh_send_producer(producer: &mut dyn ClusterProducerIo, topics: Vec<CheetahString>, timeout_ms: u64) {
    producer.set_topics(merge_topics(producer.topics(), topics));
    producer.set_send_timeout(timeout_ms.clamp(1, u64::from(u32::MAX)) as u32);
}

fn merge_topics(existing: Vec<CheetahString>, incoming: Vec<CheetahString>) -> Vec<CheetahString> {
    existing
        .into_iter()
        .chain(incoming)
        .map(|topic| topic.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(CheetahString::from)
        .collect()
}

fn collect_send_topics(request: &SendMessageRequest) -> Vec<CheetahString> {
    request
        .messages
        .iter()
        .map(|message| message.topic.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(CheetahString::from)
        .collect()
}

async fn receive_message_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: ReceiveMessageRequest,
    deadline: Option<Duration>,
) -> ProxyResult<ReceiveMessagePlan> {
    let client = state.client(config).await?;
    let broker_target = resolve_broker_target(
        client.as_ref(),
        config,
        state,
        &request.target.topic,
        request.target.queue_id,
        request.target.broker_name.as_deref(),
        request.target.broker_addr.as_deref(),
    )
    .await?;
    let timeout_ms = effective_pop_timeout_ms(config, deadline, request.long_polling_timeout);
    let request_header = build_pop_request_header(&broker_target.broker_name, &request);
    let pop_result = client
        .pop_message(
            &broker_target.broker_name,
            &broker_target.broker_addr,
            request_header,
            timeout_ms,
        )
        .await?;

    Ok(build_receive_plan(pop_result))
}

async fn pull_message_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: PullMessageRequest,
    deadline: Option<Duration>,
) -> ProxyResult<PullMessagePlan> {
    let client = state.client(config).await?;
    let broker_target = resolve_broker_target(
        client.as_ref(),
        config,
        state,
        &request.target.topic,
        request.target.queue_id,
        request.target.broker_name.as_deref(),
        request.target.broker_addr.as_deref(),
    )
    .await?;
    let timeout_ms = effective_request_timeout_ms(
        config
            .mq_client_api_timeout_ms
            .max(request.long_polling_timeout.as_millis().clamp(0, u128::from(u64::MAX)) as u64 + 500),
        deadline,
    );
    let request_header = build_pull_request_header(&broker_target.broker_name, &request);
    let pull_result = client
        .pull_outcome_from_broker(broker_target.broker_addr.as_str(), request_header, timeout_ms)
        .await?;
    Ok(build_pull_plan(pull_result))
}

async fn ack_message_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: AckMessageRequest,
    deadline: Option<Duration>,
) -> ProxyResult<Vec<AckMessageResultEntry>> {
    let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, deadline);
    let client = state.client(config).await?;
    let mut results = Vec::with_capacity(request.entries.len());

    for entry in &request.entries {
        results.push(ack_message_entry(client.as_ref(), &request.group, &request.topic, entry, timeout_ms).await);
    }

    Ok(results)
}

async fn forward_message_to_dead_letter_queue_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: ForwardMessageToDeadLetterQueueRequest,
    deadline: Option<Duration>,
) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
    let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, deadline);
    let client = state.client(config).await?;
    let topic_name = request.lite_topic.clone().unwrap_or_else(|| request.topic.to_string());
    let group_name = request.group.to_string();
    let parsed = parse_receipt_handle(
        request.receipt_handle.as_str(),
        topic_name.as_str(),
        group_name.as_str(),
    )?;
    let actual_broker_name =
        resolve_subscription_broker_name(client.as_ref(), &parsed.topic, &parsed.broker_name, parsed.queue_id).await;
    let broker_addr = find_subscribe_broker_addr(client.as_ref(), &actual_broker_name, &parsed.topic)
        .await
        .ok_or_else(|| RocketMQError::BrokerNotFound {
            name: actual_broker_name.to_string(),
        })?;
    let message = build_dead_letter_message(&request, &parsed, &actual_broker_name)?;

    client
        .consumer_send_message_back(
            broker_addr.as_str(),
            Some(actual_broker_name.as_str()),
            &message,
            group_name.as_str(),
            -1,
            timeout_ms,
            request.max_delivery_attempts,
        )
        .await?;

    Ok(ForwardMessageToDeadLetterQueuePlan {
        status: ProxyStatusMapper::ok_payload(),
    })
}

async fn change_invisible_duration_request_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: ChangeInvisibleDurationRequest,
    deadline: Option<Duration>,
) -> ProxyResult<ChangeInvisibleDurationPlan> {
    let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, deadline);
    let client = state.client(config).await?;
    change_invisible_duration_inner(client.as_ref(), config, &request, timeout_ms).await
}

async fn update_offset_request_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: UpdateOffsetRequest,
    deadline: Option<Duration>,
) -> ProxyResult<UpdateOffsetPlan> {
    let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, deadline);
    let client = state.client(config).await?;
    let broker_target = resolve_broker_target(
        client.as_ref(),
        config,
        state,
        &request.target.topic,
        request.target.queue_id,
        request.target.broker_name.as_deref(),
        request.target.broker_addr.as_deref(),
    )
    .await?;
    let request_header = build_update_offset_request_header(&broker_target.broker_name, &request);
    client
        .update_consumer_offset(&broker_target.broker_addr, request_header, timeout_ms)
        .await?;

    Ok(UpdateOffsetPlan {
        status: ProxyStatusMapper::ok_payload(),
    })
}

async fn get_offset_request_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: GetOffsetRequest,
    deadline: Option<Duration>,
) -> ProxyResult<GetOffsetPlan> {
    let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, deadline);
    let client = state.client(config).await?;
    let broker_target = resolve_broker_target(
        client.as_ref(),
        config,
        state,
        &request.target.topic,
        request.target.queue_id,
        request.target.broker_name.as_deref(),
        request.target.broker_addr.as_deref(),
    )
    .await?;
    let request_header = build_query_consumer_offset_request_header(&broker_target.broker_name, &request);
    let offset = client
        .query_consumer_offset(broker_target.broker_addr.as_str(), request_header, timeout_ms)
        .await?;

    Ok(GetOffsetPlan {
        status: ProxyStatusMapper::ok_payload(),
        offset,
    })
}

async fn query_offset_request_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: QueryOffsetRequest,
    deadline: Option<Duration>,
) -> ProxyResult<QueryOffsetPlan> {
    let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, deadline);
    let client = state.client(config).await?;
    let broker_target = resolve_broker_target(
        client.as_ref(),
        config,
        state,
        &request.target.topic,
        request.target.queue_id,
        request.target.broker_name.as_deref(),
        request.target.broker_addr.as_deref(),
    )
    .await?;
    let message_queue = build_message_queue(&request.target, &broker_target.broker_name);
    let offset = match request.policy {
        QueryOffsetPolicy::Beginning => {
            client
                .min_offset(broker_target.broker_addr.as_str(), &message_queue, timeout_ms)
                .await?
        }
        QueryOffsetPolicy::End => {
            client
                .max_offset(broker_target.broker_addr.as_str(), &message_queue, timeout_ms)
                .await?
        }
        QueryOffsetPolicy::Timestamp => {
            client
                .search_offset(
                    broker_target.broker_addr.as_str(),
                    &message_queue,
                    request.timestamp_ms.ok_or_else(|| {
                        ProxyError::illegal_offset("timestamp policy requires timestamp to be present")
                    })?,
                    BoundaryType::Lower,
                    timeout_ms,
                )
                .await?
        }
    };

    Ok(QueryOffsetPlan {
        status: ProxyStatusMapper::ok_payload(),
        offset,
    })
}

async fn end_transaction_request_inner(
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: EndTransactionRequest,
    client_id: Option<String>,
    request_id: String,
    deadline: Option<Duration>,
) -> ProxyResult<EndTransactionPlan> {
    let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, deadline);
    let client = state.client(config).await?;
    end_transaction_inner(
        client.as_ref(),
        config,
        state,
        &request,
        client_id.as_deref(),
        request_id.as_str(),
        timeout_ms,
    )
    .await
}

async fn fetch_topic_route(
    client: &dyn ClusterClientIo,
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    topic_name: &str,
) -> ProxyResult<TopicRouteData> {
    if let Some(route) = state.cached_route(topic_name) {
        return Ok(route);
    }

    let route = client
        .topic_route(topic_name, config.mq_client_api_timeout_ms)
        .await?
        .ok_or_else(|| RocketMQError::route_not_found(topic_name.to_owned()))?;
    state.cache_route(topic_name.to_owned(), route.clone(), config.route_cache_ttl());
    Ok(route)
}

async fn fetch_auth_metadata_broker_addr(
    client: &dyn ClusterClientIo,
    config: &ClusterConfig,
) -> ProxyResult<CheetahString> {
    let cluster_info = client.broker_cluster_info(config.mq_client_api_timeout_ms).await?;
    select_auth_metadata_broker_addr(&cluster_info, config.broker_cluster_name.as_str()).ok_or_else(|| {
        RocketMQError::BrokerNotFound {
            name: config.broker_cluster_name.clone(),
        }
        .into()
    })
}

fn select_auth_metadata_broker_addr(cluster_info: &ClusterInfo, cluster_name: &str) -> Option<CheetahString> {
    let broker_addr_table = cluster_info.broker_addr_table.as_ref()?;
    let broker_names = cluster_info.cluster_addr_table.as_ref()?.get(cluster_name)?;
    broker_names
        .iter()
        .filter_map(|broker_name| broker_addr_table.get(broker_name))
        .find_map(|broker_data| {
            broker_data
                .broker_addrs()
                .get(&MASTER_ID)
                .cloned()
                .or_else(|| broker_data.select_broker_addr())
        })
}

fn cached_value<K, T>(cache: &mut HashMap<K, CachedValue<T>>, key: &K) -> Option<T>
where
    K: Clone + Eq + std::hash::Hash,
    T: Clone,
{
    match cache.get(key) {
        Some(entry) if !entry.is_expired() => Some(entry.value.clone()),
        Some(_) => {
            cache.remove(key);
            None
        }
        None => None,
    }
}

fn cache_value<K, T>(cache: &mut HashMap<K, CachedValue<T>>, key: K, value: T, ttl: Duration)
where
    K: Eq + std::hash::Hash,
{
    if ttl.is_zero() {
        return;
    }
    cache.insert(key, CachedValue::new(value, ttl));
}

#[derive(Debug, Clone)]
struct BrokerTarget {
    broker_name: CheetahString,
    broker_addr: CheetahString,
}

#[derive(Debug, Clone)]
struct ParsedReceiptHandle {
    raw: CheetahString,
    broker_name: CheetahString,
    topic: CheetahString,
    queue_id: i32,
    queue_offset: i64,
}

async fn resolve_broker_target(
    client: &dyn ClusterClientIo,
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    topic: &ResourceIdentity,
    queue_id: i32,
    broker_name: Option<&str>,
    broker_addr: Option<&str>,
) -> ProxyResult<BrokerTarget> {
    if let (Some(broker_name), Some(broker_addr)) = (broker_name, broker_addr) {
        return Ok(BrokerTarget {
            broker_name: CheetahString::from(broker_name),
            broker_addr: CheetahString::from(broker_addr),
        });
    }

    let topic_name = CheetahString::from(topic.to_string());
    if let Some(broker_name) = broker_name {
        let broker_name = CheetahString::from(broker_name);
        let actual_broker_name = resolve_subscription_broker_name(client, &topic_name, &broker_name, queue_id).await;
        if let Some(broker_addr) = find_subscribe_broker_addr(client, &actual_broker_name, &topic_name).await {
            return Ok(BrokerTarget {
                broker_name: actual_broker_name,
                broker_addr,
            });
        }
    }

    let route = fetch_topic_route(client, config, state, topic_name.as_str()).await?;
    let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
        name: topic_name.to_string(),
    })?;
    let broker_name = route
        .broker_datas
        .iter()
        .find_map(|broker| {
            broker
                .broker_addrs()
                .iter()
                .find(|(_, addr)| *addr == &broker_addr)
                .map(|_| broker.broker_name().clone())
        })
        .unwrap_or_default();

    Ok(BrokerTarget {
        broker_name,
        broker_addr,
    })
}

async fn ack_message_entry(
    client: &dyn ClusterClientIo,
    group: &ResourceIdentity,
    topic: &ResourceIdentity,
    entry: &AckMessageEntry,
    timeout_ms: u64,
) -> AckMessageResultEntry {
    let status = match ack_message_entry_inner(client, group, topic, entry, timeout_ms).await {
        Ok(status) => status,
        Err(error) => ProxyStatusMapper::from_error_payload(&error),
    };

    AckMessageResultEntry {
        message_id: entry.message_id.clone(),
        receipt_handle: entry.receipt_handle.clone(),
        status,
    }
}

async fn ack_message_entry_inner(
    client: &dyn ClusterClientIo,
    group: &ResourceIdentity,
    topic: &ResourceIdentity,
    entry: &AckMessageEntry,
    timeout_ms: u64,
) -> ProxyResult<ProxyPayloadStatus> {
    let topic_name = entry.lite_topic.clone().unwrap_or_else(|| topic.to_string());
    let group_name = group.to_string();
    let parsed = parse_receipt_handle(entry.receipt_handle.as_str(), topic_name.as_str(), group_name.as_str())?;
    let actual_broker_name =
        resolve_subscription_broker_name(client, &parsed.topic, &parsed.broker_name, parsed.queue_id).await;
    let broker_addr = find_subscribe_broker_addr(client, &actual_broker_name, &parsed.topic)
        .await
        .ok_or_else(|| RocketMQError::BrokerNotFound {
            name: actual_broker_name.to_string(),
        })?;
    let request_header = AckMessageRequestHeader {
        consumer_group: CheetahString::from(group.to_string()),
        topic: parsed.topic.clone(),
        queue_id: parsed.queue_id,
        extra_info: parsed.raw.clone(),
        offset: parsed.queue_offset,
        lite_topic: entry
            .lite_topic
            .as_ref()
            .map(|topic| CheetahString::from(topic.as_str())),
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(parsed.broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let ack_result = client.ack_message(&broker_addr, request_header, timeout_ms).await?;
    Ok(ack_status_to_payload(&ack_result))
}

async fn change_invisible_duration_inner(
    client: &dyn ClusterClientIo,
    _config: &ClusterConfig,
    request: &ChangeInvisibleDurationRequest,
    timeout_ms: u64,
) -> ProxyResult<ChangeInvisibleDurationPlan> {
    let topic_name = request.lite_topic.clone().unwrap_or_else(|| request.topic.to_string());
    let group_name = request.group.to_string();
    let parsed = parse_receipt_handle(
        request.receipt_handle.as_str(),
        topic_name.as_str(),
        group_name.as_str(),
    )?;
    let actual_broker_name =
        resolve_subscription_broker_name(client, &parsed.topic, &parsed.broker_name, parsed.queue_id).await;
    let broker_addr = find_subscribe_broker_addr(client, &actual_broker_name, &parsed.topic)
        .await
        .ok_or_else(|| RocketMQError::BrokerNotFound {
            name: actual_broker_name.to_string(),
        })?;
    let request_header = ChangeInvisibleTimeRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: parsed.topic.clone(),
        queue_id: parsed.queue_id,
        extra_info: parsed.raw.clone(),
        offset: parsed.queue_offset,
        invisible_time: request.invisible_duration.as_millis().clamp(1, i64::MAX as u128) as i64,
        lite_topic: request
            .lite_topic
            .as_ref()
            .map(|topic| CheetahString::from(topic.as_str())),
        suspend: request.suspend.unwrap_or(false),
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(parsed.broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let ack_result =
        change_invisible_time(client, &parsed.broker_name, &broker_addr, request_header, timeout_ms).await?;

    Ok(ChangeInvisibleDurationPlan {
        status: ack_status_to_payload(&ack_result),
        receipt_handle: ack_result.extra_info().to_string(),
    })
}

async fn change_invisible_time(
    client: &dyn ClusterClientIo,
    broker_name: &CheetahString,
    broker_addr: &CheetahString,
    request_header: ChangeInvisibleTimeRequestHeader,
    timeout_ms: u64,
) -> ProxyResult<AckResult> {
    client
        .change_invisible_time(broker_name, broker_addr, request_header, timeout_ms)
        .await
        .map_err(Into::into)
}

async fn end_transaction_inner(
    client: &dyn ClusterClientIo,
    config: &ClusterConfig,
    state: &mut ClusterWorkerState,
    request: &EndTransactionRequest,
    client_id: Option<&str>,
    request_id: &str,
    timeout_ms: u64,
) -> ProxyResult<EndTransactionPlan> {
    let topic_name = request.topic.to_string();
    let route = fetch_topic_route(client, config, state, topic_name.as_str()).await?;
    let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
        name: topic_name.clone(),
    })?;
    let producer_group = request
        .producer_group
        .clone()
        .unwrap_or_else(|| build_proxy_producer_group(config, client_id, request_id));
    let transaction_state_table_offset = request
        .transaction_state_table_offset
        .ok_or_else(|| ProxyError::invalid_transaction_id("missing transactional message offset for endTransaction"))?;
    let transaction_state_table_offset = i64::try_from(transaction_state_table_offset)
        .map_err(|_| ProxyError::invalid_transaction_id("transaction state table offset exceeds Java long range"))?;
    let broker_message_id = decode_end_transaction_message_id(
        request
            .commit_log_message_id
            .as_deref()
            .unwrap_or(request.message_id.as_str()),
    )?;
    let request_header = EndTransactionRequestHeader {
        topic: CheetahString::from(topic_name),
        producer_group: CheetahString::from(producer_group),
        tran_state_table_offset: transaction_state_table_offset,
        commit_log_offset: broker_message_id.offset,
        commit_or_rollback: transaction_resolution_flag(request.resolution),
        from_transaction_check: matches!(request.source, TransactionSource::ServerCheck),
        msg_id: CheetahString::from(request.message_id.as_str()),
        transaction_id: Some(CheetahString::from(request.transaction_id.as_str())),
        rpc_request_header: RpcRequestHeader::default(),
    };

    client
        .end_transaction(
            &broker_addr,
            request_header,
            CheetahString::from(request.trace_context.as_deref().unwrap_or("")),
            timeout_ms,
        )
        .await?;

    Ok(EndTransactionPlan {
        status: ProxyStatusMapper::ok_payload(),
    })
}

fn build_pop_request_header(broker_name: &CheetahString, request: &ReceiveMessageRequest) -> PopMessageRequestHeader {
    PopMessageRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: CheetahString::from(request.target.topic.to_string()),
        queue_id: request.target.queue_id,
        max_msg_nums: request.batch_size,
        invisible_time: request.invisible_duration.as_millis().clamp(1, u128::from(u64::MAX)) as u64,
        poll_time: request.long_polling_timeout.as_millis().clamp(0, u128::from(u64::MAX)) as u64,
        born_time: current_millis(),
        init_mode: 0,
        exp_type: Some(CheetahString::from(request.filter_expression.expression_type.as_str())),
        exp: Some(CheetahString::from(request.filter_expression.expression.as_str())),
        order: Some(request.target.fifo),
        attempt_id: request.attempt_id.as_deref().map(CheetahString::from),
        topic_request_header: Some(PopTopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
        }),
    }
}

fn build_pull_request_header(broker_name: &CheetahString, request: &PullMessageRequest) -> PullMessageRequestHeader {
    PullMessageRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: CheetahString::from(request.target.topic.to_string()),
        lite_topic: None,
        queue_id: request.target.queue_id,
        queue_offset: request.offset,
        max_msg_nums: request.batch_size.min(i32::MAX as u32) as i32,
        sys_flag: PullSysFlag::build_sys_flag(
            false,
            !request.long_polling_timeout.is_zero(),
            true,
            request.filter_expression.expression_type != ExpressionType::TAG,
        ) as i32,
        commit_offset: 0,
        suspend_timeout_millis: request.long_polling_timeout.as_millis().clamp(0, u128::from(u64::MAX)) as u64,
        sub_version: 0,
        subscription: Some(CheetahString::from(request.filter_expression.expression.as_str())),
        expression_type: Some(CheetahString::from(request.filter_expression.expression_type.as_str())),
        max_msg_bytes: None,
        request_source: None,
        proxy_forward_client_id: None,
        topic_request: Some(PopTopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
        }),
    }
}

fn build_update_offset_request_header(
    broker_name: &CheetahString,
    request: &UpdateOffsetRequest,
) -> UpdateConsumerOffsetRequestHeader {
    UpdateConsumerOffsetRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: CheetahString::from(request.target.topic.to_string()),
        queue_id: request.target.queue_id,
        commit_offset: request.offset,
        topic_request_header: Some(PopTopicRequestHeader {
            rpc: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    }
}

fn build_query_consumer_offset_request_header(
    broker_name: &CheetahString,
    request: &GetOffsetRequest,
) -> QueryConsumerOffsetRequestHeader {
    QueryConsumerOffsetRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: CheetahString::from(request.target.topic.to_string()),
        queue_id: request.target.queue_id,
        set_zero_if_not_found: Some(false),
        topic_request_header: Some(PopTopicRequestHeader {
            rpc: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    }
}

fn build_message_queue(target: &MessageQueueTarget, broker_name: &CheetahString) -> MessageQueue {
    MessageQueue::from_parts(target.topic.to_string(), broker_name.clone(), target.queue_id)
}

fn build_receive_plan(pop_result: PopResult) -> ReceiveMessagePlan {
    let delivery_timestamp_ms = (pop_result.pop_time > 0).then_some(pop_result.pop_time as i64);
    let invisible_duration = Duration::from_millis(pop_result.invisible_time.max(1));
    match pop_result.pop_status {
        PopStatus::Found => {
            let messages = pop_result
                .msg_found_list
                .unwrap_or_default()
                .into_iter()
                .map(|message| ReceivedMessage {
                    message: message_ext_to_core(&message),
                    invisible_duration,
                })
                .collect::<Vec<_>>();
            let status = if messages.is_empty() {
                ProxyStatusMapper::from_payload_code(v2::Code::MessageNotFound, "no message available")
            } else {
                ProxyStatusMapper::ok_payload()
            };
            ReceiveMessagePlan {
                status,
                delivery_timestamp_ms,
                messages,
            }
        }
        PopStatus::NoNewMsg | PopStatus::PollingNotFound => ReceiveMessagePlan {
            status: ProxyStatusMapper::from_payload_code(v2::Code::MessageNotFound, "no message available"),
            delivery_timestamp_ms,
            messages: Vec::new(),
        },
        PopStatus::PollingFull => ReceiveMessagePlan {
            status: ProxyStatusMapper::from_payload_code(v2::Code::TooManyRequests, "broker polling queue is full"),
            delivery_timestamp_ms,
            messages: Vec::new(),
        },
    }
}

fn build_pull_plan(pull_result: rocketmq_model::result::PullOutcome<MessageExt>) -> PullMessagePlan {
    let next_offset = pull_result.next_begin_offset() as i64;
    let min_offset = pull_result.min_offset() as i64;
    let max_offset = pull_result.max_offset() as i64;
    match pull_result.pull_status() {
        PullStatus::Found => PullMessagePlan {
            status: ProxyStatusMapper::ok_payload(),
            next_offset,
            min_offset,
            max_offset,
            messages: pull_result
                .into_messages()
                .unwrap_or_default()
                .into_iter()
                .map(|message| message_ext_to_core(&message))
                .collect(),
        },
        PullStatus::NoNewMsg | PullStatus::NoMatchedMsg => PullMessagePlan {
            status: ProxyStatusMapper::from_payload_code(v2::Code::MessageNotFound, "no message available"),
            next_offset,
            min_offset,
            max_offset,
            messages: Vec::new(),
        },
        PullStatus::OffsetIllegal => PullMessagePlan {
            status: ProxyStatusMapper::from_payload_code(v2::Code::IllegalOffset, "pull offset is illegal"),
            next_offset,
            min_offset,
            max_offset,
            messages: Vec::new(),
        },
    }
}

fn ack_status_to_payload(ack_result: &AckResult) -> ProxyPayloadStatus {
    match ack_result.status() {
        AckStatus::Ok => ProxyStatusMapper::ok_payload(),
        AckStatus::NotExist => ProxyStatusMapper::from_payload_code(
            v2::Code::InvalidReceiptHandle,
            "receipt handle has expired or message no longer exists",
        ),
    }
}

fn build_dead_letter_message(
    request: &ForwardMessageToDeadLetterQueueRequest,
    parsed: &ParsedReceiptHandle,
    broker_name: &CheetahString,
) -> ProxyResult<MessageExt> {
    let broker_message_id = decode_broker_message_id(request.message_id.as_str())?;
    let mut message = MessageExt::default();
    message.set_topic(parsed.topic.clone());
    message.set_msg_id(CheetahString::from(request.message_id.as_str()));
    message.set_broker_name(broker_name.clone());
    message.set_queue_id(parsed.queue_id);
    message.set_queue_offset(parsed.queue_offset);
    message.set_commit_log_offset(broker_message_id.offset);
    message.set_reconsume_times(request.delivery_attempt.saturating_sub(1));
    Ok(message)
}

fn parse_receipt_handle(receipt_handle: &str, topic: &str, consumer_group: &str) -> ProxyResult<ParsedReceiptHandle> {
    let trimmed = receipt_handle.trim();
    if trimmed.is_empty() {
        return Err(ProxyError::invalid_receipt_handle("receipt handle must not be empty"));
    }

    let parts = ExtraInfoUtil::split(trimmed);
    let broker_name = ExtraInfoUtil::get_broker_name(parts.as_slice())
        .map(CheetahString::from_string)
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;
    let queue_id = ExtraInfoUtil::get_queue_id(parts.as_slice())
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;
    let queue_offset = ExtraInfoUtil::get_queue_offset(parts.as_slice())
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;
    let real_topic = ExtraInfoUtil::get_real_topic(parts.as_slice(), topic, consumer_group)
        .map(CheetahString::from_string)
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;

    Ok(ParsedReceiptHandle {
        raw: CheetahString::from(trimmed),
        broker_name,
        topic: real_topic,
        queue_id,
        queue_offset,
    })
}

async fn find_subscribe_broker_addr(
    client: &dyn ClusterClientIo,
    broker_name: &CheetahString,
    topic: &CheetahString,
) -> Option<CheetahString> {
    let mut result = client.find_subscribe_broker_addr(broker_name, MASTER_ID, true).await;
    if result.is_none() {
        client.refresh_topic_route(topic).await;
        result = client.find_subscribe_broker_addr(broker_name, MASTER_ID, true).await;
    }
    result
}

async fn resolve_subscription_broker_name(
    client: &dyn ClusterClientIo,
    topic: &CheetahString,
    broker_name: &CheetahString,
    queue_id: i32,
) -> CheetahString {
    if !broker_name.is_empty() && broker_name.starts_with(LOGICAL_QUEUE_MOCK_BROKER_PREFIX) {
        let queue = MessageQueue::from_parts(topic.clone(), broker_name.clone(), queue_id);
        client.broker_name_for_queue(&queue).await
    } else {
        broker_name.clone()
    }
}

fn build_send_producer(
    domain_id: u64,
    config: &ClusterConfig,
    producer_group: &str,
    timeout_ms: u64,
    rpc_hook: Option<Arc<ClientRpcHook>>,
) -> DefaultMQProducer {
    let client_config = client_config_for_managed_domain(domain_id, cluster_client_config(config));

    let mut builder = DefaultMQProducer::builder()
        .client_config(client_config)
        .producer_group(producer_group)
        .send_msg_timeout(timeout_ms as u32);
    if let Some(rpc_hook) = rpc_hook {
        builder = builder.rpc_hook(rpc_hook);
    }
    builder.build()
}

async fn send_message_entry(
    producer: &mut dyn ClusterProducerIo,
    entry: SendMessageEntry,
    timeout: u64,
) -> SendMessageResultEntry {
    match send_message_entry_inner(producer, entry, timeout).await {
        Ok(send_result) => SendMessageResultEntry {
            status: ProxyStatusMapper::from_send_result_payload(&send_result),
            send_result: Some(send_result),
        },
        Err(error) => failure_send_result_entry(&error),
    }
}

async fn send_message_entry_inner(
    producer: &mut dyn ClusterProducerIo,
    entry: SendMessageEntry,
    timeout: u64,
) -> ProxyResult<SendResult> {
    let mut message = message_from_core(&entry.message);
    attach_transaction_producer_group(&mut message, producer.producer_group());
    let queue = resolve_target_queue(producer, &entry).await?;
    let result = if let Some(queue) = queue {
        producer.send_to_queue(message, queue, timeout).await?
    } else {
        producer.send(message, timeout).await?
    };

    result.ok_or_else(|| ProxyError::Transport {
        message: format!("send result was empty for topic '{}'", entry.topic),
    })
}

async fn resolve_target_queue(
    producer: &mut dyn ClusterProducerIo,
    entry: &SendMessageEntry,
) -> ProxyResult<Option<MessageQueue>> {
    let Some(queue_id) = entry.queue_id else {
        return Ok(None);
    };

    let queues = producer
        .fetch_publish_message_queues(entry.topic.to_string().as_str())
        .await?;
    let max_queue_id = queues.iter().map(MessageQueue::queue_id).max().unwrap_or(-1);

    queues
        .into_iter()
        .find(|queue| queue.queue_id() == queue_id)
        .map(Some)
        .ok_or_else(|| {
            if max_queue_id >= 0 {
                RocketMQError::QueueIdOutOfRange {
                    topic: entry.topic.to_string(),
                    queue_id,
                    max: max_queue_id,
                }
                .into()
            } else {
                RocketMQError::QueueNotExist {
                    topic: entry.topic.to_string(),
                    queue_id,
                }
                .into()
            }
        })
}

fn failure_send_result_entry(error: &ProxyError) -> SendMessageResultEntry {
    SendMessageResultEntry {
        status: ProxyStatusMapper::from_error_payload(error),
        send_result: None,
    }
}

fn attach_transaction_producer_group(message: &mut Message, producer_group: &str) {
    if !is_transaction_prepared(message) {
        return;
    }

    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP),
        CheetahString::from(producer_group),
    );
}

fn is_transaction_prepared(message: &Message) -> bool {
    message
        .property_ref(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_PREPARED,
        ))
        .and_then(|value| value.parse().ok())
        .unwrap_or(false)
}

fn sanitize_group_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '%' | '|'))
        .collect();

    if sanitized.is_empty() {
        "proxy".to_owned()
    } else {
        sanitized
    }
}

pub(crate) fn build_proxy_producer_group(config: &ClusterConfig, client_id: Option<&str>, request_id: &str) -> String {
    let prefix = sanitize_group_component(config.producer_group_prefix.as_str());
    let identity = sanitize_group_component(client_id.unwrap_or(request_id));
    format!("{prefix}-{identity}")
}

fn transaction_resolution_flag(resolution: TransactionResolution) -> i32 {
    match resolution {
        TransactionResolution::Commit => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
        TransactionResolution::Rollback => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
    }
}

fn decode_end_transaction_message_id(message_id: &str) -> ProxyResult<MessageId> {
    MessageDecoder::decode_message_id(&CheetahString::from(message_id)).map_err(|error| {
        ProxyError::invalid_transaction_id(format!("failed to decode transactional message id: {error}"))
    })
}

fn decode_broker_message_id(message_id: &str) -> ProxyResult<MessageId> {
    MessageDecoder::decode_message_id(&CheetahString::from(message_id))
        .map_err(|error| ProxyError::illegal_message_id(format!("failed to decode broker message id: {error}")))
}

fn effective_send_timeout_ms(config: &ClusterConfig, deadline: Option<Duration>) -> u64 {
    effective_request_timeout_ms(config.send_message_timeout_ms, deadline)
}

fn effective_request_timeout_ms(configured: u64, deadline: Option<Duration>) -> u64 {
    let configured = configured.max(1);
    let Some(deadline) = deadline else {
        return configured;
    };

    let deadline_ms = deadline.as_millis().clamp(1, u128::from(u64::MAX)) as u64;
    configured.min(deadline_ms).max(1)
}

fn effective_pop_timeout_ms(config: &ClusterConfig, deadline: Option<Duration>, poll_timeout: Duration) -> u64 {
    let minimum = poll_timeout
        .as_millis()
        .saturating_add(500)
        .clamp(1, u128::from(u64::MAX)) as u64;
    effective_request_timeout_ms(config.mq_client_api_timeout_ms.max(minimum), deadline)
}

fn select_master_broker_addr(route: &TopicRouteData) -> Option<CheetahString> {
    route.broker_datas.iter().find_map(|broker| {
        broker
            .broker_addrs()
            .get(&MASTER_ID)
            .cloned()
            .or_else(|| broker.select_broker_addr())
    })
}

fn convert_topic_message_type(message_type: TopicMessageType) -> ProxyTopicMessageType {
    match message_type {
        TopicMessageType::Unspecified => ProxyTopicMessageType::Unspecified,
        TopicMessageType::Normal => ProxyTopicMessageType::Normal,
        TopicMessageType::Fifo => ProxyTopicMessageType::Fifo,
        TopicMessageType::Delay => ProxyTopicMessageType::Delay,
        TopicMessageType::Transaction => ProxyTopicMessageType::Transaction,
        TopicMessageType::Mixed => ProxyTopicMessageType::Mixed,
        TopicMessageType::Lite => ProxyTopicMessageType::Lite,
        TopicMessageType::Priority => ProxyTopicMessageType::Priority,
    }
}

fn convert_subscription_group(group_config: SubscriptionGroupConfig) -> SubscriptionGroupMetadata {
    SubscriptionGroupMetadata {
        consume_message_orderly: group_config.consume_message_orderly(),
        lite_bind_topic: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::time::Duration;
    use std::time::Instant;

    use cheetah_string::CheetahString;
    use rocketmq_client_rust::proxy_adapter_compat::TopicMessageType;
    use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;
    use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

    use super::build_send_producer;
    use super::cluster_client_config;
    use super::convert_subscription_group;
    use super::convert_topic_message_type;
    use super::select_auth_metadata_broker_addr;
    use super::select_master_broker_addr;
    use super::single_broker_and_topic;
    use super::CachedValue;
    use super::ClusterClient;
    use super::ClusterTaskExecutor;
    use super::ClusterWorkerState;
    use super::RocketmqClusterClient;
    use super::CLUSTER_COMMAND_CAPACITY;
    use crate::config::ClusterConfig;
    use rocketmq_client_rust::proxy_adapter_compat::MessageQueue;
    use rocketmq_proxy_core::ProxyError;
    use rocketmq_proxy_core::ProxyTopicMessageType;
    use rocketmq_proxy_core::ResourceIdentity;

    #[test]
    fn cluster_client_prefers_master_broker_address() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(1_u64, CheetahString::from("127.0.0.2:10911"));
        broker_addrs.insert(0_u64, CheetahString::from("127.0.0.1:10911"));
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 1, 1, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                None,
            )],
            ..Default::default()
        };

        assert_eq!(select_master_broker_addr(&route).unwrap().as_str(), "127.0.0.1:10911");
    }

    #[test]
    fn producer_uses_the_same_isolated_client_domain_as_its_worker() {
        let config = ClusterConfig::default();
        let domain_id = 71_011;
        let producer = build_send_producer(domain_id, &config, "GroupA", 3_000, None);
        let expected = rocketmq_client_rust::proxy_adapter_compat::client_config_for_managed_domain(
            domain_id,
            cluster_client_config(&config),
        );
        let other_domain = rocketmq_client_rust::proxy_adapter_compat::client_config_for_managed_domain(
            domain_id + 1,
            cluster_client_config(&config),
        );

        assert_eq!(
            producer.client_config().build_mq_client_id(),
            expected.build_mq_client_id()
        );
        assert_ne!(
            producer.client_config().build_mq_client_id(),
            other_domain.build_mq_client_id()
        );
    }

    #[test]
    fn auth_metadata_broker_selection_uses_configured_cluster_master() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(1_u64, CheetahString::from("127.0.0.2:10911"));
        broker_addrs.insert(0_u64, CheetahString::from("127.0.0.1:10911"));

        let mut broker_addr_table = HashMap::new();
        broker_addr_table.insert(
            CheetahString::from("broker-a"),
            BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                None,
            ),
        );

        let mut broker_names = HashSet::new();
        broker_names.insert(CheetahString::from("broker-a"));
        let mut cluster_addr_table = HashMap::new();
        cluster_addr_table.insert(CheetahString::from("cluster-a"), broker_names);

        let cluster_info = ClusterInfo::new(Some(broker_addr_table), Some(cluster_addr_table));

        assert_eq!(
            select_auth_metadata_broker_addr(&cluster_info, "cluster-a")
                .unwrap()
                .as_str(),
            "127.0.0.1:10911"
        );
        assert!(select_auth_metadata_broker_addr(&cluster_info, "missing").is_none());
    }

    #[test]
    fn topic_message_type_conversion_matches_proxy_model() {
        assert_eq!(
            convert_topic_message_type(TopicMessageType::Fifo),
            ProxyTopicMessageType::Fifo
        );
        assert_eq!(
            convert_topic_message_type(TopicMessageType::Lite),
            ProxyTopicMessageType::Lite
        );
        assert_eq!(
            convert_topic_message_type(TopicMessageType::Priority),
            ProxyTopicMessageType::Priority
        );
    }

    #[test]
    fn subscription_group_conversion_preserves_order_flag() {
        let mut config = SubscriptionGroupConfig::default();
        config.set_consume_message_orderly(true);

        let converted = convert_subscription_group(config);
        assert!(converted.consume_message_orderly);
    }

    #[test]
    fn cluster_worker_state_discards_expired_route_cache_entries() {
        let mut state = ClusterWorkerState::new();
        state.route_cache.insert(
            "TopicA".to_owned(),
            CachedValue {
                value: TopicRouteData::default(),
                expires_at: Instant::now() - Duration::from_millis(1),
            },
        );

        assert!(state.cached_route("TopicA").is_none());
        assert!(!state.route_cache.contains_key("TopicA"));
    }

    #[test]
    fn cluster_worker_state_returns_live_metadata_cache_entries() {
        let mut state = ClusterWorkerState::new();
        state.topic_message_type_cache.insert(
            "TopicA".to_owned(),
            CachedValue {
                value: ProxyTopicMessageType::Fifo,
                expires_at: Instant::now() + Duration::from_secs(1),
            },
        );

        assert_eq!(
            state.cached_topic_message_type("TopicA"),
            Some(ProxyTopicMessageType::Fifo)
        );
    }

    #[test]
    fn lock_batch_scope_rejects_cross_broker_and_cross_topic_sets() {
        let cross_broker = HashSet::from([
            MessageQueue::from_parts("TopicA", "broker-a", 0),
            MessageQueue::from_parts("TopicA", "broker-b", 1),
        ]);
        let error = single_broker_and_topic(&cross_broker).expect_err("cross-broker set must fail");
        assert!(matches!(error, ProxyError::InvalidMetadata { message } if message.contains("single broker")));

        let cross_topic = HashSet::from([
            MessageQueue::from_parts("TopicA", "broker-a", 0),
            MessageQueue::from_parts("TopicB", "broker-a", 1),
        ]);
        let error = single_broker_and_topic(&cross_topic).expect_err("cross-topic set must fail");
        assert!(matches!(error, ProxyError::InvalidMetadata { message } if message.contains("single topic")));
    }

    #[tokio::test]
    async fn client_without_service_context_returns_typed_startup_error() {
        let client = RocketmqClusterClient::new(ClusterConfig::default());
        let error = client
            .query_route(&ResourceIdentity::new("", "TopicA"))
            .await
            .expect_err("unmanaged client must reject work");
        assert!(matches!(error, ProxyError::Transport { message } if message.contains("ServiceContext")));
    }

    #[test]
    fn cluster_command_queue_is_bounded() {
        let executor = ClusterTaskExecutor::new(ClusterConfig::default(), None, None);
        assert_eq!(executor.sender.max_capacity(), CLUSTER_COMMAND_CAPACITY);
    }

    #[tokio::test]
    async fn managed_worker_stops_cleanly_with_its_task_group() {
        let runtime =
            rocketmq_runtime::RuntimeContext::try_from_current("proxy-cluster-test").expect("test runtime context");
        let service = runtime.service_context("proxy-cluster-test.service");
        let client = RocketmqClusterClient::with_service_context(ClusterConfig::default(), None, &service);
        assert_eq!(service.task_group().task_count(), 0);
        assert_eq!(service.task_group().child_count(), 1);

        drop(client);
        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(service.task_group().task_count(), 0);
    }
}

#[cfg(test)]
#[path = "cluster_behavior_tests.rs"]
mod behavior_tests;
