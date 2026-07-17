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

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use rocketmq_error::RocketMQError;
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;
use rocketmq_protocol::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

use crate::config::ProxyMode;
use crate::context::ProxyContext;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::processor::AckMessageRequest;
use crate::processor::AckMessageResultEntry;
use crate::processor::ChangeInvisibleDurationPlan;
use crate::processor::ChangeInvisibleDurationRequest;
use crate::processor::EndTransactionPlan;
use crate::processor::EndTransactionRequest;
use crate::processor::ForwardMessageToDeadLetterQueuePlan;
use crate::processor::ForwardMessageToDeadLetterQueueRequest;
use crate::processor::GetOffsetPlan;
use crate::processor::GetOffsetRequest;
use crate::processor::PullMessagePlan;
use crate::processor::PullMessageRequest;
use crate::processor::QueryOffsetPlan;
use crate::processor::QueryOffsetRequest;
use crate::processor::RecallMessagePlan;
use crate::processor::RecallMessageRequest;
use crate::processor::ReceiveMessagePlan;
use crate::processor::ReceiveMessageRequest;
use crate::processor::SendMessageRequest;
use crate::processor::SendMessageResultEntry;
use crate::processor::UpdateOffsetPlan;
use crate::processor::UpdateOffsetRequest;
use crate::status::ProxyStatusMapper;
use crate::ResourceIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProxyTopicMessageType {
    Unspecified,
    #[default]
    Normal,
    Fifo,
    Delay,
    Transaction,
    Mixed,
    Lite,
    Priority,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionGroupMetadata {
    pub consume_message_orderly: bool,
    pub lite_bind_topic: Option<String>,
}

#[async_trait]
pub trait RouteService: Send + Sync {
    async fn query_route(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
        endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<TopicRouteData>;
}

#[async_trait]
pub trait MetadataService: Send + Sync {
    async fn topic_message_type(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
    ) -> ProxyResult<ProxyTopicMessageType>;

    async fn subscription_group(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>>;

    async fn user(&self, _context: &ProxyContext, _username: &str) -> ProxyResult<Option<UserInfo>> {
        Ok(None)
    }

    async fn acl(&self, _context: &ProxyContext, _subject: &str) -> ProxyResult<Option<AclInfo>> {
        Ok(None)
    }
}

#[async_trait]
pub trait AssignmentService: Send + Sync {
    async fn query_assignment(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>>;
}

#[async_trait]
pub trait MessageService: Send + Sync {
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
}

#[async_trait]
pub trait ConsumerService: Send + Sync {
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
}

#[async_trait]
pub trait TransactionService: Send + Sync {
    /// Returns the backend-owned producer group used for transactional sends.
    ///
    /// Ingress uses this opaque value only to correlate a later end-transaction
    /// request. The naming policy remains inside the active backend adapter.
    fn transaction_producer_group(&self, _context: &ProxyContext) -> Option<String> {
        None
    }

    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan>;
}

pub trait ServiceManager: Send + Sync {
    fn mode(&self) -> ProxyMode;

    fn route_service(&self) -> Arc<dyn RouteService>;

    fn metadata_service(&self) -> Arc<dyn MetadataService>;

    fn assignment_service(&self) -> Arc<dyn AssignmentService>;

    fn message_service(&self) -> Arc<dyn MessageService>;

    fn consumer_service(&self) -> Arc<dyn ConsumerService>;

    fn transaction_service(&self) -> Arc<dyn TransactionService>;
}

#[derive(Debug, Default)]
pub struct UnsupportedRouteService;

pub type DefaultRouteService = UnsupportedRouteService;

#[async_trait]
impl RouteService for UnsupportedRouteService {
    async fn query_route(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<TopicRouteData> {
        Err(ProxyError::not_implemented("route service"))
    }
}

#[derive(Debug, Default)]
pub struct DefaultMetadataService;

#[async_trait]
impl MetadataService for DefaultMetadataService {
    async fn topic_message_type(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
    ) -> ProxyResult<ProxyTopicMessageType> {
        Ok(ProxyTopicMessageType::Normal)
    }

    async fn subscription_group(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        _group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        Ok(None)
    }
}

#[derive(Debug, Default)]
pub struct DefaultAssignmentService;

#[async_trait]
impl AssignmentService for DefaultAssignmentService {
    async fn query_assignment(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        _group: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        Ok(None)
    }
}

#[derive(Debug, Default)]
pub struct DefaultMessageService;

#[async_trait]
impl MessageService for DefaultMessageService {
    async fn send_message(
        &self,
        _context: &ProxyContext,
        _request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        Err(ProxyError::not_implemented("message service"))
    }

    async fn recall_message(
        &self,
        _context: &ProxyContext,
        _request: &RecallMessageRequest,
    ) -> ProxyResult<RecallMessagePlan> {
        Err(ProxyError::not_implemented("message recall service"))
    }
}

#[derive(Debug, Default)]
pub struct DefaultConsumerService;

#[async_trait]
impl ConsumerService for DefaultConsumerService {
    async fn receive_message(
        &self,
        _context: &ProxyContext,
        _request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan> {
        Err(ProxyError::not_implemented("consumer service"))
    }

    async fn pull_message(
        &self,
        _context: &ProxyContext,
        _request: &PullMessageRequest,
    ) -> ProxyResult<PullMessagePlan> {
        Err(ProxyError::not_implemented("consumer service"))
    }

    async fn ack_message(
        &self,
        _context: &ProxyContext,
        _request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        Err(ProxyError::not_implemented("consumer service"))
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        _context: &ProxyContext,
        _request: &ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
        Err(ProxyError::not_implemented("consumer service"))
    }

    async fn change_invisible_duration(
        &self,
        _context: &ProxyContext,
        _request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        Err(ProxyError::not_implemented("consumer service"))
    }

    async fn update_offset(
        &self,
        _context: &ProxyContext,
        _request: &UpdateOffsetRequest,
    ) -> ProxyResult<UpdateOffsetPlan> {
        Err(ProxyError::not_implemented("consumer service"))
    }

    async fn get_offset(&self, _context: &ProxyContext, _request: &GetOffsetRequest) -> ProxyResult<GetOffsetPlan> {
        Err(ProxyError::not_implemented("consumer service"))
    }

    async fn query_offset(
        &self,
        _context: &ProxyContext,
        _request: &QueryOffsetRequest,
    ) -> ProxyResult<QueryOffsetPlan> {
        Err(ProxyError::not_implemented("consumer service"))
    }
}

#[derive(Debug, Default)]
pub struct DefaultTransactionService;

#[async_trait]
impl TransactionService for DefaultTransactionService {
    async fn end_transaction(
        &self,
        _context: &ProxyContext,
        _request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan> {
        Err(ProxyError::not_implemented("transaction service"))
    }
}

#[derive(Clone, Default)]
pub struct StaticRouteService {
    routes: Arc<DashMap<ResourceIdentity, TopicRouteData>>,
}

impl StaticRouteService {
    pub fn insert(&self, topic: ResourceIdentity, route: TopicRouteData) {
        self.routes.insert(topic, route);
    }
}

#[async_trait]
impl RouteService for StaticRouteService {
    async fn query_route(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<TopicRouteData> {
        self.routes
            .get(topic)
            .map(|entry| entry.clone())
            .ok_or_else(|| RocketMQError::route_not_found(topic.name()).into())
    }
}

#[derive(Clone, Default)]
pub struct StaticMetadataService {
    topic_message_types: Arc<DashMap<ResourceIdentity, ProxyTopicMessageType>>,
    subscription_groups: Arc<DashMap<ResourceIdentity, SubscriptionGroupMetadata>>,
}

impl StaticMetadataService {
    pub fn set_topic_message_type(&self, topic: ResourceIdentity, message_type: ProxyTopicMessageType) {
        self.topic_message_types.insert(topic, message_type);
    }

    pub fn set_subscription_group(&self, group: ResourceIdentity, metadata: SubscriptionGroupMetadata) {
        self.subscription_groups.insert(group, metadata);
    }
}

#[async_trait]
impl MetadataService for StaticMetadataService {
    async fn topic_message_type(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
    ) -> ProxyResult<ProxyTopicMessageType> {
        Ok(self
            .topic_message_types
            .get(topic)
            .map(|entry| *entry)
            .unwrap_or(ProxyTopicMessageType::Normal))
    }

    async fn subscription_group(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        Ok(self.subscription_groups.get(group).map(|entry| entry.clone()))
    }
}

#[derive(Clone, Default)]
pub struct StaticMessageService {
    send_status: SendStatus,
}

impl StaticMessageService {
    pub fn with_send_status(send_status: SendStatus) -> Self {
        Self { send_status }
    }
}

#[async_trait]
impl MessageService for StaticMessageService {
    async fn send_message(
        &self,
        _context: &ProxyContext,
        request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        Ok(request
            .messages
            .iter()
            .enumerate()
            .map(|(index, message)| {
                let mut send_result = SendResult::new(
                    self.send_status,
                    Some(message.client_message_id.as_str().into()),
                    None,
                    None,
                    index as u64,
                );
                if message
                    .message
                    .property("TRAN_MSG")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(false)
                {
                    send_result.set_transaction_id(format!("tx-{}", message.client_message_id));
                    send_result.set_offset_msg_id(format!("offset-{}", message.client_message_id));
                }
                SendMessageResultEntry {
                    status: ProxyStatusMapper::from_send_result_payload(&send_result),
                    send_result: Some(send_result),
                }
            })
            .collect())
    }

    async fn recall_message(
        &self,
        _context: &ProxyContext,
        request: &RecallMessageRequest,
    ) -> ProxyResult<RecallMessagePlan> {
        Ok(RecallMessagePlan {
            status: ProxyStatusMapper::ok_payload(),
            message_id: request.recall_handle.clone(),
        })
    }
}
