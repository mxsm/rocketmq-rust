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

//! Backend-neutral contracts shared by Proxy ingress and adapters.
//!
//! This module is the dependency root for route, metadata, assignment, message,
//! consumer, transaction, and service-manager interfaces. It must not import
//! Proxy ingress modules.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;
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
use crate::session::LiteSubscriptionSyncRequest;
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

/// Object-safe future returned by Proxy service contracts.
pub type ProxyServiceFuture<'a, T> = Pin<Box<dyn Future<Output = ProxyResult<T>> + Send + 'a>>;

/// Route lookup contract shared by the ingress and backend adapters.
pub trait RouteService: Send + Sync {
    fn query_route<'a>(
        &'a self,
        context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, TopicRouteData>;
}

/// Metadata lookup contract shared by the ingress and backend adapters.
pub trait MetadataService: Send + Sync {
    /// Verifies that the backing metadata route is available before listeners publish readiness.
    fn readiness_check(&self) -> ProxyServiceFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn topic_message_type<'a>(
        &'a self,
        context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, ProxyTopicMessageType>;

    fn subscription_group<'a>(
        &'a self,
        context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        group: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, Option<SubscriptionGroupMetadata>>;

    fn user<'a>(&'a self, _context: &'a ProxyContext, _username: &'a str) -> ProxyServiceFuture<'a, Option<UserInfo>> {
        Box::pin(async { Ok(None) })
    }

    fn acl<'a>(&'a self, _context: &'a ProxyContext, _subject: &'a str) -> ProxyServiceFuture<'a, Option<AclInfo>> {
        Box::pin(async { Ok(None) })
    }
}

/// Assignment lookup contract shared by the ingress and backend adapters.
pub trait AssignmentService: Send + Sync {
    fn query_assignment<'a>(
        &'a self,
        context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        group: &'a ResourceIdentity,
        endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, Option<Vec<MessageQueueAssignment>>>;
}

/// Message operation contract shared by the ingress and backend adapters.
pub trait MessageService: Send + Sync {
    fn send_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a SendMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<SendMessageResultEntry>>;

    fn recall_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a RecallMessageRequest,
    ) -> ProxyServiceFuture<'a, RecallMessagePlan>;
}

/// Consumer operation contract shared by the ingress and backend adapters.
pub trait ConsumerService: Send + Sync {
    fn sync_lite_subscription<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _client_id: &'a str,
        _request: &'a LiteSubscriptionSyncRequest,
    ) -> ProxyServiceFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn receive_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a ReceiveMessageRequest,
    ) -> ProxyServiceFuture<'a, ReceiveMessagePlan>;

    fn pull_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a PullMessageRequest,
    ) -> ProxyServiceFuture<'a, PullMessagePlan>;

    fn ack_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a AckMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<AckMessageResultEntry>>;

    fn forward_message_to_dead_letter_queue<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyServiceFuture<'a, ForwardMessageToDeadLetterQueuePlan>;

    fn change_invisible_duration<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a ChangeInvisibleDurationRequest,
    ) -> ProxyServiceFuture<'a, ChangeInvisibleDurationPlan>;

    fn update_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a UpdateOffsetRequest,
    ) -> ProxyServiceFuture<'a, UpdateOffsetPlan>;

    fn get_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a GetOffsetRequest,
    ) -> ProxyServiceFuture<'a, GetOffsetPlan>;

    fn query_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a QueryOffsetRequest,
    ) -> ProxyServiceFuture<'a, QueryOffsetPlan>;
}

/// Transaction operation contract shared by the ingress and backend adapters.
pub trait TransactionService: Send + Sync {
    /// Returns the backend-owned producer group used for transactional sends.
    ///
    /// Ingress uses this opaque value only to correlate a later end-transaction
    /// request. The naming policy remains inside the active backend adapter.
    fn transaction_producer_group(&self, _context: &ProxyContext) -> Option<String> {
        None
    }

    fn end_transaction<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a EndTransactionRequest,
    ) -> ProxyServiceFuture<'a, EndTransactionPlan>;
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

impl RouteService for UnsupportedRouteService {
    fn query_route<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _topic: &'a ResourceIdentity,
        _endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, TopicRouteData> {
        Box::pin(async { Err(ProxyError::not_implemented("route service")) })
    }
}

#[derive(Debug, Default)]
pub struct DefaultMetadataService;

impl MetadataService for DefaultMetadataService {
    fn topic_message_type<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _topic: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, ProxyTopicMessageType> {
        Box::pin(async { Ok(ProxyTopicMessageType::Normal) })
    }

    fn subscription_group<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _topic: &'a ResourceIdentity,
        _group: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, Option<SubscriptionGroupMetadata>> {
        Box::pin(async { Ok(None) })
    }
}

#[derive(Debug, Default)]
pub struct DefaultAssignmentService;

impl AssignmentService for DefaultAssignmentService {
    fn query_assignment<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _topic: &'a ResourceIdentity,
        _group: &'a ResourceIdentity,
        _endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, Option<Vec<MessageQueueAssignment>>> {
        Box::pin(async { Ok(None) })
    }
}

#[derive(Debug, Default)]
pub struct DefaultMessageService;

impl MessageService for DefaultMessageService {
    fn send_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a SendMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<SendMessageResultEntry>> {
        Box::pin(async { Err(ProxyError::not_implemented("message service")) })
    }

    fn recall_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a RecallMessageRequest,
    ) -> ProxyServiceFuture<'a, RecallMessagePlan> {
        Box::pin(async { Err(ProxyError::not_implemented("message recall service")) })
    }
}

#[derive(Debug, Default)]
pub struct DefaultConsumerService;

impl ConsumerService for DefaultConsumerService {
    fn receive_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a ReceiveMessageRequest,
    ) -> ProxyServiceFuture<'a, ReceiveMessagePlan> {
        Box::pin(async { Err(ProxyError::not_implemented("consumer service")) })
    }

    fn pull_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a PullMessageRequest,
    ) -> ProxyServiceFuture<'a, PullMessagePlan> {
        Box::pin(async { Err(ProxyError::not_implemented("consumer service")) })
    }

    fn ack_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a AckMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<AckMessageResultEntry>> {
        Box::pin(async { Err(ProxyError::not_implemented("consumer service")) })
    }

    fn forward_message_to_dead_letter_queue<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyServiceFuture<'a, ForwardMessageToDeadLetterQueuePlan> {
        Box::pin(async { Err(ProxyError::not_implemented("consumer service")) })
    }

    fn change_invisible_duration<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a ChangeInvisibleDurationRequest,
    ) -> ProxyServiceFuture<'a, ChangeInvisibleDurationPlan> {
        Box::pin(async { Err(ProxyError::not_implemented("consumer service")) })
    }

    fn update_offset<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a UpdateOffsetRequest,
    ) -> ProxyServiceFuture<'a, UpdateOffsetPlan> {
        Box::pin(async { Err(ProxyError::not_implemented("consumer service")) })
    }

    fn get_offset<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a GetOffsetRequest,
    ) -> ProxyServiceFuture<'a, GetOffsetPlan> {
        Box::pin(async { Err(ProxyError::not_implemented("consumer service")) })
    }

    fn query_offset<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a QueryOffsetRequest,
    ) -> ProxyServiceFuture<'a, QueryOffsetPlan> {
        Box::pin(async { Err(ProxyError::not_implemented("consumer service")) })
    }
}

#[derive(Debug, Default)]
pub struct DefaultTransactionService;

impl TransactionService for DefaultTransactionService {
    fn end_transaction<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _request: &'a EndTransactionRequest,
    ) -> ProxyServiceFuture<'a, EndTransactionPlan> {
        Box::pin(async { Err(ProxyError::not_implemented("transaction service")) })
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

impl RouteService for StaticRouteService {
    fn query_route<'a>(
        &'a self,
        _context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        _endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, TopicRouteData> {
        Box::pin(async move {
            self.routes
                .get(topic)
                .map(|entry| entry.clone())
                .ok_or_else(|| RocketMQError::route_not_found(topic.name()).into())
        })
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

impl MetadataService for StaticMetadataService {
    fn topic_message_type<'a>(
        &'a self,
        _context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, ProxyTopicMessageType> {
        Box::pin(async move {
            Ok(self
                .topic_message_types
                .get(topic)
                .map(|entry| *entry)
                .unwrap_or(ProxyTopicMessageType::Normal))
        })
    }

    fn subscription_group<'a>(
        &'a self,
        _context: &'a ProxyContext,
        _topic: &'a ResourceIdentity,
        group: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, Option<SubscriptionGroupMetadata>> {
        Box::pin(async move { Ok(self.subscription_groups.get(group).map(|entry| entry.clone())) })
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

impl MessageService for StaticMessageService {
    fn send_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        request: &'a SendMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<SendMessageResultEntry>> {
        Box::pin(async move {
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
        })
    }

    fn recall_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        request: &'a RecallMessageRequest,
    ) -> ProxyServiceFuture<'a, RecallMessagePlan> {
        Box::pin(async move {
            Ok(RecallMessagePlan {
                status: ProxyStatusMapper::ok_payload(),
                message_id: request.recall_handle.clone(),
            })
        })
    }
}
