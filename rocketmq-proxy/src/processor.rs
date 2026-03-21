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
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::context::ProxyContext;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyResult;
use crate::service::ProxyTopicMessageType;
use crate::service::ResourceIdentity;
use crate::service::ServiceManager;
use crate::service::SubscriptionGroupMetadata;

#[derive(Debug, Clone)]
pub struct QueryRouteRequest {
    pub topic: ResourceIdentity,
    pub endpoints: Vec<ResolvedEndpoint>,
}

#[derive(Debug, Clone)]
pub struct QueryRoutePlan {
    pub route: TopicRouteData,
    pub topic_message_type: ProxyTopicMessageType,
}

#[derive(Debug, Clone)]
pub struct QueryAssignmentRequest {
    pub topic: ResourceIdentity,
    pub group: ResourceIdentity,
    pub endpoints: Vec<ResolvedEndpoint>,
}

#[derive(Debug, Clone)]
pub struct QueryAssignmentPlan {
    pub route: TopicRouteData,
    pub assignments: Option<Vec<MessageQueueAssignment>>,
    pub subscription_group: Option<SubscriptionGroupMetadata>,
}

#[async_trait]
pub trait MessagingProcessor: Send + Sync {
    async fn query_route(&self, context: &ProxyContext, request: QueryRouteRequest) -> ProxyResult<QueryRoutePlan>;

    async fn query_assignment(
        &self,
        context: &ProxyContext,
        request: QueryAssignmentRequest,
    ) -> ProxyResult<QueryAssignmentPlan>;
}

#[derive(Clone)]
pub struct DefaultMessagingProcessor {
    service_manager: Arc<dyn ServiceManager>,
}

impl DefaultMessagingProcessor {
    pub fn new(service_manager: Arc<dyn ServiceManager>) -> Self {
        Self { service_manager }
    }

    pub fn service_manager(&self) -> &Arc<dyn ServiceManager> {
        &self.service_manager
    }
}

#[async_trait]
impl MessagingProcessor for DefaultMessagingProcessor {
    async fn query_route(&self, context: &ProxyContext, request: QueryRouteRequest) -> ProxyResult<QueryRoutePlan> {
        let route_service = self.service_manager.route_service();
        let metadata_service = self.service_manager.metadata_service();

        let route = route_service
            .query_route(context, &request.topic, &request.endpoints)
            .await?;
        let topic_message_type = metadata_service.topic_message_type(context, &request.topic).await?;

        Ok(QueryRoutePlan {
            route,
            topic_message_type,
        })
    }

    async fn query_assignment(
        &self,
        context: &ProxyContext,
        request: QueryAssignmentRequest,
    ) -> ProxyResult<QueryAssignmentPlan> {
        let route_service = self.service_manager.route_service();
        let assignment_service = self.service_manager.assignment_service();
        let metadata_service = self.service_manager.metadata_service();

        let route = route_service
            .query_route(context, &request.topic, &request.endpoints)
            .await?;
        let assignments = assignment_service
            .query_assignment(context, &request.topic, &request.group, &request.endpoints)
            .await?;
        let subscription_group = metadata_service
            .subscription_group(context, &request.topic, &request.group)
            .await?;

        Ok(QueryAssignmentPlan {
            route,
            assignments,
            subscription_group,
        })
    }
}
