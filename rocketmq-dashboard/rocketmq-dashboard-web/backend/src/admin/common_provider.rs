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
use crate::admin::DashboardAdminClient;
use crate::error::DashboardError;
use crate::model::BrokerConfigUpdateRequest;
use crate::model::BrokerConfigView;
use crate::model::BrokerListView;
use crate::model::BrokerRuntimeStats;
use crate::model::ConsumerListView;
use crate::model::ConsumerProgress;
use crate::model::ConsumerResetOffsetRequest;
use crate::model::DashboardOverview;
use crate::model::MessageListView;
use crate::model::MessageResendRequest;
use crate::model::MessageTraceView;
use crate::model::MutationResult;
use crate::model::ProducerConnectionView;
use crate::model::ProducerInfo;
use crate::model::TopicInfo;
use crate::model::TopicListView;
use crate::model::TopicMutationRequest;
use crate::model::TopicRouteInfo;
use crate::model::TopicStatsInfo;
use rocketmq_dashboard_common::AdminFuture;
use rocketmq_dashboard_common::DashboardAdminProvider;

impl DashboardAdminProvider for DashboardAdminClient {
    type Error = DashboardError;
    type DashboardOverview = DashboardOverview;
    type TopicList = TopicListView;
    type Topic = TopicInfo;
    type TopicRoute = TopicRouteInfo;
    type TopicStats = TopicStatsInfo;
    type TopicMutationRequest = TopicMutationRequest;
    type TopicMutationResult = MutationResult;
    type ConsumerList = ConsumerListView;
    type ConsumerProgress = ConsumerProgress;
    type ConsumerOffsetResetRequest = ConsumerResetOffsetRequest;
    type ConsumerOffsetResetResult = MutationResult;
    type ProducerList = Vec<ProducerInfo>;
    type ProducerConnections = ProducerConnectionView;
    type BrokerList = BrokerListView;
    type BrokerRuntimeStats = BrokerRuntimeStats;
    type BrokerConfig = BrokerConfigView;
    type BrokerConfigUpdateRequest = BrokerConfigUpdateRequest;
    type BrokerConfigUpdateResult = MutationResult;
    type MessageList = MessageListView;
    type Message = MessageListView;
    type MessageTrace = MessageTraceView;
    type MessageResendRequest = MessageResendRequest;
    type MessageResendResult = MutationResult;

    fn dashboard_overview(&self) -> AdminFuture<'_, Self::DashboardOverview, Self::Error> {
        Box::pin(DashboardAdminClient::dashboard_overview(self))
    }

    fn list_topics(&self) -> AdminFuture<'_, Self::TopicList, Self::Error> {
        Box::pin(DashboardAdminClient::list_topics(self))
    }

    fn get_topic<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Self::Topic, Self::Error> {
        Box::pin(DashboardAdminClient::get_topic(self, topic))
    }

    fn topic_route<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Self::TopicRoute, Self::Error> {
        Box::pin(DashboardAdminClient::topic_route(self, topic))
    }

    fn topic_stats<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Self::TopicStats, Self::Error> {
        Box::pin(DashboardAdminClient::topic_stats(self, topic))
    }

    fn create_or_update_topic(
        &self,
        request: Self::TopicMutationRequest,
    ) -> AdminFuture<'_, Self::TopicMutationResult, Self::Error> {
        Box::pin(DashboardAdminClient::create_or_update_topic(self, request))
    }

    fn delete_topic<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Self::TopicMutationResult, Self::Error> {
        Box::pin(DashboardAdminClient::delete_topic(self, topic))
    }

    fn list_consumer_groups(&self) -> AdminFuture<'_, Self::ConsumerList, Self::Error> {
        Box::pin(DashboardAdminClient::list_consumer_groups(self))
    }

    fn consumer_progress<'a>(&'a self, group: &'a str) -> AdminFuture<'a, Self::ConsumerProgress, Self::Error> {
        Box::pin(DashboardAdminClient::consumer_progress(self, group))
    }

    fn reset_consumer_offset(
        &self,
        group: String,
        request: Self::ConsumerOffsetResetRequest,
    ) -> AdminFuture<'_, Self::ConsumerOffsetResetResult, Self::Error> {
        Box::pin(async move { DashboardAdminClient::reset_consumer_offset(self, &group, request).await })
    }

    fn list_producers(&self) -> AdminFuture<'_, Self::ProducerList, Self::Error> {
        Box::pin(DashboardAdminClient::list_producers(self))
    }

    fn producer_connections(
        &self,
        topic: Option<String>,
        producer_group: Option<String>,
    ) -> AdminFuture<'_, Self::ProducerConnections, Self::Error> {
        Box::pin(async move {
            let topic = topic
                .ok_or_else(|| DashboardError::Validation("Producer connection query requires topic".to_string()))?;
            let producer_group = producer_group.ok_or_else(|| {
                DashboardError::Validation("Producer connection query requires producerGroup".to_string())
            })?;
            DashboardAdminClient::producer_connections(self, &topic, &producer_group).await
        })
    }

    fn list_brokers(&self) -> AdminFuture<'_, Self::BrokerList, Self::Error> {
        Box::pin(DashboardAdminClient::list_brokers(self))
    }

    fn broker_runtime_stats<'a>(
        &'a self,
        broker_name: &'a str,
    ) -> AdminFuture<'a, Self::BrokerRuntimeStats, Self::Error> {
        Box::pin(DashboardAdminClient::broker_runtime_stats(self, broker_name))
    }

    fn broker_config<'a>(&'a self, broker_name: &'a str) -> AdminFuture<'a, Self::BrokerConfig, Self::Error> {
        Box::pin(DashboardAdminClient::broker_config(self, broker_name))
    }

    fn update_broker_config(
        &self,
        broker_name: String,
        request: Self::BrokerConfigUpdateRequest,
    ) -> AdminFuture<'_, Self::BrokerConfigUpdateResult, Self::Error> {
        Box::pin(async move { DashboardAdminClient::update_broker_config(self, &broker_name, request).await })
    }

    fn query_message_by_key(&self, topic: String, key: String) -> AdminFuture<'_, Self::MessageList, Self::Error> {
        Box::pin(async move { DashboardAdminClient::query_message_by_key(self, &topic, &key).await })
    }

    fn query_message_by_id(&self, message_id: String) -> AdminFuture<'_, Self::Message, Self::Error> {
        Box::pin(async move { DashboardAdminClient::query_message_by_id(self, &message_id).await })
    }

    fn message_trace(
        &self,
        message_id: String,
        topic: String,
        trace_topic: Option<String>,
    ) -> AdminFuture<'_, Self::MessageTrace, Self::Error> {
        Box::pin(async move {
            let trace_topic = trace_topic.unwrap_or_else(|| "RMQ_SYS_TRACE_TOPIC".to_string());
            DashboardAdminClient::message_trace(self, &message_id, Some(&topic), &trace_topic).await
        })
    }

    fn resend_message(
        &self,
        message_id: String,
        request: Self::MessageResendRequest,
    ) -> AdminFuture<'_, Self::MessageResendResult, Self::Error> {
        Box::pin(async move { DashboardAdminClient::resend_message(self, &message_id, request).await })
    }
}
