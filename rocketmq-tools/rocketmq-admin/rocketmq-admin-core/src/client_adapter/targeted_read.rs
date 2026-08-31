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

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::client_connection::QueryTopicProducerConnectionsRequest;
use crate::core::client_connection::QueryTopicProducerConnectionsResult;
use crate::core::config_state::ConfigStateQueryAdmin;
use crate::core::config_state::ConsumerGroupConfigStateRequest;
use crate::core::config_state::ConsumerGroupConfigStateResult;
use crate::core::config_state::TopicConfigStateRequest;
use crate::core::config_state::TopicConfigStateResult;
use crate::core::message::MessageMetadata;
use crate::core::message::MessageMetadataQueryAdmin;
use crate::core::message::MessageMetadataRequest;
use crate::core::query::AdminQueryResult;
use crate::core::AdminFuture;

impl MessageMetadataQueryAdmin for AdminSession {
    fn query_message_metadata<'a>(
        &'a mut self,
        request: &'a MessageMetadataRequest,
    ) -> AdminFuture<'a, MessageMetadata> {
        Box::pin(async move {
            self.ensure_open()?;
            crate::read_queries::query_message_metadata(&self.inner, request).await
        })
    }
}

impl ConfigStateQueryAdmin for AdminSession {
    fn query_topic_config_state<'a>(
        &'a mut self,
        request: &'a TopicConfigStateRequest,
    ) -> AdminFuture<'a, AdminQueryResult<TopicConfigStateResult>> {
        Box::pin(async move {
            self.ensure_open()?;
            crate::read_queries::query_topic_config_state(&self.inner, request).await
        })
    }

    fn query_consumer_group_config_state<'a>(
        &'a mut self,
        request: &'a ConsumerGroupConfigStateRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ConsumerGroupConfigStateResult>> {
        Box::pin(async move {
            self.ensure_open()?;
            crate::read_queries::query_consumer_group_config_state(&self.inner, request).await
        })
    }
}

pub(crate) fn query_topic_producer_connections<'a>(
    session: &'a mut AdminSession,
    request: &'a QueryTopicProducerConnectionsRequest,
) -> AdminFuture<'a, AdminQueryResult<QueryTopicProducerConnectionsResult>> {
    Box::pin(async move {
        session.ensure_open()?;
        crate::read_queries::query_topic_producer_connections(&session.inner, request).await
    })
}
