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

//! The Consumer Groups that consume one Topic.

use rocketmq_admin_core::core::topic_observation::QueryTopicSubscribersRequest;
use rocketmq_admin_core::core::topic_observation::TopicSubscriberQueryAdmin;

use super::map_admin_error;
use super::AdminCoreSession;
use crate::infrastructure::snapshot::RetainedSize;
use crate::model::contract::QueryPayload;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

/// The Consumer Group names one Topic is consumed by, sorted and without duplicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionTopicConsumers {
    pub groups: Vec<String>,
    /// Whether names beyond the bound of one observation were left out.
    pub truncated: bool,
}

impl RetainedSize for SessionTopicConsumers {
    fn retained_heap_size(&self) -> usize {
        self.groups.retained_heap_size()
    }
}

impl AdminCoreSession {
    pub(super) async fn query_topic_consumers_observation(
        &mut self,
        topic: &str,
    ) -> Result<QueryPayload<SessionTopicConsumers>, ToolFailure> {
        let request = QueryTopicSubscribersRequest::try_new(self.cluster.rocketmq_cluster_name.clone(), topic)
            .map_err(|_| ToolFailure::Rejected(ToolRejection::InvalidArguments { _source: None }))?;
        let result = self
            .admin_mut()?
            .query_topic_subscribers(&request)
            .await
            .map_err(map_admin_error(NotFoundEntity::Topic))?;
        Ok(QueryPayload::from_admin(result).map(|result| SessionTopicConsumers {
            groups: result.consumer_groups,
            truncated: result.truncated,
        }))
    }
}
