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

//! Calls of the observation Tools that have no Resource of their own.
//!
//! Each one decodes its arguments, reads through the adapter and composes the result. An error at
//! any of these steps is returned, and the dispatcher renders it like every other Tool error.

use rmcp::model::JsonObject;

use super::decode_args;
use super::success_unlinked_result;
use super::ToolExecutor;
use super::ToolFailure;
use crate::adapter::query_facade::ReadOnlyQuery;
use crate::guard::sanitizer::ToolOutput;
use crate::tools::broker_tools;
use crate::tools::catalog::ToolDescriptor;
use crate::tools::catalog::ToolId;
use crate::tools::consumer_tools;
use crate::tools::message_tools;
#[cfg(feature = "security-admin")]
use crate::tools::security_tools;
use crate::tools::topic_tools;

/// Returns the argument by which a Tool that looks messages up names what it looks for.
pub(super) const fn lookup_argument(tool: ToolId) -> &'static str {
    match tool {
        ToolId::FindMessagesByKey => "/key",
        _ => "/message_id",
    }
}

impl<A> ToolExecutor<A>
where
    A: ReadOnlyQuery,
{
    pub(super) async fn list_topic_consumers(
        &self,
        descriptor: ToolDescriptor,
        request_id: &str,
        arguments: JsonObject,
    ) -> Result<ToolOutput, ToolFailure> {
        let args = decode_args::<topic_tools::ListTopicConsumersArgs>(arguments)?;
        let output = self.adapter.topic_consumers(args).await?;
        let summary = format!(
            "Topic {} on cluster {} returned {} of {} consumer groups.",
            output.topic, output.cluster, output.page.count, output.page.total_count
        );
        let cluster = output.cluster.clone();
        success_unlinked_result(descriptor, request_id, cluster, summary, output)
    }

    pub(super) async fn consumer_running_info(
        &self,
        descriptor: ToolDescriptor,
        request_id: &str,
        arguments: JsonObject,
    ) -> Result<ToolOutput, ToolFailure> {
        let args = decode_args::<consumer_tools::GetConsumerRunningInfoArgs>(arguments)?;
        let output = self.adapter.consumer_running_info(args).await?;
        let summary = format!(
            "Client {} of consumer group {} on cluster {} holds {} subscriptions and returned {} of {} queues.",
            output.client_alias,
            output.consumer_group,
            output.cluster,
            output.subscriptions.len(),
            output.page.count,
            output.queue_count
        );
        let cluster = output.cluster.clone();
        success_unlinked_result(descriptor, request_id, cluster, summary, output)
    }

    pub(super) async fn dlq_summary(
        &self,
        descriptor: ToolDescriptor,
        request_id: &str,
        arguments: JsonObject,
    ) -> Result<ToolOutput, ToolFailure> {
        let args = decode_args::<consumer_tools::GetDlqSummaryArgs>(arguments)?;
        let output = self.adapter.dlq_summary(args).await?;
        let summary = format!(
            "Consumer group {} on cluster {} holds {} dead letters in {} queues on {} Brokers.",
            output.consumer_group,
            output.cluster,
            output.message_count,
            output.queue_count,
            output.brokers.len()
        );
        let cluster = output.cluster.clone();
        success_unlinked_result(descriptor, request_id, cluster, summary, output)
    }

    pub(super) async fn lite_broker_info(
        &self,
        descriptor: ToolDescriptor,
        request_id: &str,
        arguments: JsonObject,
    ) -> Result<ToolOutput, ToolFailure> {
        let args = decode_args::<broker_tools::GetLiteBrokerInfoArgs>(arguments)?;
        let output = self.adapter.lite_broker_info(args).await?;
        let summary = format!(
            "Broker {} on cluster {} holds {} of at most {} Lite Topic queues and knows {} Lite Topics.",
            output.broker_name, output.cluster, output.current_lmq_count, output.max_lmq_count, output.lite_topic_count
        );
        let cluster = output.cluster.clone();
        success_unlinked_result(descriptor, request_id, cluster, summary, output)
    }

    /// Lists the ACL users of a cluster. The summary gives counts and names no user.
    #[cfg(feature = "security-admin")]
    pub(super) async fn list_acl_users(
        &self,
        descriptor: ToolDescriptor,
        request_id: &str,
        arguments: JsonObject,
    ) -> Result<ToolOutput, ToolFailure> {
        let args = decode_args::<security_tools::ListAclUsersArgs>(arguments)?;
        let output = self.adapter.list_acl_users(args).await?;
        let summary = format!(
            "Cluster {} holds {} ACL users in {} rows; {} of {} Brokers answered.",
            output.cluster,
            output.user_count,
            output.users.len(),
            output
                .queried_broker_count
                .saturating_sub(output.unanswered_brokers.len()),
            output.queried_broker_count
        );
        let cluster = output.cluster.clone();
        success_unlinked_result(descriptor, request_id, cluster, summary, output)
    }

    /// Runs one of the two Tools that look messages up. Neither summary repeats what the caller
    /// named the message by.
    pub(super) async fn look_up_messages(
        &self,
        tool: ToolId,
        descriptor: ToolDescriptor,
        request_id: &str,
        arguments: JsonObject,
    ) -> Result<ToolOutput, ToolFailure> {
        if matches!(tool, ToolId::FindMessagesByKey) {
            let args = decode_args::<message_tools::FindMessagesByKeyArgs>(arguments)?;
            let output = self.adapter.find_messages_by_key(args).await?;
            let summary = format!(
                "Topic {} on cluster {} returned {} messages that carry the key, stored between {} and {}.",
                output.topic,
                output.cluster,
                output.messages.len(),
                output.window_begin_at,
                output.window_end_at
            );
            let cluster = output.cluster.clone();
            return success_unlinked_result(descriptor, request_id, cluster, summary, output);
        }
        let args = decode_args::<message_tools::QueryMessageTraceArgs>(arguments)?;
        let output = self.adapter.message_trace(args).await?;
        let summary = if output.trace_topic_found {
            format!(
                "Message {} on cluster {} has {} trace records.",
                output.unique_message_alias,
                output.cluster,
                output.records.len()
            )
        } else {
            format!(
                "Cluster {} has no trace Topic {}, so no message is traced there.",
                output.cluster, output.trace_topic
            )
        };
        let cluster = output.cluster.clone();
        success_unlinked_result(descriptor, request_id, cluster, summary, output)
    }
}
