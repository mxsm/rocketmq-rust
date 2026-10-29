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

//! Read-only Trace projection with no payload or arbitrary SDK object in the UI.

use super::{GpuiAdminProvider, ProviderFailure, messages::require_direct, query_for_revision, select_admin};
use rocketmq_admin_core::core::{message_index, message_trace::MessageTraceLookupRequest};
use rocketmq_dashboard_common::{
    MessageIdentity, MessageTextPreview, MessageTrace, MessageTraceNode, MessageTraceQuery, MessageTraceStep,
};
use std::sync::Arc;

impl GpuiAdminProvider {
    pub async fn message_trace(
        self: &Arc<Self>,
        revision: u64,
        query: MessageTraceQuery,
    ) -> Result<MessageTrace, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-message-trace", move |cancellation| async move {
            require_direct(this.snapshot_for_revision(revision)?.scope)?;
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let request = MessageTraceLookupRequest {
                topic: query.identity.topic.clone(),
                message_id: query.identity.message_id.clone(),
                trace_topic: query.trace_topic.clone(),
            };
            let result = select_admin(cancellation, session.message_trace(&request)).await?;
            if result.topic != query.identity.topic
                || result.message_id != query.identity.message_id
                || result.trace_topic != query.trace_topic
            {
                return Err(ProviderFailure::invalid_data(
                    "Trace response did not match the selected message and Trace Topic.",
                ));
            }
            let preview = |value: Option<String>| value.as_deref().map(MessageTextPreview::bounded);
            let mut trace = MessageTrace {
                identity: MessageIdentity {
                    topic: result.topic,
                    message_id: result.message_id,
                },
                producer_message_id: preview(result.producer_message_id),
                trace_topic: result.trace_topic,
                trace_topic_found: result.trace_topic_found,
                limit_reached: result.limit_reached,
                unanswered_brokers: result
                    .unanswered_brokers
                    .iter()
                    .map(|name| MessageTextPreview::bounded(name))
                    .collect(),
                nodes: result
                    .nodes
                    .into_iter()
                    .map(|node| MessageTraceNode {
                        step: match node.step {
                            message_index::MessageTraceStep::Publish => MessageTraceStep::Publish,
                            message_index::MessageTraceStep::ConsumeStart => MessageTraceStep::ConsumeStart,
                            message_index::MessageTraceStep::ConsumeEnd => MessageTraceStep::ConsumeEnd,
                            message_index::MessageTraceStep::EndTransaction => MessageTraceStep::EndTransaction,
                            message_index::MessageTraceStep::Recall => MessageTraceStep::Recall,
                        },
                        timestamp: node.timestamp,
                        group: preview(node.group),
                        success: node.success,
                        cost_time_ms: node.cost_time_ms,
                        client_host: preview(node.client_host),
                        store_host: preview(node.store_host),
                        offset_message_id: preview(node.offset_message_id),
                        retry_times: node.retry_times,
                        transaction_state: preview(node.transaction_state),
                    })
                    .collect(),
            };
            trace.order_nodes();
            Ok(trace)
        })
        .await
    }
}
