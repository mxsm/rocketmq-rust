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

//! Body-free Trace observations shared by native pages and Message inspectors.

use crate::{MessageIdentity, MessageSearchQuery, MessageTextPreview, MessageValidationError, TopicIdentity};

pub const DEFAULT_TRACE_TOPIC: &str = "RMQ_SYS_TRACE_TOPIC";
pub const TRACE_NODE_LIMIT: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageTraceQuery {
    pub identity: MessageIdentity,
    pub trace_topic: String,
}
impl MessageTraceQuery {
    pub fn validate(&self) -> Result<(), MessageTraceValidationError> {
        MessageSearchQuery::MessageId {
            topic: self.identity.topic.clone(),
            message_id: self.identity.message_id.clone(),
        }
        .validate()
        .map_err(MessageTraceValidationError::Message)?;
        TopicIdentity::parse(&self.trace_topic).map_err(|_| MessageTraceValidationError::TraceTopic)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageTraceValidationError {
    Message(MessageValidationError),
    TraceTopic,
}
impl MessageTraceValidationError {
    pub const fn summary(self) -> &'static str {
        match self {
            Self::Message(error) => error.summary(),
            Self::TraceTopic => "A valid Trace Topic is required.",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageTraceStep {
    Publish,
    ConsumeStart,
    ConsumeEnd,
    EndTransaction,
    Recall,
}
impl MessageTraceStep {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Publish => "Publish",
            Self::ConsumeStart => "Consume start",
            Self::ConsumeEnd => "Consume end",
            Self::EndTransaction => "End transaction",
            Self::Recall => "Recall",
        }
    }
}

/// An actual client-reported event. Optional observations never imply zero, success or failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageTraceNode {
    pub step: MessageTraceStep,
    pub timestamp: Option<i64>,
    pub group: Option<MessageTextPreview>,
    pub success: Option<bool>,
    pub cost_time_ms: Option<u32>,
    pub client_host: Option<MessageTextPreview>,
    pub store_host: Option<MessageTextPreview>,
    pub offset_message_id: Option<MessageTextPreview>,
    pub retry_times: Option<u32>,
    pub transaction_state: Option<MessageTextPreview>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct MessageTrace {
    pub identity: MessageIdentity,
    pub producer_message_id: Option<MessageTextPreview>,
    pub trace_topic: String,
    pub trace_topic_found: bool,
    pub nodes: Vec<MessageTraceNode>,
    pub limit_reached: bool,
    pub unanswered_brokers: Vec<MessageTextPreview>,
}
impl MessageTrace {
    /// Stable ordering is shared by every presentation. Unknown timestamps follow known ones;
    /// equal timestamps retain source order and do not acquire invented causal edges.
    pub fn order_nodes(&mut self) {
        self.nodes
            .sort_by_key(|node| (node.timestamp.is_none(), node.timestamp.unwrap_or_default()));
        if self.nodes.len() > TRACE_NODE_LIMIT {
            self.nodes.truncate(TRACE_NODE_LIMIT);
            self.limit_reached = true;
        }
    }
}
impl std::fmt::Debug for MessageTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageTrace")
            .field("node_count", &self.nodes.len())
            .field("limit_reached", &self.limit_reached)
            .field("unanswered_broker_count", &self.unanswered_brokers.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(step: MessageTraceStep, timestamp: Option<i64>) -> MessageTraceNode {
        MessageTraceNode {
            step,
            timestamp,
            group: None,
            success: None,
            cost_time_ms: None,
            client_host: None,
            store_host: None,
            offset_message_id: None,
            retry_times: None,
            transaction_state: None,
        }
    }
    #[test]
    fn trace_order_preserves_equal_timestamp_source_order_and_unknown_observations() {
        let mut trace = MessageTrace {
            identity: MessageIdentity {
                topic: "orders".into(),
                message_id: "physical".into(),
            },
            producer_message_id: None,
            trace_topic: DEFAULT_TRACE_TOPIC.into(),
            trace_topic_found: true,
            nodes: vec![
                node(MessageTraceStep::ConsumeEnd, Some(20)),
                node(MessageTraceStep::Recall, None),
                node(MessageTraceStep::ConsumeStart, Some(10)),
                node(MessageTraceStep::Publish, Some(10)),
            ],
            limit_reached: false,
            unanswered_brokers: Vec::new(),
        };
        trace.order_nodes();
        assert_eq!(
            trace.nodes.iter().map(|node| node.step).collect::<Vec<_>>(),
            vec![
                MessageTraceStep::ConsumeStart,
                MessageTraceStep::Publish,
                MessageTraceStep::ConsumeEnd,
                MessageTraceStep::Recall
            ]
        );
        assert!(trace
            .nodes
            .iter()
            .all(|node| node.success.is_none() && node.cost_time_ms.is_none()));
        assert!(trace.nodes[3].timestamp.is_none());
    }
    #[test]
    fn trace_query_requires_both_message_topic_identity_and_explicit_trace_topic() {
        let mut query = MessageTraceQuery {
            identity: MessageIdentity {
                topic: "orders".into(),
                message_id: "id".into(),
            },
            trace_topic: DEFAULT_TRACE_TOPIC.into(),
        };
        assert!(query.validate().is_ok());
        query.trace_topic.clear();
        assert_eq!(query.validate(), Err(MessageTraceValidationError::TraceTopic));
        query.trace_topic = DEFAULT_TRACE_TOPIC.into();
        query.identity.topic.clear();
        assert!(matches!(query.validate(), Err(MessageTraceValidationError::Message(_))));
    }
}
