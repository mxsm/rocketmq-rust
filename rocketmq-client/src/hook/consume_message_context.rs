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

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_rust::ArcMut;

use crate::base::access_channel::AccessChannel;

/// Context information for message consumption operations.
///
/// Contains metadata about a message consumption attempt, including the consumer group,
/// message list, target queue, and consumption result. This context is passed to
/// consumption hooks for monitoring and tracing purposes.
///
/// # Examples
///
/// ```ignore
/// use rocketmq_client::hook::consume_message_context::ConsumeMessageContext;
/// use cheetah_string::CheetahString;
///
/// let context = ConsumeMessageContext::new(
///     CheetahString::from("test_group"),
///     &messages
/// ).with_success(true)
///   .with_status("CONSUME_SUCCESS");
/// ```
#[derive(Default)]
pub struct ConsumeMessageContext<'a> {
    /// Consumer group name
    pub consumer_group: CheetahString,
    /// List of messages being consumed
    pub msg_list: &'a [ArcMut<MessageExt>],
    /// Target message queue
    pub mq: Option<MessageQueue>,
    /// Whether consumption succeeded
    pub success: bool,
    /// Consumption status description
    pub status: CheetahString,
    /// Trace context for distributed tracing
    pub mq_trace_context: Option<Arc<dyn Any + Send + Sync>>,
    /// Additional properties
    pub props: HashMap<CheetahString, CheetahString>,
    /// Namespace
    pub namespace: CheetahString,
    /// Access channel
    pub access_channel: Option<AccessChannel>,
}

impl<'a> ConsumeMessageContext<'a> {
    /// Creates a new consumption context.
    #[inline]
    pub fn new(consumer_group: CheetahString, msg_list: &'a [ArcMut<MessageExt>]) -> Self {
        Self {
            consumer_group,
            msg_list,
            ..Default::default()
        }
    }

    /// Sets the message queue.
    #[inline]
    pub fn with_mq(mut self, mq: MessageQueue) -> Self {
        self.mq = Some(mq);
        self
    }

    /// Sets the success flag.
    #[inline]
    pub fn with_success(mut self, success: bool) -> Self {
        self.success = success;
        self
    }

    /// Sets the status description.
    #[inline]
    pub fn with_status(mut self, status: impl Into<CheetahString>) -> Self {
        self.status = status.into();
        self
    }

    /// Sets the trace context.
    #[inline]
    pub fn with_trace_context(mut self, context: Arc<dyn Any + Send + Sync>) -> Self {
        self.mq_trace_context = Some(context);
        self
    }

    /// Sets the namespace.
    #[inline]
    pub fn with_namespace(mut self, namespace: impl Into<CheetahString>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Sets the access channel.
    #[inline]
    pub fn with_access_channel(mut self, channel: AccessChannel) -> Self {
        self.access_channel = Some(channel);
        self
    }

    /// Adds a property.
    #[inline]
    pub fn add_prop(&mut self, key: impl Into<CheetahString>, value: impl Into<CheetahString>) {
        self.props.insert(key.into(), value.into());
    }

    /// Gets the number of messages in this context.
    #[inline]
    pub fn message_count(&self) -> usize {
        self.msg_list.len()
    }
}

impl<'a> fmt::Display for ConsumeMessageContext<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConsumeMessageContext {{ consumer_group: {}, msg_count: {}, ",
            self.consumer_group,
            self.msg_list.len()
        )?;

        if let Some(ref mq) = self.mq {
            write!(f, "mq: {}, ", mq)?;
        }

        write!(f, "success: {}, status: {}", self.success, self.status)?;

        if !self.namespace.is_empty() {
            write!(f, ", namespace: {}", self.namespace)?;
        }

        if let Some(ref channel) = self.access_channel {
            write!(f, ", access_channel: {:?}", channel)?;
        }

        write!(f, " }}")
    }
}

impl<'a> fmt::Debug for ConsumeMessageContext<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumeMessageContext")
            .field("consumer_group", &self.consumer_group)
            .field("msg_list_len", &self.msg_list.len())
            .field("mq", &self.mq)
            .field("success", &self.success)
            .field("status", &self.status)
            .field("mq_trace_context", &self.mq_trace_context.as_ref().map(|_| "Some(_)"))
            .field("props_count", &self.props.len())
            .field("namespace", &self.namespace)
            .field("access_channel", &self.access_channel)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_message_context_new() {
        let group = CheetahString::from("test_group");
        let msg_list = vec![ArcMut::new(MessageExt::default())];
        let context = ConsumeMessageContext::new(group.clone(), &msg_list);

        assert_eq!(context.consumer_group, group);
        assert_eq!(context.message_count(), 1);
        assert!(context.mq.is_none());
        assert!(!context.success);
    }

    #[test]
    fn consume_message_context_builder_methods() {
        let group = CheetahString::from("test_group");
        let msg_list = vec![];
        let mq = MessageQueue::from_parts("topic", "broker", 1);

        let context = ConsumeMessageContext::new(group, &msg_list)
            .with_mq(mq.clone())
            .with_success(true)
            .with_status("SUCCESS")
            .with_namespace("test_ns")
            .with_access_channel(AccessChannel::Cloud);

        assert_eq!(context.mq.unwrap(), mq);
        assert!(context.success);
        assert_eq!(context.status, "SUCCESS");
        assert_eq!(context.namespace, "test_ns");
        assert_eq!(context.access_channel.unwrap(), AccessChannel::Cloud);
    }

    #[test]
    fn consume_message_context_add_prop() {
        let mut context = ConsumeMessageContext::<'static>::default();
        context.add_prop("key1", "value1");
        assert_eq!(context.props.get(&CheetahString::from("key1")).unwrap(), "value1");
    }

    #[test]
    fn consume_message_context_display() {
        let group = CheetahString::from("test_group");
        let msg_list = vec![];
        let context = ConsumeMessageContext::new(group, &msg_list)
            .with_success(true)
            .with_status("OK");

        let display = format!("{}", context);
        assert!(display.contains("consumer_group: test_group"));
        assert!(display.contains("msg_count: 0"));
        assert!(display.contains("success: true"));
        assert!(display.contains("status: OK"));
    }

    #[test]
    fn consume_message_context_debug() {
        let context = ConsumeMessageContext::<'static>::default();
        let debug = format!("{:?}", context);
        assert!(debug.contains("ConsumeMessageContext"));
        assert!(debug.contains("consumer_group"));
    }

    #[test]
    fn consume_message_context_with_trace_context() {
        let context =
            ConsumeMessageContext::<'static>::default().with_trace_context(Arc::new("trace_data".to_string()));
        assert!(context.mq_trace_context.is_some());
    }
}
