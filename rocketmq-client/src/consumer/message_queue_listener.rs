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

use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_common::common::message::message_queue::MessageQueue;

/// A listener that is notified when message queue allocation changes during consumer rebalancing.
///
/// This trait is used to track changes in the message queues assigned to a consumer instance.
/// It is particularly useful for monitoring and debugging purposes, allowing applications to react
/// to rebalancing events in real-time.
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`) as they may be called from multiple threads.
///
/// # Examples
///
/// ```rust
/// use rocketmq_client::consumer::message_queue_listener::MessageQueueListener;
/// use rocketmq_common::common::message::message_queue::MessageQueue;
/// use std::collections::HashSet;
///
/// struct MyQueueListener;
///
/// impl MessageQueueListener for MyQueueListener {
///     fn message_queue_changed(
///         &self,
///         topic: &str,
///         mq_all: &HashSet<MessageQueue>,
///         mq_assigned: &HashSet<MessageQueue>,
///     ) {
///         println!("Topic: {}", topic);
///         println!("All queues: {:?}", mq_all.len());
///         println!("Assigned queues: {:?}", mq_assigned.len());
///     }
/// }
/// ```
pub trait MessageQueueListener: Send + Sync {
    /// Called when the message queue allocation for a topic has changed.
    ///
    /// This method is invoked during consumer rebalancing when the set of message queues
    /// assigned to this consumer instance changes.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic name for which the queue allocation has changed.
    /// * `mq_all` - All message queues available for this topic across all consumers.
    /// * `mq_assigned` - The subset of message queues assigned to this consumer instance after
    ///   rebalancing.
    ///
    /// # Notes
    ///
    /// This method should execute quickly and avoid blocking operations, as it is called
    /// during the rebalancing process.
    fn message_queue_changed(&self, topic: &str, mq_all: &HashSet<MessageQueue>, mq_assigned: &HashSet<MessageQueue>);
}

/// Type alias for an atomically reference-counted message queue listener.
///
/// This is the standard way to share a [`MessageQueueListener`] implementation across multiple
/// threads or components. The `Arc` provides thread-safe reference counting.
///
/// # Examples
///
/// ```rust
/// use std::sync::Arc;
/// use rocketmq_client::consumer::message_queue_listener::{MessageQueueListener, ArcMessageQueueListener};
///
/// # struct MyListener;
/// # impl MessageQueueListener for MyListener {
/// #     fn message_queue_changed(&self, _: &str, _: &std::collections::HashSet<rocketmq_common::common::message::message_queue::MessageQueue>, _: &std::collections::HashSet<rocketmq_common::common::message::message_queue::MessageQueue>) {}
/// # }
/// let listener: ArcMessageQueueListener = Arc::new(MyListener);
/// ```
pub type ArcMessageQueueListener = Arc<dyn MessageQueueListener>;
