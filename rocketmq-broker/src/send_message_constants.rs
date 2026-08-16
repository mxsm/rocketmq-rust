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

//! Send message processor constants
//!
//! This module provides compile-time constants used throughout the message sending
//! process. Constants are organized into semantic groups for better maintainability
//! and discoverability.
//!
//! # Design Principles
//! - **Type Safety**: Use `const` instead of magic numbers
//! - **Performance**: Zero runtime cost, compile-time evaluation
//! - **Maintainability**: Centralized location for all limits and configurations
//! - **Documentation**: Each constant has clear usage documentation
//!
//! # Usage Example
//! ```rust,ignore
//! use rocketmq_broker::send_message_constants::message_limits;
//!
//! if topic.len() > message_limits::MAX_TOPIC_LENGTH {
//!     // Handle error
//! }
//! ```

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::lite::to_lmq_name;

/// Returns whether a compaction message has the Java-compatible raw `KEYS` identity.
///
/// Accepted keys are preserved byte-for-byte by the Store. This validation only rejects a
/// missing, empty, or whitespace-only value; it deliberately does not trim or split the key.
pub fn has_valid_compaction_key(properties: &HashMap<CheetahString, CheetahString>) -> bool {
    properties
        .get(MessageConst::PROPERTY_KEYS)
        .is_some_and(|value| !value.is_empty() && !value.chars().all(char::is_whitespace))
}

/// Applies Java-compatible priority queue selection and Lite multi-dispatch projection.
///
/// Message properties determine the delivery behavior independently of topic metadata. The
/// Proxy owns topic/message-type admission; a Broker receiving a direct request preserves the
/// Java behavior and does not introduce a second metadata gate.
pub fn apply_topic_delivery_properties(
    topic_config: &TopicConfig,
    parent_topic: &CheetahString,
    properties: &mut HashMap<CheetahString, CheetahString>,
    queue_id: &mut i32,
) {
    let transaction = properties
        .get(MessageConst::PROPERTY_TRANSACTION_PREPARED)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let delayed = [
        MessageConst::PROPERTY_DELAY_TIME_LEVEL,
        MessageConst::PROPERTY_TIMER_DELIVER_MS,
        MessageConst::PROPERTY_TIMER_DELAY_SEC,
        MessageConst::PROPERTY_TIMER_DELAY_MS,
    ]
    .iter()
    .any(|key| properties.contains_key(*key));
    let fifo = properties.contains_key(MessageConst::PROPERTY_SHARDING_KEY);
    if !transaction && !delayed && !fifo && properties.contains_key(MessageConst::PROPERTY_PRIORITY) {
        if let Some(priority) = properties
            .get(MessageConst::PROPERTY_PRIORITY)
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|priority| *priority >= 0)
        {
            if topic_config.write_queue_nums > 0 {
                *queue_id = priority.min(i64::from(topic_config.write_queue_nums - 1)) as i32;
            }
        } else {
            properties.remove(MessageConst::PROPERTY_PRIORITY);
        }
    } else {
        properties.remove(MessageConst::PROPERTY_PRIORITY);
    }

    if let Some(lite_topic) = properties
        .get(MessageConst::PROPERTY_LITE_TOPIC)
        .map(CheetahString::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        if let Some(lmq_name) = to_lmq_name(parent_topic, lite_topic) {
            properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
                CheetahString::from_string(lmq_name),
            );
        }
    }
}

/// Message-related limit constants
///
/// These constants define the maximum sizes for various message components.
/// Used for validation before message persistence.
pub mod message_limits {
    /// Maximum topic name length (127 bytes)
    pub const MAX_TOPIC_LENGTH: usize = i8::MAX as usize;

    /// Maximum message body size (4MB)
    pub const MAX_MESSAGE_BODY_SIZE: usize = 4 * 1024 * 1024;

    /// Maximum properties size (32KB)
    pub const MAX_PROPERTIES_SIZE: usize = 32 * 1024;

    /// Maximum batch size
    pub const MAX_BATCH_SIZE: usize = 500;
}

/// Retry and DLQ related constants
pub mod retry_config {
    /// DLQ queue numbers per consumer group
    pub const DLQ_NUMS_PER_GROUP: u32 = 1;

    /// Default retry delay level
    /// Level 3 = 10s, Level 4 = 30s, Level 5 = 1min, Level 6 = 2min, ...
    pub const DEFAULT_RETRY_DELAY_LEVEL: i32 = 3;

    /// Maximum reconsume times
    pub const MAX_RECONSUME_TIMES: i32 = 16;
}

/// Queue related constants
pub mod queue_config {
    /// Default write queue numbers
    pub const DEFAULT_WRITE_QUEUE_NUMS: i32 = 8;

    /// Default read queue numbers
    pub const DEFAULT_READ_QUEUE_NUMS: i32 = 8;

    /// Random queue ID range upper bound
    pub const RANDOM_QUEUE_RANGE: u32 = 99999999;
}

/// Error message constants
pub mod error_messages {
    /// Service not available
    pub const SERVICE_NOT_AVAILABLE: &str =
        "Service not available. Possible causes: disk full, slave mode, or shutdown in progress.";

    /// Mapped file creation failed
    pub const MAPPED_FILE_CREATE_FAILED: &str = "Failed to create mapped file. Server may be busy or out of resources.";

    /// Message illegal
    pub const MESSAGE_ILLEGAL: &str =
        "Message validation failed. Check body length (<4MB) and properties size (<32KB).";

    /// OS page cache busy
    pub const OS_PAGE_CACHE_BUSY: &str = "[PC_SYNCHRONIZED] Broker busy, starting flow control";

    /// In-sync replicas not enough
    pub const IN_SYNC_REPLICAS_NOT_ENOUGH: &str = "In-sync replicas not enough for synchronous replication";

    /// LMQ consume queue number exceeded
    pub const LMQ_QUEUE_NUM_EXCEEDED: &str =
        "[LMQ_CONSUME_QUEUE_NUM_EXCEEDED] Light message queue number exceeds limit (default: 20000)";

    /// Timer flow control
    pub const TIMER_FLOW_CONTROL: &str = "Timer message under flow control. Check configuration limits.";

    /// Timer message illegal
    pub const TIMER_MSG_ILLEGAL: &str = "Timer message validation failed. Check delay time constraints.";

    /// Timer not enabled
    pub const TIMER_NOT_ENABLED: &str = "Accurate timer message not enabled. Set timerWheelEnable=true.";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_limits() {
        assert_eq!(message_limits::MAX_TOPIC_LENGTH, 127);
        assert_eq!(message_limits::MAX_MESSAGE_BODY_SIZE, 4 * 1024 * 1024);
        assert_eq!(message_limits::MAX_PROPERTIES_SIZE, 32 * 1024);
    }

    #[test]
    fn test_retry_config() {
        assert_eq!(retry_config::DLQ_NUMS_PER_GROUP, 1);
        assert_eq!(retry_config::DEFAULT_RETRY_DELAY_LEVEL, 3);
        assert_eq!(retry_config::MAX_RECONSUME_TIMES, 16);
    }

    #[test]
    fn test_queue_config() {
        assert_eq!(queue_config::DEFAULT_WRITE_QUEUE_NUMS, 8);
        assert_eq!(queue_config::DEFAULT_READ_QUEUE_NUMS, 8);
    }
}
