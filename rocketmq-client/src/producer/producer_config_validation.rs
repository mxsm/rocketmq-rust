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

//! Producer configuration validation module
//!
//! This module provides validation functions for ProducerConfig fields,
//! ensuring that configuration values are within acceptable bounds and
//! properly formatted.

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

/// Validator for ProducerConfig fields
///
/// Provides validation methods for all producer configuration parameters,
/// ensuring they meet RocketMQ requirements.
pub struct ProducerConfigValidator;

impl ProducerConfigValidator {
    // =========================================================================
    // Validation Constants
    // =========================================================================

    /// Minimum send message timeout (100ms)
    pub const MIN_SEND_MSG_TIMEOUT: u32 = 100;

    /// Maximum send message timeout (5 minutes)
    pub const MAX_SEND_MSG_TIMEOUT: u32 = 300_000;

    /// Minimum maximum message size (1KB)
    pub const MIN_MAX_MESSAGE_SIZE: u32 = 1024;

    /// Maximum maximum message size (128MB)
    pub const MAX_MAX_MESSAGE_SIZE: u32 = 128 * 1024 * 1024;

    /// Minimum default topic queue numbers
    pub const MIN_DEFAULT_TOPIC_QUEUE_NUMS: u32 = 1;

    /// Maximum default topic queue numbers
    pub const MAX_DEFAULT_TOPIC_QUEUE_NUMS: u32 = 1024;

    /// Minimum retry times
    pub const MIN_RETRY_TIMES: u32 = 0;

    /// Maximum retry times
    pub const MAX_RETRY_TIMES: u32 = 16;

    /// Minimum compress message body threshold (1KB)
    pub const MIN_COMPRESS_MSG_BODY_OVER_HOWMUCH: u32 = 1024;

    /// Maximum compress message body threshold (64MB)
    pub const MAX_COMPRESS_MSG_BODY_OVER_HOWMUCH: u32 = 64 * 1024 * 1024;

    /// Minimum batch max delay (0ms)
    pub const MIN_BATCH_MAX_DELAY_MS: u32 = 0;

    /// Maximum batch max delay (60 seconds)
    pub const MAX_BATCH_MAX_DELAY_MS: u32 = 60_000;

    /// Minimum back pressure for async send num (10)
    pub const MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM: u32 = 10;

    /// Maximum back pressure for async send num (100,000)
    pub const MAX_BACK_PRESSURE_FOR_ASYNC_SEND_NUM: u32 = 100_000;

    /// Minimum back pressure for async send size (1MB)
    pub const MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE: u32 = 1024 * 1024;

    /// Maximum back pressure for async send size (1GB)
    pub const MAX_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE: u32 = 1024 * 1024 * 1024;

    // =========================================================================
    // Validation Methods
    // =========================================================================

    /// Validate send message timeout
    ///
    /// Ensures the timeout is between 100ms and 5 minutes.
    pub fn validate_send_msg_timeout(timeout: u32) -> RocketMQResult<()> {
        if !(Self::MIN_SEND_MSG_TIMEOUT..=Self::MAX_SEND_MSG_TIMEOUT).contains(&timeout) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "send_msg_timeout",
                value: timeout.to_string(),
                reason: format!(
                    "must be between {} and {} milliseconds",
                    Self::MIN_SEND_MSG_TIMEOUT,
                    Self::MAX_SEND_MSG_TIMEOUT
                ),
            });
        }
        Ok(())
    }

    /// Validate send message max timeout per request
    ///
    /// None means no limit, otherwise validates the timeout.
    pub fn validate_send_msg_max_timeout_per_request(timeout: Option<u32>) -> RocketMQResult<()> {
        if let Some(t) = timeout {
            if !(Self::MIN_SEND_MSG_TIMEOUT..=Self::MAX_SEND_MSG_TIMEOUT).contains(&t) {
                return Err(RocketMQError::ConfigInvalidValue {
                    key: "send_msg_max_timeout_per_request",
                    value: t.to_string(),
                    reason: format!(
                        "must be between {} and {} milliseconds, or None for no limit",
                        Self::MIN_SEND_MSG_TIMEOUT,
                        Self::MAX_SEND_MSG_TIMEOUT
                    ),
                });
            }
        }
        Ok(())
    }

    /// Validate maximum message size
    ///
    /// Ensures the size is between 1KB and 128MB.
    pub fn validate_max_message_size(size: u32) -> RocketMQResult<()> {
        if !(Self::MIN_MAX_MESSAGE_SIZE..=Self::MAX_MAX_MESSAGE_SIZE).contains(&size) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "max_message_size",
                value: size.to_string(),
                reason: format!(
                    "must be between {} and {} bytes",
                    Self::MIN_MAX_MESSAGE_SIZE,
                    Self::MAX_MAX_MESSAGE_SIZE
                ),
            });
        }
        Ok(())
    }

    /// Validate default topic queue numbers
    ///
    /// Ensures the queue count is between 1 and 1024.
    pub fn validate_default_topic_queue_nums(nums: u32) -> RocketMQResult<()> {
        if !(Self::MIN_DEFAULT_TOPIC_QUEUE_NUMS..=Self::MAX_DEFAULT_TOPIC_QUEUE_NUMS).contains(&nums) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "default_topic_queue_nums",
                value: nums.to_string(),
                reason: format!(
                    "must be between {} and {}",
                    Self::MIN_DEFAULT_TOPIC_QUEUE_NUMS,
                    Self::MAX_DEFAULT_TOPIC_QUEUE_NUMS
                ),
            });
        }
        Ok(())
    }

    /// Validate retry times when send failed
    ///
    /// Ensures the retry count is between 0 and 16.
    pub fn validate_retry_times_when_send_failed(times: u32) -> RocketMQResult<()> {
        if times > Self::MAX_RETRY_TIMES {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "retry_times_when_send_failed",
                value: times.to_string(),
                reason: format!("must not exceed {}", Self::MAX_RETRY_TIMES),
            });
        }
        Ok(())
    }

    /// Validate retry times when send async failed
    ///
    /// Ensures the retry count is between 0 and 16.
    pub fn validate_retry_times_when_send_async_failed(times: u32) -> RocketMQResult<()> {
        if times > Self::MAX_RETRY_TIMES {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "retry_times_when_send_async_failed",
                value: times.to_string(),
                reason: format!("must not exceed {}", Self::MAX_RETRY_TIMES),
            });
        }
        Ok(())
    }

    /// Validate compress message body threshold
    ///
    /// Ensures the threshold is between 1KB and 64MB.
    pub fn validate_compress_msg_body_over_howmuch(threshold: u32) -> RocketMQResult<()> {
        if !(Self::MIN_COMPRESS_MSG_BODY_OVER_HOWMUCH..=Self::MAX_COMPRESS_MSG_BODY_OVER_HOWMUCH).contains(&threshold) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "compress_msg_body_over_howmuch",
                value: threshold.to_string(),
                reason: format!(
                    "must be between {} and {} bytes",
                    Self::MIN_COMPRESS_MSG_BODY_OVER_HOWMUCH,
                    Self::MAX_COMPRESS_MSG_BODY_OVER_HOWMUCH
                ),
            });
        }
        Ok(())
    }

    /// Validate batch max delay
    ///
    /// None means no delay, otherwise validates the delay is within bounds.
    pub fn validate_batch_max_delay_ms(delay: Option<u32>) -> RocketMQResult<()> {
        if let Some(d) = delay {
            if d > Self::MAX_BATCH_MAX_DELAY_MS {
                return Err(RocketMQError::ConfigInvalidValue {
                    key: "batch_max_delay_ms",
                    value: d.to_string(),
                    reason: format!(
                        "must be at most {} milliseconds, or None for no delay",
                        Self::MAX_BATCH_MAX_DELAY_MS
                    ),
                });
            }
        }
        Ok(())
    }

    /// Validate back pressure for async send num
    ///
    /// Ensures the value is between 10 and 100,000.
    pub fn validate_back_pressure_for_async_send_num(num: u32) -> RocketMQResult<()> {
        if !(Self::MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM..=Self::MAX_BACK_PRESSURE_FOR_ASYNC_SEND_NUM).contains(&num) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "back_pressure_for_async_send_num",
                value: num.to_string(),
                reason: format!(
                    "must be between {} and {}",
                    Self::MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM,
                    Self::MAX_BACK_PRESSURE_FOR_ASYNC_SEND_NUM
                ),
            });
        }
        Ok(())
    }

    /// Validate back pressure for async send size
    ///
    /// Ensures the size is between 1MB and 1GB.
    pub fn validate_back_pressure_for_async_send_size(size: u32) -> RocketMQResult<()> {
        if !(Self::MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE..=Self::MAX_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE).contains(&size)
        {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "back_pressure_for_async_send_size",
                value: size.to_string(),
                reason: format!(
                    "must be between {} and {} bytes",
                    Self::MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE,
                    Self::MAX_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE
                ),
            });
        }
        Ok(())
    }

    /// Validate batch max bytes
    ///
    /// None means no limit, otherwise validates the size is within reasonable bounds.
    /// Max batch size should not exceed 32MB (matches Java: batchMaxBytes).
    pub fn validate_batch_max_bytes(size: Option<u64>) -> RocketMQResult<()> {
        const MIN_BATCH_MAX_BYTES: u64 = 1024; // 1KB
        const MAX_BATCH_MAX_BYTES: u64 = 32 * 1024 * 1024; // 32MB

        if let Some(s) = size {
            if !(MIN_BATCH_MAX_BYTES..=MAX_BATCH_MAX_BYTES).contains(&s) {
                return Err(RocketMQError::ConfigInvalidValue {
                    key: "batch_max_bytes",
                    value: s.to_string(),
                    reason: format!(
                        "must be between {} and {} bytes, or None for no limit",
                        MIN_BATCH_MAX_BYTES, MAX_BATCH_MAX_BYTES
                    ),
                });
            }
        }
        Ok(())
    }

    /// Validate total batch max bytes
    ///
    /// None means no limit, otherwise validates the size is within reasonable bounds.
    /// Max total batch size should not exceed 256MB
    pub fn validate_total_batch_max_bytes(size: Option<u64>) -> RocketMQResult<()> {
        const MIN_TOTAL_BATCH_MAX_BYTES: u64 = 1024; // 1KB
        const MAX_TOTAL_BATCH_MAX_BYTES: u64 = 256 * 1024 * 1024; // 256MB

        if let Some(s) = size {
            if !(MIN_TOTAL_BATCH_MAX_BYTES..=MAX_TOTAL_BATCH_MAX_BYTES).contains(&s) {
                return Err(RocketMQError::ConfigInvalidValue {
                    key: "total_batch_max_bytes",
                    value: s.to_string(),
                    reason: format!(
                        "must be between {} and {} bytes, or None for no limit",
                        MIN_TOTAL_BATCH_MAX_BYTES, MAX_TOTAL_BATCH_MAX_BYTES
                    ),
                });
            }
        }
        Ok(())
    }

    /// Validate producer group
    ///
    /// Ensures the producer group is not empty.
    pub fn validate_producer_group(group: &str) -> RocketMQResult<()> {
        if group.is_empty() {
            return Err(RocketMQError::ConfigMissing { key: "producer_group" });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_send_msg_timeout_valid() {
        assert!(ProducerConfigValidator::validate_send_msg_timeout(3000).is_ok());
        assert!(ProducerConfigValidator::validate_send_msg_timeout(100).is_ok());
        assert!(ProducerConfigValidator::validate_send_msg_timeout(300_000).is_ok());
    }

    #[test]
    fn test_validate_send_msg_timeout_invalid() {
        assert!(ProducerConfigValidator::validate_send_msg_timeout(10).is_err());
        assert!(ProducerConfigValidator::validate_send_msg_timeout(400_000).is_err());
    }

    #[test]
    fn test_validate_send_msg_max_timeout_per_request_none() {
        assert!(ProducerConfigValidator::validate_send_msg_max_timeout_per_request(None).is_ok());
    }

    #[test]
    fn test_validate_send_msg_max_timeout_per_request_valid() {
        assert!(ProducerConfigValidator::validate_send_msg_max_timeout_per_request(Some(3000)).is_ok());
    }

    #[test]
    fn test_validate_send_msg_max_timeout_per_request_invalid() {
        assert!(ProducerConfigValidator::validate_send_msg_max_timeout_per_request(Some(10)).is_err());
    }

    #[test]
    fn test_validate_max_message_size_valid() {
        assert!(ProducerConfigValidator::validate_max_message_size(4 * 1024 * 1024).is_ok());
        assert!(ProducerConfigValidator::validate_max_message_size(1024).is_ok());
        assert!(ProducerConfigValidator::validate_max_message_size(128 * 1024 * 1024).is_ok());
    }

    #[test]
    fn test_validate_max_message_size_invalid() {
        assert!(ProducerConfigValidator::validate_max_message_size(512).is_err());
        assert!(ProducerConfigValidator::validate_max_message_size(256 * 1024 * 1024).is_err());
    }

    #[test]
    fn test_validate_retry_times_when_send_failed_valid() {
        assert!(ProducerConfigValidator::validate_retry_times_when_send_failed(0).is_ok());
        assert!(ProducerConfigValidator::validate_retry_times_when_send_failed(2).is_ok());
        assert!(ProducerConfigValidator::validate_retry_times_when_send_failed(16).is_ok());
    }

    #[test]
    fn test_validate_retry_times_when_send_failed_invalid() {
        assert!(ProducerConfigValidator::validate_retry_times_when_send_failed(17).is_err());
    }

    #[test]
    fn test_validate_back_pressure_for_async_send_num_valid() {
        assert!(ProducerConfigValidator::validate_back_pressure_for_async_send_num(1024).is_ok());
        assert!(ProducerConfigValidator::validate_back_pressure_for_async_send_num(10).is_ok());
    }

    #[test]
    fn test_validate_back_pressure_for_async_send_num_invalid() {
        assert!(ProducerConfigValidator::validate_back_pressure_for_async_send_num(9).is_err());
        assert!(ProducerConfigValidator::validate_back_pressure_for_async_send_num(100_001).is_err());
    }

    #[test]
    fn test_validate_producer_group_empty() {
        assert!(ProducerConfigValidator::validate_producer_group("").is_err());
    }

    #[test]
    fn test_validate_producer_group_valid() {
        assert!(ProducerConfigValidator::validate_producer_group("test_group").is_ok());
    }

    #[test]
    fn test_validate_batch_max_delay_ms_none() {
        assert!(ProducerConfigValidator::validate_batch_max_delay_ms(None).is_ok());
    }

    #[test]
    fn test_validate_batch_max_delay_ms_valid() {
        assert!(ProducerConfigValidator::validate_batch_max_delay_ms(Some(1000)).is_ok());
    }

    #[test]
    fn test_validate_batch_max_delay_ms_invalid() {
        assert!(ProducerConfigValidator::validate_batch_max_delay_ms(Some(70_000)).is_err());
    }

    #[test]
    fn test_validate_batch_max_bytes_none() {
        assert!(ProducerConfigValidator::validate_batch_max_bytes(None).is_ok());
    }

    #[test]
    fn test_validate_batch_max_bytes_valid() {
        assert!(ProducerConfigValidator::validate_batch_max_bytes(Some(1024)).is_ok());
        assert!(ProducerConfigValidator::validate_batch_max_bytes(Some(4 * 1024 * 1024)).is_ok());
        assert!(ProducerConfigValidator::validate_batch_max_bytes(Some(32 * 1024 * 1024)).is_ok());
    }

    #[test]
    fn test_validate_batch_max_bytes_invalid() {
        assert!(ProducerConfigValidator::validate_batch_max_bytes(Some(512)).is_err());
        assert!(ProducerConfigValidator::validate_batch_max_bytes(Some(64 * 1024 * 1024)).is_err());
    }

    #[test]
    fn test_validate_total_batch_max_bytes_none() {
        assert!(ProducerConfigValidator::validate_total_batch_max_bytes(None).is_ok());
    }

    #[test]
    fn test_validate_total_batch_max_bytes_valid() {
        assert!(ProducerConfigValidator::validate_total_batch_max_bytes(Some(1024)).is_ok());
        assert!(ProducerConfigValidator::validate_total_batch_max_bytes(Some(64 * 1024 * 1024)).is_ok());
        assert!(ProducerConfigValidator::validate_total_batch_max_bytes(Some(256 * 1024 * 1024)).is_ok());
    }

    #[test]
    fn test_validate_total_batch_max_bytes_invalid() {
        assert!(ProducerConfigValidator::validate_total_batch_max_bytes(Some(512)).is_err());
        assert!(ProducerConfigValidator::validate_total_batch_max_bytes(Some(512 * 1024 * 1024)).is_err());
    }
}
