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

//! Client configuration validation module
//!
//! This module provides validation functions for ClientConfig fields,
//! ensuring that configuration values are within acceptable bounds and
//! properly formatted.

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

/// Validator for ClientConfig fields
///
/// Provides validation methods for all configuration parameters,
/// ensuring they meet RocketMQ requirements.
pub struct ClientConfigValidator;

impl ClientConfigValidator {
    // =========================================================================
    // Validation Constants
    // =========================================================================

    /// Minimum poll name server interval (10 seconds)
    pub const MIN_POLL_NAME_SERVER_INTERVAL: u32 = 10_000;

    /// Maximum poll name server interval (10 minutes)
    pub const MAX_POLL_NAME_SERVER_INTERVAL: u32 = 600_000;

    /// Minimum heartbeat broker interval (10 seconds)
    pub const MIN_HEARTBEAT_BROKER_INTERVAL: u32 = 10_000;

    /// Maximum heartbeat broker interval (10 minutes)
    pub const MAX_HEARTBEAT_BROKER_INTERVAL: u32 = 600_000;

    /// Minimum persist consumer offset interval (1 second)
    pub const MIN_PERSIST_CONSUMER_OFFSET_INTERVAL: u32 = 1_000;

    /// Maximum persist consumer offset interval (1 minute)
    pub const MAX_PERSIST_CONSUMER_OFFSET_INTERVAL: u32 = 60_000;

    /// Minimum trace message batch number
    pub const MIN_TRACE_MSG_BATCH_NUM: usize = 1;

    /// Maximum trace message batch number
    pub const MAX_TRACE_MSG_BATCH_NUM: usize = 10_000;

    /// Minimum max page size in get metadata
    pub const MIN_MAX_PAGE_SIZE_IN_GET_METADATA: usize = 1;

    /// Maximum max page size in get metadata
    pub const MAX_MAX_PAGE_SIZE_IN_GET_METADATA: usize = 100_000;

    /// Minimum MQ client API timeout (100ms)
    pub const MIN_MQ_CLIENT_API_TIMEOUT: u64 = 100;

    /// Maximum MQ client API timeout (60 seconds)
    pub const MAX_MQ_CLIENT_API_TIMEOUT: u64 = 60_000;

    // =========================================================================
    // Validation Methods
    // =========================================================================

    /// Validate poll name server interval
    ///
    /// Ensures the interval is between 10 seconds and 10 minutes.
    pub fn validate_poll_name_server_interval(interval: u32) -> RocketMQResult<()> {
        if !(Self::MIN_POLL_NAME_SERVER_INTERVAL..=Self::MAX_POLL_NAME_SERVER_INTERVAL).contains(&interval) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "poll_name_server_interval",
                value: interval.to_string(),
                reason: format!(
                    "must be between {} and {} milliseconds",
                    Self::MIN_POLL_NAME_SERVER_INTERVAL,
                    Self::MAX_POLL_NAME_SERVER_INTERVAL
                ),
            });
        }
        Ok(())
    }

    /// Validate heartbeat broker interval
    ///
    /// Ensures the interval is between 10 seconds and 10 minutes.
    pub fn validate_heartbeat_broker_interval(interval: u32) -> RocketMQResult<()> {
        if !(Self::MIN_HEARTBEAT_BROKER_INTERVAL..=Self::MAX_HEARTBEAT_BROKER_INTERVAL).contains(&interval) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "heartbeat_broker_interval",
                value: interval.to_string(),
                reason: format!(
                    "must be between {} and {} milliseconds",
                    Self::MIN_HEARTBEAT_BROKER_INTERVAL,
                    Self::MAX_HEARTBEAT_BROKER_INTERVAL
                ),
            });
        }
        Ok(())
    }

    /// Validate persist consumer offset interval
    ///
    /// Ensures the interval is between 1 second and 1 minute.
    pub fn validate_persist_consumer_offset_interval(interval: u32) -> RocketMQResult<()> {
        if !(Self::MIN_PERSIST_CONSUMER_OFFSET_INTERVAL..=Self::MAX_PERSIST_CONSUMER_OFFSET_INTERVAL)
            .contains(&interval)
        {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "persist_consumer_offset_interval",
                value: interval.to_string(),
                reason: format!(
                    "must be between {} and {} milliseconds",
                    Self::MIN_PERSIST_CONSUMER_OFFSET_INTERVAL,
                    Self::MAX_PERSIST_CONSUMER_OFFSET_INTERVAL
                ),
            });
        }
        Ok(())
    }

    /// Validate trace message batch number
    ///
    /// Ensures the batch number is between 1 and 10,000.
    pub fn validate_trace_msg_batch_num(num: usize) -> RocketMQResult<()> {
        if !(Self::MIN_TRACE_MSG_BATCH_NUM..=Self::MAX_TRACE_MSG_BATCH_NUM).contains(&num) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "trace_msg_batch_num",
                value: num.to_string(),
                reason: format!(
                    "must be between {} and {}",
                    Self::MIN_TRACE_MSG_BATCH_NUM,
                    Self::MAX_TRACE_MSG_BATCH_NUM
                ),
            });
        }
        Ok(())
    }

    /// Validate max page size in get metadata
    ///
    /// Ensures the page size is between 1 and 100,000.
    pub fn validate_max_page_size_in_get_metadata(size: usize) -> RocketMQResult<()> {
        if !(Self::MIN_MAX_PAGE_SIZE_IN_GET_METADATA..=Self::MAX_MAX_PAGE_SIZE_IN_GET_METADATA).contains(&size) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "max_page_size_in_get_metadata",
                value: size.to_string(),
                reason: format!(
                    "must be between {} and {}",
                    Self::MIN_MAX_PAGE_SIZE_IN_GET_METADATA,
                    Self::MAX_MAX_PAGE_SIZE_IN_GET_METADATA
                ),
            });
        }
        Ok(())
    }

    /// Validate MQ client API timeout
    ///
    /// Ensures the timeout is between 100ms and 60 seconds.
    pub fn validate_mq_client_api_timeout(timeout: u64) -> RocketMQResult<()> {
        if !(Self::MIN_MQ_CLIENT_API_TIMEOUT..=Self::MAX_MQ_CLIENT_API_TIMEOUT).contains(&timeout) {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "mq_client_api_timeout",
                value: timeout.to_string(),
                reason: format!(
                    "must be between {} and {} milliseconds",
                    Self::MIN_MQ_CLIENT_API_TIMEOUT,
                    Self::MAX_MQ_CLIENT_API_TIMEOUT
                ),
            });
        }
        Ok(())
    }

    /// Validate concurrent heartbeat thread pool size
    ///
    /// Ensures the thread pool size is greater than 0 and doesn't exceed
    /// a reasonable maximum (CPU cores * 4).
    pub fn validate_concurrent_heartbeat_thread_pool_size(size: usize) -> RocketMQResult<()> {
        if size == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "concurrent_heartbeat_thread_pool_size",
                value: size.to_string(),
                reason: "must be greater than 0".to_string(),
            });
        }

        // Get CPU count with a reasonable default if num_cpus fails
        #[allow(clippy::redundant_closure)]
        let cpu_count = std::panic::catch_unwind(|| num_cpus::get()).unwrap_or(4);
        let max_size = cpu_count * 4;

        if size > max_size {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "concurrent_heartbeat_thread_pool_size",
                value: size.to_string(),
                reason: format!("should not exceed {} (CPU cores * 4)", max_size),
            });
        }

        Ok(())
    }

    /// Validate client callback executor threads
    ///
    /// Ensures the thread count is greater than 0.
    pub fn validate_client_callback_executor_threads(threads: usize) -> RocketMQResult<()> {
        if threads == 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "client_callback_executor_threads",
                value: threads.to_string(),
                reason: "must be greater than 0".to_string(),
            });
        }

        // Get CPU count with a reasonable default if num_cpus fails
        #[allow(clippy::redundant_closure)]
        let cpu_count = std::panic::catch_unwind(|| num_cpus::get()).unwrap_or(4);
        let max_threads = cpu_count * 8;

        if threads > max_threads {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "client_callback_executor_threads",
                value: threads.to_string(),
                reason: format!("should not exceed {} (CPU cores * 8)", max_threads),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_poll_name_server_interval_valid() {
        assert!(ClientConfigValidator::validate_poll_name_server_interval(30_000).is_ok());
        assert!(ClientConfigValidator::validate_poll_name_server_interval(10_000).is_ok());
        assert!(ClientConfigValidator::validate_poll_name_server_interval(600_000).is_ok());
    }

    #[test]
    fn test_validate_poll_name_server_interval_invalid() {
        assert!(ClientConfigValidator::validate_poll_name_server_interval(1_000).is_err());
        assert!(ClientConfigValidator::validate_poll_name_server_interval(700_000).is_err());
    }

    #[test]
    fn test_validate_heartbeat_broker_interval_valid() {
        assert!(ClientConfigValidator::validate_heartbeat_broker_interval(30_000).is_ok());
        assert!(ClientConfigValidator::validate_heartbeat_broker_interval(10_000).is_ok());
        assert!(ClientConfigValidator::validate_heartbeat_broker_interval(600_000).is_ok());
    }

    #[test]
    fn test_validate_heartbeat_broker_interval_invalid() {
        assert!(ClientConfigValidator::validate_heartbeat_broker_interval(1_000).is_err());
        assert!(ClientConfigValidator::validate_heartbeat_broker_interval(700_000).is_err());
    }

    #[test]
    fn test_validate_trace_msg_batch_num_valid() {
        assert!(ClientConfigValidator::validate_trace_msg_batch_num(1).is_ok());
        assert!(ClientConfigValidator::validate_trace_msg_batch_num(10).is_ok());
        assert!(ClientConfigValidator::validate_trace_msg_batch_num(10_000).is_ok());
    }

    #[test]
    fn test_validate_trace_msg_batch_num_invalid() {
        assert!(ClientConfigValidator::validate_trace_msg_batch_num(0).is_err());
        assert!(ClientConfigValidator::validate_trace_msg_batch_num(10_001).is_err());
    }

    #[test]
    fn test_validate_concurrent_heartbeat_thread_pool_size_valid() {
        assert!(ClientConfigValidator::validate_concurrent_heartbeat_thread_pool_size(1).is_ok());
        assert!(ClientConfigValidator::validate_concurrent_heartbeat_thread_pool_size(4).is_ok());
    }

    #[test]
    fn test_validate_mq_client_api_timeout_valid() {
        assert!(ClientConfigValidator::validate_mq_client_api_timeout(3_000).is_ok());
        assert!(ClientConfigValidator::validate_mq_client_api_timeout(100).is_ok());
        assert!(ClientConfigValidator::validate_mq_client_api_timeout(60_000).is_ok());
    }

    #[test]
    fn test_validate_mq_client_api_timeout_invalid() {
        assert!(ClientConfigValidator::validate_mq_client_api_timeout(50).is_err());
        assert!(ClientConfigValidator::validate_mq_client_api_timeout(70_000).is_err());
    }
}
