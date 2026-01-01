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

//! Basic tests for optimized CommitLog recovery module

use std::collections::BTreeMap;
use std::sync::Arc;

use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::commit_log_recovery::RecoveryContext;
use rocketmq_store::log_file::commit_log_recovery::RecoveryStatistics;

#[test]
fn test_recovery_context_creation() {
    let config = Arc::new(MessageStoreConfig::default());
    let delay_table = BTreeMap::new();

    let ctx = RecoveryContext::new(
        true,  // check_crc
        false, // check_dup_info
        config,
        16, // max_delay_level
        delay_table,
    );

    assert!(ctx.check_crc);
    assert!(!ctx.check_dup_info);
    assert_eq!(ctx.max_delay_level, 16);
}

#[test]
fn test_recovery_statistics_default() {
    let stats = RecoveryStatistics::default();

    assert_eq!(stats.files_processed, 0);
    assert_eq!(stats.messages_recovered, 0);
    assert_eq!(stats.bytes_processed, 0);
    assert_eq!(stats.invalid_messages, 0);
    assert_eq!(stats.recovery_time_ms, 0);
}

#[test]
fn test_recovery_statistics_updates() {
    let stats = RecoveryStatistics {
        files_processed: 10,
        messages_recovered: 1000,
        bytes_processed: 1024 * 1024,
        invalid_messages: 5,
        recovery_time_ms: 500,
    };

    assert_eq!(stats.files_processed, 10);
    assert_eq!(stats.messages_recovered, 1000);
    assert_eq!(stats.bytes_processed, 1024 * 1024);
    assert_eq!(stats.invalid_messages, 5);
    assert_eq!(stats.recovery_time_ms, 500);
}

#[test]
fn test_recovery_statistics_clone() {
    let stats = RecoveryStatistics {
        files_processed: 5,
        messages_recovered: 500,
        bytes_processed: 512 * 1024,
        invalid_messages: 2,
        recovery_time_ms: 250,
    };

    let cloned = stats.clone();

    assert_eq!(cloned.files_processed, stats.files_processed);
    assert_eq!(cloned.messages_recovered, stats.messages_recovered);
    assert_eq!(cloned.bytes_processed, stats.bytes_processed);
    assert_eq!(cloned.invalid_messages, stats.invalid_messages);
    assert_eq!(cloned.recovery_time_ms, stats.recovery_time_ms);
}

#[test]
fn test_module_compiles() {
    // This test ensures the recovery module compiles and can be used
    let _config = Arc::new(MessageStoreConfig::default());
    let _stats = RecoveryStatistics::default();

    // If we get here, the module compiled successfully
}
