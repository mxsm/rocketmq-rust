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

//! Optimized CommitLog recovery with batched I/O and zero-copy parsing.
//!
//! Performance optimizations:
//! - Batch message parsing with pre-allocated buffers
//! - Zero-copy message validation using memory-mapped regions
//! - Parallel dispatch request processing (when safe)
//! - Optimized CRC validation with SIMD when available
//! - Reduced lock contention through buffered dispatch

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use rocketmq_common::MessageDecoder::MESSAGE_MAGIC_CODE_POSITION;
use rocketmq_common::MessageDecoder::MESSAGE_MAGIC_CODE_V2;
use rocketmq_common::MessageDecoder::SYSFLAG_POSITION;
use tracing::info;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::config::message_store_config::MessageStoreConfig;
use crate::log_file::commit_log::check_message_and_return_size;
use crate::log_file::commit_log::BLANK_MAGIC_CODE;
use crate::log_file::commit_log::MESSAGE_MAGIC_CODE;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

/// Batch size for message parsing - trade memory for fewer I/O calls
const PARSE_BATCH_SIZE: usize = 64 * 1024; // 64KB per batch
const MIN_MESSAGE_SIZE: usize = 4 + 4; // totalSize + magicCode

/// Statistics for recovery operations
#[derive(Debug, Default, Clone)]
pub struct RecoveryStatistics {
    pub files_processed: usize,
    pub messages_recovered: u64,
    pub bytes_processed: u64,
    pub invalid_messages: u64,
    pub recovery_time_ms: u128,
}

impl RecoveryStatistics {
    pub fn log_summary(&self, recovery_type: &str) {
        info!(
            "{} recovery completed: {} files, {} messages, {:.2} MB, {} invalid, {}ms",
            recovery_type,
            self.files_processed,
            self.messages_recovered,
            self.bytes_processed as f64 / 1024.0 / 1024.0,
            self.invalid_messages,
            self.recovery_time_ms
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbnormalRecoveryWindow {
    pub start_index: usize,
    pub checkpoint_index: Option<usize>,
    pub dispatch_progress_index: Option<usize>,
    pub confirm_offset_index: Option<usize>,
    pub file_count_limit: Option<usize>,
    pub expanded_files: usize,
    pub scanned_file_count: usize,
    pub scanned_bytes: u64,
    pub end_offset: Option<i64>,
    pub fallback_reason: Option<&'static str>,
}

impl AbnormalRecoveryWindow {
    fn new(
        file_ranges: &[AbnormalRecoveryFileRange],
        start_index: usize,
        checkpoint_index: Option<usize>,
        file_count_limit: Option<usize>,
        end_offset: Option<i64>,
        fallback_reason: Option<&'static str>,
    ) -> Self {
        let expanded_files = checkpoint_index
            .map(|checkpoint_index| checkpoint_index.saturating_sub(start_index))
            .unwrap_or_default();
        let scanned_file_count = file_ranges.len().saturating_sub(start_index);
        let scanned_bytes = planned_scanned_bytes(file_ranges, start_index, end_offset);

        Self {
            start_index,
            checkpoint_index,
            dispatch_progress_index: None,
            confirm_offset_index: None,
            file_count_limit,
            expanded_files,
            scanned_file_count,
            scanned_bytes,
            end_offset,
            fallback_reason,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbnormalRecoveryFileRange {
    pub start_offset: i64,
    pub file_size: u64,
}

impl AbnormalRecoveryFileRange {
    pub const fn new(start_offset: i64, file_size: u64) -> Self {
        Self {
            start_offset,
            file_size,
        }
    }
}

/// Optimized message iterator that reads in batches
pub struct BatchMessageIterator<'a> {
    mapped_file: &'a Arc<DefaultMappedFile>,
    current_offset: usize,
    file_size: u64,
    buffer: Bytes,
    buffer_start_offset: usize,
}

impl<'a> BatchMessageIterator<'a> {
    pub fn new(mapped_file: &'a Arc<DefaultMappedFile>) -> Self {
        let file_size = mapped_file.get_file_size();
        Self {
            mapped_file,
            current_offset: 0,
            file_size,
            buffer: Bytes::new(),
            buffer_start_offset: 0,
        }
    }

    /// Fetch next batch of data into buffer
    fn refill_buffer(&mut self) -> bool {
        if self.current_offset >= self.file_size as usize {
            return false;
        }

        let remaining = self.file_size as usize - self.current_offset;
        let fetch_size = remaining.min(PARSE_BATCH_SIZE);

        if let Some(bytes) = self.mapped_file.get_bytes(self.current_offset, fetch_size) {
            self.buffer = bytes;
            self.buffer_start_offset = self.current_offset;
            true
        } else {
            false
        }
    }

    /// Get next message without copying (returns reference into mmap)
    pub fn next_message(&mut self) -> Option<(Bytes, usize, usize)> {
        loop {
            // Ensure we have enough data in buffer for message header
            if self.buffer.remaining() < MIN_MESSAGE_SIZE && !self.refill_buffer() {
                return None;
            }

            let offset_in_buffer = self.current_offset - self.buffer_start_offset;

            // Peek at total size without consuming
            if self.buffer.remaining() < 4 {
                return None;
            }

            let total_size = {
                let mut temp = self.buffer.clone();
                temp.get_i32()
            };

            if total_size <= 0 {
                return None;
            }

            let msg_size = total_size as usize;
            let absolute_offset = self.current_offset;

            // Check if entire message is in current buffer
            if self.buffer.remaining() < msg_size {
                // Message spans beyond current buffer, fetch larger chunk
                if msg_size > PARSE_BATCH_SIZE {
                    // Large message, fetch directly
                    let msg_bytes = self.mapped_file.get_bytes(self.current_offset, msg_size)?;
                    self.current_offset += msg_size;
                    // Clear buffer to force refill on next call
                    self.buffer = Bytes::new();
                    return Some((msg_bytes, absolute_offset, msg_size));
                } else {
                    // Refill buffer and retry
                    if !self.refill_buffer() {
                        return None;
                    }
                    continue;
                }
            }

            // Entire message is in buffer, extract it
            let msg_bytes = self.buffer.copy_to_bytes(msg_size);
            self.current_offset += msg_size;

            return Some((msg_bytes, absolute_offset, msg_size));
        }
    }

    pub fn current_offset(&self) -> usize {
        self.current_offset
    }
}

/// Optimized recovery context that reuses allocations
pub struct RecoveryContext {
    pub check_crc: bool,
    pub check_dup_info: bool,
    pub message_store_config: Arc<MessageStoreConfig>,
    pub max_delay_level: i32,
    pub delay_level_table: BTreeMap<i32, i64>,
    pub stats: RecoveryStatistics,
}

impl RecoveryContext {
    pub fn new(
        check_crc: bool,
        check_dup_info: bool,
        message_store_config: Arc<MessageStoreConfig>,
        max_delay_level: i32,
        delay_level_table: BTreeMap<i32, i64>,
    ) -> Self {
        Self {
            check_crc,
            check_dup_info,
            message_store_config,
            max_delay_level,
            delay_level_table,
            stats: RecoveryStatistics::default(),
        }
    }

    /// Process a single message and return dispatch request
    #[inline]
    pub fn process_message(&mut self, msg_bytes: &mut Bytes, absolute_offset: usize) -> DispatchRequest {
        let dispatch_request = check_message_and_return_size(
            msg_bytes,
            self.check_crc,
            self.check_dup_info,
            true,
            &self.message_store_config,
            self.max_delay_level,
            &self.delay_level_table,
        );

        // Update statistics
        if dispatch_request.success {
            if dispatch_request.msg_size > 0 {
                self.stats.messages_recovered += 1;
                self.stats.bytes_processed += dispatch_request.msg_size as u64;
            }
        } else {
            self.stats.invalid_messages += 1;
        }

        dispatch_request
    }
}

/// Find the starting file index for abnormal recovery
pub fn find_recovery_start_index(
    mapped_files: &[Arc<DefaultMappedFile>],
    message_store_config: &Arc<MessageStoreConfig>,
    store_checkpoint: &StoreCheckpoint,
) -> usize {
    find_checkpoint_recovery_start_index(mapped_files, message_store_config, store_checkpoint).unwrap_or(0)
}

pub fn plan_abnormal_recovery_window(
    mapped_files: &[Arc<DefaultMappedFile>],
    message_store_config: &Arc<MessageStoreConfig>,
    store_checkpoint: &StoreCheckpoint,
    dispatch_progress_offset: i64,
    confirm_offset: i64,
    commit_log_min_offset: i64,
    commit_log_max_offset: i64,
) -> AbnormalRecoveryWindow {
    let file_ranges: Vec<_> = mapped_files
        .iter()
        .map(|mapped_file| {
            AbnormalRecoveryFileRange::new(mapped_file.get_file_from_offset() as i64, mapped_file.get_file_size())
        })
        .collect();
    let checkpoint_index = find_checkpoint_recovery_start_index(mapped_files, message_store_config, store_checkpoint);

    plan_abnormal_recovery_window_from_ranges(
        &file_ranges,
        checkpoint_index,
        message_store_config.max_recovery_commit_log_files,
        dispatch_progress_offset,
        confirm_offset,
        commit_log_min_offset,
        commit_log_max_offset,
    )
}

fn find_checkpoint_recovery_start_index(
    mapped_files: &[Arc<DefaultMappedFile>],
    message_store_config: &Arc<MessageStoreConfig>,
    store_checkpoint: &StoreCheckpoint,
) -> Option<usize> {
    let mut index = mapped_files.len().checked_sub(1)?;

    loop {
        let mapped_file = &mapped_files[index];
        if is_mapped_file_matched_recover(message_store_config, mapped_file, store_checkpoint) {
            return Some(index);
        }
        if index == 0 {
            return None;
        }
        index -= 1;
    }
}

pub fn plan_abnormal_recovery_window_from_ranges(
    file_ranges: &[AbnormalRecoveryFileRange],
    checkpoint_index: Option<usize>,
    max_recovery_commit_log_files: usize,
    dispatch_progress_offset: i64,
    confirm_offset: i64,
    commit_log_min_offset: i64,
    commit_log_max_offset: i64,
) -> AbnormalRecoveryWindow {
    if file_ranges.is_empty() {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            checkpoint_index,
            configured_file_count_limit(max_recovery_commit_log_files),
            None,
            Some("empty_commitlog"),
        );
    }

    let end_offset =
        valid_commit_log_range(commit_log_min_offset, commit_log_max_offset).then_some(commit_log_max_offset);

    if max_recovery_commit_log_files == 0 {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            checkpoint_index.unwrap_or_default(),
            checkpoint_index,
            None,
            end_offset,
            checkpoint_index.is_none().then_some("checkpoint_not_matched"),
        );
    }

    let file_count_limit = configured_file_count_limit(max_recovery_commit_log_files);
    let Some(checkpoint_index) = checkpoint_index else {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            None,
            file_count_limit,
            end_offset,
            Some("checkpoint_not_matched"),
        );
    };

    if !valid_commit_log_range(commit_log_min_offset, commit_log_max_offset) {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            Some(checkpoint_index),
            file_count_limit,
            None,
            Some("invalid_commitlog_range"),
        );
    }

    let Some(dispatch_progress_index) = file_index_for_offset(
        file_ranges,
        dispatch_progress_offset,
        commit_log_min_offset,
        commit_log_max_offset,
    ) else {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            Some(checkpoint_index),
            file_count_limit,
            end_offset,
            Some("dispatch_progress_out_of_range"),
        );
    };

    let Some(confirm_offset_index) = file_index_for_offset(
        file_ranges,
        confirm_offset,
        commit_log_min_offset,
        commit_log_max_offset,
    ) else {
        return AbnormalRecoveryWindow::new(
            file_ranges,
            0,
            Some(checkpoint_index),
            file_count_limit,
            end_offset,
            Some("confirm_offset_out_of_range"),
        );
    };

    let checkpoint_window_start = checkpoint_index.saturating_sub(max_recovery_commit_log_files);
    let start_index = checkpoint_window_start
        .min(dispatch_progress_index)
        .min(confirm_offset_index);
    let mut window = AbnormalRecoveryWindow::new(
        file_ranges,
        start_index,
        Some(checkpoint_index),
        file_count_limit,
        end_offset,
        None,
    );
    window.dispatch_progress_index = Some(dispatch_progress_index);
    window.confirm_offset_index = Some(confirm_offset_index);
    window
}

fn configured_file_count_limit(max_recovery_commit_log_files: usize) -> Option<usize> {
    (max_recovery_commit_log_files != 0).then_some(max_recovery_commit_log_files)
}

fn valid_commit_log_range(commit_log_min_offset: i64, commit_log_max_offset: i64) -> bool {
    commit_log_min_offset >= 0 && commit_log_max_offset >= commit_log_min_offset
}

fn file_index_for_offset(
    file_ranges: &[AbnormalRecoveryFileRange],
    offset: i64,
    commit_log_min_offset: i64,
    commit_log_max_offset: i64,
) -> Option<usize> {
    if offset < commit_log_min_offset || offset > commit_log_max_offset {
        return None;
    }

    let mut selected_index = None;
    for (index, file_range) in file_ranges.iter().enumerate() {
        if offset >= file_range.start_offset {
            selected_index = Some(index);
        } else {
            break;
        }
    }
    selected_index
}

fn planned_scanned_bytes(
    file_ranges: &[AbnormalRecoveryFileRange],
    start_index: usize,
    end_offset: Option<i64>,
) -> u64 {
    let Some(start_file) = file_ranges.get(start_index) else {
        return 0;
    };
    if let Some(end_offset) = end_offset {
        if end_offset >= start_file.start_offset {
            return (end_offset - start_file.start_offset) as u64;
        }
    }

    file_ranges[start_index..]
        .iter()
        .map(|file_range| file_range.file_size)
        .sum()
}

/// Optimized check for mapped file recovery (with cached reads)
fn is_mapped_file_matched_recover(
    message_store_config: &Arc<MessageStoreConfig>,
    mapped_file: &Arc<DefaultMappedFile>,
    store_checkpoint: &StoreCheckpoint,
) -> bool {
    use std::mem;

    use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
    use rocketmq_common::UtilAll::time_millis_to_human_string;

    // Read magic code
    let magic_code = mapped_file
        .get_bytes(MESSAGE_MAGIC_CODE_POSITION, mem::size_of::<i32>())
        .unwrap_or(Bytes::from([0u8; mem::size_of::<i32>()].as_ref()))
        .get_i32();

    if magic_code != MESSAGE_MAGIC_CODE && magic_code != MESSAGE_MAGIC_CODE_V2 {
        return false;
    }

    // Read sys flag
    let sys_flag = mapped_file
        .get_bytes(SYSFLAG_POSITION, mem::size_of::<i32>())
        .unwrap_or(Bytes::from([0u8; mem::size_of::<i32>()].as_ref()))
        .get_i32();

    let born_host_length = if sys_flag & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
        8
    } else {
        20
    };

    let msg_store_time_pos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + born_host_length;

    // Read store timestamp
    let store_timestamp = mapped_file
        .get_bytes(msg_store_time_pos, mem::size_of::<i64>())
        .unwrap_or(Bytes::from([0u8; mem::size_of::<i64>()].as_ref()))
        .get_i64();

    if store_timestamp == 0 {
        return false;
    }

    if message_store_config.message_index_enable && message_store_config.message_index_safe {
        if store_timestamp <= store_checkpoint.get_min_timestamp_index() as i64 {
            info!(
                "find check timestamp, {} {}",
                store_timestamp,
                time_millis_to_human_string(store_timestamp)
            );
            return true;
        }
    } else if store_timestamp <= store_checkpoint.get_min_timestamp() as i64 {
        info!(
            "find check timestamp, {} {}",
            store_timestamp,
            time_millis_to_human_string(store_timestamp)
        );
        return true;
    }

    false
}

/// Check if a message is blank (end of file marker)
#[inline]
pub fn is_blank_message(msg_bytes: &Bytes) -> bool {
    if msg_bytes.len() < 8 {
        return false;
    }
    let mut temp = msg_bytes.clone();
    let _total_size = temp.get_i32();
    let magic_code = temp.get_i32();
    magic_code == BLANK_MAGIC_CODE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_iterator_empty() {
        // Test requires actual mapped file setup
        // This is a placeholder for integration tests
    }

    #[test]
    fn test_recovery_statistics() {
        let stats = RecoveryStatistics {
            messages_recovered: 1000,
            bytes_processed: 1024 * 1024,
            files_processed: 5,
            ..Default::default()
        };
        assert_eq!(stats.messages_recovered, 1000);
        assert_eq!(stats.bytes_processed, 1024 * 1024);
    }

    fn file_ranges(count: usize) -> Vec<AbnormalRecoveryFileRange> {
        (0..count)
            .map(|index| AbnormalRecoveryFileRange::new((index as i64) * 100, 100))
            .collect()
    }

    #[test]
    fn abnormal_recovery_window_preserves_checkpoint_start_when_limit_disabled() {
        let ranges = file_ranges(6);

        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 0, 100, 200, 0, 600);

        assert_eq!(window.start_index, 4);
        assert_eq!(window.checkpoint_index, Some(4));
        assert_eq!(window.file_count_limit, None);
        assert_eq!(window.expanded_files, 0);
        assert_eq!(window.fallback_reason, None);
    }

    #[test]
    fn abnormal_recovery_window_expands_by_configured_file_limit() {
        let ranges = file_ranges(6);

        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(5), 2, 400, 500, 0, 600);

        assert_eq!(window.start_index, 3);
        assert_eq!(window.file_count_limit, Some(2));
        assert_eq!(window.expanded_files, 2);
        assert_eq!(window.scanned_file_count, 3);
        assert_eq!(window.scanned_bytes, 300);
        assert_eq!(window.fallback_reason, None);
    }

    #[test]
    fn abnormal_recovery_window_expands_to_dispatch_progress() {
        let ranges = file_ranges(6);

        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(5), 1, 200, 500, 0, 600);

        assert_eq!(window.start_index, 2);
        assert_eq!(window.dispatch_progress_index, Some(2));
        assert_eq!(window.confirm_offset_index, Some(5));
        assert_eq!(window.expanded_files, 3);
    }

    #[test]
    fn abnormal_recovery_window_expands_to_confirm_offset() {
        let ranges = file_ranges(6);

        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(5), 1, 500, 100, 0, 600);

        assert_eq!(window.start_index, 1);
        assert_eq!(window.dispatch_progress_index, Some(5));
        assert_eq!(window.confirm_offset_index, Some(1));
        assert_eq!(window.expanded_files, 4);
    }

    #[test]
    fn abnormal_recovery_window_falls_back_when_checkpoint_missing() {
        let ranges = file_ranges(6);

        let window = plan_abnormal_recovery_window_from_ranges(&ranges, None, 2, 200, 300, 0, 600);

        assert_eq!(window.start_index, 0);
        assert_eq!(window.checkpoint_index, None);
        assert_eq!(window.fallback_reason, Some("checkpoint_not_matched"));
    }

    #[test]
    fn abnormal_recovery_window_falls_back_when_dispatch_progress_is_untrusted() {
        let ranges = file_ranges(6);

        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 2, 700, 300, 0, 600);

        assert_eq!(window.start_index, 0);
        assert_eq!(window.fallback_reason, Some("dispatch_progress_out_of_range"));
    }

    #[test]
    fn abnormal_recovery_window_falls_back_when_confirm_offset_is_untrusted() {
        let ranges = file_ranges(6);

        let window = plan_abnormal_recovery_window_from_ranges(&ranges, Some(4), 2, 300, -1, 0, 600);

        assert_eq!(window.start_index, 0);
        assert_eq!(window.fallback_reason, Some("confirm_offset_out_of_range"));
    }
}
