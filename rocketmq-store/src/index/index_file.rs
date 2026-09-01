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

use std::cell::Cell;
use std::io;
use std::sync::Arc;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use rocketmq_model::common::hasher::string_hasher::JavaStringHasher;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_local::index::codec::index_file_total_size as local_index_file_total_size;
use rocketmq_store_local::index::codec::INDEX_ENTRY_SIZE;
use rocketmq_store_local::index::codec::INDEX_HASH_SLOT_SIZE;
use rocketmq_store_local::index::file::drive_index_put;
use rocketmq_store_local::index::file::is_index_time_matched;
use rocketmq_store_local::index::file::normalize_index_key_hash;
use rocketmq_store_local::index::file::query_index_offsets;
use rocketmq_store_local::index::file::IndexFileSnapshot;
use rocketmq_store_local::index::file::IndexHeaderUpdate;
use rocketmq_store_local::index::file::IndexPutOutcome;
#[cfg(test)]
use rocketmq_store_local::mapped_file::MappedFileAdmissionState;
use rocketmq_store_local::mapped_file::MappedFileDestroyOutcome;
use tracing::info;
use tracing::warn;

use crate::index::index_header::IndexHeader;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

/// Default hash slot count (5 million slots)
/// Same as Java: org.apache.rocketmq.store.config.MessageStoreConfig.maxHashSlotNum
pub const DEFAULT_HASH_SLOT_NUM: usize = 5_000_000;

/// Default max index count (20 million entries = 5M slots * 4)
/// Same as Java: org.apache.rocketmq.store.config.MessageStoreConfig.maxIndexNum
pub const DEFAULT_INDEX_NUM: usize = 20_000_000;

/// Index file for fast message lookup by Key or time range.
///
/// # File Structure
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────────────────┐
/// │                         Index File Header (40 Bytes)                       │
/// │  beginTimestamp(8) + endTimestamp(8) + beginPhyOffset(8) +                │
/// │  endPhyOffset(8) + hashSlotCount(4) + indexCount(4)                       │
/// ├────────────────────────────────────────────────────────────────────────────┤
/// │                    Hash Slot Table (5M * 4 Bytes)                          │
/// │  Each slot stores the latest index position (i32) for that hash bucket    │
/// ├────────────────────────────────────────────────────────────────────────────┤
/// │                    Index Entry Array (20M * 20 Bytes)                      │
/// │  Each entry: keyHash(4) + phyOffset(8) + timeDiff(4) + nextIndex(4)      │
/// └────────────────────────────────────────────────────────────────────────────┘
/// ```
///
/// # Index Entry Format
///
/// ```text
/// ┌───────────────┬───────────────────────────────┬───────────────┬───────────────┐
/// │ Key HashCode  │        Physical Offset        │   Time Diff   │ Next Index Pos│
/// │   (4 Bytes)   │          (8 Bytes)            │   (4 Bytes)   │   (4 Bytes)   │
/// ├───────────────┴───────────────────────────────┴───────────────┴───────────────┤
/// │                                 Index Store Unit                              │
/// │                                                                               │
/// ```
///
/// # Hash Collision Handling
///
/// Uses **chained hashing with head insertion**:
/// - When collision occurs, new entry's `nextIndex` points to old slot value
/// - Slot is updated to point to new entry
/// - Forms a linked list: Slot → Entry_N → Entry_N-1 → ... → Entry_1
///
/// # Thread Safety
///
/// This structure is NOT thread-safe. External synchronization (e.g., by IndexService) required.
///
/// # Binary Compatibility
///
/// **MUST** maintain binary compatibility with Java RocketMQ IndexFile format.
/// Uses Big-Endian byte order (`to_be_bytes()`) to match Java's default.
pub struct IndexFile {
    hash_slot_num: usize,
    index_num: usize,
    file_total_size: usize,
    mapped_file: Arc<DefaultMappedFile>,
    index_header: IndexHeader,
    operation_closing: RwLock<bool>,
}

impl PartialEq for IndexFile {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self as *const IndexFile, other as *const IndexFile)
    }
}

impl IndexFile {
    pub fn new(
        file_name: &str,
        hash_slot_num: usize,
        index_num: usize,
        end_phy_offset: i64,
        end_timestamp: i64,
    ) -> IndexFile {
        Self::try_new(file_name, hash_slot_num, index_num, end_phy_offset, end_timestamp)
            .expect("Create index file failed")
    }

    pub fn try_new(
        file_name: &str,
        hash_slot_num: usize,
        index_num: usize,
        end_phy_offset: i64,
        end_timestamp: i64,
    ) -> io::Result<IndexFile> {
        let file_total_size = index_file_total_size(hash_slot_num, index_num)?;
        let file_total_size_i32 = i32::try_from(file_total_size).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "index file size exceeds i32 progress range",
            )
        })?;
        let mapped_file = Arc::new(DefaultMappedFile::try_new(
            CheetahString::from_slice(file_name),
            file_total_size as u64,
        )?);
        // IndexFile performs random-access writes across the preallocated mapping rather than
        // append-position writes. Mark the complete mapping readable so flush covers every header,
        // slot, and entry update.
        mapped_file.set_wrote_position(file_total_size_i32);

        let index_header = IndexHeader::new(mapped_file.clone());
        let index_file = IndexFile {
            hash_slot_num,
            index_num,
            file_total_size,
            mapped_file,
            index_header,
            operation_closing: RwLock::new(false),
        };

        let mut initial_updates = Vec::with_capacity(4);
        if end_phy_offset > 0 {
            initial_updates.push(IndexHeaderUpdate::SetBeginPhyOffset(end_phy_offset));
            initial_updates.push(IndexHeaderUpdate::SetEndPhyOffset(end_phy_offset));
        }

        if end_timestamp > 0 {
            initial_updates.push(IndexHeaderUpdate::SetBeginTimestamp(end_timestamp));
            initial_updates.push(IndexHeaderUpdate::SetEndTimestamp(end_timestamp));
        }
        if !initial_updates.is_empty() && !index_file.index_header.try_apply_updates(&initial_updates) {
            return Err(io::Error::other("failed to initialize index file header"));
        }
        Ok(index_file)
    }

    #[inline]
    pub fn get_file_name(&self) -> &CheetahString {
        self.mapped_file.get_file_name()
    }

    #[inline]
    pub fn get_file_size(&self) -> usize {
        self.file_total_size
    }

    #[inline]
    pub fn load(&self) {
        let _ = self.try_load_with(|| self.index_header.load());
    }

    #[inline]
    pub fn shutdown(&self) {
        let begin_time = std::time::Instant::now();
        let mut closing = self.operation_closing.write();
        if *closing {
            MappedFile::shutdown(self.mapped_file.as_ref(), 0);
            return;
        }
        *closing = true;

        match self.flush_header_and_mapping(|| self.index_header.try_update_byte_buffer()) {
            Ok(_) => {
                info!(
                    "final index file flush elapsed time(ms) {}",
                    begin_time.elapsed().as_millis()
                );
            }
            Err(error) => {
                warn!(
                    file_name = %self.get_file_name(),
                    error = %error,
                    "failed to perform final index file flush during shutdown"
                );
            }
        }
        MappedFile::shutdown(self.mapped_file.as_ref(), 0);
    }

    pub fn flush(&self) {
        let begin_time = std::time::Instant::now();
        match self.try_flush() {
            Ok(_) => {
                info!("flush index file elapsed time(ms) {}", begin_time.elapsed().as_millis());
            }
            Err(error) => {
                warn!(
                    file_name = %self.get_file_name(),
                    error = %error,
                    "failed to flush index file"
                );
            }
        }
    }

    fn try_flush(&self) -> Result<i32, StoreError> {
        self.try_flush_with_header_update(|| self.index_header.try_update_byte_buffer())
    }

    fn try_flush_with_header_update<F>(&self, update_header: F) -> Result<i32, StoreError>
    where
        F: FnOnce() -> bool,
    {
        let Some(_operation) = self.try_enter_operation() else {
            return Err(Self::closing_error());
        };
        self.flush_header_and_mapping(update_header)
    }

    fn flush_header_and_mapping<F>(&self, update_header: F) -> Result<i32, StoreError>
    where
        F: FnOnce() -> bool,
    {
        if !update_header() {
            return Err(
                StoreError::new(&rocketmq_error::STORAGE_INTERNAL_FAILURE, StoreOperation::Flush)
                    .in_component(StoreComponent::MappedFile),
            );
        }
        self.mapped_file.try_flush(0)
    }

    #[inline]
    pub fn is_write_full(&self) -> bool {
        self.index_header.get_index_count() >= self.index_num as i32
    }

    #[inline]
    pub fn destroy(&self, interval_forcibly: u64) -> bool {
        self.try_destroy(interval_forcibly).is_namespace_removed()
    }

    #[inline]
    pub fn try_destroy(&self, interval_forcibly: u64) -> MappedFileDestroyOutcome {
        self.try_destroy_with_callbacks(interval_forcibly, || {}, || {})
    }

    #[cfg(test)]
    pub(crate) fn hold_for_testing(&self) -> bool {
        self.mapped_file.hold()
    }

    #[cfg(test)]
    pub(crate) fn release_for_testing(&self) {
        self.mapped_file.release();
    }

    pub fn put_key(&self, key: &str, phy_offset: i64, store_timestamp: i64) -> bool {
        self.put_key_with(
            key,
            phy_offset,
            store_timestamp,
            || {},
            |position, bytes| self.mapped_file.write_bytes_segment(bytes, position, 0, bytes.len()),
        )
    }

    fn put_key_with<OnEntered, WriteBytes>(
        &self,
        key: &str,
        phy_offset: i64,
        store_timestamp: i64,
        on_entered: OnEntered,
        mut write_bytes: WriteBytes,
    ) -> bool
    where
        OnEntered: FnOnce(),
        WriteBytes: FnMut(usize, &[u8]) -> bool,
    {
        let Some(_operation) = self.try_enter_operation() else {
            return false;
        };
        on_entered();

        let writes_succeeded = Cell::new(true);
        let mut header_updates = Vec::with_capacity(6);
        let outcome = drive_index_put(
            self.snapshot(),
            self.index_key_hash_method(key),
            phy_offset,
            store_timestamp,
            |position| self.read_index_bytes(position),
            |position, bytes| {
                if writes_succeeded.get() && !write_bytes(position, bytes) {
                    writes_succeeded.set(false);
                }
            },
            |update| header_updates.push(update),
        );
        match outcome {
            IndexPutOutcome::Written => {
                if !writes_succeeded.get() {
                    warn!(file_name = %self.get_file_name(), "failed to write index entry");
                    return false;
                }
                if !self.index_header.try_apply_updates(&header_updates) {
                    warn!(file_name = %self.get_file_name(), "failed to publish index header");
                    return false;
                }
                true
            }
            IndexPutOutcome::Full => {
                warn!(
                    "Over index file capacity: index count = {}; index max num = {}",
                    self.index_header.get_index_count(),
                    self.index_num
                );
                false
            }
            IndexPutOutcome::SlotUnavailable => {
                warn!("Index hash slot is outside the mapped file");
                false
            }
            IndexPutOutcome::LayoutOverflow => {
                warn!("Index file layout position overflow");
                false
            }
        }
    }

    pub fn index_key_hash_method(&self, key: &str) -> i32 {
        normalize_index_key_hash(JavaStringHasher::hash_str(key))
    }

    #[inline]
    pub fn get_begin_timestamp(&self) -> i64 {
        self.index_header.get_begin_timestamp()
    }

    #[inline]
    pub fn get_end_timestamp(&self) -> i64 {
        self.index_header.get_end_timestamp()
    }

    #[inline]
    pub fn get_end_phy_offset(&self) -> i64 {
        self.index_header.get_end_phy_offset()
    }

    #[inline]
    pub fn has_entries(&self) -> bool {
        self.index_header.get_index_count() > 1
    }

    pub fn is_time_matched(&self, begin: i64, end: i64) -> bool {
        is_index_time_matched(
            self.index_header.get_begin_timestamp(),
            self.index_header.get_end_timestamp(),
            begin,
            end,
        )
    }

    pub fn select_phy_offset(&self, phy_offsets: &mut Vec<i64>, key: &str, max_num: usize, begin: i64, end: i64) {
        let Some(_operation) = self.try_enter_operation() else {
            return;
        };
        query_index_offsets(
            self.snapshot(),
            self.index_key_hash_method(key),
            max_num,
            begin,
            end,
            phy_offsets,
            |position| self.read_index_bytes::<INDEX_HASH_SLOT_SIZE>(position),
            |position| self.read_index_bytes::<INDEX_ENTRY_SIZE>(position),
        );
    }

    fn snapshot(&self) -> IndexFileSnapshot {
        IndexFileSnapshot::new(
            self.hash_slot_num,
            self.index_num,
            self.index_header.get_index_count(),
            self.index_header.get_begin_timestamp(),
        )
    }

    fn read_index_bytes<const N: usize>(&self, position: usize) -> Option<[u8; N]> {
        self.mapped_file.get_slice(position, N)?.as_ref().try_into().ok()
    }

    fn try_load_with<F>(&self, load: F) -> bool
    where
        F: FnOnce(),
    {
        let Some(_operation) = self.try_enter_operation() else {
            return false;
        };
        load();
        true
    }

    fn try_enter_operation(&self) -> Option<RwLockReadGuard<'_, bool>> {
        let operation = self.operation_closing.read();
        if *operation {
            return None;
        }
        Some(operation)
    }

    fn try_destroy_with_callbacks<BeforeWait, OnClosing>(
        &self,
        interval_forcibly: u64,
        before_wait: BeforeWait,
        on_closing: OnClosing,
    ) -> MappedFileDestroyOutcome
    where
        BeforeWait: FnOnce(),
        OnClosing: FnOnce(),
    {
        before_wait();
        let mut closing = self.operation_closing.write();
        *closing = true;
        on_closing();
        self.mapped_file.try_destroy(interval_forcibly)
    }

    fn closing_error() -> StoreError {
        StoreError::new(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, StoreOperation::Flush)
            .in_component(StoreComponent::MappedFile)
    }
}

fn index_file_total_size(hash_slot_num: usize, index_num: usize) -> io::Result<usize> {
    local_index_file_total_size(hash_slot_num, index_num)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid or overflowing index file layout"))
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;
    use std::panic::AssertUnwindSafe;
    use std::sync::mpsc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_index_key_hash_method_consistency() {
        // Test hash algorithm matches Java's String.hashCode()
        let file = create_test_index_file("20000000000000");

        // Test cases verified against Java
        assert_eq!(file.index_key_hash_method("hello"), 99162322);
        assert_eq!(file.index_key_hash_method(""), 0);
        assert_eq!(file.index_key_hash_method("test"), 3556498);

        // Test i32::MIN edge case
        let hash_result = file.index_key_hash_method("some_key_that_produces_min");
        assert!(hash_result >= 0, "Hash should be positive after abs()");
    }

    #[test]
    fn test_put_key_basic() {
        let file = create_test_index_file("20000000000001");

        // Put first key
        assert!(file.put_key("key1", 1000, 1000000000000));
        assert_eq!(file.index_header.get_index_count(), 2); // Starts at 1, increments to 2

        // Put second key
        assert!(file.put_key("key2", 2000, 1000000001000));
        assert_eq!(file.index_header.get_index_count(), 3);

        // Verify timestamps
        assert_eq!(file.index_header.get_begin_timestamp(), 1000000000000);
        assert_eq!(file.index_header.get_end_timestamp(), 1000000001000);
    }

    #[test]
    fn test_put_key_hash_collision() {
        let file = create_test_index_file("20000000000002");

        // Generate keys with same hash slot (key1 and key2 collide modulo hashSlotNum)
        let key1 = "collision_test_1";
        let key2 = "collision_test_2";

        file.put_key(key1, 1000, 1000000000000);
        file.put_key(key2, 2000, 1000000001000);

        // Both should succeed
        assert_eq!(file.index_header.get_index_count(), 3); // 1 initial + 2 puts
    }

    #[test]
    fn test_is_write_full() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("20000000000003");
        let file_path_str = file_path.to_str().unwrap();

        let file = IndexFile::new(file_path_str, 100, 5, 0, 0); // Only 5 slots

        assert!(!file.is_write_full());

        // Fill up the file
        for i in 0..4 {
            file.put_key(&format!("key{}", i), i as i64 * 1000, 1000000000000 + i as i64 * 1000);
        }

        assert!(file.is_write_full());

        // Should reject new writes
        assert!(!file.put_key("overflow_key", 9999, 1000000009999));

        // temp_dir auto-cleanup on drop
    }

    #[test]
    fn try_new_rejects_invalid_index_dimensions() {
        use std::io::ErrorKind;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("20000000000030");
        let file_path_str = file_path.to_str().unwrap();

        let zero_slots_error = match IndexFile::try_new(file_path_str, 0, 1, 0, 0) {
            Ok(_) => panic!("zero hash slots should be rejected"),
            Err(error) => error,
        };
        assert_eq!(zero_slots_error.kind(), ErrorKind::InvalidInput);

        let zero_indexes_error = match IndexFile::try_new(file_path_str, 1, 0, 0, 0) {
            Ok(_) => panic!("zero index entries should be rejected"),
            Err(error) => error,
        };
        assert_eq!(zero_indexes_error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn test_time_diff_overflow_handling() {
        let file = create_test_index_file("20000000000004");

        // Test normal time diff
        file.put_key("key1", 1000, 1000000000000);
        assert_eq!(file.index_header.get_begin_timestamp(), 1000000000000);

        // Test time diff > i32::MAX seconds (should clamp to MAX)
        let huge_timestamp = 1000000000000 + (i32::MAX as i64 + 1000) * 1000;
        file.put_key("key2", 2000, huge_timestamp);

        // Should succeed without panic
        assert_eq!(file.index_header.get_index_count(), 3);
    }

    #[test]
    fn test_time_diff_negative_handling() {
        let file = create_test_index_file("20000000000005");

        file.put_key("key1", 1000, 1000000000000);

        // Put a key with earlier timestamp (should clamp timeDiff to 0)
        file.put_key("key2", 2000, 999999999000);

        assert_eq!(file.index_header.get_index_count(), 3);
    }

    #[test]
    fn test_select_phy_offset_basic() {
        let file = create_test_index_file("20000000000006");

        let begin_time = 1000000000000;
        file.put_key("search_key", 12345, begin_time);
        file.put_key("search_key", 23456, begin_time + 1000);
        file.put_key("other_key", 99999, begin_time + 2000);

        let mut results = Vec::new();
        file.select_phy_offset(&mut results, "search_key", 10, begin_time - 1000, begin_time + 3000);

        // Should find 2 entries for "search_key"
        assert_eq!(results.len(), 2);
        assert!(results.contains(&12345));
        assert!(results.contains(&23456));
    }

    #[test]
    fn test_select_phy_offset_time_range_filter() {
        let file = create_test_index_file("20000000000007");

        let base_time = 1000000000000;
        file.put_key("key", 1000, base_time);
        file.put_key("key", 2000, base_time + 5000);
        file.put_key("key", 3000, base_time + 10000);

        let mut results = Vec::new();
        // Query range: [base_time + 3000, base_time + 7000]
        // Should only find entry at base_time + 5000
        file.select_phy_offset(&mut results, "key", 10, base_time + 3000, base_time + 7000);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 2000);
    }

    #[test]
    fn test_select_phy_offset_max_num_limit() {
        let file = create_test_index_file("20000000000008");

        let base_time = 1000000000000;
        // Put 10 entries with same key
        for i in 0..10 {
            file.put_key("same_key", (i * 1000) as i64, base_time + i as i64 * 1000);
        }

        let mut results = Vec::new();
        // Limit to 5 results
        file.select_phy_offset(&mut results, "same_key", 5, base_time - 1000, base_time + 20000);

        assert_eq!(results.len(), 5, "Should respect max_num limit");
    }

    #[test]
    fn query_reads_entries_from_the_second_half_of_the_index_file() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("20000000000031");
        let file = IndexFile::new(file_path.to_str().unwrap(), 1, 10, 0, 0);
        let base_time = 1_000_000_000_000;
        for index in 0..8 {
            assert!(file.put_key("same_key", index, base_time + index * 1000));
        }

        let mut results = Vec::new();
        file.select_phy_offset(&mut results, "same_key", 10, base_time, base_time + 10_000);

        assert_eq!(results.len(), 8);
        assert!(results.contains(&0));
        assert!(results.contains(&7));
    }

    #[test]
    fn panic_during_flush_preparation_does_not_leak_a_mapped_file_lease() {
        let file = create_test_index_file("20000000000032");
        let leases_before = file.mapped_file.lifecycle_snapshot().active_leases;

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = file.try_flush_with_header_update(|| panic!("injected header update panic"));
        }));

        assert!(result.is_err());
        assert_eq!(file.mapped_file.lifecycle_snapshot().active_leases, leases_before);
    }

    #[test]
    fn closed_file_flush_and_select_do_not_advance_or_leak() {
        let file = create_test_index_file("20000000000033");
        let timestamp = 1_000_000_000_000;
        assert!(file.put_key("closed-key", 42, timestamp));
        let flushed_before = file.mapped_file.get_flushed_position();

        MappedFile::shutdown(file.mapped_file.as_ref(), 0);
        let leases_before = file.mapped_file.lifecycle_snapshot().active_leases;
        assert!(file.try_flush().is_err());
        assert_eq!(file.mapped_file.get_flushed_position(), flushed_before);

        let mut offsets = vec![7];
        file.select_phy_offset(&mut offsets, "closed-key", 10, timestamp - 1, timestamp + 1);

        assert_eq!(offsets, vec![7]);
        assert_eq!(file.mapped_file.lifecycle_snapshot().active_leases, leases_before);
    }

    #[test]
    fn failed_index_write_does_not_publish_header_or_report_success() {
        let file = create_test_index_file("20000000000034");
        let writes = Cell::new(0);
        let index_count_before = file.index_header.get_index_count();
        let end_offset_before = file.index_header.get_end_phy_offset();
        let end_timestamp_before = file.index_header.get_end_timestamp();

        let written = file.put_key_with(
            "retry-key",
            91,
            1_000_000_000_000,
            || {},
            |position, bytes| {
                let attempt = writes.get() + 1;
                writes.set(attempt);
                attempt != 2 && file.mapped_file.write_bytes_segment(bytes, position, 0, bytes.len())
            },
        );

        assert!(!written);
        assert_eq!(writes.get(), 2);
        assert_eq!(file.index_header.get_index_count(), index_count_before);
        assert_eq!(file.index_header.get_end_phy_offset(), end_offset_before);
        assert_eq!(file.index_header.get_end_timestamp(), end_timestamp_before);
        assert!(file.put_key("retry-key", 91, 1_000_000_000_000));
        assert_eq!(file.index_header.get_index_count(), index_count_before + 1);
    }

    #[test]
    fn shutdown_final_flushes_then_rejects_late_operations() {
        let file = create_test_index_file("20000000000036");
        assert!(file.put_key("before-shutdown", 17, 1_000_000_000_000));
        assert_eq!(file.mapped_file.get_flushed_position(), 0);

        file.shutdown();

        assert_eq!(
            file.mapped_file.get_flushed_position(),
            file.file_total_size as i32,
            "shutdown must flush the complete random-access index mapping"
        );
        assert_eq!(
            file.mapped_file.lifecycle_snapshot().state,
            MappedFileAdmissionState::Closing
        );
        let index_count = file.index_header.get_index_count();
        assert!(!file.put_key("after-shutdown", 18, 1_000_000_001_000));
        assert_eq!(file.index_header.get_index_count(), index_count);
        assert!(file.try_flush().is_err());
    }

    #[test]
    fn destroy_waits_for_entered_operation_and_rejects_new_operations() {
        let file = Arc::new(create_test_index_file("20000000000035"));
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let operation_file = Arc::clone(&file);
        let writer_file = Arc::clone(&file);
        let operation_entered = Arc::clone(&entered);
        let operation_release = Arc::clone(&release);
        let operation = thread::spawn(move || {
            operation_file.put_key_with(
                "fenced-key",
                17,
                1_000_000_000_000,
                || {
                    operation_entered.wait();
                    operation_release.wait();
                },
                |position, bytes| {
                    writer_file
                        .mapped_file
                        .write_bytes_segment(bytes, position, 0, bytes.len())
                },
            )
        });
        entered.wait();

        let (waiting_tx, waiting_rx) = mpsc::channel();
        let (closing_tx, closing_rx) = mpsc::channel();
        let destroy_file = Arc::clone(&file);
        let destroy = thread::spawn(move || {
            destroy_file.try_destroy_with_callbacks(
                0,
                || waiting_tx.send(()).expect("publish destroy wait"),
                || closing_tx.send(()).expect("publish closing"),
            )
        });

        waiting_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("destroy reached the operation fence");
        assert!(matches!(closing_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

        release.wait();
        assert!(operation.join().expect("join active index operation"));
        closing_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("destroy entered closing after the operation drained");
        assert!(destroy.join().expect("join index destroy").is_namespace_removed());

        let index_count = file.index_header.get_index_count();
        assert!(!file.put_key("late-key", 18, 1_000_000_001_000));
        assert_eq!(file.index_header.get_index_count(), index_count);
        let mut offsets = vec![99];
        file.select_phy_offset(&mut offsets, "fenced-key", 10, 0, i64::MAX);
        assert_eq!(offsets, vec![99]);
        assert!(file.try_flush().is_err());
        let load_called = Cell::new(false);
        assert!(!file.try_load_with(|| load_called.set(true)));
        assert!(!load_called.get());
    }

    #[test]
    fn test_is_time_matched() {
        let file = create_test_index_file("20000000000009");

        file.put_key("key1", 1000, 1000000000000);
        file.put_key("key2", 2000, 1000000010000);

        let begin_ts = file.index_header.get_begin_timestamp();
        let end_ts = file.index_header.get_end_timestamp();

        // Query range fully contains file range
        assert!(file.is_time_matched(begin_ts - 1000, end_ts + 1000));

        // Query range partially overlaps (begin inside)
        assert!(file.is_time_matched(begin_ts + 1000, end_ts + 1000));

        // Query range partially overlaps (end inside)
        assert!(file.is_time_matched(begin_ts - 1000, end_ts - 1000));

        // Query range fully inside file range
        assert!(file.is_time_matched(begin_ts + 1000, end_ts - 1000));

        // Query range outside file range
        assert!(!file.is_time_matched(begin_ts - 5000, begin_ts - 2000));
        assert!(!file.is_time_matched(end_ts + 2000, end_ts + 5000));
    }

    // Helper function to create test IndexFile with temporary file
    // Note: filename must be numeric (timestamp format) for DefaultMappedFile
    // Uses tempfile crate for automatic cleanup
    fn create_test_index_file(filename: &str) -> TestIndexFile {
        use tempfile::TempDir;

        // Create temporary directory (auto-deleted when TempDir is dropped)
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join(filename);
        let file_path_str = file_path.to_str().unwrap().to_string();

        let index_file = IndexFile::new(&file_path_str, 100, 1000, 0, 0);

        TestIndexFile {
            index_file,
            _temp_dir: temp_dir, // Keep temp_dir alive, auto-cleanup on drop
        }
    }

    // Wrapper for IndexFile that auto-cleans up on drop via TempDir
    struct TestIndexFile {
        index_file: IndexFile,
        _temp_dir: tempfile::TempDir, // Underscore prefix indicates intentionally unused
    }

    impl std::ops::Deref for TestIndexFile {
        type Target = IndexFile;

        fn deref(&self) -> &Self::Target {
            &self.index_file
        }
    }
}
