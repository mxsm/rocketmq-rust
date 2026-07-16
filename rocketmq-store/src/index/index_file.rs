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

use std::io;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::hasher::string_hasher::JavaStringHasher;
use rocketmq_store_local::index::codec::index_file_total_size as local_index_file_total_size;
use rocketmq_store_local::index::codec::IndexLayoutError;
use rocketmq_store_local::index::codec::INDEX_ENTRY_SIZE;
use rocketmq_store_local::index::codec::INDEX_HASH_SLOT_SIZE;
use rocketmq_store_local::index::file::drive_index_put;
use rocketmq_store_local::index::file::is_index_time_matched;
use rocketmq_store_local::index::file::normalize_index_key_hash;
use rocketmq_store_local::index::file::query_index_offsets;
use rocketmq_store_local::index::file::IndexFileSnapshot;
use rocketmq_store_local::index::file::IndexHeaderUpdate;
use rocketmq_store_local::index::file::IndexPutOutcome;
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
        let mapped_file = Arc::new(DefaultMappedFile::try_new(
            CheetahString::from_slice(file_name),
            file_total_size as u64,
        )?);

        let index_header = IndexHeader::new(mapped_file.clone());
        let index_file = IndexFile {
            hash_slot_num,
            index_num,
            file_total_size,
            mapped_file,
            index_header,
        };

        if end_phy_offset > 0 {
            index_file.index_header.set_begin_phy_offset(end_phy_offset);
            index_file.index_header.set_end_phy_offset(end_phy_offset);
        }

        if end_timestamp > 0 {
            index_file.index_header.set_begin_timestamp(end_timestamp);
            index_file.index_header.set_end_timestamp(end_timestamp);
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
        self.index_header.load();
    }

    #[inline]
    pub fn shutdown(&self) {
        self.flush();
    }

    pub fn flush(&self) {
        let begin_time = std::time::Instant::now();
        if self.mapped_file.hold() {
            self.index_header.update_byte_buffer();
            self.mapped_file.flush(0);
            self.mapped_file.release();
            info!("flush index file elapsed time(ms) {}", begin_time.elapsed().as_millis());
        }
    }

    #[inline]
    pub fn is_write_full(&self) -> bool {
        self.index_header.get_index_count() >= self.index_num as i32
    }

    #[inline]
    pub fn destroy(&self, interval_forcibly: u64) -> bool {
        self.mapped_file.destroy(interval_forcibly)
    }

    pub fn put_key(&self, key: &str, phy_offset: i64, store_timestamp: i64) -> bool {
        let outcome = drive_index_put(
            self.snapshot(),
            self.index_key_hash_method(key),
            phy_offset,
            store_timestamp,
            |position| self.read_index_bytes(position),
            |position, bytes| {
                self.mapped_file.write_bytes_segment(bytes, position, 0, bytes.len());
            },
            |update| self.apply_header_update(update),
        );
        match outcome {
            IndexPutOutcome::Written => true,
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
        // CRITICAL: Must hold and release mapped_file to prevent resource leak
        if !self.mapped_file.hold() {
            return;
        }

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
        self.mapped_file.release();
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
        self.mapped_file.get_slice(position, N)?.try_into().ok()
    }

    fn apply_header_update(&self, update: IndexHeaderUpdate) {
        match update {
            IndexHeaderUpdate::SetBeginPhyOffset(offset) => self.index_header.set_begin_phy_offset(offset),
            IndexHeaderUpdate::SetBeginTimestamp(timestamp) => self.index_header.set_begin_timestamp(timestamp),
            IndexHeaderUpdate::IncrementHashSlotCount => self.index_header.inc_hash_slot_count(),
            IndexHeaderUpdate::IncrementIndexCount => self.index_header.inc_index_count(),
            IndexHeaderUpdate::SetEndPhyOffset(offset) => self.index_header.set_end_phy_offset(offset),
            IndexHeaderUpdate::SetEndTimestamp(timestamp) => self.index_header.set_end_timestamp(timestamp),
        }
    }
}

fn index_file_total_size(hash_slot_num: usize, index_num: usize) -> io::Result<usize> {
    local_index_file_total_size(hash_slot_num, index_num).map_err(|error| {
        let message = match error {
            IndexLayoutError::ZeroHashSlots => "index hash slot number must be positive",
            IndexLayoutError::ZeroIndexEntries => "index entry number must be positive",
            IndexLayoutError::HashSlotSectionOverflow => "index hash slot section size overflow",
            IndexLayoutError::IndexEntrySectionOverflow => "index entry section size overflow",
            IndexLayoutError::TotalSizeOverflow => "index file total size overflow",
        };
        io::Error::new(io::ErrorKind::InvalidInput, message)
    })
}

#[cfg(test)]
mod tests {
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
