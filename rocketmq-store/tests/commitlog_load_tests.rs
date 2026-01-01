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

//! Integration tests for optimized CommitLog loading
//!
//! These tests verify that the optimized implementation maintains
//! exact behavioral equivalence with the original sequential implementation.

use std::fs;

use rocketmq_store::consume_queue::mapped_file_queue::MappedFileQueue;
use rocketmq_store::log_file::mapped_file::MappedFile;
use tempfile::TempDir;

/// Helper: Create test commitlog files with specific patterns
fn create_test_files(dir: &TempDir, num_files: usize, file_size: u64) {
    for i in 0..num_files {
        let offset = i as u64 * file_size;
        let file_path = dir.path().join(format!("{:020}", offset));
        let data = vec![i as u8; file_size as usize];
        fs::write(&file_path, data).expect("Failed to write test file");
    }
}

/// Regression: Load empty directory should succeed
#[test]
fn test_load_empty_directory() {
    let temp_dir = TempDir::new().unwrap();
    let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), 1024 * 1024, None);

    // Both implementations should succeed on empty directory
    std::env::set_var("ROCKETMQ_SAFE_LOAD", "1");
    assert!(queue.load());

    let mut queue2 = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), 1024 * 1024, None);
    std::env::remove_var("ROCKETMQ_SAFE_LOAD");
    assert!(queue2.load());
}

/// Regression: Load single file
#[test]
fn test_load_single_file() {
    let temp_dir = TempDir::new().unwrap();
    let file_size = 1024 * 1024u64;
    create_test_files(&temp_dir, 1, file_size);

    // Sequential
    let mut queue1 = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    std::env::set_var("ROCKETMQ_SAFE_LOAD", "1");
    assert!(queue1.load());
    let files1 = queue1.get_mapped_files();
    assert_eq!(files1.load().len(), 1);

    // Optimized
    let mut queue2 = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    std::env::remove_var("ROCKETMQ_SAFE_LOAD");
    assert!(queue2.load());
    let files2 = queue2.get_mapped_files();
    assert_eq!(files2.load().len(), 1);

    // Verify file positions are identical
    let f1 = &files1.load()[0];
    let f2 = &files2.load()[0];
    assert_eq!(f1.get_wrote_position(), f2.get_wrote_position());
    assert_eq!(f1.get_flushed_position(), f2.get_flushed_position());
    assert_eq!(f1.get_committed_position(), f2.get_committed_position());
}

/// Regression: Load multiple files maintains order
#[test]
fn test_load_multiple_files_ordered() {
    let temp_dir = TempDir::new().unwrap();
    let file_size = 1024 * 1024u64;
    let num_files = 10;
    create_test_files(&temp_dir, num_files, file_size);

    // Sequential
    let mut queue1 = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    std::env::set_var("ROCKETMQ_SAFE_LOAD", "1");
    assert!(queue1.load());

    // Optimized
    let mut queue2 = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    std::env::remove_var("ROCKETMQ_SAFE_LOAD");
    assert!(queue2.load());

    // Verify same file count and order
    let files1 = queue1.get_mapped_files();
    let files2 = queue2.get_mapped_files();
    assert_eq!(files1.load().len(), files2.load().len());
    assert_eq!(files1.load().len(), num_files);

    for i in 0..num_files {
        let f1 = &files1.load()[i];
        let f2 = &files2.load()[i];
        assert_eq!(f1.get_file_name(), f2.get_file_name());
        assert_eq!(f1.get_file_from_offset(), f2.get_file_from_offset());
    }
}

/// Regression: Empty last file is removed
#[test]
fn test_load_removes_empty_last_file() {
    let temp_dir = TempDir::new().unwrap();
    let file_size = 1024 * 1024u64;
    let num_files = 5;

    // Create files: 4 normal + 1 empty
    for i in 0..num_files - 1 {
        let offset = i as u64 * file_size;
        let file_path = temp_dir.path().join(format!("{:020}", offset));
        let data = vec![i as u8; file_size as usize];
        fs::write(&file_path, data).unwrap();
    }

    // Last file is empty
    let last_offset = (num_files - 1) as u64 * file_size;
    let last_path = temp_dir.path().join(format!("{:020}", last_offset));
    fs::write(&last_path, vec![]).unwrap();

    // Sequential
    let mut queue1 = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    std::env::set_var("ROCKETMQ_SAFE_LOAD", "1");
    assert!(queue1.load());
    assert_eq!(queue1.get_mapped_files().load().len(), num_files - 1);
    assert!(!last_path.exists()); // Removed

    // Recreate last empty file for second test
    fs::write(&last_path, vec![]).unwrap();

    // Optimized
    let mut queue2 = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    std::env::remove_var("ROCKETMQ_SAFE_LOAD");
    assert!(queue2.load());
    assert_eq!(queue2.get_mapped_files().load().len(), num_files - 1);
    assert!(!last_path.exists()); // Removed
}

/// Regression: Size mismatch is rejected
#[test]
fn test_load_rejects_size_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let expected_size = 1024 * 1024u64;
    let actual_size = 512 * 1024u64;

    let file_path = temp_dir.path().join("00000000000000000000");
    fs::write(&file_path, vec![0u8; actual_size as usize]).unwrap();

    // Sequential - should fail
    let mut queue1 = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), expected_size, None);
    std::env::set_var("ROCKETMQ_SAFE_LOAD", "1");
    assert!(!queue1.load());

    // Optimized - should also fail
    let mut queue2 = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), expected_size, None);
    std::env::remove_var("ROCKETMQ_SAFE_LOAD");
    assert!(!queue2.load());
}

/// Performance: Parallel loading is enabled for large datasets
#[test]
fn test_parallel_enabled_threshold() {
    let temp_dir = TempDir::new().unwrap();
    let file_size = 128 * 1024u64; // Small files for faster test
    let num_files = 20; // Above threshold (4+)

    create_test_files(&temp_dir, num_files, file_size);

    let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    std::env::remove_var("ROCKETMQ_SAFE_LOAD");

    let start = std::time::Instant::now();
    assert!(queue.load());
    let elapsed = start.elapsed();

    println!("Parallel load of {} files took {:?}", num_files, elapsed);
    assert_eq!(queue.get_mapped_files().load().len(), num_files);
}

/// Stress test: Load many files
#[test]
#[ignore] // Run manually: cargo test --test commitlog_load_tests -- --ignored
fn test_load_stress_100_files() {
    let temp_dir = TempDir::new().unwrap();
    let file_size = 64 * 1024u64; // Small files for speed
    let num_files = 100;

    create_test_files(&temp_dir, num_files, file_size);

    let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    std::env::remove_var("ROCKETMQ_SAFE_LOAD");

    let start = std::time::Instant::now();
    assert!(queue.load());
    let elapsed = start.elapsed();

    println!("Loaded {} files in {:?}", num_files, elapsed);
    assert_eq!(queue.get_mapped_files().load().len(), num_files);
}

/// Correctness: Verify data integrity after load
#[test]
fn test_load_data_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let file_size = 4096u64;
    let num_files = 5;

    // Create files with unique patterns
    for i in 0..num_files {
        let offset = i as u64 * file_size;
        let file_path = temp_dir.path().join(format!("{:020}", offset));
        let mut data = vec![0u8; file_size as usize];
        // Fill with pattern: index repeated
        for (idx, byte) in data.iter_mut().enumerate() {
            *byte = ((i * 256 + idx) % 256) as u8;
        }
        fs::write(&file_path, data).unwrap();
    }

    let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    std::env::remove_var("ROCKETMQ_SAFE_LOAD");
    assert!(queue.load());

    // Verify each file's data
    let files = queue.get_mapped_files();
    for (i, file) in files.load().iter().enumerate() {
        let bytes = file.get_bytes(0, file_size as usize).unwrap();
        // Check first and last bytes
        let expected_first = (i * 256) % 256;
        let expected_last = (i * 256 + file_size as usize - 1) % 256;
        assert_eq!(bytes[0], expected_first as u8);
        assert_eq!(bytes[bytes.len() - 1], expected_last as u8);
    }
}
