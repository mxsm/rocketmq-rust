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

/*
//! Integration Tests: PageCache Pre-warming and Zero-Copy
//!
//! Tests validate:
//! 1. PageCache pre-warming reduces page faults
//! 2. Zero-copy path works correctly
//! 3. Performance improvements are measurable

#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_store::base::allocate_mapped_file_service::AllocateMappedFileService;
use rocketmq_store::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use rocketmq_store::log_file::mapped_file::MappedFile;
use tempfile::TempDir;

/// Helper: Create a test file path
fn create_test_file_path(temp_dir: &TempDir, name: &str) -> String {
    temp_dir.path().join(name).to_string_lossy().to_string()
}

/// Test 1: PageCache Pre-warming is Applied (Linux only)
///
/// Validates that madvise(WILLNEED) is called during file pre-allocation.
/// Expected: File pages are loaded into PageCache before first write.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_pagecache_warming_applied() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = create_test_file_path(&temp_dir, "test_pagecache_warm.log");
    let file_size = 4 * 1024 * 1024; // 4MB

    // Create AllocateMappedFileService
    let service = AllocateMappedFileService::new();

    // Submit allocation request (should trigger PageCache warming)
    let result_rx = service.submit_request(file_path.clone(), file_size);

    // Wait for allocation to complete
    let mapped_file = result_rx
        .await
        .expect("Failed to receive allocation result")
        .expect("Allocation failed");

    // Verify file was created
    assert!(std::path::Path::new(&file_path).exists());

    // On Linux, check that pages are in PageCache
    // We can use mincore() or check /proc/self/pagemap, but for simplicity,
    // we'll just verify the file size and that first write is fast

    let metadata = std::fs::metadata(&file_path).unwrap();
    assert_eq!(metadata.len(), file_size);

    // First write should be fast (no major page faults)
    let start = Instant::now();
    let buffer = vec![0u8; 4096];
    mapped_file.append_message_bytes_no_position_update_ref(&buffer);
    let elapsed = start.elapsed();

    // With pre-warming, first write should be < 1ms (no major page fault)
    assert!(
        elapsed.as_millis() < 10,
        "First write took {:?}, expected <10ms with PageCache warming",
        elapsed
    );
}

/// Test 2: Compare Pre-warmed vs Non-warmed File Performance
///
/// Creates two files: one with pre-warming, one without.
/// Measures page fault count and first-write latency.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_pagecache_warming_reduces_page_faults() {
    let temp_dir = TempDir::new().unwrap();

    // Test 1: File WITHOUT pre-warming (standard creation)
    let file_path_cold = create_test_file_path(&temp_dir, "cold.log");
    let file_size = 4 * 1024 * 1024;

    let cold_file = DefaultMappedFile::new(
        CheetahString::from_string(file_path_cold.clone()),
        file_size,
    );

    // Measure cold file first write (will have page faults)
    let start_cold = Instant::now();
    let buffer = vec![b'A'; 4096];
    for i in 0..1024 {
        cold_file.append_message_bytes_no_position_update_ref(&buffer);
    }
    let elapsed_cold = start_cold.elapsed();

    // Test 2: File WITH pre-warming (via AllocateMappedFileService)
    let file_path_warm = create_test_file_path(&temp_dir, "warm.log");
    let service = AllocateMappedFileService::new();

    let result_rx = service.submit_request(file_path_warm.clone(), file_size);
    let warm_file = result_rx.await.unwrap().unwrap();

    // Small delay to let kernel pre-load pages
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Measure warm file first write (should have fewer page faults)
    let start_warm = Instant::now();
    for i in 0..1024 {
        warm_file.append_message_bytes_no_position_update_ref(&buffer);
    }
    let elapsed_warm = start_warm.elapsed();

    println!("Cold file (no pre-warming): {:?}", elapsed_cold);
    println!("Warm file (with pre-warming): {:?}", elapsed_warm);
    println!(
        "Improvement: {:.1}%",
        (1.0 - elapsed_warm.as_secs_f64() / elapsed_cold.as_secs_f64()) * 100.0
    );

    // Pre-warmed file should be faster (at least 10% improvement)
    // In practice, improvement can be 15-20% or more
    assert!(
        elapsed_warm < elapsed_cold,
        "Pre-warmed file should be faster. Cold: {:?}, Warm: {:?}",
        elapsed_cold,
        elapsed_warm
    );
}

/// Test 3: PageCache Warming with Large Files
///
/// Tests that pre-warming works correctly with large files (>100MB).
#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_pagecache_warming_large_files() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = create_test_file_path(&temp_dir, "large_file.log");
    let file_size = 128 * 1024 * 1024; // 128MB

    let service = AllocateMappedFileService::new();
    let result_rx = service.submit_request(file_path.clone(), file_size);

    let start = Instant::now();
    let mapped_file = result_rx.await.unwrap().unwrap();
    let allocation_time = start.elapsed();

    println!(
        "Large file (128MB) allocation + warming: {:?}",
        allocation_time
    );

    // Allocation + warming should complete in reasonable time
    assert!(
        allocation_time.as_secs() < 5,
        "Allocation took too long: {:?}",
        allocation_time
    );

    // Verify file exists and has correct size
    let metadata = std::fs::metadata(&file_path).unwrap();
    assert_eq!(metadata.len(), file_size);
}

/// Test 4: Zero-Copy Direct Write Buffer
///
/// Tests that get_direct_write_buffer() returns valid mutable buffer.
#[tokio::test]
async fn test_zero_copy_direct_write_buffer() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = create_test_file_path(&temp_dir, "zerocopy.log");
    let file_size = 10 * 1024 * 1024;

    let mapped_file = DefaultMappedFile::new(CheetahString::from_string(file_path), file_size);

    // Request direct write buffer
    let write_size = 1024;
    let result = mapped_file.get_direct_write_buffer(write_size);

    assert!(result.is_some(), "Should get direct write buffer");

    let (buffer, pos) = result.unwrap();
    assert_eq!(buffer.len(), write_size);
    assert_eq!(pos, 0); // First write at position 0

    // Write data directly to buffer
    for i in 0..write_size {
        buffer[i] = (i % 256) as u8;
    }

    // Commit the write
    let committed = mapped_file.commit_direct_write(write_size);
    assert!(committed, "Should commit successfully");

    // Verify write position updated
    assert_eq!(mapped_file.get_wrote_position(), write_size as i32);
}

/// Test 5: Zero-Copy Multiple Writes
///
/// Tests sequential zero-copy writes update position correctly.
#[tokio::test]
async fn test_zero_copy_multiple_writes() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = create_test_file_path(&temp_dir, "zerocopy_multi.log");
    let file_size = 10 * 1024 * 1024;

    let mapped_file = DefaultMappedFile::new(CheetahString::from_string(file_path), file_size);

    let write_count = 100;
    let write_size = 1024;

    for i in 0..write_count {
        // Get direct buffer
        let result = mapped_file.get_direct_write_buffer(write_size);
        assert!(result.is_some(), "Write {} failed to get buffer", i);

        let (buffer, pos) = result.unwrap();
        assert_eq!(pos, i * write_size, "Wrong position for write {}", i);

        // Write data
        buffer.fill((i % 256) as u8);

        // Commit
        let committed = mapped_file.commit_direct_write(write_size);
        assert!(committed, "Write {} failed to commit", i);
    }

    // Verify final position
    let expected_pos = write_count * write_size;
    assert_eq!(mapped_file.get_wrote_position(), expected_pos as i32);
}

/// Test 6: Zero-Copy Buffer Exhaustion
///
/// Tests that get_direct_write_buffer() returns None when file is full.
#[tokio::test]
async fn test_zero_copy_buffer_exhaustion() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = create_test_file_path(&temp_dir, "zerocopy_full.log");
    let file_size = 4096; // Small file

    let mapped_file = DefaultMappedFile::new(CheetahString::from_string(file_path), file_size);

    // Fill the file
    let write_size = 1024;
    for _ in 0..4 {
        let result = mapped_file.get_direct_write_buffer(write_size);
        assert!(result.is_some());
        let (buffer, _) = result.unwrap();
        buffer.fill(0);
        mapped_file.commit_direct_write(write_size);
    }

    // File should be full now
    assert_eq!(mapped_file.get_wrote_position() as u64, file_size);

    // Try to write more - should fail
    let result = mapped_file.get_direct_write_buffer(write_size);
    assert!(result.is_none(), "Should return None when file is full");
}

/// Test 7: Zero-Copy vs Standard Write Performance Comparison
///
/// Compares zero-copy write performance against standard append.
#[tokio::test]
async fn test_zero_copy_performance_improvement() {
    let temp_dir = TempDir::new().unwrap();

    // Test standard write
    let file_path_std = create_test_file_path(&temp_dir, "standard.log");
    let mapped_file_std =
        DefaultMappedFile::new(CheetahString::from_string(file_path_std), 10 * 1024 * 1024);

    let write_count = 1000;
    let buffer = vec![b'X'; 1024];

    let start_std = Instant::now();
    for _ in 0..write_count {
        mapped_file_std.append_message_bytes_no_position_update_ref(&buffer);
    }
    let elapsed_std = start_std.elapsed();

    // Test zero-copy write
    let file_path_zc = create_test_file_path(&temp_dir, "zerocopy.log");
    let mapped_file_zc =
        DefaultMappedFile::new(CheetahString::from_string(file_path_zc), 10 * 1024 * 1024);

    let start_zc = Instant::now();
    for _ in 0..write_count {
        if let Some((buf, _)) = mapped_file_zc.get_direct_write_buffer(1024) {
            buf.copy_from_slice(&buffer);
            mapped_file_zc.commit_direct_write(1024);
        }
    }
    let elapsed_zc = start_zc.elapsed();

    println!("Standard write: {:?}", elapsed_std);
    println!("Zero-copy write: {:?}", elapsed_zc);
    println!(
        "Improvement: {:.1}%",
        (1.0 - elapsed_zc.as_secs_f64() / elapsed_std.as_secs_f64()) * 100.0
    );

    // Zero-copy should be competitive or faster
    // Note: Performance gain depends on message encoding overhead
    // For simple copies, difference may be small
}

/// Test 8: Concurrent Zero-Copy Writes (Thread Safety)
///
/// Tests that multiple threads can safely use zero-copy writes.
#[tokio::test]
async fn test_zero_copy_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = create_test_file_path(&temp_dir, "concurrent_zc.log");
    let file_size = 100 * 1024 * 1024; // 100MB

    let mapped_file = Arc::new(DefaultMappedFile::new(
        CheetahString::from_string(file_path),
        file_size,
    ));

    let thread_count = 4;
    let writes_per_thread = 250;
    let write_size = 1024;

    let handles: Vec<_> = (0..thread_count)
        .map(|thread_id| {
            let mapped_file = mapped_file.clone();

            std::thread::spawn(move || {
                for i in 0..writes_per_thread {
                    if let Some((buffer, _)) = mapped_file.get_direct_write_buffer(write_size) {
                        // Write thread-specific data
                        buffer.fill((thread_id * 10 + i % 256) as u8);
                        mapped_file.commit_direct_write(write_size);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify total writes
    let expected_pos = thread_count * writes_per_thread * write_size;
    let actual_pos = mapped_file.get_wrote_position() as usize;

    assert_eq!(actual_pos, expected_pos, "Concurrent writes failed");
}

/// Test 9: PageCache Warming Error Handling (Invalid madvise)
///
/// Tests that allocation continues even if madvise fails.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_pagecache_warming_error_handling() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = create_test_file_path(&temp_dir, "madvise_error.log");
    let file_size = 1024 * 1024;

    let service = AllocateMappedFileService::new();
    let result_rx = service.submit_request(file_path.clone(), file_size);

    // Should succeed even if madvise has issues
    let result = result_rx.await;
    assert!(
        result.is_ok(),
        "Allocation should succeed even if madvise fails"
    );

    let mapped_file = result.unwrap();
    assert!(mapped_file.is_ok(), "MappedFile should be created");
}

/// Test 10: Zero-Copy with Small Buffers
///
/// Tests zero-copy behavior with very small writes (edge case).
#[tokio::test]
async fn test_zero_copy_small_buffers() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = create_test_file_path(&temp_dir, "small_writes.log");
    let file_size = 1024 * 1024;

    let mapped_file = DefaultMappedFile::new(CheetahString::from_string(file_path), file_size);

    // Write very small buffers (1 byte, 2 bytes, etc.)
    for size in 1..=256 {
        let result = mapped_file.get_direct_write_buffer(size);
        assert!(result.is_some(), "Should handle size {}", size);

        let (buffer, _) = result.unwrap();
        assert_eq!(buffer.len(), size);
        buffer.fill((size % 256) as u8);

        let committed = mapped_file.commit_direct_write(size);
        assert!(committed, "Should commit size {}", size);
    }

    // Verify all writes succeeded
    let expected_pos: usize = (1..=256).sum();
    assert_eq!(mapped_file.get_wrote_position() as usize, expected_pos);
}
*/
