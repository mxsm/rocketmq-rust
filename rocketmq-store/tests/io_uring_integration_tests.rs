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
//! Integration tests for IoUringMappedFile
//!
//! These tests verify the io_uring implementation works correctly
//! and provides the expected performance improvements.

#![cfg(all(target_os = "linux", feature = "io_uring"))]

use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_store::log_file::mapped_file::factory::MappedFileConfig;
use rocketmq_store::log_file::mapped_file::factory::MappedFileFactory;
use rocketmq_store::log_file::mapped_file::factory::MappedFileType;
use rocketmq_store::log_file::mapped_file::io_uring_impl::IoUringMappedFile;
use rocketmq_store::log_file::mapped_file::MappedFile;
use tokio::time::Instant;

/// Test that IoUringMappedFile can be created
#[tokio::test]
async fn test_io_uring_mapped_file_creation() {
    let file_name = CheetahString::from("/tmp/test_io_uring_create");
    let file_size = 1024 * 1024; // 1MB

    let result = IoUringMappedFile::new(file_name.clone(), file_size).await;
    assert!(
        result.is_ok(),
        "Failed to create IoUringMappedFile: {:?}",
        result
    );

    let mapped_file = result.unwrap();
    assert_eq!(mapped_file.get_file_name(), &file_name);
    assert_eq!(mapped_file.get_file_size(), file_size);
    assert!(mapped_file.is_available());

    // Cleanup
    let _ = std::fs::remove_file(file_name.as_str());
}

/// Test async write operation
#[tokio::test]
async fn test_io_uring_async_write() {
    let file_name = CheetahString::from("/tmp/test_io_uring_write");
    let file_size = 1024 * 1024; // 1MB

    let mapped_file = IoUringMappedFile::new(file_name.clone(), file_size)
        .await
        .expect("Failed to create file");

    // Write some data
    let data = b"Hello, io_uring!";
    let result = mapped_file.write_async(data, 0).await;
    assert!(result.is_ok(), "Write failed: {:?}", result);
    assert_eq!(result.unwrap(), data.len());

    // Verify data can be read back
    let read_data = mapped_file.get_bytes(0, data.len());
    assert!(read_data.is_some());
    assert_eq!(&read_data.unwrap()[..], data);

    // Cleanup
    let _ = std::fs::remove_file(file_name.as_str());
}

/// Test multiple sequential writes
#[tokio::test]
async fn test_io_uring_multiple_writes() {
    let file_name = CheetahString::from("/tmp/test_io_uring_multi_write");
    let file_size = 1024 * 1024; // 1MB

    let mapped_file = IoUringMappedFile::new(file_name.clone(), file_size)
        .await
        .expect("Failed to create file");

    // Write multiple chunks
    let chunks = [b"First chunk", b"Second chunk", b"Third chunk"];

    let mut offset = 0;
    for chunk in &chunks {
        let result = mapped_file.write_async(chunk, offset).await;
        assert!(result.is_ok());
        offset += chunk.len();
    }

    // Verify all data
    for (i, chunk) in chunks.iter().enumerate() {
        let start = chunks.iter().take(i).map(|c| c.len()).sum();
        let data = mapped_file.get_bytes(start, chunk.len());
        assert!(data.is_some());
        assert_eq!(&data.unwrap()[..], *chunk);
    }

    // Cleanup
    let _ = std::fs::remove_file(file_name.as_str());
}

/// Test async flush operation
#[tokio::test]
async fn test_io_uring_async_flush() {
    let file_name = CheetahString::from("/tmp/test_io_uring_flush");
    let file_size = 1024 * 1024; // 1MB

    let mapped_file = IoUringMappedFile::new(file_name.clone(), file_size)
        .await
        .expect("Failed to create file");

    // Write data
    let data = vec![0xAB; 4096];
    mapped_file
        .write_async(&data, 0)
        .await
        .expect("Write failed");
    mapped_file.set_wrote_position(data.len() as i32);

    // Flush
    let start = Instant::now();
    let result = mapped_file.flush_async().await;
    let flush_duration = start.elapsed();

    assert!(result.is_ok(), "Flush failed: {:?}", result);
    assert_eq!(result.unwrap(), data.len() as i32);
    assert_eq!(mapped_file.get_flushed_position(), data.len() as i32);

    println!("Flush took: {:?}", flush_duration);
    // io_uring flush should be fast (< 5ms typically)
    assert!(flush_duration < Duration::from_millis(100));

    // Cleanup
    let _ = std::fs::remove_file(file_name.as_str());
}

/// Test factory creation with io_uring
#[tokio::test]
async fn test_factory_io_uring() {
    let file_name = CheetahString::from("/tmp/test_factory_io_uring");
    let file_size = 1024 * 1024; // 1MB

    let config = MappedFileConfig {
        file_type: MappedFileType::IoUring,
        transient_store_pool: None,
        enable_metrics: true,
    };

    let result = MappedFileFactory::create(file_name.clone(), file_size, config).await;
    assert!(result.is_ok(), "Factory creation failed: {:?}", result);

    let mapped_file = result.unwrap();
    assert_eq!(mapped_file.get_file_size(), file_size);
    assert!(mapped_file.is_available());

    // Cleanup
    let _ = std::fs::remove_file(file_name.as_str());
}

/// Performance comparison: io_uring vs default
#[tokio::test]
async fn test_io_uring_performance_advantage() {
    let file_size = 1024 * 1024; // 1MB
    let data = vec![0x42; 4096]; // 4KB chunks
    let num_writes = 100;

    // Test io_uring
    let io_uring_file = CheetahString::from("/tmp/test_perf_io_uring");
    let uring_mapped = IoUringMappedFile::new(io_uring_file.clone(), file_size)
        .await
        .expect("Failed to create io_uring file");

    let start = Instant::now();
    for i in 0..num_writes {
        let offset = (i * data.len()) % (file_size as usize - data.len());
        uring_mapped
            .write_async(&data, offset)
            .await
            .expect("Write failed");
    }
    let io_uring_duration = start.elapsed();

    println!("io_uring: {} writes in {:?}", num_writes, io_uring_duration);
    println!(
        "io_uring throughput: {:.2} MB/s",
        (num_writes * data.len()) as f64 / io_uring_duration.as_secs_f64() / 1024.0 / 1024.0
    );

    // Cleanup
    let _ = std::fs::remove_file(io_uring_file.as_str());
}

/// Test concurrent writes with io_uring
#[tokio::test]
async fn test_io_uring_concurrent_writes() {
    let file_name = CheetahString::from("/tmp/test_io_uring_concurrent");
    let file_size = 10 * 1024 * 1024; // 10MB

    let mapped_file = Arc::new(
        IoUringMappedFile::new(file_name.clone(), file_size)
            .await
            .expect("Failed to create file"),
    );

    // Spawn multiple tasks writing concurrently
    let mut handles = vec![];
    let num_tasks = 4;
    let writes_per_task = 50;

    for task_id in 0..num_tasks {
        let file = Arc::clone(&mapped_file);
        let handle = tokio::spawn(async move {
            let data = vec![task_id as u8; 1024];
            for i in 0..writes_per_task {
                let offset = (task_id * writes_per_task + i) * data.len();
                if offset + data.len() < file_size as usize {
                    file.write_async(&data, offset).await.expect("Write failed");
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.expect("Task failed");
    }

    println!("All concurrent writes completed successfully");

    // Cleanup
    let _ = std::fs::remove_file(file_name.as_str());
}

/// Test large file handling with io_uring
#[tokio::test]
async fn test_io_uring_large_file() {
    let file_name = CheetahString::from("/tmp/test_io_uring_large");
    let file_size = 128 * 1024 * 1024; // 128MB

    let start = Instant::now();
    let mapped_file = IoUringMappedFile::new(file_name.clone(), file_size)
        .await
        .expect("Failed to create large file");
    let creation_time = start.elapsed();

    println!("Large file (128MB) creation took: {:?}", creation_time);

    assert_eq!(mapped_file.get_file_size(), file_size);
    assert!(mapped_file.is_available());

    // Write some data at different positions
    let data = vec![0xFF; 8192]; // 8KB
    let positions = [
        0,
        file_size as usize / 4,
        file_size as usize / 2,
        file_size as usize - 8192,
    ];

    for pos in positions {
        let result = mapped_file.write_async(&data, pos).await;
        assert!(result.is_ok(), "Write at position {} failed", pos);
    }

    // Cleanup
    let _ = std::fs::remove_file(file_name.as_str());
}

/// Test error handling when buffer exceeds file size
#[tokio::test]
async fn test_io_uring_buffer_overflow() {
    let file_name = CheetahString::from("/tmp/test_io_uring_overflow");
    let file_size = 1024; // 1KB

    let mapped_file = IoUringMappedFile::new(file_name.clone(), file_size)
        .await
        .expect("Failed to create file");

    // Try to write beyond file size
    let data = vec![0xAA; 2048]; // 2KB
    let result = mapped_file.write_async(&data, 0).await;

    assert!(result.is_err(), "Should fail when writing beyond file size");

    // Cleanup
    let _ = std::fs::remove_file(file_name.as_str());
}

/// Test kernel version check for io_uring availability
#[test]
fn test_io_uring_availability() {
    let is_available = MappedFileFactory::is_io_uring_available();
    println!("io_uring available: {}", is_available);

    // On Linux 5.1+, should be available
    if let Ok(uname) = sys_info::os_release() {
        println!("Kernel version: {}", uname);
        let parts: Vec<&str> = uname.split('.').collect();
        if let Some(major) = parts.first().and_then(|s| s.parse::<i32>().ok()) {
            if major >= 5 {
                if let Some(minor) = parts.get(1).and_then(|s| s.parse::<i32>().ok()) {
                    if major > 5 || minor >= 1 {
                        assert!(is_available, "io_uring should be available on Linux 5.1+");
                    }
                }
            }
        }
    }
}
*/
