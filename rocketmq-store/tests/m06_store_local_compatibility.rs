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

use std::sync::Arc;
use std::time::Duration;

use memmap2::MmapOptions;
use parking_lot::RwLock;
use rocketmq_store::log_file::mapped_file::io_uring_impl as legacy_io_uring;
use rocketmq_store::log_file::mapped_file::DirectIoBuffer as LegacyDirectIoBuffer;
use rocketmq_store::log_file::mapped_file::DirectIoRequest as LegacyDirectIoRequest;
use rocketmq_store::log_file::mapped_file::DirectIoValidationError as LegacyDirectIoValidationError;
use rocketmq_store::log_file::mapped_file::FlushStrategy as LegacyFlushStrategy;
use rocketmq_store::log_file::mapped_file::MappedBuffer as LegacyMappedBuffer;
use rocketmq_store::log_file::mapped_file::MappedFileError as LegacyMappedFileError;
use rocketmq_store::log_file::mapped_file::MappedFileMetrics as LegacyMappedFileMetrics;
use rocketmq_store_local::mapped_file::io_uring_impl as canonical_io_uring;
use rocketmq_store_local::mapped_file::DirectIoBuffer as CanonicalDirectIoBuffer;
use rocketmq_store_local::mapped_file::DirectIoRequest as CanonicalDirectIoRequest;
use rocketmq_store_local::mapped_file::DirectIoValidationError as CanonicalDirectIoValidationError;
use rocketmq_store_local::mapped_file::FlushStrategy as CanonicalFlushStrategy;
use rocketmq_store_local::mapped_file::MappedBuffer as CanonicalMappedBuffer;
use rocketmq_store_local::mapped_file::MappedFileError as CanonicalMappedFileError;
use rocketmq_store_local::mapped_file::MappedFileMetrics as CanonicalMappedFileMetrics;

fn canonical_direct_buffer(value: LegacyDirectIoBuffer) -> CanonicalDirectIoBuffer {
    value
}

fn canonical_direct_request(value: LegacyDirectIoRequest) -> CanonicalDirectIoRequest {
    value
}

fn canonical_direct_error(value: LegacyDirectIoValidationError) -> CanonicalDirectIoValidationError {
    value
}

fn canonical_flush_strategy(value: LegacyFlushStrategy) -> CanonicalFlushStrategy {
    value
}

fn canonical_mapped_buffer(value: LegacyMappedBuffer) -> CanonicalMappedBuffer {
    value
}

fn legacy_mapped_error(value: CanonicalMappedFileError) -> LegacyMappedFileError {
    value
}

fn canonical_metrics(value: LegacyMappedFileMetrics) -> CanonicalMappedFileMetrics {
    value
}

#[test]
fn direct_io_and_flush_strategy_preserve_identity_and_behavior() {
    let invalid = LegacyDirectIoBuffer::new(4097, 4096).expect_err("unaligned length must fail");
    let invalid = canonical_direct_error(invalid);
    assert_eq!(
        invalid,
        CanonicalDirectIoValidationError::UnalignedLength {
            len: 4097,
            alignment: 4096,
        }
    );
    assert_eq!(
        invalid.to_string(),
        "direct I/O buffer length must be aligned: len=4097, alignment=4096"
    );

    let buffer = canonical_direct_buffer(LegacyDirectIoBuffer::new(4096, 4096).expect("aligned buffer"));
    assert_eq!((buffer.as_ptr() as usize) % 4096, 0);
    let request =
        canonical_direct_request(LegacyDirectIoRequest::new(8192, buffer).expect("aligned request and file offset"));
    assert_eq!(request.file_offset(), 8192);
    assert_eq!(request.len(), 4096);

    let strategy = canonical_flush_strategy(LegacyFlushStrategy::hybrid(16, Duration::from_millis(200)));
    assert_eq!(
        strategy,
        CanonicalFlushStrategy::Hybrid {
            pages: 16,
            duration: Duration::from_millis(200),
        }
    );
    assert_eq!(strategy.name(), "Hybrid");
}

#[test]
fn mapped_buffer_error_and_metrics_preserve_identity_and_behavior() {
    let file = tempfile::tempfile().expect("temporary mapped file");
    file.set_len(4096).expect("size temporary mapped file");
    // SAFETY: The file remains open for the mapping lifetime and is not resized while mapped.
    let mmap = unsafe { MmapOptions::new().len(4096).map_mut(&file).expect("map temporary file") };
    let buffer = canonical_mapped_buffer(
        LegacyMappedBuffer::new(Arc::new(RwLock::new(mmap)), 0, 4096).expect("valid mapped region"),
    );

    buffer.write(4, b"local-leaf").expect("write mapped buffer");
    assert_eq!(&buffer.read(4..14).expect("read mapped buffer")[..], b"local-leaf");

    let error = buffer.read(4090..4100).expect_err("out-of-bounds read must fail");
    let legacy_error = legacy_mapped_error(error);
    assert!(legacy_error.is_recoverable());
    assert!(!legacy_error.is_io_error());

    let metrics = canonical_metrics(LegacyMappedFileMetrics::new());
    metrics.record_write(10);
    metrics.record_read(10, true);
    metrics.record_flush(Duration::from_micros(25));
    assert_eq!(metrics.total_writes(), 1);
    assert_eq!(metrics.total_bytes_read(), 10);
    assert_eq!(metrics.avg_flush_duration(), Duration::from_micros(25));
}

#[test]
fn io_uring_status_and_capability_preserve_identity() {
    let canonical_status: canonical_io_uring::IoUringBackendStatus = legacy_io_uring::io_uring_backend_status();
    assert_eq!(
        canonical_status.as_str(),
        canonical_io_uring::io_uring_backend_status().as_str()
    );

    let canonical_capability: canonical_io_uring::IoUringRuntimeCapability =
        legacy_io_uring::probe_io_uring_runtime_capability();
    assert_eq!(canonical_capability.backend_status, canonical_status);
    assert!(canonical_capability.experimental);

    let legacy_capability: legacy_io_uring::IoUringRuntimeCapability = canonical_capability;
    assert_eq!(legacy_capability.fallback_reason, canonical_capability.fallback_reason);
}
