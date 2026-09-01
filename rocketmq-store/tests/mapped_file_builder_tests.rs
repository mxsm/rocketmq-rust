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

use rocketmq_store::FlushStrategy;
use rocketmq_store::MappedFile;
use rocketmq_store::MappedFileBuilder;
use tempfile::TempDir;

#[test]
fn builder_creates_a_writable_mapped_file_with_the_declared_offset() {
    let temp_dir = TempDir::new().expect("temporary mapped-file directory");
    let path = temp_dir.path().join("00000000000000000042");

    let mapped_file = MappedFileBuilder::new(&path)
        .size(4096)
        .file_from_offset(42)
        .build()
        .expect("build mapped file");

    assert_eq!(mapped_file.get_file_size(), 4096);
    assert_eq!(mapped_file.get_file_from_offset(), 42);
    assert!(mapped_file.append_message_bytes(b"builder-contract"));
    assert_eq!(
        mapped_file.get_bytes_readable_checked(0, 16).as_deref(),
        Some(&b"builder-contract"[..])
    );
    assert!(mapped_file.destroy(0));
    assert!(!path.exists());
}

#[test]
fn builder_rejects_offset_mismatch_before_creating_the_file() {
    let temp_dir = TempDir::new().expect("temporary mapped-file directory");
    let path = temp_dir.path().join("00000000000000000042");

    let result = MappedFileBuilder::new(&path).size(4096).file_from_offset(43).build();
    if let Ok(mapped_file) = &result {
        mapped_file.destroy(0);
    }

    match result {
        Ok(_) => panic!("expected invalid configuration"),
        Err(error) => assert_eq!(error.code().as_str(), "storage.request.invalid"),
    }
    assert!(!path.exists());
}

#[test]
fn builder_rejects_invalid_sizes_and_file_names_before_creation() {
    let temp_dir = TempDir::new().expect("temporary mapped-file directory");
    let missing_size = temp_dir.path().join("00000000000000000001");
    let zero_size = temp_dir.path().join("00000000000000000002");
    let too_large = temp_dir.path().join("00000000000000000003");
    let invalid_name = temp_dir.path().join("not-an-offset");
    let cases = [
        (missing_size.clone(), MappedFileBuilder::new(&missing_size)),
        (zero_size.clone(), MappedFileBuilder::new(&zero_size).size(0)),
        (
            too_large.clone(),
            MappedFileBuilder::new(&too_large).size(16 * 1024 * 1024 * 1024 + 1),
        ),
        (invalid_name.clone(), MappedFileBuilder::new(&invalid_name).size(4096)),
    ];

    for (path, builder) in cases {
        match builder.build() {
            Ok(_) => panic!("expected invalid configuration"),
            Err(error) => assert_eq!(error.code().as_str(), "storage.request.invalid"),
        }
        assert!(!path.exists());
    }
}

#[test]
fn builder_rejects_unsupported_options_without_filesystem_side_effects() {
    let temp_dir = TempDir::new().expect("temporary mapped-file directory");
    let sync_path = temp_dir.path().join("00000000000000000001");
    let metrics_path = temp_dir.path().join("00000000000000000002");
    let warmup_path = temp_dir.path().join("00000000000000000003");
    let transient_path = temp_dir.path().join("00000000000000000004");
    let cases = [
        (
            sync_path.clone(),
            MappedFileBuilder::new(&sync_path)
                .size(4096)
                .flush_strategy(FlushStrategy::Sync),
        ),
        (
            metrics_path.clone(),
            MappedFileBuilder::new(&metrics_path).size(4096).disable_metrics(),
        ),
        (
            warmup_path.clone(),
            MappedFileBuilder::new(&warmup_path).size(4096).warmup(true),
        ),
        (
            transient_path.clone(),
            MappedFileBuilder::new(&transient_path)
                .size(4096)
                .enable_transient_store_pool(),
        ),
    ];

    for (path, builder) in cases {
        let result = builder.build();
        if let Ok(mapped_file) = &result {
            mapped_file.destroy(0);
        }
        match result {
            Ok(_) => panic!("expected invalid configuration"),
            Err(error) => assert_eq!(error.code().as_str(), "storage.request.invalid"),
        }
        assert!(!path.exists());
    }
    assert_eq!(
        std::fs::read_dir(temp_dir.path())
            .expect("read temporary directory")
            .count(),
        0
    );
}

#[test]
fn builder_preserves_parent_path_io_failures() {
    let temp_dir = TempDir::new().expect("temporary mapped-file directory");
    let parent = temp_dir.path().join("not-a-directory");
    std::fs::write(&parent, b"occupied").expect("create regular parent path");
    let path = parent.join("00000000000000000000");

    let result = MappedFileBuilder::new(&path).size(4096).build();

    match result {
        Ok(_) => panic!("expected io failure"),
        Err(error) => assert_eq!(error.code().as_str(), "storage.io.failed"),
    }
    assert!(!path.exists());
}
