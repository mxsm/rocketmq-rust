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

use std::collections::BinaryHeap;

use rocketmq_store_local::mapped_file::allocation_request::MappedFileAllocationRequestKey;

fn request(path: &str, offset: i64, size: i32) -> MappedFileAllocationRequestKey {
    let file_path = std::path::Path::new(path).join(format!("{offset:020}"));
    MappedFileAllocationRequestKey::new(file_path.to_string_lossy().into_owned(), size)
}

#[test]
fn allocation_request_key_preserves_identity_accessors_and_display() {
    let key = request("root", 10, 1024);
    let same = MappedFileAllocationRequestKey::new(key.file_path().to_owned(), 1024);

    assert_eq!(key.file_size(), 1024);
    assert_eq!(key, same);
    assert_eq!(
        key.to_string(),
        format!("AllocateRequest[file_path={},file_size=1024]", key.file_path())
    );
}

#[test]
fn allocation_request_key_preserves_platform_separator_offset_parsing() {
    assert_eq!(request("root", 123, 1).file_offset(), 123);
    assert_eq!(
        MappedFileAllocationRequestKey::new("123".to_owned(), 1).file_offset(),
        0
    );
    let invalid = std::path::Path::new("root").join("not-an-offset");
    assert_eq!(
        MappedFileAllocationRequestKey::new(invalid.to_string_lossy().into_owned(), 1).file_offset(),
        0
    );
}

#[test]
fn allocation_request_key_orders_lower_offsets_first_in_binary_heap() {
    let mut requests = BinaryHeap::from([
        request("root", 300, 1),
        request("root", 100, 1),
        request("root", 200, 1),
    ]);

    assert_eq!(requests.pop().map(|key| key.file_offset()), Some(100));
    assert_eq!(requests.pop().map(|key| key.file_offset()), Some(200));
    assert_eq!(requests.pop().map(|key| key.file_offset()), Some(300));
}

#[test]
fn allocation_request_key_keeps_offset_only_ordering_and_full_identity_equality() {
    let first = request("root-a", 100, 1);
    let second = request("root-b", 100, 2);

    assert_ne!(first, second);
    assert_eq!(first.cmp(&second), std::cmp::Ordering::Equal);
}
