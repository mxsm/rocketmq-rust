// Copyright 2026 The RocketMQ Rust Authors
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

#![cfg(unix)]

use std::fs;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_store_local::mapped_file::DefaultMappedFile;
use rocketmq_store_local::mapped_file::MappedFile;

#[test]
fn unlink_does_not_invalidate_a_live_owner_bound_read_lease() {
    let root = tempfile::tempdir().expect("temporary segment directory");
    let path = root.path().join("00000000000000000000");
    let mapped: Arc<DefaultMappedFile> = Arc::new(
        DefaultMappedFile::try_new(CheetahString::from_string(path.to_string_lossy().into_owned()), 64)
            .expect("create mapped file"),
    );
    assert!(mapped.append_message_bytes(b"old-incarnation"));
    assert!(mapped.try_seal_readable().expect("seal mapped file"));
    let lease = mapped
        .try_mapped_read_lease(0, b"old-incarnation".len())
        .expect("read is admitted")
        .expect("sealed generation is mapped");

    fs::remove_file(&path).expect("Unix unlinks the canonical name while the owner remains live");
    fs::write(&path, b"new-incarnation").expect("reuse canonical name with a different file");

    assert_eq!(lease.as_ref(), b"old-incarnation");
    assert_eq!(fs::read(&path).expect("replacement remains"), b"new-incarnation");
    drop(lease);
    MappedFile::shutdown(mapped.as_ref(), 0);
}
