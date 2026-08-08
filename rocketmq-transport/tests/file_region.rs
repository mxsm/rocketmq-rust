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

use std::io::Write;
use std::sync::Arc;

use rocketmq_transport::api::v1::FileRegion;

#[test]
fn file_region_rejects_zero_length_overflow_and_past_eof() {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(b"0123456789").unwrap();
    let lease = Arc::new(file);

    assert!(FileRegion::try_new(lease.clone(), 0, 0).is_err());
    assert!(FileRegion::try_new(lease.clone(), u64::MAX, 2).is_err());
    assert!(FileRegion::try_new(lease, 8, 3).is_err());
}

#[test]
fn cloned_region_keeps_the_source_file_lease_alive() {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(b"leased-body").unwrap();
    let region = FileRegion::try_new(Arc::new(file), 1, 5).unwrap();
    let cloned = region.clone();
    drop(region);

    assert_eq!(cloned.offset(), 1);
    assert_eq!(cloned.len(), 5);
}
