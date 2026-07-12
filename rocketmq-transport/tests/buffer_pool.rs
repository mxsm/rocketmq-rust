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

use rocketmq_transport::buffer::ByteBufferPool;

#[test]
fn byte_pool_rejects_before_allocation_and_releases_capacity_on_drop() {
    let pool = ByteBufferPool::new(16);
    let mut first = pool.try_acquire(12).unwrap();
    first.extend_from_slice(b"hello");
    assert_eq!(pool.used_bytes(), 12);
    assert!(pool.try_acquire(5).is_err());
    assert_eq!(pool.rejected(), 1);
    drop(first);
    assert_eq!(pool.used_bytes(), 0);
    assert!(pool.try_acquire(16).is_ok());
}
