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

use std::collections::BTreeSet;

use rocketmq_store_local::base::transient_store_pool::TransientStorePool;

#[test]
fn clones_share_queue_and_real_commit_state_without_capacity_or_length_validation() {
    let pool = TransientStorePool::new(1, 4);
    let clone = pool.clone();

    assert_eq!(pool.available_buffer_nums(), 0);
    assert!(pool.is_real_commit());
    clone.set_real_commit(false);
    assert!(!pool.is_real_commit());

    pool.return_buffer(vec![1]);
    pool.return_buffer(vec![2; 7]);
    assert_eq!(clone.available_buffer_nums(), 2);
    assert_eq!(clone.borrow_buffer(), Some(vec![2; 7]));
    assert_eq!(pool.borrow_buffer(), Some(vec![1]));
}

#[test]
fn cloned_pool_serializes_concurrent_returns_without_losing_buffers() {
    let pool = TransientStorePool::new(1, 1);
    let workers = (0u8..8)
        .map(|value| {
            let pool = pool.clone();
            std::thread::spawn(move || pool.return_buffer(vec![value]))
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker.join().expect("return worker must not panic");
    }

    assert_eq!(pool.available_buffer_nums(), 8);
    let returned = (0..8)
        .map(|_| pool.borrow_buffer().expect("every returned buffer remains available")[0])
        .collect::<BTreeSet<_>>();
    assert_eq!(returned, (0u8..8).collect());
    assert_eq!(pool.available_buffer_nums(), 0);
}
