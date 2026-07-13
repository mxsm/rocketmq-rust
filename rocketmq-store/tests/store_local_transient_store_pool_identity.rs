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

use rocketmq_store::base::transient_store_pool::TransientStorePool as LegacyTransientStorePool;
use rocketmq_store_local::base::transient_store_pool::TransientStorePool as CanonicalTransientStorePool;

fn to_canonical(value: LegacyTransientStorePool) -> CanonicalTransientStorePool {
    value
}

fn to_legacy(value: CanonicalTransientStorePool) -> LegacyTransientStorePool {
    value
}

#[test]
fn legacy_transient_store_pool_is_the_canonical_local_type() {
    let legacy = to_legacy(to_canonical(LegacyTransientStorePool::new(2, 16)));
    let canonical: CanonicalTransientStorePool = legacy.clone();

    canonical.return_buffer(vec![7; 3]);
    assert_eq!(legacy.borrow_buffer(), Some(vec![7; 3]));

    canonical.set_real_commit(false);
    assert!(!legacy.is_real_commit());
}
