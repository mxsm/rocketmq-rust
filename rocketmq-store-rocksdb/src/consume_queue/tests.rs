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

use super::*;
use crate::config::RocksDbConfig;

#[test]
fn owner_aware_consume_queue_lookup_retains_query_operation() -> Result<(), StoreError> {
    let temp = tempfile::tempdir().expect("create consume queue owner fixture");
    let store = Arc::new(
        RocksDbStore::open(RocksDbConfig {
            enabled: true,
            path: temp.path().join("consume-queue-owner"),
            ..RocksDbConfig::default()
        })?
        .ok_or_else(|| crate::error::internal_failure(StoreOperation::Load))?,
    );
    let key = ConsumeQueueKey {
        topic: "TopicA".to_owned(),
        queue_id: 0,
        cq_offset: 7,
    };
    let mut encoded_key = Vec::with_capacity(key.encoded_len());
    key.encode(StoreOperation::AppendDerived, &mut encoded_key)?;
    store.put_cf(
        StoreOperation::AppendDerived,
        RocksDbColumnFamily::Default.name(),
        &encoded_key,
        b"invalid",
    )?;

    let writer = RocksDbConsumeQueueBatchWriter::new(store.as_ref());
    let error = writer
        .get_cq_value_with_operation(StoreOperation::QueryOffset, "TopicA", 0, 7)
        .expect_err("a malformed query result must retain the query owner");

    assert_eq!(error.operation(), StoreOperation::QueryOffset);
    assert_eq!(error.component(), rocketmq_store_api::StoreComponent::RocksDb);
    Ok(())
}
