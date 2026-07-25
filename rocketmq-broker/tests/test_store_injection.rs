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

use std::future::ready;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_broker::QueryMessageProcessor;
use rocketmq_broker::QueryMessageStore;
use rocketmq_store::base::query_message_result::QueryMessageResult;
use rocketmq_store::base::select_result::SelectMappedBufferResult;
use rocketmq_store_api::StoreError;

#[derive(Clone, Default)]
struct TestStore {
    query_count: Arc<AtomicUsize>,
}

impl QueryMessageStore for TestStore {
    fn query_message(
        &self,
        _topic: &CheetahString,
        _key: &CheetahString,
        _max_num: i32,
        _begin_timestamp: i64,
        _end_timestamp: i64,
    ) -> impl Future<Output = Result<Option<QueryMessageResult>, StoreError>> + Send {
        self.query_count.fetch_add(1, Ordering::SeqCst);
        ready(Ok(Some(QueryMessageResult::default())))
    }

    fn select_message_by_offset(&self, _offset: i64) -> Result<Option<SelectMappedBufferResult>, StoreError> {
        Ok(None)
    }
}

#[tokio::test]
async fn query_processor_accepts_a_test_store_without_a_backend_facade() {
    let store = TestStore::default();
    let _processor = QueryMessageProcessor::new(32, store.clone());

    let topic = CheetahString::from_static_str("TestTopic");
    let key = CheetahString::from_static_str("TestKey");
    let result = store
        .query_message(&topic, &key, 32, 0, i64::MAX)
        .await
        .expect("test store query should succeed");

    assert!(result.is_some());
    assert_eq!(1, store.query_count.load(Ordering::SeqCst));
}

#[test]
fn query_processor_source_depends_on_the_narrow_query_capability() {
    let source = include_str!("../src/processor/query_message_processor.rs");

    assert!(source.contains("S: QueryMessageStore"));
    assert!(!source.contains("QueryMessageProcessor<MS: MessageStore>"));
    assert!(!source.contains("LocalFileMessageStore"));
    assert!(!source.contains("RocksDBMessageStore"));
}
