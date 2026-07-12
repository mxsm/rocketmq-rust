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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::query_message_result::QueryMessageResult;
use rocketmq_store::base::select_result::SelectMappedBufferCacheState;
use rocketmq_store::base::select_result::SelectMappedBufferResult;
use rocketmq_store::base::select_result::SelectMappedBufferSourceKind;
use rocketmq_store::store_api_adapter::get_result_from_legacy;
use rocketmq_store::store_api_adapter::query_result_from_legacy;
use rocketmq_store::store_api_adapter::selected_result_from_legacy;
use rocketmq_store::store_api_adapter::LegacyMessageStoreReadAdapter;
use rocketmq_store::store_api_adapter::LegacyReadCallBoundary;
use rocketmq_store::store_api_adapter::LegacyReadRequest;
use rocketmq_store::store_api_adapter::LegacyReadResult;
use rocketmq_store_api::GetStatus;
use rocketmq_store_api::MessageReader;
use rocketmq_store_api::ReadCacheState;

fn selected(payload: Bytes, start_offset: u64, cache_state: SelectMappedBufferCacheState) -> SelectMappedBufferResult {
    SelectMappedBufferResult {
        start_offset,
        size: i32::try_from(payload.len()).expect("fixture payload fits i32"),
        bytes: Some(payload),
        mapped_file: None,
        is_in_cache: cache_state == SelectMappedBufferCacheState::Hot,
        source_kind: SelectMappedBufferSourceKind::Bytes,
        file_offset: 0,
        cache_state,
    }
}

fn legacy_get_result() -> GetMessageResult {
    let mut result = GetMessageResult::new();
    result.set_status(Some(GetMessageStatus::OffsetReset));
    result.set_next_begin_offset(9);
    result.set_min_offset(2);
    result.set_max_offset(20);
    result.set_suggest_pulling_from_slave(true);
    result.set_cold_data_sum(64);
    result.add_message(
        selected(Bytes::from_static(b"payload"), 128, SelectMappedBufferCacheState::Hot),
        7,
        2,
    );
    result
}

fn legacy_query_result() -> QueryMessageResult {
    let mut result = QueryMessageResult {
        index_last_update_timestamp: 123,
        index_last_update_phyoffset: 456,
        index_query_safe: false,
        index_safe_phyoffset: 400,
        index_confirm_phyoffset: 420,
        ..QueryMessageResult::default()
    };
    result.add_message(selected(
        Bytes::from_static(b"query"),
        256,
        SelectMappedBufferCacheState::Cold,
    ));
    result
}

#[derive(Default)]
struct ReadProbe {
    normal_get_calls: AtomicUsize,
    size_limited_get_calls: AtomicUsize,
    query_calls: AtomicUsize,
    select_calls: AtomicUsize,
    last_size_limit: AtomicI32,
    return_none: AtomicBool,
}

impl LegacyReadCallBoundary for ReadProbe {
    fn get_message(
        &self,
        _group: &CheetahString,
        _topic: &CheetahString,
        _queue_id: i32,
        _offset: i64,
        _max_messages: i32,
    ) -> impl std::future::Future<Output = Option<GetMessageResult>> + Send {
        self.normal_get_calls.fetch_add(1, Ordering::SeqCst);
        ready((!self.return_none.load(Ordering::SeqCst)).then(legacy_get_result))
    }

    fn get_message_with_size_limit(
        &self,
        _group: &CheetahString,
        _topic: &CheetahString,
        _queue_id: i32,
        _offset: i64,
        _max_messages: i32,
        max_total_size: i32,
    ) -> impl std::future::Future<Output = Option<GetMessageResult>> + Send {
        self.size_limited_get_calls.fetch_add(1, Ordering::SeqCst);
        self.last_size_limit.store(max_total_size, Ordering::SeqCst);
        ready((!self.return_none.load(Ordering::SeqCst)).then(legacy_get_result))
    }

    fn query_message(
        &self,
        _topic: &CheetahString,
        _key: &CheetahString,
        _max_messages: i32,
        _begin: i64,
        _end: i64,
    ) -> impl std::future::Future<Output = Option<QueryMessageResult>> + Send {
        self.query_calls.fetch_add(1, Ordering::SeqCst);
        ready((!self.return_none.load(Ordering::SeqCst)).then(legacy_query_result))
    }

    fn select_message(&self, physical_offset: i64, size: Option<i32>) -> Option<SelectMappedBufferResult> {
        self.select_calls.fetch_add(1, Ordering::SeqCst);
        if self.return_none.load(Ordering::SeqCst) {
            return None;
        }
        let size = size.unwrap_or(6);
        Some(selected(
            Bytes::from(vec![b's'; usize::try_from(size).expect("positive fixture size")]),
            u64::try_from(physical_offset).expect("positive fixture offset"),
            SelectMappedBufferCacheState::Unknown,
        ))
    }
}

#[tokio::test]
async fn read_dispatches_size_limited_get_and_projects_every_get_field() {
    let probe = ReadProbe::default();
    let adapter = LegacyMessageStoreReadAdapter::new(&probe);

    let result = adapter
        .read(LegacyReadRequest::Get {
            group: CheetahString::from_static_str("group"),
            topic: CheetahString::from_static_str("topic"),
            queue_id: 3,
            offset: 4,
            max_messages: 16,
            max_total_size: Some(512),
        })
        .await
        .expect("read succeeds")
        .expect("get result exists");

    assert_eq!(0, probe.normal_get_calls.load(Ordering::SeqCst));
    assert_eq!(1, probe.size_limited_get_calls.load(Ordering::SeqCst));
    assert_eq!(512, probe.last_size_limit.load(Ordering::SeqCst));
    let LegacyReadResult::Get(result) = result else {
        panic!("expected get result");
    };
    assert_eq!(Some(GetStatus::OffsetReset), result.status);
    assert_eq!(vec![7], result.queue_offsets);
    assert_eq!(9, result.next_begin_offset);
    assert_eq!(2, result.min_offset);
    assert_eq!(20, result.max_offset);
    assert_eq!(7, result.buffer_total_size);
    assert_eq!(2, result.message_count);
    assert!(result.suggest_pulling_from_replica);
    assert_eq!(1, result.commercial_message_count);
    assert_eq!(4096, result.commercial_size_per_message);
    assert_eq!(64, result.cold_data_sum);
    assert_eq!(128, result.records[0].start_offset());
    assert_eq!(ReadCacheState::Hot, result.records[0].cache_state());
    assert_eq!(b"payload", result.records[0].data().bytes().as_ref());
}

#[tokio::test]
async fn read_dispatches_query_and_select_and_preserves_none() {
    let probe = ReadProbe::default();
    let adapter = LegacyMessageStoreReadAdapter::new(&probe);

    let query = adapter
        .read(LegacyReadRequest::Query {
            topic: CheetahString::from_static_str("topic"),
            key: CheetahString::from_static_str("key"),
            max_messages: 8,
            begin: 10,
            end: 20,
        })
        .await
        .expect("query succeeds")
        .expect("query result exists");
    let LegacyReadResult::Query(query) = query else {
        panic!("expected query result");
    };
    assert_eq!(123, query.index_last_update_timestamp);
    assert_eq!(456, query.index_last_update_physical_offset);
    assert_eq!(5, query.buffer_total_size);
    assert!(!query.index_query_safe);
    assert_eq!(400, query.index_safe_physical_offset);
    assert_eq!(420, query.index_confirm_physical_offset);
    assert_eq!(ReadCacheState::Cold, query.records[0].cache_state());

    let selected = adapter
        .read(LegacyReadRequest::Select {
            physical_offset: 300,
            size: Some(3),
        })
        .await
        .expect("select succeeds")
        .expect("select result exists");
    let LegacyReadResult::Select(selected) = selected else {
        panic!("expected select result");
    };
    assert_eq!(300, selected.start_offset());
    assert_eq!(3, selected.size());
    assert_eq!(b"sss", selected.data().bytes().as_ref());

    probe.return_none.store(true, Ordering::SeqCst);
    let missing = adapter
        .read(LegacyReadRequest::Get {
            group: CheetahString::from_static_str("group"),
            topic: CheetahString::from_static_str("topic"),
            queue_id: 0,
            offset: 0,
            max_messages: 1,
            max_total_size: None,
        })
        .await
        .expect("missing read is not an error");
    assert!(missing.is_none());

    let missing_query = adapter
        .read(LegacyReadRequest::Query {
            topic: CheetahString::from_static_str("topic"),
            key: CheetahString::from_static_str("missing"),
            max_messages: 1,
            begin: 0,
            end: 1,
        })
        .await
        .expect("missing query is not an error");
    assert!(missing_query.is_none());

    let missing_select = adapter
        .read(LegacyReadRequest::Select {
            physical_offset: 301,
            size: None,
        })
        .await
        .expect("missing select is not an error");
    assert!(missing_select.is_none());
}

#[test]
fn direct_get_and_query_projection_preserve_fields() {
    let get = get_result_from_legacy(legacy_get_result());
    assert_eq!(Some(GetStatus::OffsetReset), get.status);
    assert_eq!(vec![7], get.queue_offsets);
    assert_eq!(b"payload", get.records[0].data().bytes().as_ref());

    let query = query_result_from_legacy(legacy_query_result());
    assert_eq!(123, query.index_last_update_timestamp);
    assert_eq!(456, query.index_last_update_physical_offset);
    assert_eq!(b"query", query.records[0].data().bytes().as_ref());
}

struct BytesOwner {
    payload: &'static [u8],
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for BytesOwner {
    fn as_ref(&self) -> &[u8] {
        self.payload
    }
}

impl Drop for BytesOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn observed_selected(drops: Arc<AtomicUsize>) -> SelectMappedBufferResult {
    selected(
        Bytes::from_owner(BytesOwner {
            payload: b"leased",
            drops,
        }),
        512,
        SelectMappedBufferCacheState::Hot,
    )
}

#[test]
fn selected_projection_retains_and_releases_the_legacy_lease() {
    let drops = Arc::new(AtomicUsize::new(0));
    let selected = selected_result_from_legacy(observed_selected(drops.clone()));

    assert_eq!(b"leased", selected.data().bytes().as_ref());
    assert_eq!(0, drops.load(Ordering::SeqCst));
    drop(selected);
    assert_eq!(1, drops.load(Ordering::SeqCst));
}

#[test]
fn into_bytes_stays_valid_after_the_legacy_lease_is_released() {
    let drops = Arc::new(AtomicUsize::new(0));
    let selected = selected_result_from_legacy(observed_selected(drops.clone()));

    let bytes = selected.into_data().into_bytes();

    assert_eq!(b"leased", bytes.as_ref());
    assert_eq!(0, drops.load(Ordering::SeqCst));
    drop(bytes);
    assert_eq!(1, drops.load(Ordering::SeqCst));
}
