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

use bytes::Bytes;
use rocketmq_store_api::GetResult;
use rocketmq_store_api::GetStatus;
use rocketmq_store_api::LeasedBytes;
use rocketmq_store_api::QueryResult;
use rocketmq_store_api::ReadCacheState;
use rocketmq_store_api::SelectResult;

#[test]
fn select_result_exposes_bytes_and_neutral_location_metadata() {
    let selected = SelectResult::new(
        128,
        LeasedBytes::new(Bytes::from_static(b"payload"), ()),
        ReadCacheState::Hot,
    );

    assert_eq!(128, selected.start_offset());
    assert_eq!(7, selected.size());
    assert_eq!(b"payload", selected.data().bytes().as_ref());
    assert_eq!(ReadCacheState::Hot, selected.cache_state());
}

#[test]
fn get_result_preserves_legacy_navigation_and_accounting_fields() {
    let result = GetResult {
        records: Vec::<SelectResult<()>>::new(),
        queue_offsets: vec![7, 8],
        status: Some(GetStatus::OffsetReset),
        next_begin_offset: 9,
        min_offset: 2,
        max_offset: 20,
        buffer_total_size: 128,
        message_count: 2,
        suggest_pulling_from_replica: true,
        commercial_message_count: 1,
        commercial_size_per_message: 4096,
        cold_data_sum: 64,
    };

    assert_eq!(Some(GetStatus::OffsetReset), result.status);
    assert_eq!(vec![7, 8], result.queue_offsets);
    assert_eq!(9, result.next_begin_offset);
    assert!(result.suggest_pulling_from_replica);
}

#[test]
fn query_result_preserves_index_safety_and_progress() {
    let result = QueryResult {
        records: Vec::<SelectResult<()>>::new(),
        index_last_update_timestamp: 123,
        index_last_update_physical_offset: 456,
        buffer_total_size: 0,
        index_query_safe: false,
        index_safe_physical_offset: 400,
        index_confirm_physical_offset: 420,
    };

    assert!(!result.index_query_safe);
    assert_eq!(400, result.index_safe_physical_offset);
    assert_eq!(420, result.index_confirm_physical_offset);
}
