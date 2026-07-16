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

use std::sync::Arc;

use rocketmq_store_local::flush::group_commit::complete_group_commit_batch;
use rocketmq_store_local::flush::group_commit::complete_group_commit_batch_error;
use rocketmq_store_local::flush::group_commit::GroupCommitRequest;
use rocketmq_store_local::flush::group_commit::GroupCommitStatus;
use rocketmq_store_local::flush::group_commit::SyncFlushStats;

#[tokio::test]
async fn batch_completion_uses_final_durable_watermark_for_every_waiter() {
    let stats = SyncFlushStats::default();
    let (request_64, response_64) = GroupCommitRequest::<&'static str>::new(64, 5_000);
    let (request_96, response_96) = GroupCommitRequest::<&'static str>::new(96, 5_000);
    stats.record_enqueue(request_64.enqueue_time_millis());
    stats.record_enqueue(request_96.enqueue_time_millis());

    complete_group_commit_batch(vec![request_64, request_96], 80, &stats);

    assert_eq!(response_64.await.unwrap().unwrap(), GroupCommitStatus::Flushed);
    assert_eq!(response_96.await.unwrap().unwrap(), GroupCommitStatus::TimedOut);
    let snapshot = stats.snapshot();
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.enqueue_total, 2);
    assert_eq!(snapshot.completed_total, 2);
    assert_eq!(snapshot.timeout_total, 1);
}

#[tokio::test]
async fn batch_error_is_shared_without_erasing_the_typed_source() {
    let stats = SyncFlushStats::default();
    let (first, first_response) = GroupCommitRequest::new(64, 5_000);
    let (second, second_response) = GroupCommitRequest::new(96, 5_000);
    let error = Arc::new(String::from("injected flush failure"));

    complete_group_commit_batch_error(vec![first, second], error.clone(), &stats);

    assert!(Arc::ptr_eq(&first_response.await.unwrap().unwrap_err(), &error));
    assert!(Arc::ptr_eq(&second_response.await.unwrap().unwrap_err(), &error));
    assert_eq!(stats.snapshot().completed_total, 2);
    assert_eq!(stats.snapshot().timeout_total, 2);
}

#[test]
fn expiration_preserves_zero_timeout_contract() {
    let (request, _response) = GroupCommitRequest::<()>::new(1, 0);

    assert!(request.is_expired());
}
