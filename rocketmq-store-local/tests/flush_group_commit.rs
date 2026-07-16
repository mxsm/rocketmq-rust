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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_store_local::flush::group_commit::complete_group_commit_batch;
use rocketmq_store_local::flush::group_commit::complete_group_commit_batch_error;
use rocketmq_store_local::flush::group_commit::run_group_commit_worker;
use rocketmq_store_local::flush::group_commit::GroupCommitRequest;
use rocketmq_store_local::flush::group_commit::GroupCommitStatus;
use rocketmq_store_local::flush::group_commit::GroupCommitWorkerConfig;
use rocketmq_store_local::flush::group_commit::GroupCommitWorkerPorts;
use rocketmq_store_local::flush::group_commit::SyncFlushStats;
use rocketmq_store_local::flush::FlushProgress;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

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

#[tokio::test]
async fn worker_batches_waiters_and_applies_checkpoint_after_completion() {
    let stats = SyncFlushStats::default();
    let (first, first_response) = GroupCommitRequest::<String>::new(64, 5_000);
    let (second, second_response) = GroupCommitRequest::<String>::new(96, 5_000);
    stats.record_enqueue(first.enqueue_time_millis());
    stats.record_enqueue(second.enqueue_time_millis());
    let (sender, receiver) = mpsc::channel(2);
    sender.send(first).await.unwrap();
    sender.send(second).await.unwrap();
    drop(sender);

    let durable = Arc::new(AtomicI64::new(96));
    let timestamp = Arc::new(AtomicU64::new(77));
    let checkpoint = Arc::new(AtomicU64::new(0));
    let flush_calls = Arc::new(AtomicU64::new(0));
    let ports = GroupCommitWorkerPorts::new(
        {
            let durable = durable.clone();
            let timestamp = timestamp.clone();
            let flush_calls = flush_calls.clone();
            move || {
                flush_calls.fetch_add(1, Ordering::Relaxed);
                let durable = durable.load(Ordering::Relaxed);
                let timestamp = timestamp.load(Ordering::Relaxed);
                async move {
                    Ok::<_, Arc<String>>(FlushProgress {
                        appended: durable,
                        durable_before: durable,
                        durable,
                        store_timestamp: timestamp,
                    })
                }
            }
        },
        {
            let durable = durable.clone();
            move || durable.load(Ordering::Relaxed)
        },
        move || timestamp.load(Ordering::Relaxed),
        {
            let checkpoint = checkpoint.clone();
            move |value| checkpoint.store(value, Ordering::Relaxed)
        },
        |_| panic!("flush failure adapter must not run"),
        |_| async {},
    );

    run_group_commit_worker(
        receiver,
        Arc::new(Notify::new()),
        CancellationToken::new(),
        stats,
        None,
        GroupCommitWorkerConfig::legacy(),
        ports,
    )
    .await;

    assert_eq!(first_response.await.unwrap().unwrap(), GroupCommitStatus::Flushed);
    assert_eq!(second_response.await.unwrap().unwrap(), GroupCommitStatus::Flushed);
    assert_eq!(flush_calls.load(Ordering::Relaxed), 0);
    assert_eq!(checkpoint.load(Ordering::Relaxed), 77);
}

#[tokio::test]
async fn worker_propagates_forced_error_once_to_the_whole_batch() {
    let stats = SyncFlushStats::default();
    let (request, response) = GroupCommitRequest::<String>::new(64, 5_000);
    stats.record_enqueue(request.enqueue_time_millis());
    let (sender, receiver) = mpsc::channel(1);
    sender.send(request).await.unwrap();
    drop(sender);

    let error = Arc::new(String::from("injected flush failure"));
    let failure_calls = Arc::new(AtomicU64::new(0));
    let flush_calls = Arc::new(AtomicU64::new(0));
    let ports = GroupCommitWorkerPorts::new(
        {
            let flush_calls = flush_calls.clone();
            move || {
                flush_calls.fetch_add(1, Ordering::Relaxed);
                async { Ok::<_, Arc<String>>(FlushProgress::default()) }
            }
        },
        || 0,
        || 0,
        |_| panic!("checkpoint adapter must not run"),
        {
            let failure_calls = failure_calls.clone();
            move |_| {
                failure_calls.fetch_add(1, Ordering::Relaxed);
            }
        },
        |_| async {},
    );

    run_group_commit_worker(
        receiver,
        Arc::new(Notify::new()),
        CancellationToken::new(),
        stats,
        Some(error.clone()),
        GroupCommitWorkerConfig::legacy(),
        ports,
    )
    .await;

    let received = response.await.unwrap().unwrap_err();
    assert!(Arc::ptr_eq(&received, &error));
    assert_eq!(failure_calls.load(Ordering::Relaxed), 1);
    assert_eq!(flush_calls.load(Ordering::Relaxed), 0);
}
