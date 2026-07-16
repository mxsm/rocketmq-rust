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

use std::future;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_store_local::flush::worker::run_commit_real_time_worker;
use rocketmq_store_local::flush::worker::run_flush_real_time_worker;
use rocketmq_store_local::flush::worker::CommitRealTimeWorkerConfig;
use rocketmq_store_local::flush::worker::CommitRealTimeWorkerPorts;
use rocketmq_store_local::flush::worker::CommitWorkerProgress;
use rocketmq_store_local::flush::worker::FlushRealTimeWorkerConfig;
use rocketmq_store_local::flush::worker::FlushRealTimeWorkerPorts;
use rocketmq_store_local::flush::worker::FlushWorkerFailurePhase;
use rocketmq_store_local::flush::FlushProgress;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn cancelled_flush_worker_still_runs_one_final_flush_and_checkpoint() {
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let checkpoint = Arc::new(AtomicU64::new(0));
    let ports = FlushRealTimeWorkerPorts::new(
        {
            let calls = calls.clone();
            move |least_pages| {
                calls.lock().unwrap().push(least_pages);
                async {
                    Ok::<_, Arc<String>>(FlushProgress {
                        store_timestamp: 42,
                        ..Default::default()
                    })
                }
            }
        },
        |_, _| panic!("failure adapter must not run"),
        {
            let checkpoint = checkpoint.clone();
            move |timestamp| checkpoint.store(timestamp, Ordering::Relaxed)
        },
        |_| future::pending::<()>(),
    );

    run_flush_real_time_worker(
        Arc::new(Notify::new()),
        shutdown,
        FlushRealTimeWorkerConfig::legacy(false, 500, 4, 10_000),
        ports,
    )
    .await;

    assert_eq!(*calls.lock().unwrap(), vec![0]);
    assert_eq!(checkpoint.load(Ordering::Relaxed), 42);
}

#[tokio::test]
async fn periodic_flush_failure_is_classified_before_final_flush() {
    let notified = Arc::new(Notify::new());
    notified.notify_one();
    let calls = Arc::new(AtomicU64::new(0));
    let phase = Arc::new(Mutex::new(None));
    let checkpoint = Arc::new(AtomicU64::new(0));
    let ports = FlushRealTimeWorkerPorts::new(
        {
            let calls = calls.clone();
            move |_| {
                let call = calls.fetch_add(1, Ordering::Relaxed);
                async move {
                    if call == 0 {
                        Err(Arc::new(String::from("injected periodic failure")))
                    } else {
                        Ok(FlushProgress {
                            store_timestamp: 55,
                            ..Default::default()
                        })
                    }
                }
            }
        },
        {
            let phase = phase.clone();
            move |value, _| *phase.lock().unwrap() = Some(value)
        },
        {
            let checkpoint = checkpoint.clone();
            move |timestamp| checkpoint.store(timestamp, Ordering::Relaxed)
        },
        |_| future::pending::<()>(),
    );

    run_flush_real_time_worker(
        notified,
        CancellationToken::new(),
        FlushRealTimeWorkerConfig::legacy(false, 500, 4, 10_000),
        ports,
    )
    .await;

    assert_eq!(*phase.lock().unwrap(), Some(FlushWorkerFailurePhase::Periodic));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(checkpoint.load(Ordering::Relaxed), 55);
}

#[tokio::test]
async fn commit_progress_wakes_flush_and_final_commit_updates_checkpoint() {
    let shutdown = CancellationToken::new();
    let notified = Arc::new(Notify::new());
    notified.notify_one();
    let calls = Arc::new(AtomicU64::new(0));
    let pages = Arc::new(Mutex::new(Vec::new()));
    let wakeups = Arc::new(AtomicU64::new(0));
    let checkpoint = Arc::new(AtomicU64::new(0));
    let ports = CommitRealTimeWorkerPorts::new(
        {
            let shutdown = shutdown.clone();
            let calls = calls.clone();
            let pages = pages.clone();
            move |least_pages| {
                pages.lock().unwrap().push(least_pages);
                let call = calls.fetch_add(1, Ordering::Relaxed);
                if call == 0 {
                    shutdown.cancel();
                }
                async move { Some(CommitWorkerProgress::new(false, if call == 0 { 0 } else { 99 })) }
            }
        },
        {
            let wakeups = wakeups.clone();
            move || {
                wakeups.fetch_add(1, Ordering::Relaxed);
            }
        },
        {
            let checkpoint = checkpoint.clone();
            move |timestamp| checkpoint.store(timestamp, Ordering::Relaxed)
        },
        |_| future::pending::<()>(),
    );

    run_commit_real_time_worker(
        notified,
        shutdown,
        CommitRealTimeWorkerConfig::legacy(500, 4, 10_000),
        ports,
    )
    .await;

    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(*pages.lock().unwrap(), vec![0, 0]);
    assert_eq!(wakeups.load(Ordering::Relaxed), 1);
    assert_eq!(checkpoint.load(Ordering::Relaxed), 99);
}
