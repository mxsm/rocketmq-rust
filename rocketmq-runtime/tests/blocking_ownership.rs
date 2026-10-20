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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingLane;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::RuntimeOwner;

fn owner() -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig::for_parallelism("blocking-owner", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(8 * 1024 * 1024).unwrap())
        .build()
        .unwrap()
}

fn foreign_host() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_name("foreign-blocking-host")
        .build()
        .unwrap()
}

#[test]
fn managed_lanes_execute_on_the_owner_when_called_from_a_foreign_runtime() {
    let owner = owner();
    let service = owner.root_context().component("component").component("child");
    let owner_id = owner.block_on(async { tokio::runtime::Handle::current().id() });
    let foreign = foreign_host();
    foreign.block_on(async {
        assert_ne!(tokio::runtime::Handle::current().id(), owner_id);
        for lane in [
            BlockingLane::StorageIo,
            BlockingLane::MetadataIo,
            BlockingLane::CpuCrypto,
        ] {
            let executor = service.blocking(lane).clone();
            let actual = executor
                .spawn_io("runtime-identity", || tokio::runtime::Handle::current().id())
                .await
                .unwrap();
            assert_eq!(actual, owner_id, "{lane:?} must use its injected runtime");
            assert!(executor.snapshot().tasks.is_empty());
            assert_eq!(executor.snapshot().global_running, 0);
        }
    });
    assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
}

#[test]
fn isolated_executor_uses_the_supplied_group_even_when_constructed_outside_tokio() {
    let owner = owner();
    let service = owner.root_context().component("isolated");
    let owner_id = owner.block_on(async { tokio::runtime::Handle::current().id() });
    assert!(tokio::runtime::Handle::try_current().is_err());
    let executor = BlockingExecutor::new(BlockingPoolPolicy::default(), service.task_group().clone()).unwrap();
    let foreign = foreign_host();
    let actual = foreign
        .block_on(executor.spawn_io("runtime-identity", || tokio::runtime::Handle::current().id()))
        .unwrap();
    assert_eq!(actual, owner_id);
    assert_eq!(executor.snapshot().global_running, 0);
    assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
}

#[test]
fn the_owner_report_counts_a_closure_still_running_on_an_isolated_executor() {
    let owner = owner();
    let service = owner.root_context().component("isolated-report");
    let policy = BlockingPoolPolicy {
        task_timeout: Duration::from_millis(50),
        ..BlockingPoolPolicy::default()
    };
    let executor = BlockingExecutor::new(policy, service.task_group().clone()).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let waited = owner.block_on(executor.spawn_io("held-isolated", move || {
        started_tx.send(()).unwrap();
        let _ = release_rx.recv();
    }));
    started_rx.recv().unwrap();
    assert!(
        waited.is_err(),
        "the caller stops waiting while the closure keeps running"
    );

    let report = owner.block_on(owner.shutdown_tasks());
    assert_eq!(report.blocking_still_running, 1);
    assert!(!report.is_healthy());
    assert!(report.blocking_tasks.iter().any(|task| task.name == "held-isolated"));

    // Dropping every executor handle does not hide the running closure.
    drop(executor);
    let report = owner.block_on(owner.shutdown_tasks());
    assert_eq!(report.blocking_still_running, 1);

    release_tx.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while owner.block_on(owner.shutdown_tasks()).blocking_still_running != 0 {
        assert!(Instant::now() < deadline, "the released closure should exit");
        std::thread::yield_now();
    }
    assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_runtime_context_report_counts_its_isolated_executors() {
    let context = RuntimeContext::try_from_current("isolated-context-report").unwrap();
    let executor = BlockingExecutor::new(
        BlockingPoolPolicy {
            task_timeout: Duration::from_millis(50),
            ..BlockingPoolPolicy::default()
        },
        context.service_context("isolated").task_group().clone(),
    )
    .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let waited = executor
        .spawn_io("held-context", move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
        })
        .await;
    started_rx.await.unwrap();
    assert!(waited.is_err());

    let report = context.shutdown_tasks(Duration::from_secs(1)).await;
    assert_eq!(report.blocking_still_running, 1);
    assert!(!report.is_healthy());
    assert_eq!(
        report
            .annotations
            .iter()
            .filter(|annotation| annotation.message.contains("blocking_still_running"))
            .count(),
        1,
        "running blocking work is annotated once"
    );
    release_tx.send(()).unwrap();
}

#[test]
fn a_retained_executor_cannot_execute_on_another_runtime_after_its_owner_is_destroyed() {
    let owner = owner();
    let executor = owner.root_context().component("retained").metadata_io().clone();
    assert!(owner.shutdown_runtime_blocking().unwrap().is_healthy());
    let foreign = foreign_host();
    let ran = Arc::new(AtomicBool::new(false));
    let operation_ran = ran.clone();
    foreign
        .block_on(executor.spawn_io("after-runtime-shutdown", move || {
            operation_ran.store(true, Ordering::Release);
        }))
        .expect_err("a destroyed runtime cannot execute a retained capability");
    assert!(!ran.load(Ordering::Acquire));
    let snapshot = executor.snapshot();
    assert!(snapshot.tasks.is_empty());
    assert_eq!(snapshot.global_running, 0);
}
