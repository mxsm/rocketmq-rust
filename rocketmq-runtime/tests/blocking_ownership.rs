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
use std::sync::Arc;

use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingLane;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::RuntimeConfig;
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
