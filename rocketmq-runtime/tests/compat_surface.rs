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

use std::time::Duration;

use rocketmq_runtime::compat::FuturesExecutorServiceBuilder;
use rocketmq_runtime::compat::SchedulerConfig;
use rocketmq_runtime::compat::TaskScheduler;
use rocketmq_runtime::prelude::*;
use rocketmq_runtime::RuntimeConfig;

/// The recommended path and the compatibility facade are both usable, and the
/// facade states which entry points belong to it.
#[test]
fn the_recommended_path_and_the_compatibility_facade_coexist() {
    let owner = RuntimeOwner::plan(RuntimeConfig::default())
        .expect("the default configuration is valid")
        .build()
        .expect("the owner should start");
    let component = owner.root_context().component("compat-surface");
    component
        .spawn_service("compat-surface.service", async {})
        .expect("the component is open");

    // The compatibility facade is reachable through its own module and keeps the
    // older helpers usable.
    let executor = FuturesExecutorServiceBuilder::new()
        .pool_size(1)
        .into_plan()
        .expect("a positive pool size is valid")
        .build()
        .expect("the futures pool should build");
    executor.spawn(async {});
    drop(executor);

    let scheduler = TaskScheduler::new_legacy_compatibility(SchedulerConfig {
        check_interval: Duration::from_millis(10),
        ..SchedulerConfig::default()
    });
    drop(scheduler);

    let report = owner
        .shutdown_runtime_blocking()
        .expect("shutdown should report its result");
    assert_eq!(report.timed_out, 0);
}
