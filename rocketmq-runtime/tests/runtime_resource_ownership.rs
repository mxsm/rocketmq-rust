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

use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

#[test]
fn child_contexts_share_the_runtime_owners_process_budget() {
    let memory_limit = ProcessMemoryLimit::configured(1024).expect("configured memory limit");
    let owner = RuntimeOwner::plan(RuntimeConfig::default())
        .expect("valid runtime configuration")
        .with_memory_limit(memory_limit)
        .build()
        .expect("runtime owner");
    let producer = owner.root_context().component("producer");
    let transport = owner.root_context().component("transport");

    assert_eq!(owner.resources().memory_limit(), memory_limit);
    let producer_budget = producer.process_budget();
    let transport_budget = transport.process_budget();
    let permit = producer_budget
        .try_acquire(768, BudgetClass::Data)
        .expect("producer reservation");

    assert_eq!(transport_budget.snapshot().current_bytes, 768);
    let exhausted = transport_budget
        .try_acquire(300, BudgetClass::Data)
        .expect_err("shared process ceiling must reject aggregate overcommit");
    assert_eq!(exhausted.exhausted_path(), "process");

    drop(permit);
    assert_eq!(owner.resources().process_budget().snapshot().current_bytes, 0);
}
