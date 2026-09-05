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

#![recursion_limit = "256"]

use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

#[test]
fn producer_to_writer_transfer_keeps_one_process_charge() {
    let owner = RuntimeOwner::plan(RuntimeConfig::default())
        .expect("default runtime configuration is valid")
        .with_memory_limit(ProcessMemoryLimit::configured(4096).expect("memory limit"))
        .build()
        .expect("runtime owner");
    let process = owner.root_context().component("client").process_budget();
    let producer = process
        .child("producer-egress", BudgetLimit::new(8, 4096, FullPolicy::Reject))
        .expect("producer budget");
    let writer = process
        .child("transport-writer", BudgetLimit::new(8, 4096, FullPolicy::Reject))
        .expect("writer budget");
    let mut permit = producer
        .try_acquire(1024, BudgetClass::Data)
        .expect("producer admission");

    assert_eq!(process.snapshot().current_bytes, 1024);
    permit.try_rebind(&writer).expect("writer rebind");
    assert_eq!(process.snapshot().current_bytes, 1024);
    assert_eq!(producer.snapshot().current_bytes, 0);
    assert_eq!(writer.snapshot().current_bytes, 1024);

    drop(permit);
    assert_eq!(process.snapshot().current_bytes, 0);
}

#[test]
fn aggregate_producer_and_writer_usage_cannot_exceed_process_ceiling() {
    let owner = RuntimeOwner::plan(RuntimeConfig::default())
        .expect("default runtime configuration is valid")
        .with_memory_limit(ProcessMemoryLimit::configured(1024).expect("memory limit"))
        .build()
        .expect("runtime owner");
    let process = owner.root_context().component("client").process_budget();
    let producer = process
        .child("producer-egress", BudgetLimit::new(8, 1024, FullPolicy::Reject))
        .expect("producer budget");
    let writer = process
        .child("transport-writer", BudgetLimit::new(8, 1024, FullPolicy::Reject))
        .expect("writer budget");
    let _in_writer = writer.try_acquire(768, BudgetClass::Data).expect("writer reservation");

    assert!(producer.try_acquire(512, BudgetClass::Data).is_err());
}
