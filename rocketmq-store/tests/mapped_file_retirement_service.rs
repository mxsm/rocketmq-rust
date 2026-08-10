// Copyright 2026 The RocketMQ Rust Authors
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

use rocketmq_store::MessageStoreShutdownReport;
use rocketmq_store_local::mapped_file::ManagedRetirementStage;

#[test]
fn shutdown_report_preserves_every_durable_retirement_backlog_dimension() {
    let report = MessageStoreShutdownReport {
        mapped_file_retirement_pending_tickets: 7,
        mapped_file_retirement_tombstone_backlog: 3,
        mapped_file_retirement_oldest_pending_age: Duration::from_secs(11),
        mapped_file_retirement_last_failure_stage: Some(ManagedRetirementStage::TombstoneRemoval),
        mapped_file_retirement_recovery_required: true,
        ..MessageStoreShutdownReport::default()
    };

    assert_eq!(report.mapped_file_retirement_pending_tickets, 7);
    assert_eq!(report.mapped_file_retirement_tombstone_backlog, 3);
    assert_eq!(
        report.mapped_file_retirement_oldest_pending_age,
        Duration::from_secs(11)
    );
    assert_eq!(
        report.mapped_file_retirement_last_failure_stage,
        Some(ManagedRetirementStage::TombstoneRemoval)
    );
    assert!(report.mapped_file_retirement_recovery_required);
}

#[test]
fn service_source_owns_scheduling_blocking_drain_and_await_boundaries() {
    let service = include_str!("../src/message_store/local_file_message_store/mapped_file_retirement_service.rs")
        .replace("\r\n", "\n");
    let lifecycle = include_str!("../src/message_store/local_file_message_store/lifecycle.rs").replace("\r\n", "\n");

    assert!(service.contains("ScheduledTaskGroup::new"));
    assert!(service.contains("schedule_fixed_delay"));
    assert!(service.contains("spawn_io(\"mapped-file-retirement-batch\""));
    assert!(service.contains("drain_pending(first_error, \"mapped-file-retirement-drain\""));
    assert!(service.contains("spawn_io_until(task_name, deadline"));
    assert!(service.contains("task_group.shutdown"));
    assert!(service.contains("driver.begin_shutdown()"));
    assert!(!service.contains("tokio::spawn"));
    assert!(!service.contains("spawn_blocking"));
    assert!(!service.contains("std::thread"));

    assert!(lifecycle.contains("service.start()?"));
    assert!(lifecycle.contains("service.cancel_drain_and_await().await"));
    assert!(lifecycle.contains("mapped_file_retirement_pending_tickets"));
    assert!(lifecycle.contains("mapped_file_retirement_tombstone_backlog"));
    assert!(lifecycle.contains("mapped_file_retirement_oldest_pending_age"));
    assert!(lifecycle.contains("mapped_file_retirement_last_failure_stage"));
}
