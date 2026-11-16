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

use std::error::Error as _;

use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeErrorKind;
use rocketmq_runtime::TaskGroupLifecycleState;
use tempfile::TempDir;

use super::shutdown_diagnostics::ScheduleShutdownFailure;
use super::*;
use crate::broker_runtime::BrokerRuntime;
use crate::schedule::delay_offset_serialize_wrapper::DelayOffsetSerializeWrapper;

#[tokio::test]
async fn schedule_stop_persists_offsets_while_an_unrelated_parent_service_remains_live() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().to_string_lossy().into_owned();
    let runtime = RuntimeContext::from_current("schedule-sibling-owner-test");
    let context = runtime.service_context("broker");
    let parent = context.task_group().clone();
    let cancelled = parent.cancellation_token();
    let (entered, live) = oneshot::channel();
    // Reload-enabled Brokers own their TTL controller directly in this parent.
    // Keep the same ownership shape without a global logging subscriber.
    let sibling = context
        .spawn_service("broker.log-filter-ttl.test-probe", async move {
            let _ = entered.send(());
            cancelled.cancelled().await;
        })
        .unwrap();
    live.await.unwrap();
    let mut broker = BrokerRuntime::new_with_service_context(
        Arc::new(BrokerConfig {
            store_path_root_dir: root.clone().into(),
            ..BrokerConfig::default()
        }),
        Arc::new(MessageStoreConfig {
            store_path_root_dir: root.into(),
            ..MessageStoreConfig::default()
        }),
        context.clone(),
    );
    let schedule = broker
        .runtime_state_mut()
        .schedule_message_service_for_test()
        .unwrap()
        .clone();
    schedule.offset_state.update_offset(1, 7, 1, 0);
    let persisted = ScheduleMessageService::start_persist_task_for_probe(schedule.clone(), Duration::ZERO)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), persisted)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    schedule.offset_state.update_offset(1, 11, 1, 0);
    let first_stop = schedule.stop().await;
    let failure = first_stop.as_ref().err().and_then(|error| {
        error
            .source()
            .and_then(|source| source.downcast_ref::<ScheduleShutdownFailure>())
            .cloned()
    });
    let own_tasks_after_stop = schedule.task_count();
    let sibling_after_stop = parent.contains_task(sibling);
    let parent_open = parent.lifecycle_state() == TaskGroupLifecycleState::Open;
    let first_snapshot = std::fs::read_to_string(schedule.config_file_path()).unwrap();
    let first_snapshot: DelayOffsetSerializeWrapper = serde_json::from_str(&first_snapshot).unwrap();

    let mut restarted = false;
    let mut finalized = false;
    if first_stop.as_ref().is_ok_and(|stopped| *stopped) {
        restarted = ScheduleMessageService::start_persist_task_for_probe(schedule.clone(), Duration::from_secs(60))
            .await
            .is_ok();
        schedule.offset_state.update_offset(1, 13, 1, 0);
        finalized = schedule.shutdown().await.is_ok();
    }
    let final_snapshot = std::fs::read_to_string(schedule.config_file_path()).unwrap();
    let final_snapshot: DelayOffsetSerializeWrapper = serde_json::from_str(&final_snapshot).unwrap();
    let sibling_after_finalize = parent.contains_task(sibling);
    let (admitted, admission) = oneshot::channel();
    let parent_admitted = context
        .spawn_service("broker.parent-admission.test-probe", async move {
            let _ = admitted.send(());
        })
        .is_ok();
    if parent_admitted {
        admission.await.unwrap();
    }

    // Drain all owned work before assertions, including on the unfixed red path.
    let report = parent.shutdown(Duration::from_secs(5)).await;
    assert!(
        first_stop.is_ok_and(|stopped| stopped),
        "schedule's own operation drained but its parent sibling stayed live: failure={failure:?}, own_tasks={own_tasks_after_stop}, sibling_live={sibling_after_stop}, parent_open={parent_open}"
    );
    assert_eq!(own_tasks_after_stop, 0);
    assert!(sibling_after_stop && parent_open);
    assert_eq!(first_snapshot.offset_table().unwrap().get(&1), Some(&11));
    assert!(restarted && finalized);
    assert_eq!(final_snapshot.offset_table().unwrap().get(&1), Some(&13));
    assert!(sibling_after_finalize && parent_admitted);
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn schedule_final_persistence_keeps_closed_owner_and_real_io_failures_unhealthy() {
    for closed_owner in [false, true] {
        let directory = TempDir::new().unwrap();
        let private_root = directory.path().join("PRIVATE_SCHEDULE_PATH_CANARY");
        let root = private_root.to_string_lossy().into_owned();
        let runtime = RuntimeContext::from_current("schedule-shutdown-failure-test");
        let context = runtime.service_context("broker");
        let parent = context.task_group().clone();
        let mut broker = BrokerRuntime::new_with_service_context(
            Arc::new(BrokerConfig {
                store_path_root_dir: root.clone().into(),
                ..BrokerConfig::default()
            }),
            Arc::new(MessageStoreConfig {
                store_path_root_dir: root.into(),
                ..MessageStoreConfig::default()
            }),
            context,
        );
        let schedule = broker
            .runtime_state_mut()
            .schedule_message_service_for_test()
            .unwrap()
            .clone();
        schedule.offset_state.update_offset(1, 17, 1, 0);
        if closed_owner {
            parent.cancel();
        } else {
            // A directory at the exact file path is a genuine durable-write failure.
            std::fs::create_dir_all(schedule.config_file_path()).unwrap();
        }
        let result = schedule.shutdown().await;
        let report = parent.shutdown(Duration::from_secs(5)).await;
        let error = result.expect_err("shutdown must not hide closed admission or failed persistence");
        let failure = error
            .source()
            .and_then(|source| source.downcast_ref::<ScheduleShutdownFailure>())
            .unwrap();
        assert_eq!(
            failure.stage,
            if closed_owner {
                ScheduleShutdownStage::PersistAdmission
            } else {
                ScheduleShutdownStage::PersistWrite
            }
        );
        let source = failure.source().unwrap().downcast_ref::<RuntimeError>().unwrap();
        if closed_owner {
            assert_eq!(source.kind(), RuntimeErrorKind::Closed);
            assert!(failure.scope_cancelled);
        } else {
            assert!(matches!(
                source.kind(),
                RuntimeErrorKind::Io | RuntimeErrorKind::Internal
            ));
            assert!(failure.scope_open && !failure.scope_cancelled);
        }
        assert!(!schedule.lifecycle.lock().await.finalized);
        assert!(!format!("{failure:?}").contains("PRIVATE_SCHEDULE_PATH_CANARY"));
        assert!(!error.to_string().contains("PRIVATE_SCHEDULE_PATH_CANARY"));
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}
