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

use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::TimerMessageStore;
use rocketmq_store_api::TimerStoreMode;

fn timer_store(
    owner: &RuntimeOwner,
    source_workers: usize,
    due_workers: usize,
    queue_messages: usize,
    queue_bytes: usize,
) -> (tempfile::TempDir, Arc<TimerMessageStore>) {
    let directory = tempfile::tempdir().expect("timer pipeline root");
    let config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(directory.path().to_string_lossy().into_owned()),
        read_uncommitted: true,
        timer_precision_ms: 100,
        timer_put_message_thread_num: source_workers,
        timer_get_message_thread_num: due_workers,
        timer_pipeline_queue_messages: queue_messages,
        timer_pipeline_queue_bytes: queue_bytes,
        ..MessageStoreConfig::default()
    });
    let store = Arc::new(TimerMessageStore::new_with_message_store_config(
        config,
        owner.root_context().component("timer-pipeline-test"),
    ));
    assert!(store.load());
    (directory, store)
}

#[test]
fn worker_count_maps_to_managed_pipeline_workers() {
    for workers in [2usize, 4, 8] {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default(format!(
            "timer-{workers}-worker-count-test"
        )))
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime");
        let (_directory, store) = timer_store(&owner, workers, workers, 32, 4_096);

        owner.block_on(async {
            store.set_should_running_dequeue(true);
            store.start();
            store.start();
            tokio::time::sleep(Duration::from_millis(125)).await;

            let diagnostics = store.pipeline_diagnostics().expect("running pipeline diagnostics");
            assert_eq!(diagnostics.configured_source_workers(), workers);
            assert_eq!(diagnostics.configured_due_workers(), workers);
            let report = store
                .shutdown_gracefully_with_report()
                .await
                .expect("managed shutdown report");
            assert!(report.is_healthy(), "{}", report.to_json());
            assert!(store.pipeline_diagnostics().is_none());
        });
        let report = owner.shutdown_runtime_blocking().expect("runtime shutdown");
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}

#[test]
fn bounded_queues_never_exceed_message_or_byte_capacity() {
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("timer-budget-test"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime");
    let (_directory, store) = timer_store(&owner, 2, 2, 2, 128);

    owner.block_on(async {
        store.set_should_running_dequeue(true);
        store.start();
        tokio::time::sleep(Duration::from_millis(450)).await;
        let diagnostics = store.pipeline_diagnostics().expect("running pipeline diagnostics");
        assert!(diagnostics.source_queue_messages() <= 2);
        assert!(diagnostics.due_queue_messages() <= 2);
        assert!(diagnostics.source_queue_bytes() <= 128);
        assert!(diagnostics.due_queue_bytes() <= 128);
        store.shutdown_gracefully().await;
    });
    let report = owner.shutdown_runtime_blocking().expect("runtime shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[test]
fn due_priority_still_reserves_source_admission_each_tick() {
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("timer-priority-test"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime");
    let (_directory, store) = timer_store(&owner, 1, 1, 8, 1_024);

    owner.block_on(async {
        store.set_should_running_dequeue(true);
        store.start();
        tokio::time::sleep(Duration::from_millis(350)).await;
        let diagnostics = store.pipeline_diagnostics().expect("running pipeline diagnostics");
        assert_eq!(diagnostics.configured_source_workers(), 1);
        assert_eq!(diagnostics.configured_due_workers(), 1);
        assert_eq!(diagnostics.rejected_submissions(), 0);
        store.shutdown_gracefully().await;
    });
    let report = owner.shutdown_runtime_blocking().expect("runtime shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[test]
fn unsupported_extended_mode_fails_closed_during_load() {
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("timer-extended-closed-test"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime");
    let directory = tempfile::tempdir().expect("timer root");
    let config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(directory.path().to_string_lossy().into_owned()),
        timer_store_mode: TimerStoreMode::ExtendedTimeline,
        ..MessageStoreConfig::default()
    });
    let store = TimerMessageStore::new_with_message_store_config(
        config,
        owner.root_context().component("timer-extended-closed-test"),
    );
    assert!(!store.load());
    let report = owner.shutdown_runtime_blocking().expect("runtime shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[test]
fn unsupported_skip_unknown_policy_fails_closed_during_load() {
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("timer-skip-unknown-closed-test"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("runtime");
    let directory = tempfile::tempdir().expect("timer root");
    let config = Arc::new(MessageStoreConfig {
        store_path_root_dir: CheetahString::from_string(directory.path().to_string_lossy().into_owned()),
        timer_skip_unknown_error: true,
        ..MessageStoreConfig::default()
    });
    let store = TimerMessageStore::new_with_message_store_config(
        config,
        owner.root_context().component("timer-skip-unknown-closed-test"),
    );
    assert!(!store.load());
    let report = owner.shutdown_runtime_blocking().expect("runtime shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());
}
