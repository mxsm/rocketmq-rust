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

use rocketmq_client_rust::run_concurrent_clean_expire_lifecycle_probe;
use rocketmq_client_rust::run_connection_event_listener_lifecycle_probe;
use rocketmq_client_rust::run_consumer_stats_manager_lifecycle_probe;
use rocketmq_client_rust::run_latency_fault_detector_lifecycle_probe;
use rocketmq_client_rust::run_lite_pull_task_lifecycle_probe;
use rocketmq_client_rust::run_local_file_offset_store_lifecycle_probe;
use rocketmq_client_rust::run_namesrv_refresh_lifecycle_probe;
use rocketmq_client_rust::run_orderly_lock_periodic_lifecycle_probe;
use rocketmq_client_rust::run_pop_orderly_lock_refresh_lifecycle_probe;
use rocketmq_client_rust::run_produce_accumulator_guard_lifecycle_probe;
use rocketmq_client_rust::run_pull_message_service_lifecycle_probe;
use rocketmq_client_rust::run_rebalance_service_lifecycle_probe;
use rocketmq_client_rust::run_request_future_holder_lifecycle_probe;
use rocketmq_client_rust::run_trace_worker_lifecycle_probe;

#[tokio::test]
async fn client_service_lifecycle_probes_report_clean_shutdown() {
    let connection_events = run_connection_event_listener_lifecycle_probe().await;
    assert!(connection_events.healthy, "{connection_events:?}");
    assert_eq!(connection_events.task_count_after_shutdown, 0);

    let namesrv_refresh = run_namesrv_refresh_lifecycle_probe().await;
    assert!(namesrv_refresh.healthy, "{namesrv_refresh:?}");
    assert_eq!(namesrv_refresh.task_count_after_shutdown, 0);

    let consumer_stats = run_consumer_stats_manager_lifecycle_probe().await;
    assert!(consumer_stats.healthy, "{consumer_stats:?}");
    assert_eq!(consumer_stats.task_count_after_shutdown, 0);

    let latency_fault_detector = run_latency_fault_detector_lifecycle_probe().await;
    assert!(latency_fault_detector.healthy, "{latency_fault_detector:?}");
    assert_eq!(latency_fault_detector.task_count_after_shutdown, 0);
    assert_eq!(latency_fault_detector.scheduled_failures, 0);

    let concurrent_clean_expire = run_concurrent_clean_expire_lifecycle_probe().await;
    assert!(concurrent_clean_expire.healthy, "{concurrent_clean_expire:?}");
    assert_eq!(concurrent_clean_expire.task_count_after_shutdown, 0);

    let orderly_lock = run_orderly_lock_periodic_lifecycle_probe().await;
    assert!(orderly_lock.healthy, "{orderly_lock:?}");
    assert_eq!(orderly_lock.task_count_after_shutdown, 0);
    assert_eq!(orderly_lock.scheduled_failures, 0);

    let pop_orderly_lock = run_pop_orderly_lock_refresh_lifecycle_probe().await;
    assert!(pop_orderly_lock.healthy, "{pop_orderly_lock:?}");
    assert_eq!(pop_orderly_lock.task_count_after_shutdown, 0);
    assert_eq!(pop_orderly_lock.scheduled_failures, 0);

    let lite_pull = run_lite_pull_task_lifecycle_probe().await;
    assert!(lite_pull.healthy, "{lite_pull:?}");
    assert_eq!(lite_pull.task_count_after_shutdown, 0);

    let pull_message = run_pull_message_service_lifecycle_probe().await;
    assert!(pull_message.healthy, "{pull_message:?}");
    assert_eq!(pull_message.task_count_after_shutdown, 0);

    let rebalance = run_rebalance_service_lifecycle_probe().await;
    assert!(rebalance.healthy, "{rebalance:?}");
    assert_eq!(rebalance.task_count_after_shutdown, 0);

    let local_offset_store = run_local_file_offset_store_lifecycle_probe().await;
    assert!(local_offset_store.healthy, "{local_offset_store:?}");
    assert_eq!(local_offset_store.task_count_after_shutdown, 0);
    assert_eq!(local_offset_store.scheduled_failures, 0);

    let produce_accumulator = run_produce_accumulator_guard_lifecycle_probe().await;
    assert!(produce_accumulator.healthy, "{produce_accumulator:?}");
    assert_eq!(produce_accumulator.task_count_after_shutdown, 0);

    let request_future_holder = run_request_future_holder_lifecycle_probe().await;
    assert!(request_future_holder.healthy, "{request_future_holder:?}");
    assert_eq!(request_future_holder.task_count_after_shutdown, 0);
    assert_eq!(request_future_holder.scheduled_failures, 0);

    let trace_worker = run_trace_worker_lifecycle_probe().await;
    assert!(trace_worker.healthy, "{trace_worker:?}");
    assert_eq!(trace_worker.remaining_tasks_after_shutdown, 0);
    assert_eq!(trace_worker.timed_out, 0);
}
