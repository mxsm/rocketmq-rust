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

//! Explicit test and benchmark fixtures.
//!
//! This module is excluded from default production builds. Consumers must opt
//! in with the `test-support` feature.

pub use crate::consumer::consumer_impl::consume_message_concurrently_service::{
    run_concurrent_clean_expire_lifecycle_probe, ConcurrentCleanExpireLifecycleProbe,
};
pub use crate::consumer::consumer_impl::consume_message_orderly_service::{
    run_orderly_lock_periodic_lifecycle_probe, OrderlyLockPeriodicLifecycleProbe,
};
pub use crate::consumer::consumer_impl::consume_message_pop_orderly_service::{
    run_pop_orderly_lock_refresh_lifecycle_probe, PopOrderlyLockRefreshLifecycleProbe,
};
pub use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::{
    run_lite_pull_assignment_registry_probe, run_lite_pull_concurrency_contract_probe,
    run_lite_pull_task_lifecycle_probe, LitePullAssignmentRegistryProbe, LitePullConcurrencyContractProbe,
    LitePullTaskLifecycleProbe,
};
pub use crate::consumer::consumer_impl::process_queue::{
    run_process_queue_has_temp_message_probe, run_process_queue_max_span_only_probe, run_process_queue_put_probe,
    run_process_queue_remove_probe, run_process_queue_take_probe, ProcessQueue, ProcessQueueOperationFixture,
};
pub use crate::consumer::consumer_impl::pull_message_service::{
    run_pull_message_service_lifecycle_probe, PullMessageService, PullMessageServiceLifecycleProbe,
    PullMessageServiceShardSnapshot,
};
pub use crate::consumer::consumer_impl::pull_request::PullRequest;
pub use crate::consumer::consumer_impl::re_balance::rebalance_service::{
    run_rebalance_service_lifecycle_probe, RebalanceServiceLifecycleProbe,
};
pub use crate::consumer::store::local_file_offset_store::{
    run_local_file_offset_store_lifecycle_probe, LocalFileOffsetStoreLifecycleProbe,
};
pub use crate::factory::mq_client_instance::{
    run_connection_event_listener_lifecycle_probe, run_heartbeat_route_index_probe,
    run_route_refresh_concurrent_stale_guard_probe, run_route_refresh_shard_probe,
    ConnectionEventListenerLifecycleProbe, HeartbeatRouteIndexProbe, MQClientInstance, RouteRefreshConcurrentProbe,
    RouteRefreshShardProbe,
};
pub use crate::implementation::mq_client_api_factory::{
    run_namesrv_refresh_lifecycle_probe, NamesrvRefreshLifecycleProbe,
};
pub use crate::latency::latency_fault_tolerance_impl::{
    run_latency_fault_detector_lifecycle_probe, LatencyFaultDetectorLifecycleProbe,
};
pub use crate::producer::produce_accumulator::{
    run_produce_accumulator_guard_lifecycle_probe, ProduceAccumulatorGuardLifecycleProbe,
};
pub use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
pub use crate::producer::request_future_holder::{
    run_request_future_holder_lifecycle_probe, run_request_future_holder_scan_probe, RequestFutureHolderLifecycleProbe,
    RequestFutureHolderScanProbe,
};
pub use crate::stat::consumer_stats_manager::{
    run_consumer_stats_manager_lifecycle_probe, ConsumerStatsManagerLifecycleProbe,
};
pub use crate::trace::async_trace_dispatcher::{
    run_trace_queue_depth_accounting_probe, run_trace_worker_lifecycle_probe, TraceWorkerLifecycleProbe,
};
