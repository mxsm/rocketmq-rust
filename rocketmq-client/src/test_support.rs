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

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_runtime::ChildServiceContext;

use crate::base::client_config::ClientConfig;
use crate::latency::latency_fault_tolerance_impl::ManualFaultClock;
use crate::latency::mq_fault_strategy::MQFaultStrategy;

/// Deterministic state captured from the real latency-fault table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LatencyFaultJavaCompatState {
    pub available: bool,
    pub reachable: bool,
    pub unavailable_until_ms: u64,
    pub current_latency_ms: u64,
}

/// Manual-clock facade used by the Java 5.5 latency-fault acceptance corpus.
pub struct LatencyFaultJavaCompatHarness {
    clock: Arc<ManualFaultClock>,
    strategy: MQFaultStrategy,
}

impl LatencyFaultJavaCompatHarness {
    pub fn new(
        service_context: ChildServiceContext,
        now_ms: u64,
        latency_max_ms: Vec<u64>,
        not_available_duration_ms: Vec<u64>,
    ) -> Self {
        assert!(!latency_max_ms.is_empty(), "latency thresholds must not be empty");
        assert_eq!(
            latency_max_ms.len(),
            not_available_duration_ms.len(),
            "latency thresholds and durations must have equal lengths"
        );
        let clock = Arc::new(ManualFaultClock::new(now_ms));
        let mut config = ClientConfig::default();
        config.set_send_latency_enable(true);
        let mut strategy = MQFaultStrategy::new_with_fault_clock(service_context, &config, clock.clone());
        strategy.set_latency_max(latency_max_ms);
        strategy.set_not_available_duration(not_available_duration_ms);
        Self { clock, strategy }
    }

    pub fn set_now_ms(&self, now_ms: u64) {
        self.clock.set_now_millis(now_ms);
    }

    pub fn not_available_duration(&self, latency_ms: u64, isolation: bool) -> u64 {
        self.strategy.not_available_duration_for_test(latency_ms, isolation)
    }

    pub async fn update(&self, broker: &str, latency_ms: u64, isolation: bool, reachable: bool) {
        self.strategy
            .update_fault_item(CheetahString::from_slice(broker), latency_ms, isolation, reachable)
            .await;
    }

    pub fn state(&self, broker: &str) -> Option<LatencyFaultJavaCompatState> {
        self.strategy
            .fault_item_state_for_test(&CheetahString::from_slice(broker))
            .map(
                |(available, reachable, unavailable_until_ms, current_latency_ms)| LatencyFaultJavaCompatState {
                    available,
                    reachable,
                    unavailable_until_ms,
                    current_latency_ms,
                },
            )
    }

    pub fn select(&self, brokers: &[String], last_broker: Option<&str>) -> Option<String> {
        let mut topic_publish_info = TopicPublishInfo::new();
        topic_publish_info.message_queue_list = brokers
            .iter()
            .enumerate()
            .map(|(queue_id, broker)| {
                MessageQueue::from_parts(
                    "LatencyFaultJavaCompat",
                    broker.as_str(),
                    i32::try_from(queue_id).expect("compatibility corpus queue count fits i32"),
                )
            })
            .collect();
        let last_broker = last_broker.map(CheetahString::from_slice);
        self.strategy
            .select_one_message_queue(&topic_publish_info, last_broker.as_ref(), true)
            .map(|queue| queue.broker_name().to_string())
    }
}

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
    run_lite_pull_suspend_timeout_probe, run_lite_pull_task_lifecycle_probe, LitePullAssignmentRegistryProbe,
    LitePullConcurrencyContractProbe, LitePullSuspendTimeoutProbe, LitePullTaskLifecycleProbe,
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
#[cfg(feature = "nameserver-dns-discovery")]
pub use crate::nameserver_discovery::supervisor::{
    run_nameserver_discovery_lifecycle_probe, NameServerDiscoveryLifecycleProbe,
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
