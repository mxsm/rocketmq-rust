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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Default)]
pub struct TieredStoreMetrics {
    dispatch_requests: AtomicU64,
    dispatch_behind: AtomicU64,
    commit_failures: AtomicU64,
    fetch_requests: AtomicU64,
    messages_dispatch_total: AtomicU64,
    messages_out_total: AtomicU64,
    get_message_fallback_total: AtomicU64,
    provider_upload_bytes: AtomicU64,
    provider_download_bytes: AtomicU64,
    read_ahead_cache_access_total: AtomicU64,
    read_ahead_cache_hit_total: AtomicU64,
    read_ahead_cache_count: AtomicU64,
    read_ahead_cache_bytes: AtomicU64,
}

impl TieredStoreMetrics {
    pub fn record_dispatch_request(&self) {
        self.dispatch_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dispatch_dequeued(&self) {
        let _ = self
            .dispatch_behind
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_sub(1));
    }

    pub fn record_commit_failure(&self) {
        self.commit_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fetch_request(&self) {
        self.fetch_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_messages_dispatch(&self, topic: &str, queue_id: i32, file_type: &str, count: u64) {
        self.messages_dispatch_total.fetch_add(count, Ordering::Relaxed);

        #[cfg(feature = "otel-metrics")]
        rocketmq_observability::metrics::tiered_store::record_messages_dispatch(topic, queue_id, file_type, count);

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (topic, queue_id, file_type);
    }

    pub fn record_messages_out(&self, topic: &str, group: &str, count: u64) {
        self.messages_out_total.fetch_add(count, Ordering::Relaxed);

        #[cfg(feature = "otel-metrics")]
        rocketmq_observability::metrics::tiered_store::record_messages_out(topic, group, count);

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (topic, group);
    }

    pub fn record_get_message_fallback(&self, topic: &str, group: &str) {
        self.get_message_fallback_total.fetch_add(1, Ordering::Relaxed);

        #[cfg(feature = "otel-metrics")]
        rocketmq_observability::metrics::tiered_store::record_get_message_fallback(topic, group);

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (topic, group);
    }

    pub fn record_dispatch_latency(&self, topic: &str, queue_id: i32, file_type: &str, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        rocketmq_observability::metrics::tiered_store::record_dispatch_latency(topic, queue_id, file_type, latency_ms);

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (topic, queue_id, file_type, latency_ms);
    }

    pub fn record_api_latency(&self, operation: &str, success: bool, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        rocketmq_observability::metrics::tiered_store::record_api_latency(operation, success, latency_ms);

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (operation, success, latency_ms);
    }

    pub fn record_read_ahead_cache_access(&self, success: bool, count: u64) {
        self.read_ahead_cache_access_total.fetch_add(count, Ordering::Relaxed);

        #[cfg(feature = "otel-metrics")]
        rocketmq_observability::metrics::tiered_store::record_read_ahead_cache_access(success, count);

        #[cfg(not(feature = "otel-metrics"))]
        let _ = success;
    }

    pub fn record_read_ahead_cache_hit(&self, success: bool, count: u64) {
        self.read_ahead_cache_hit_total.fetch_add(count, Ordering::Relaxed);

        #[cfg(feature = "otel-metrics")]
        rocketmq_observability::metrics::tiered_store::record_read_ahead_cache_hit(success, count);

        #[cfg(not(feature = "otel-metrics"))]
        let _ = success;
    }

    pub fn set_read_ahead_cache_gauges(&self, count: u64, bytes: u64) {
        self.read_ahead_cache_count.store(count, Ordering::Relaxed);
        self.read_ahead_cache_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn dispatch_requests(&self) -> u64 {
        self.dispatch_requests.load(Ordering::Relaxed)
    }

    pub fn commit_failures(&self) -> u64 {
        self.commit_failures.load(Ordering::Relaxed)
    }

    pub fn fetch_requests(&self) -> u64 {
        self.fetch_requests.load(Ordering::Relaxed)
    }

    pub fn messages_dispatch_total(&self) -> u64 {
        self.messages_dispatch_total.load(Ordering::Relaxed)
    }

    pub fn messages_out_total(&self) -> u64 {
        self.messages_out_total.load(Ordering::Relaxed)
    }

    pub fn get_message_fallback_total(&self) -> u64 {
        self.get_message_fallback_total.load(Ordering::Relaxed)
    }

    pub fn provider_upload_bytes(&self) -> u64 {
        self.provider_upload_bytes.load(Ordering::Relaxed)
    }

    pub fn provider_download_bytes(&self) -> u64 {
        self.provider_download_bytes.load(Ordering::Relaxed)
    }

    pub fn read_ahead_cache_access_total(&self) -> u64 {
        self.read_ahead_cache_access_total.load(Ordering::Relaxed)
    }

    pub fn read_ahead_cache_hit_total(&self) -> u64 {
        self.read_ahead_cache_hit_total.load(Ordering::Relaxed)
    }

    #[cfg(feature = "otel-metrics")]
    pub fn observable_values(&self) -> rocketmq_observability::metrics::tiered_store::TieredStoreObservableValues {
        let dispatch_behind = self.dispatch_behind.load(Ordering::Relaxed) as i64;
        rocketmq_observability::metrics::tiered_store::TieredStoreObservableValues {
            dispatch_behind: vec![rocketmq_observability::metrics::tiered_store::TieredDispatchBehind {
                topic: "ALL".to_owned(),
                queue_id: -1,
                file_type: "commitlog".to_owned(),
                count: dispatch_behind,
            }],
            read_ahead_cache_count: self.read_ahead_cache_count.load(Ordering::Relaxed) as i64,
            read_ahead_cache_bytes: self.read_ahead_cache_bytes.load(Ordering::Relaxed) as i64,
        }
    }
}

pub(crate) fn record_dispatch_queued(metrics: &TieredStoreMetrics) {
    metrics.record_dispatch_request();
    metrics.dispatch_behind.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_provider_read(path: &str, bytes: u64, success: bool, latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    {
        rocketmq_observability::metrics::tiered_store::record_provider_download_bytes("read", success, path, bytes);
        rocketmq_observability::metrics::tiered_store::record_provider_rpc_latency("read", success, path, latency_ms);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = (path, bytes, success, latency_ms);
}

pub(crate) fn record_provider_write(path: &str, bytes: u64, success: bool, latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    {
        rocketmq_observability::metrics::tiered_store::record_provider_upload_bytes("write", success, path, bytes);
        rocketmq_observability::metrics::tiered_store::record_provider_rpc_latency("write", success, path, latency_ms);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = (path, bytes, success, latency_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiered_store_metrics_counts_local_events() {
        let metrics = TieredStoreMetrics::default();

        record_dispatch_queued(&metrics);
        metrics.record_dispatch_dequeued();
        metrics.record_commit_failure();
        metrics.record_fetch_request();
        metrics.record_messages_dispatch("TopicA", 0, "commitlog", 2);
        metrics.record_messages_out("TopicA", "GroupA", 3);
        metrics.record_get_message_fallback("TopicA", "GroupA");
        metrics.record_read_ahead_cache_access(true, 4);
        metrics.record_read_ahead_cache_hit(true, 2);

        assert_eq!(metrics.dispatch_requests(), 1);
        assert_eq!(metrics.commit_failures(), 1);
        assert_eq!(metrics.fetch_requests(), 1);
        assert_eq!(metrics.messages_dispatch_total(), 2);
        assert_eq!(metrics.messages_out_total(), 3);
        assert_eq!(metrics.get_message_fallback_total(), 1);
        assert_eq!(metrics.read_ahead_cache_access_total(), 4);
        assert_eq!(metrics.read_ahead_cache_hit_total(), 2);
    }
}
