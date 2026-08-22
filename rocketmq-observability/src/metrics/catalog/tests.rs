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

use std::collections::HashSet;

use super::*;

#[test]
fn client_nameserver_metrics_expose_only_low_cardinality_labels() {
    let metric_names = [
        metrics::CLIENT_NAMESRV_DISCOVERY_REFRESH_TOTAL,
        metrics::CLIENT_NAMESRV_DISCOVERY_ENDPOINT_COUNT,
        metrics::CLIENT_NAMESRV_DISCOVERY_FRESHNESS,
        metrics::CLIENT_NAMESRV_DISCOVERY_SNAPSHOT_AGE,
        metrics::CLIENT_NAMESRV_FAILOVER_TOTAL,
    ];
    let forbidden = [
        labels::ADDRESS,
        labels::TOPIC,
        labels::GROUP,
        labels::CONSUMER_GROUP,
        "fqdn",
        "ip",
        "pod",
        "namespace",
    ];

    for name in metric_names {
        let descriptor = RUST_METRICS
            .iter()
            .find(|descriptor| descriptor.name == name)
            .expect("client NameServer metric descriptor");
        assert_eq!(descriptor.source, MetricSource::Client);
        assert!(descriptor.labels.iter().all(|label| !forbidden.contains(label)));
    }
}

const EXPECTED_JAVA_METRIC_NAMES: &[&str] = &[
    "active_broker_num",
    "dledger_disk_usage",
    "dledger_op_latency",
    "dledger_op_total",
    "election_total",
    "request_latency",
    "request_total",
    "rocketmq_broker_permission",
    "rocketmq_broker_up",
    "rocketmq_commit_messages_total",
    "rocketmq_consumer_connections",
    "rocketmq_consumer_group_create_execution_time",
    "rocketmq_consumer_group_number",
    "rocketmq_consumer_inflight_messages",
    "rocketmq_consumer_lag_latency",
    "rocketmq_consumer_lag_messages",
    "rocketmq_consumer_queueing_latency",
    "rocketmq_consumer_ready_messages",
    "rocketmq_delay_message_latency",
    "rocketmq_finish_message_latency",
    "rocketmq_half_messages",
    "rocketmq_message_size",
    "rocketmq_messages_in_total",
    "rocketmq_messages_out_total",
    "rocketmq_pop_buffer_scan_time_consume",
    "rocketmq_pop_checkpoint_buffer_size",
    "rocketmq_pop_offset_buffer_size",
    "rocketmq_pop_revive_in_message_total",
    "rocketmq_pop_revive_lag",
    "rocketmq_pop_revive_latency",
    "rocketmq_pop_revive_out_message_total",
    "rocketmq_pop_revive_retry_messages_total",
    "rocketmq_processor_watermark",
    "rocketmq_producer_connections",
    "rocketmq_proxy_up",
    "rocketmq_rocksdb_bytes_read",
    "rocketmq_rocksdb_bytes_written",
    "rocketmq_rocksdb_rate_cache_hit",
    "rocketmq_rocksdb_read_amplification_bytes",
    "rocketmq_rocksdb_times_compressed",
    "rocketmq_rocksdb_times_read",
    "rocketmq_rocksdb_times_written_other",
    "rocketmq_rocksdb_times_written_self",
    "rocketmq_rollback_messages_total",
    "rocketmq_rpc_latency",
    "rocketmq_send_to_dlq_messages_total",
    "rocketmq_storage_dispatch_behind_bytes",
    "rocketmq_storage_flush_behind_bytes",
    "rocketmq_storage_message_reserve_time",
    "rocketmq_storage_size",
    "rocketmq_store_commitlog_segment_lease_active",
    "rocketmq_store_ha_ack_latency_millis",
    "rocketmq_store_ha_replication_lag_bytes",
    "rocketmq_store_linux_sendfile_bytes_total",
    "rocketmq_store_linux_locked_bytes",
    "rocketmq_store_linux_mlock_attempt_total",
    "rocketmq_store_linux_mlock_bytes",
    "rocketmq_store_linux_mlock_failure_total",
    "rocketmq_store_linux_mlock_skipped_total",
    "rocketmq_store_linux_mlock_success_total",
    "rocketmq_store_linux_munlock_failure_total",
    "rocketmq_store_linux_page_cache_warmup_millis",
    "rocketmq_store_linux_storage_degradation_total",
    "rocketmq_store_transfer_batch_total",
    "rocketmq_store_transfer_bytes_total",
    "rocketmq_store_transfer_engine_total",
    "rocketmq_store_transfer_fallback_total",
    "rocketmq_store_transfer_partial_write_total",
    "rocketmq_throughput_in_total",
    "rocketmq_throughput_out_total",
    "rocketmq_tiered_store_api_latency",
    "rocketmq_tiered_store_dispatch_behind",
    "rocketmq_tiered_store_dispatch_latency",
    "rocketmq_tiered_store_get_message_fallback_total",
    "rocketmq_tiered_store_messages_dispatch_total",
    "rocketmq_tiered_store_messages_out_total",
    "rocketmq_tiered_store_provider_download_bytes",
    "rocketmq_tiered_store_provider_rpc_latency",
    "rocketmq_tiered_store_provider_upload_bytes",
    "rocketmq_tiered_store_read_ahead_cache_access_total",
    "rocketmq_tiered_store_read_ahead_cache_bytes",
    "rocketmq_tiered_store_read_ahead_cache_count",
    "rocketmq_tiered_store_read_ahead_cache_hit_total",
    "rocketmq_timer_dequeue_lag",
    "rocketmq_timer_dequeue_latency",
    "rocketmq_timer_dequeue_total",
    "rocketmq_timer_enqueue_lag",
    "rocketmq_timer_enqueue_latency",
    "rocketmq_timer_enqueue_total",
    "rocketmq_timer_message_snapshot",
    "rocketmq_timing_messages",
    "rocketmq_topic_create_execution_time",
    "rocketmq_topic_number",
    "role",
];

#[test]
fn java_metric_catalog_contains_every_java_metric_once() {
    let actual = JAVA_METRICS
        .iter()
        .map(|descriptor| descriptor.name)
        .collect::<HashSet<_>>();
    let expected = EXPECTED_JAVA_METRIC_NAMES.iter().copied().collect::<HashSet<_>>();

    assert_eq!(actual, expected);
    assert_eq!(JAVA_METRICS.len(), actual.len(), "duplicate metric names in catalog");
}

#[test]
fn combined_catalog_contains_every_semantic_metric_once() {
    let combined = JAVA_METRICS
        .iter()
        .chain(RUST_METRICS)
        .map(|descriptor| descriptor.name)
        .collect::<HashSet<_>>();

    assert_eq!(JAVA_METRICS.len(), 94);
    assert_eq!(RUST_METRICS.len(), 116);
    assert_eq!(combined.len(), 210, "duplicate metric names across catalogs");
}

#[test]
fn rust_catalog_covers_native_sources() {
    let sources = RUST_METRICS
        .iter()
        .map(|descriptor| descriptor.source)
        .collect::<HashSet<_>>();

    assert!(sources.contains(&MetricSource::Broker));
    assert!(sources.contains(&MetricSource::Client));
    assert!(sources.contains(&MetricSource::NameServer));
    assert!(sources.contains(&MetricSource::Remoting));
    assert!(sources.contains(&MetricSource::Store));
    assert!(sources.contains(&MetricSource::Proxy));
    assert!(sources.contains(&MetricSource::Controller));
    assert!(sources.contains(&MetricSource::Observability));
    assert!(sources.contains(&MetricSource::Mcp));
    assert!(sources.contains(&MetricSource::Runtime));
}

#[test]
fn namesrv_metrics_reject_unbounded_identity_labels() {
    const FORBIDDEN: &[&str] = &[
        labels::TOPIC,
        labels::ADDRESS,
        labels::NODE_ID,
        labels::GROUP,
        labels::CONSUMER_GROUP,
    ];
    for descriptor in RUST_METRICS
        .iter()
        .filter(|descriptor| descriptor.source == MetricSource::NameServer)
    {
        assert!(
            descriptor.labels.iter().all(|label| !FORBIDDEN.contains(label)),
            "{} contains an unbounded identity label: {:?}",
            descriptor.name,
            descriptor.labels
        );
    }
}

#[test]
fn log_filter_metrics_have_exact_catalog_contracts() {
    let expected = [
        (metrics::LOG_FILTER_RELOAD_TOTAL, MetricKind::Counter, "{reload}"),
        (metrics::LOG_FILTER_ACTIVE, MetricKind::Gauge, "1"),
        (metrics::LOG_FILTER_EXPIRY_TIMESTAMP_SECONDS, MetricKind::Gauge, "s"),
        (
            metrics::LOG_FILTER_AUDIT_FAILURE_TOTAL,
            MetricKind::Counter,
            "{failure}",
        ),
        (
            metrics::LOG_FILTER_AUTO_RESTORE_FAILURE_TOTAL,
            MetricKind::Counter,
            "{failure}",
        ),
        (
            metrics::LOG_FILTER_ROLLBACK_FAILURE_TOTAL,
            MetricKind::Counter,
            "{failure}",
        ),
    ];

    for (name, kind, unit) in expected {
        let descriptor = RUST_METRICS
            .iter()
            .find(|descriptor| descriptor.name == name)
            .expect("log filter metric descriptor");
        assert_eq!(descriptor.kind, kind);
        assert_eq!(descriptor.unit, unit);
        assert_eq!(descriptor.source, MetricSource::Observability);
        assert!(descriptor.labels.is_empty());
    }
}

#[test]
fn release_info_has_low_cardinality_catalog_contract() {
    let descriptor = RUST_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::RELEASE_INFO)
        .expect("release info metric descriptor");

    assert_eq!(descriptor.kind, MetricKind::Gauge);
    assert_eq!(descriptor.unit, "1");
    assert_eq!(
        descriptor.labels,
        &[labels::SERVICE, labels::RELEASE_COMMIT, labels::RELEASE_NONCE]
    );
    assert_eq!(descriptor.source, MetricSource::Observability);
}

#[test]
fn java_metric_catalog_has_required_label_sets() {
    let rpc_latency = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::RPC_LATENCY)
        .expect("rpc latency descriptor");
    assert_eq!(rpc_latency.kind, MetricKind::Histogram);
    assert_eq!(rpc_latency.labels, REMOTING_RPC_LABELS);

    let proxy_up = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::PROXY_UP)
        .expect("proxy up descriptor");
    assert_eq!(proxy_up.kind, MetricKind::ObservableGauge);
    assert_eq!(proxy_up.labels, PROXY_UP_LABELS);

    let controller_role = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::CONTROLLER_ROLE)
        .expect("controller role descriptor");
    assert_eq!(controller_role.kind, MetricKind::UpDownCounter);

    let producer_connections = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::PRODUCER_CONNECTIONS)
        .expect("producer connections descriptor");
    assert!(producer_connections.labels.contains(&labels::PROTOCOL_TYPE));

    let consumer_connections = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::CONSUMER_CONNECTIONS)
        .expect("consumer connections descriptor");
    assert!(consumer_connections.labels.contains(&labels::PROTOCOL_TYPE));

    let delay_message_latency = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::DELAY_MESSAGE_LATENCY)
        .expect("delay message latency descriptor");
    assert_eq!(delay_message_latency.unit, "seconds");
    assert_eq!(delay_message_latency.labels, STORE_TOPIC_LABELS);

    let transfer_batch = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_TRANSFER_BATCH_TOTAL)
        .expect("transfer batch descriptor");
    assert_eq!(transfer_batch.kind, MetricKind::Counter);
    assert_eq!(transfer_batch.unit, "{batch}");
    assert_eq!(transfer_batch.source, MetricSource::Store);
    assert!(transfer_batch.labels.is_empty());

    let transfer_engine = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_TRANSFER_ENGINE_TOTAL)
        .expect("transfer engine descriptor");
    assert_eq!(transfer_engine.kind, MetricKind::Counter);
    assert_eq!(transfer_engine.labels, &[labels::ENGINE]);

    let transfer_fallback = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_TRANSFER_FALLBACK_TOTAL)
        .expect("transfer fallback descriptor");
    assert_eq!(transfer_fallback.kind, MetricKind::Counter);
    assert_eq!(transfer_fallback.labels, &[labels::FROM, labels::TO, labels::REASON]);

    let linux_sendfile_bytes = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_SENDFILE_BYTES_TOTAL)
        .expect("linux sendfile bytes descriptor");
    assert_eq!(linux_sendfile_bytes.kind, MetricKind::Counter);
    assert_eq!(linux_sendfile_bytes.unit, "By");
    assert_eq!(linux_sendfile_bytes.source, MetricSource::Store);

    let ha_replication_lag = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_HA_REPLICATION_LAG_BYTES)
        .expect("ha replication lag descriptor");
    assert_eq!(ha_replication_lag.kind, MetricKind::ObservableGauge);
    assert_eq!(ha_replication_lag.unit, "By");
    assert_eq!(ha_replication_lag.source, MetricSource::Store);
    assert!(ha_replication_lag.labels.is_empty());

    let ha_ack_latency = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_HA_ACK_LATENCY_MILLIS)
        .expect("ha ack latency descriptor");
    assert_eq!(ha_ack_latency.kind, MetricKind::Histogram);
    assert_eq!(ha_ack_latency.unit, "ms");
    assert_eq!(ha_ack_latency.source, MetricSource::Store);
    assert!(ha_ack_latency.labels.is_empty());

    let linux_mlock_bytes = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_BYTES)
        .expect("linux mlock bytes descriptor");
    assert_eq!(linux_mlock_bytes.kind, MetricKind::ObservableGauge);
    assert_eq!(linux_mlock_bytes.unit, "By");
    assert_eq!(linux_mlock_bytes.source, MetricSource::Store);
    assert!(linux_mlock_bytes.labels.is_empty());

    let warmup_latency = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_PAGE_CACHE_WARMUP_MILLIS)
        .expect("linux page-cache warmup descriptor");
    assert_eq!(warmup_latency.kind, MetricKind::Histogram);
    assert_eq!(warmup_latency.unit, "ms");
    assert_eq!(warmup_latency.source, MetricSource::Store);
    assert!(warmup_latency.labels.is_empty());

    let lease_active = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_COMMITLOG_SEGMENT_LEASE_ACTIVE)
        .expect("commitlog segment lease descriptor");
    assert_eq!(lease_active.kind, MetricKind::ObservableGauge);
    assert_eq!(lease_active.unit, "{lease}");
    assert_eq!(lease_active.source, MetricSource::Store);
    assert!(lease_active.labels.is_empty());

    let mlock_attempt = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_ATTEMPT_TOTAL)
        .expect("linux mlock attempt descriptor");
    assert_eq!(mlock_attempt.kind, MetricKind::Counter);
    assert_eq!(mlock_attempt.unit, "{operation}");
    assert_eq!(mlock_attempt.labels, &[labels::CATEGORY]);
    assert_eq!(mlock_attempt.source, MetricSource::Store);

    let mlock_success = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_SUCCESS_TOTAL)
        .expect("linux mlock success descriptor");
    assert_eq!(mlock_success.kind, MetricKind::Counter);
    assert_eq!(mlock_success.labels, &[labels::CATEGORY]);

    let mlock_failure = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_FAILURE_TOTAL)
        .expect("linux mlock failure descriptor");
    assert_eq!(mlock_failure.kind, MetricKind::Counter);
    assert_eq!(mlock_failure.labels, &[labels::CATEGORY, labels::ERRNO]);

    let mlock_skipped = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MLOCK_SKIPPED_TOTAL)
        .expect("linux mlock skipped descriptor");
    assert_eq!(mlock_skipped.kind, MetricKind::Counter);
    assert_eq!(mlock_skipped.labels, &[labels::CATEGORY, labels::REASON]);

    let locked_bytes = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_LOCKED_BYTES)
        .expect("linux locked bytes descriptor");
    assert_eq!(locked_bytes.kind, MetricKind::ObservableGauge);
    assert_eq!(locked_bytes.unit, "By");
    assert_eq!(locked_bytes.labels, &[labels::CATEGORY]);

    let munlock_failure = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_MUNLOCK_FAILURE_TOTAL)
        .expect("linux munlock failure descriptor");
    assert_eq!(munlock_failure.kind, MetricKind::Counter);
    assert_eq!(munlock_failure.labels, &[labels::CATEGORY, labels::ERRNO]);

    let storage_degradation = JAVA_METRICS
        .iter()
        .find(|descriptor| descriptor.name == metrics::STORE_LINUX_STORAGE_DEGRADATION_TOTAL)
        .expect("linux storage degradation descriptor");
    assert_eq!(storage_degradation.kind, MetricKind::Counter);
    assert_eq!(storage_degradation.unit, "{operation}");
    assert_eq!(
        storage_degradation.labels,
        &[labels::OPERATION, labels::REASON, labels::ERRNO]
    );
}
