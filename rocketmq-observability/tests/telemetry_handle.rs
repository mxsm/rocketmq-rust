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

use rocketmq_observability::TelemetryHandle;
use rocketmq_observability::TelemetryState;
use rocketmq_observability::METRIC_LABEL_SENTINEL;

#[test]
fn dropping_provider_guard_closes_surviving_handle_clones() {
    let config = rocketmq_observability::ObservabilityConfig {
        enabled: true,
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    let guard = rocketmq_observability::init_observability(&config)
        .expect("provider guard without enabled signals should initialize");
    let handle = guard.handle();

    assert_eq!(handle.state(), TelemetryState::Active);
    drop(guard);

    assert_eq!(handle.state(), TelemetryState::Closed);
    assert!(!handle.is_active());
}

#[test]
fn noop_handle_is_cloneable_and_permanently_closed() {
    let handle = TelemetryHandle::noop();
    let clone = handle.clone();

    assert_eq!(handle.state(), TelemetryState::Closed);
    assert_eq!(clone.state(), TelemetryState::Closed);
    assert!(!handle.is_active());
    assert_eq!(handle.trace_policy(), clone.trace_policy());
    assert!(!handle.release_identity_registered());
}

#[test]
fn metric_label_policy_uses_config_and_keeps_runtime_budgets_isolated() {
    let mut first_config = rocketmq_observability::ObservabilityConfig {
        enabled: true,
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    first_config.metrics.cardinality_limit = 1;
    first_config.metrics.topic_label_enabled = true;
    first_config.metrics.consumer_group_label_enabled = false;

    let mut second_config = first_config.clone();
    second_config.metrics.consumer_group_label_enabled = true;

    let first_guard =
        rocketmq_observability::init_observability(&first_config).expect("first runtime should initialize");
    let second_guard =
        rocketmq_observability::init_observability(&second_config).expect("second runtime should initialize");
    let first = first_guard.handle().metric_label_policy();
    let first_clone = first_guard.handle().metric_label_policy();
    let second = second_guard.handle().metric_label_policy();

    assert_eq!(first.normalize_topic("topic-a"), "topic-a");
    assert_eq!(first_clone.normalize_topic("topic-b"), METRIC_LABEL_SENTINEL);
    assert_eq!(first.normalize_consumer_group("group-a"), METRIC_LABEL_SENTINEL);

    assert_eq!(second.normalize_topic("topic-b"), "topic-b");
    assert_eq!(second.normalize_consumer_group("group-a"), "group-a");
    assert_eq!(first.dropped_labels(), 2);
    assert_eq!(second.dropped_labels(), 0);
}

#[cfg(feature = "otel-metrics")]
mod metrics {
    use opentelemetry::metrics::Meter;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry::InstrumentationScope;
    use rocketmq_observability::init_observability;
    use rocketmq_observability::metrics::release_identity::ReleaseIdentityRegistrationStatus;
    use rocketmq_observability::metrics::release_identity::ValidatedReleaseIdentity;
    use rocketmq_observability::MetricsExporter;
    use rocketmq_observability::ObservabilityConfig;
    use rocketmq_observability::TelemetryState;
    use rocketmq_observability::BROKER_METER_SCOPE;
    use rocketmq_observability::STORE_METER_SCOPE;

    use super::TelemetryHandle;

    #[derive(Debug)]
    struct PanicOnMeterRead;

    impl MeterProvider for PanicOnMeterRead {
        fn meter_with_scope(&self, _scope: InstrumentationScope) -> Meter {
            panic!("no-op TelemetryHandle read the process-global meter provider");
        }
    }

    fn metrics_config() -> ObservabilityConfig {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = MetricsExporter::Disable;
        config
    }

    fn release_identity(service: &str, nonce: &str) -> ValidatedReleaseIdentity {
        ValidatedReleaseIdentity::try_new(service, "0123456789abcdef0123456789abcdef01234567", nonce)
            .expect("test release identity should be valid")
    }

    #[test]
    fn noop_handle_never_reads_global_meter_provider() {
        opentelemetry::global::set_meter_provider(PanicOnMeterRead);

        let handle = TelemetryHandle::noop();
        assert!(!handle.child(BROKER_METER_SCOPE).is_active());

        let namesrv = rocketmq_observability::metrics::namesrv::NameServerMetrics::from_handle(&handle);
        namesrv.record_route_request(std::time::Duration::from_millis(2));
        namesrv.record_broker_registration(1);

        let proxy = rocketmq_observability::metrics::proxy::ProxyMetrics::from_handle(&handle);
        proxy.record_grpc_requests_total(1);
        proxy.record_active_connections(1);

        let store = rocketmq_observability::metrics::store::StoreMetricsRecorder::from_handle(&handle);
        store.record_append_latency(1);
        store.record_flush_latency(1);

        let tiered = rocketmq_observability::metrics::tiered_store::TieredStoreMetricsRecorder::from_handle(&handle);
        tiered.record_provider_read("TopicA/0/commitlog/0", 1, true, 1);
        tiered.record_provider_write("TopicB/3/commitlog/9", 1, true, 1);
        tiered.record_messages_dispatch("TopicA", 0, "commitlog", 1);

        assert!(!store.is_enabled());
        assert!(!tiered.is_enabled());
    }

    #[test]
    fn active_handle_clones_share_lifecycle_gate() {
        let guard = init_observability(&metrics_config()).expect("metrics runtime should initialize");
        let handle = guard.handle();
        let clone = handle.clone();

        assert_eq!(handle.state(), TelemetryState::Active);
        assert!(handle.child(BROKER_METER_SCOPE).is_active());
        assert!(clone.child(STORE_METER_SCOPE).is_active());

        guard
            .shutdown()
            .into_result()
            .expect("metrics runtime should shut down");

        assert_eq!(handle.state(), TelemetryState::Closed);
        assert_eq!(clone.state(), TelemetryState::Closed);
        assert!(!handle.child(BROKER_METER_SCOPE).is_active());
        assert!(!clone.child(STORE_METER_SCOPE).is_active());
    }

    #[test]
    fn component_recorders_share_one_handle_label_budget() {
        let mut config = metrics_config();
        config.metrics.cardinality_limit = 1;
        let guard = init_observability(&config).expect("metrics runtime should initialize");
        let handle = guard.handle();
        let policy = handle.metric_label_policy();
        let store = rocketmq_observability::metrics::store::StoreMetricsRecorder::from_handle(&handle);
        let timer = rocketmq_observability::metrics::timer::TimerMetricsRecorder::from_handle(&handle);
        let tiered = rocketmq_observability::metrics::tiered_store::TieredStoreMetricsRecorder::from_handle(&handle);

        store.record_delay_message_latency(1, Some("topic-a"));
        timer.record_enqueue_total(Some("topic-b"));
        tiered.record_messages_dispatch("topic-c", 0, "commitlog", 1);
        tiered.record_messages_out("topic-a", "group-a", 1);
        tiered.record_get_message_fallback("topic-a", "group-b");

        assert_eq!(policy.dropped_labels(), 3);
    }

    #[test]
    fn two_runtimes_keep_meters_release_identity_and_shutdown_isolated() {
        let first_guard = init_observability(&metrics_config()).expect("first runtime should initialize");
        let second_guard = init_observability(&metrics_config()).expect("second runtime should initialize");
        let first = first_guard.handle();
        let second = second_guard.handle();
        let first_store = rocketmq_observability::metrics::store::StoreMetricsRecorder::from_handle(&first);
        let second_store = rocketmq_observability::metrics::store::StoreMetricsRecorder::from_handle(&second);
        let first_tiered =
            rocketmq_observability::metrics::tiered_store::TieredStoreMetricsRecorder::from_handle(&first);
        let second_tiered =
            rocketmq_observability::metrics::tiered_store::TieredStoreMetricsRecorder::from_handle(&second);

        assert!(first_store.is_enabled());
        assert!(second_store.is_enabled());
        assert!(first_tiered.is_enabled());
        assert!(second_tiered.is_enabled());

        assert_eq!(
            first
                .register_release_identity(release_identity(BROKER_METER_SCOPE, "first"))
                .expect("first identity should register"),
            ReleaseIdentityRegistrationStatus::Registered
        );
        assert_eq!(
            second
                .register_release_identity(release_identity(BROKER_METER_SCOPE, "second"))
                .expect("second identity should register"),
            ReleaseIdentityRegistrationStatus::Registered
        );
        assert!(first.release_identity_registered());
        assert!(second.release_identity_registered());

        first_guard
            .shutdown()
            .into_result()
            .expect("first runtime should shut down");

        assert_eq!(first.state(), TelemetryState::Closed);
        assert!(!first.release_identity_registered());
        assert!(!first_store.is_enabled());
        assert!(!first_tiered.is_enabled());
        assert_eq!(second.state(), TelemetryState::Active);
        assert!(second.release_identity_registered());
        assert!(second_store.is_enabled());
        assert!(second_tiered.is_enabled());

        second_guard
            .shutdown()
            .into_result()
            .expect("second runtime should shut down");
        assert_eq!(second.state(), TelemetryState::Closed);
    }

    #[test]
    fn release_identity_registration_is_idempotent_and_conflicts_fail_closed() {
        let guard = init_observability(&metrics_config()).expect("metrics runtime should initialize");
        let handle = guard.handle();
        let identity = release_identity(BROKER_METER_SCOPE, "rollout-01");

        assert_eq!(
            handle
                .register_release_identity(identity.clone())
                .expect("identity should register"),
            ReleaseIdentityRegistrationStatus::Registered
        );
        assert_eq!(
            handle
                .register_release_identity(identity)
                .expect("same identity should be idempotent"),
            ReleaseIdentityRegistrationStatus::AlreadyRegistered
        );
        assert!(handle
            .register_release_identity(release_identity(BROKER_METER_SCOPE, "rollout-02"))
            .is_err());

        guard
            .shutdown()
            .into_result()
            .expect("metrics runtime should shut down");
    }
}
