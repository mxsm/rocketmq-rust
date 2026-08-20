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

use crate::metrics::release_identity::ProcessTelemetryConfig;
use crate::metrics::release_identity::ProcessTelemetryConfigError;
use crate::metrics::release_identity::ReleaseIdentityError;
use crate::metrics::release_identity::ValidatedReleaseIdentity;
use crate::MetricsExporter;
use crate::ObservabilityConfig;
use std::ffi::OsStr;

const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
const OTHER_COMMIT: &str = "89abcdef0123456789abcdef0123456789abcdef";
const NULL_COMMIT: &str = "0000000000000000000000000000000000000000";

fn real_embedded_build_commit() -> Option<&'static str> {
    option_env!("ROCKETMQ_BUILD_COMMIT").filter(|commit| {
        *commit != NULL_COMMIT
            && commit.len() == 40
            && commit
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    })
}

#[test]
fn validates_canonical_release_identity() {
    let identity =
        ValidatedReleaseIdentity::try_new("rocketmq-broker", COMMIT, "rollout-01").expect("canonical release identity");

    assert_eq!(identity.service(), "rocketmq-broker");
    assert_eq!(identity.commit(), COMMIT);
    assert_eq!(identity.nonce(), "rollout-01");
}

#[test]
fn rejects_noncanonical_commit_forms() {
    let uppercase = "0123456789ABCDEF0123456789ABCDEF01234567";
    let non_hex = "g123456789abcdef0123456789abcdef01234567";
    let short = "0123456789abcdef0123456789abcdef0123456";

    for commit in [uppercase, non_hex, short] {
        assert_eq!(
            ValidatedReleaseIdentity::try_new("rocketmq-broker", commit, "rollout-01"),
            Err(ReleaseIdentityError::InvalidCommit)
        );
    }
    assert_eq!(
        ValidatedReleaseIdentity::try_new("rocketmq-broker", "", "rollout-01"),
        Err(ReleaseIdentityError::InvalidCommit)
    );
}

#[test]
fn rejects_unbounded_or_unstable_service_and_nonce() {
    for service in ["", "-broker", "broker-", "Rocketmq-broker", "rocketmq_broker"] {
        assert_eq!(
            ValidatedReleaseIdentity::try_new(service, COMMIT, "rollout-01"),
            Err(ReleaseIdentityError::InvalidService)
        );
    }
    assert_eq!(
        ValidatedReleaseIdentity::try_new("s".repeat(64), COMMIT, "rollout-01"),
        Err(ReleaseIdentityError::InvalidService)
    );

    for nonce in ["", "-rollout", "rollout-", "Rollout-01", "rollout_01"] {
        assert_eq!(
            ValidatedReleaseIdentity::try_new("rocketmq-broker", COMMIT, nonce),
            Err(ReleaseIdentityError::InvalidNonce)
        );
    }
    assert_eq!(
        ValidatedReleaseIdentity::try_new("rocketmq-broker", COMMIT, "n".repeat(64)),
        Err(ReleaseIdentityError::InvalidNonce)
    );
}

#[test]
fn process_values_default_to_local_disabled_telemetry() {
    let process = ProcessTelemetryConfig::try_from_values("rocketmq-broker", None, None, None, None, None, None)
        .expect("local process telemetry defaults");
    let expected_commit = real_embedded_build_commit().unwrap_or(NULL_COMMIT);

    assert_eq!(process.release_identity().commit(), expected_commit);
    assert_eq!(process.release_identity().nonce(), "local");
    assert!(!process.metrics_enabled());
    assert_eq!(process.metrics_exporter(), MetricsExporter::Disable);
    assert_eq!(process.prometheus_host(), "127.0.0.1");
    assert_eq!(process.prometheus_port(), 5557);
    assert_eq!(process.prometheus_path(), "/metrics");
    assert_eq!(process.prometheus_listener_addr(), None);

    let mut observability = ObservabilityConfig::default();
    process.apply_to(&mut observability);
    assert!(!observability.enabled);
    assert!(!observability.metrics.enabled);
    assert_eq!(observability.metrics.exporter, MetricsExporter::Disable);
}

#[test]
fn real_embedded_build_commit_rejects_mismatched_runtime_identity() {
    let Some(build_commit) = real_embedded_build_commit() else {
        return;
    };
    let runtime_commit = if build_commit == COMMIT { OTHER_COMMIT } else { COMMIT };

    assert_eq!(
        ProcessTelemetryConfig::try_from_values(
            "rocketmq-broker",
            Some(runtime_commit),
            Some("rollout-01"),
            None,
            None,
            None,
            None,
        ),
        Err(ProcessTelemetryConfigError::ReleaseCommitMismatch)
    );
}

#[test]
fn explicit_null_process_commit_fails_closed() {
    assert_eq!(
        ProcessTelemetryConfig::try_from_values(
            "rocketmq-broker",
            Some(NULL_COMMIT),
            Some("rollout-01"),
            None,
            None,
            None,
            None,
        ),
        Err(ProcessTelemetryConfigError::NullReleaseCommit)
    );
}

#[test]
fn prometheus_values_enable_observability_and_apply_listener() {
    let process = ProcessTelemetryConfig::try_from_values(
        "rocketmq-controller",
        Some(COMMIT),
        Some("rollout-02"),
        Some("true"),
        Some("prometheus"),
        Some("0.0.0.0:9464"),
        Some("/internal/metrics"),
    )
    .expect("validated Prometheus process telemetry");
    let mut observability = ObservabilityConfig::default();

    process.apply_to(&mut observability);

    assert!(observability.enabled);
    assert!(observability.metrics.enabled);
    assert_eq!(observability.metrics.exporter, MetricsExporter::Prometheus);
    assert_eq!(observability.service_name, "rocketmq-controller");
    assert_eq!(observability.prometheus.host, "0.0.0.0");
    assert_eq!(observability.prometheus.port, 9464);
    assert_eq!(observability.prometheus.path, "/internal/metrics");
    assert_eq!(
        process.prometheus_listener_addr(),
        Some("0.0.0.0:9464".parse().expect("Prometheus listener address"))
    );
    assert_eq!(process.release_identity().commit(), COMMIT);
}

#[test]
fn absent_process_values_preserve_file_metrics_configuration() {
    let mut base = ObservabilityConfig::default();
    base.metrics.enabled = true;
    base.metrics.exporter = MetricsExporter::Prometheus;
    base.prometheus.host = "0.0.0.0".to_owned();
    base.prometheus.port = 9464;
    base.prometheus.path = "/rocketmq".to_owned();

    let process = ProcessTelemetryConfig::try_from_observability_and_values(
        "rocketmq-broker",
        &base,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("missing process values should preserve file metrics");

    assert!(process.metrics_enabled());
    assert_eq!(process.metrics_exporter(), MetricsExporter::Prometheus);
    assert_eq!(process.prometheus_host(), "0.0.0.0");
    assert_eq!(process.prometheus_port(), 9464);
    assert_eq!(process.prometheus_path(), "/rocketmq");
}

#[test]
fn explicit_metrics_disable_normalizes_the_final_exporter() {
    let mut base = ObservabilityConfig::default();
    base.metrics.enabled = true;
    base.metrics.exporter = MetricsExporter::Prometheus;

    let process = ProcessTelemetryConfig::try_from_observability_and_values(
        "rocketmq-broker",
        &base,
        None,
        None,
        Some(OsStr::new("false")),
        Some(OsStr::new("otlp_grpc")),
        None,
        None,
    )
    .expect("explicit metrics disable should normalize the exporter");

    assert!(!process.metrics_enabled());
    assert_eq!(process.metrics_exporter(), MetricsExporter::Disable);
}

#[test]
fn explicit_metrics_enable_requires_a_non_disabled_final_exporter() {
    assert!(matches!(
        ProcessTelemetryConfig::try_from_observability_and_values(
            "rocketmq-broker",
            &ObservabilityConfig::default(),
            None,
            None,
            Some(OsStr::new("true")),
            None,
            None,
            None,
        ),
        Err(ProcessTelemetryConfigError::InconsistentMetricsSelection)
    ));
}

#[test]
fn explicit_invalid_process_values_fail_closed() {
    let parse = |enabled, exporter, bind, path| {
        ProcessTelemetryConfig::try_from_values(
            "rocketmq-broker",
            Some(COMMIT),
            Some("rollout-01"),
            enabled,
            exporter,
            bind,
            path,
        )
    };

    assert_eq!(
        parse(Some("1"), Some("prometheus"), None, None),
        Err(ProcessTelemetryConfigError::InvalidMetricsEnabled)
    );
    assert_eq!(
        parse(Some("true"), Some("prom"), None, None),
        Err(ProcessTelemetryConfigError::InvalidMetricsExporter)
    );
    assert_eq!(
        parse(Some("true"), Some("disable"), None, None),
        Err(ProcessTelemetryConfigError::InconsistentMetricsSelection)
    );
    assert_eq!(
        parse(None, None, Some("localhost:5557"), None),
        Err(ProcessTelemetryConfigError::InvalidPrometheusBindAddress)
    );
    assert_eq!(
        parse(None, None, Some("127.0.0.1:0"), None),
        Err(ProcessTelemetryConfigError::InvalidPrometheusBindAddress)
    );
    assert_eq!(
        parse(None, None, None, Some("metrics")),
        Err(ProcessTelemetryConfigError::InvalidPrometheusPath)
    );
    assert_eq!(
        parse(None, None, None, Some("/metrics?token=secret")),
        Err(ProcessTelemetryConfigError::InvalidPrometheusPath)
    );
}

#[cfg(feature = "otel-metrics")]
mod metrics_enabled {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::metrics::release_identity::ReleaseIdentityRegistration;
    use crate::metrics::release_identity::ReleaseIdentityRegistrationStatus;
    use crate::semantic::metrics::RELEASE_INFO;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry::Value;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::data::AggregatedMetrics;
    use opentelemetry_sdk::metrics::data::MetricData;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::Temporality;

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct CapturingExporter {
        points: Arc<Mutex<Vec<CapturedPoint>>>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CapturedPoint {
        value: u64,
        attributes: BTreeMap<String, String>,
    }

    impl CapturingExporter {
        fn points(&self) -> Vec<CapturedPoint> {
            self.points.lock().expect("capture lock").clone()
        }
    }

    impl PushMetricExporter for CapturingExporter {
        async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
            let mut captured = Vec::new();
            for scope in metrics.scope_metrics() {
                for metric in scope.metrics().filter(|metric| metric.name() == RELEASE_INFO) {
                    let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
                        panic!("release info must be a u64 gauge")
                    };
                    for point in gauge.data_points() {
                        let attributes = point
                            .attributes()
                            .map(|attribute| {
                                let value = match &attribute.value {
                                    Value::String(value) => value.to_string(),
                                    other => panic!("release info attribute must be a string, got {other:?}"),
                                };
                                (attribute.key.as_str().to_owned(), value)
                            })
                            .collect();
                        captured.push(CapturedPoint {
                            value: point.value(),
                            attributes,
                        });
                    }
                }
            }
            *self.points.lock().expect("capture lock") = captured;
            Ok(())
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }

    fn test_provider(exporter: CapturingExporter) -> SdkMeterProvider {
        let reader = PeriodicReader::builder(exporter)
            .with_interval(Duration::from_secs(3_600))
            .build();
        SdkMeterProvider::builder().with_reader(reader).build()
    }

    #[test]
    fn emits_exact_release_info_attributes_once() {
        let exporter = CapturingExporter::default();
        let provider = test_provider(exporter.clone());
        let meter = provider.meter("release-identity-test");
        let registration = ReleaseIdentityRegistration::new(
            ValidatedReleaseIdentity::try_new("rocketmq-broker", COMMIT, "rollout-01").expect("valid release identity"),
        );

        assert_eq!(
            registration.register(&meter),
            ReleaseIdentityRegistrationStatus::Registered
        );
        assert_eq!(
            registration.clone().register(&meter),
            ReleaseIdentityRegistrationStatus::AlreadyRegistered
        );
        assert!(registration.is_registered());
        provider.force_flush().expect("collect release identity");

        assert_eq!(
            exporter.points(),
            vec![CapturedPoint {
                value: 1,
                attributes: BTreeMap::from([
                    ("release_commit".to_owned(), COMMIT.to_owned()),
                    ("release_nonce".to_owned(), "rollout-01".to_owned()),
                    ("service".to_owned(), "rocketmq-broker".to_owned()),
                ]),
            }]
        );
        provider.shutdown().expect("shutdown test provider");
    }

    #[test]
    fn separately_constructed_identities_have_isolated_registration_state() {
        let exporter = CapturingExporter::default();
        let provider = test_provider(exporter.clone());
        let meter = provider.meter("release-identity-isolation-test");
        let broker = ReleaseIdentityRegistration::new(
            ValidatedReleaseIdentity::try_new("rocketmq-broker", COMMIT, "broker-01").expect("broker release identity"),
        );
        let controller = ReleaseIdentityRegistration::new(
            ValidatedReleaseIdentity::try_new("rocketmq-controller", COMMIT, "controller-01")
                .expect("controller release identity"),
        );

        assert_eq!(broker.register(&meter), ReleaseIdentityRegistrationStatus::Registered);
        assert!(broker.is_registered());
        assert!(!controller.is_registered());
        assert_eq!(
            controller.register(&meter),
            ReleaseIdentityRegistrationStatus::Registered
        );
        provider.force_flush().expect("collect release identities");

        let points = exporter.points();
        assert_eq!(points.len(), 2);
        assert!(points.iter().any(|point| {
            point.attributes.get("service").map(String::as_str) == Some("rocketmq-broker")
                && point.attributes.get("release_nonce").map(String::as_str) == Some("broker-01")
        }));
        assert!(points.iter().any(|point| {
            point.attributes.get("service").map(String::as_str) == Some("rocketmq-controller")
                && point.attributes.get("release_nonce").map(String::as_str) == Some("controller-01")
        }));
        provider.shutdown().expect("shutdown test provider");
    }
}
