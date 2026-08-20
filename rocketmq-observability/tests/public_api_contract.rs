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

use rocketmq_observability::LogsOverrides;
use rocketmq_observability::MetricsExporter;
use rocketmq_observability::MetricsOverrides;
use rocketmq_observability::MetricsRuntimePolicy;
use rocketmq_observability::ObservabilityError;
use rocketmq_observability::ObservabilityOverrides;
use rocketmq_observability::OtlpOverrides;
use rocketmq_observability::PrometheusOverrides;
use rocketmq_observability::SamplingGate;
use rocketmq_observability::TelemetryBootstrapConfig;
use rocketmq_observability::TelemetryEnvironmentSpec;
use rocketmq_observability::TelemetryEnvironmentValues;
use rocketmq_observability::TelemetryResolution;
use rocketmq_observability::TracesOverrides;

#[test]
fn observability_implementation_modules_remain_private() {
    let source = include_str!("../src/lib.rs");
    for module in [
        "attributes",
        "config",
        "error",
        "exporter",
        "exporter_types",
        "init",
        "legacy_logging",
        "log_filter",
        "logging",
        "noop",
        "propagation",
        "resource",
        "sampling",
    ] {
        assert!(
            !source.contains(&format!("pub mod {module};")),
            "`rocketmq-observability` implementation module `{module}` must remain private"
        );
    }

    let _ = TelemetryBootstrapConfig::default();
    let _ = MetricsExporter::Disable;
    let _: Option<ObservabilityError> = None;
    let _: Option<SamplingGate> = None;
}

#[test]
fn file_and_environment_resolution_types_remain_public() {
    let overrides = ObservabilityOverrides {
        metrics: MetricsOverrides::default(),
        traces: TracesOverrides::default(),
        logs: LogsOverrides::default(),
        otlp: OtlpOverrides::default(),
        prometheus: PrometheusOverrides::default(),
        ..ObservabilityOverrides::default()
    };
    let environment = TelemetryEnvironmentValues::default();
    let spec = TelemetryEnvironmentSpec::default();

    let resolution: TelemetryResolution = rocketmq_observability::resolve_telemetry_values(
        "public-api-contract",
        TelemetryBootstrapConfig::default(),
        &overrides,
        &environment,
        spec,
    )
    .expect("default public resolution inputs should remain valid");
    rocketmq_observability::normalize_and_validate(&resolution.bootstrap.observability)
        .expect("the resolved public configuration should remain valid");

    let policy = MetricsRuntimePolicy::default();
    assert!(!policy.enabled);
    assert_eq!(policy.sample_ratio, 0.0);
    assert_eq!(policy.export_interval_millis, 0);
    assert_eq!(policy.cardinality_limit, 0);
}
