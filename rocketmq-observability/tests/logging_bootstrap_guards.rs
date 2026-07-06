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

#[cfg(feature = "otel-traces")]
#[test]
fn best_effort_traces_report_blocked_subscriber_install_status() {
    install_blocking_subscriber();

    let config = traces_config(rocketmq_observability::SubscriberInstallPolicy::BestEffort);

    let guard = rocketmq_observability::init_observability(&config)
        .expect("best-effort mode should keep existing compatibility behavior");

    assert!(
        guard.tracer_provider().is_some(),
        "the tracer provider initializes even though the tracing subscriber layer is blocked"
    );
    assert_eq!(
        guard.subscriber_install_status(),
        rocketmq_observability::SubscriberInstallStatus {
            attempted: true,
            installed: false,
        }
    );

    guard.shutdown().expect("trace provider shutdown should succeed");
}

#[cfg(feature = "otel-traces")]
#[test]
fn required_traces_report_subscriber_install_failure() {
    install_blocking_subscriber();

    let config = traces_config(rocketmq_observability::SubscriberInstallPolicy::Required);

    let error = rocketmq_observability::init_observability(&config)
        .expect_err("required mode should fail when the subscriber layer cannot be installed");

    assert!(matches!(
        error,
        rocketmq_observability::ObservabilityError::SubscriberInstallFailed {
            attempted: true,
            installed: false
        }
    ));
}

#[cfg(feature = "otel-traces")]
fn traces_config(
    subscriber_install_policy: rocketmq_observability::SubscriberInstallPolicy,
) -> rocketmq_observability::ObservabilityConfig {
    let mut config = rocketmq_observability::ObservabilityConfig {
        enabled: true,
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    config.traces.enabled = true;
    config.traces.exporter = rocketmq_observability::config::TraceExporter::Disable;
    config.subscriber_install_policy = subscriber_install_policy;
    config
}

#[cfg(feature = "otel-traces")]
fn install_blocking_subscriber() {
    let _ = tracing_subscriber::fmt().with_writer(std::io::sink).try_init();
}
