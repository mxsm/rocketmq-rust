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
fn enabled_traces_still_initialize_when_subscriber_install_is_blocked() {
    let _ = tracing_subscriber::fmt().with_writer(std::io::sink).try_init();

    let mut config = rocketmq_observability::ObservabilityConfig {
        enabled: true,
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    config.traces.enabled = true;
    config.traces.exporter = rocketmq_observability::config::TraceExporter::Disable;

    let guard = rocketmq_observability::init_observability(&config)
        .expect("current init path does not report subscriber install failure");

    assert!(
        guard.tracer_provider().is_some(),
        "the tracer provider initializes even though the tracing subscriber layer is blocked"
    );

    guard.shutdown().expect("trace provider shutdown should succeed");
}
