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

use rocketmq_observability::MetricsExporter;
use rocketmq_observability::ObservabilityError;
use rocketmq_observability::SamplingGate;
use rocketmq_observability::TelemetryBootstrapConfig;

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
