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

fn main() -> Result<(), rocketmq_observability::ObservabilityError> {
    let config = rocketmq_observability::TelemetryBootstrapConfig::default();
    let guard = rocketmq_observability::install_global(&config)?;

    tracing::info!(
        service = "logging-bootstrap-example",
        subscriber_installed = guard.subscriber_install_status().installed,
        file_log_enabled = guard.logging_guard().file_sink_count() > 0,
        "logging bootstrap initialized"
    );

    guard.shutdown().into_result().map(|_| ())
}
