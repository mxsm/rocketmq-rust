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

use rocketmq_error::ObservabilityError;
use rocketmq_error::RocketMQError;

#[test]
fn observability_error_preserves_existing_variants() {
    let error = ObservabilityError::invalid_log_filter("rocketmq_store==debug", "invalid directive");

    assert!(matches!(
        error,
        ObservabilityError::InvalidLogFilter { filter, error }
            if filter == "rocketmq_store==debug" && error == "invalid directive"
    ));
}

#[test]
fn observability_subscriber_install_failure_uses_primitive_status() {
    let error = ObservabilityError::subscriber_install_failed(true, false);

    assert!(matches!(
        error,
        ObservabilityError::SubscriberInstallFailed {
            attempted: true,
            installed: false
        }
    ));
}

#[test]
fn observability_error_converts_to_rocketmq_error() {
    let error = RocketMQError::from(ObservabilityError::metrics_init("exporter failed"));

    assert!(matches!(error, RocketMQError::Observability(_)));
}
