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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceExposure;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::prometheus::PrometheusSource;
use crate::ConnectorError;

const METRICS: [(&str, &str); 4] = [
    ("requests", "range/rocketmq_transport_requests_total"),
    ("request_latency", "range/rocketmq_transport_request_latency"),
    ("network_bytes", "range/rocketmq_transport_network_bytes"),
    ("rpc_latency", "range/rocketmq_rpc_latency"),
];

#[allow(
    clippy::too_many_arguments,
    reason = "component evidence collection keeps source bounds explicit"
)]
pub(super) async fn query(
    prometheus: &PrometheusSource,
    cluster: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    max_rows: usize,
    max_bytes: usize,
    deadline: DateTime<Utc>,
    cancel: &CancelSignal,
) -> Result<SourceOutput, ConnectorError> {
    let mut signals = Map::new();
    let mut missing = 0usize;
    for (name, resource) in METRICS {
        match prometheus
            .query(
                cluster,
                resource,
                start,
                end,
                max_rows.saturating_div(METRICS.len()).max(1),
                max_bytes.saturating_div(METRICS.len()).max(1024),
                deadline,
                cancel,
            )
            .await
        {
            Ok(output) => {
                signals.insert(name.to_owned(), output.content);
            }
            Err(error) if error.code == crate::ConnectorErrorCode::SourceUnavailable => {
                missing = missing.saturating_add(1);
                signals.insert(
                    name.to_owned(),
                    json!({"status": "missing", "reason_code": "source_unavailable"}),
                );
            }
            Err(error) => return Err(error),
        }
    }
    let observed_at = Utc::now();
    let mut output = SourceOutput::available(
        json!({
            "schema_version": "rocketmq.remoting-diagnostics.v1",
            "observed_at": observed_at,
            "telemetry": signals,
            "admission": unsupported("remoting_admission_process_diagnostics_not_exposed"),
            "pending_requests": unsupported("remoting_pending_request_diagnostics_not_exposed"),
            "connection_pool": unsupported("remoting_connection_pool_diagnostics_not_exposed"),
            "nameserver_latency": {
                "coverage": if missing < METRICS.len() { "partial" } else { "missing" },
                "metric": "rpc_latency"
            },
            "nameserver_circuit": unsupported("nameserver_circuit_diagnostics_not_exposed")
        }),
        observed_at,
    )
    .with_exposure(EvidenceExposure::PrometheusApi);
    output.partial = true;
    output.coverage = if missing == METRICS.len() {
        CoverageStatus::Missing
    } else {
        CoverageStatus::Partial
    };
    output
        .warnings
        .push("remoting_process_diagnostics_not_production_queryable".to_owned());
    if missing > 0 {
        output.warnings.push("remoting_metric_source_partial".to_owned());
    }
    Ok(output)
}

fn unsupported(reason_code: &str) -> Value {
    json!({
        "coverage": "not_production_verified",
        "reason_code": reason_code
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remoting_process_only_fields_fail_closed() {
        assert_eq!(
            unsupported("remoting_pending_request_diagnostics_not_exposed")["coverage"],
            "not_production_verified"
        );
    }
}
