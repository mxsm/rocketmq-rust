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
use rocketmq_admin_core::core::broker::BROKER_DIAGNOSTICS_SCHEMA_VERSION;
use rocketmq_admin_core::core::broker::BrokerDiagnosticsCoverage;
use rocketmq_admin_core::core::broker::QueryBrokerDiagnosticsResult;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceExposure;
use serde_json::Value;
use serde_json::json;

use super::common::SourceOutput;
use crate::ConnectorError;
use crate::ConnectorErrorCode;

pub(super) fn project(result: QueryBrokerDiagnosticsResult) -> Result<SourceOutput, ConnectorError> {
    validate_schema(&result.schema_version)?;
    let observed_at = millis_to_datetime(result.observed_at_millis).unwrap_or_else(Utc::now);
    let source_partial = result.partial
        || result.unavailable_brokers > 0
        || result
            .brokers
            .iter()
            .any(|broker| broker.coverage != BrokerDiagnosticsCoverage::Available);
    let mut unsupported = 0usize;
    let brokers = result
        .brokers
        .into_iter()
        .map(|broker| {
            if !broker.auth.supported {
                unsupported = unsupported.saturating_add(1);
            }
            json!({
                "broker_name": broker.broker_name,
                "broker_id": broker.broker_id,
                "coverage": if broker.auth.supported { "available" } else { "not_production_verified" },
                "authentication_enabled": broker.auth.authentication_enabled,
                "authorization_enabled": broker.auth.authorization_enabled,
                "acl_file_watch_enabled": broker.auth.acl_file_watch_enabled,
                "acl_generation": broker.auth.acl_generation,
                "reload": {
                    "attempts": broker.auth.acl_reload_attempts,
                    "successes": broker.auth.acl_reload_successes,
                    "failures": broker.auth.acl_reload_failures,
                    "skipped": broker.auth.acl_reload_skipped
                },
                "credential_rotation": {
                    "coverage": if broker.auth.credential_rotation_supported {
                        "available"
                    } else {
                        "not_production_verified"
                    },
                    "reason_code": if broker.auth.credential_rotation_supported {
                        Value::Null
                    } else {
                        Value::String("credential_rotation_diagnostics_not_exposed".to_owned())
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    let total = brokers.len();
    let mut output = SourceOutput::available(
        json!({
            "schema_version": "rocketmq.auth-security-diagnostics.v1",
            "observed_at": observed_at,
            "brokers": brokers
        }),
        observed_at,
    )
    .with_exposure(EvidenceExposure::AdminRpc);
    if source_partial || unsupported > 0 {
        output.partial = true;
        output.coverage = if total > 0 && unsupported == total {
            CoverageStatus::NotProductionVerified
        } else {
            CoverageStatus::Partial
        };
    }
    if unsupported > 0 {
        output.warnings.push("auth_diagnostics_not_exposed".to_owned());
    }
    if result.unavailable_brokers > 0 {
        output.warnings.push("auth_diagnostics_source_unavailable".to_owned());
    }
    Ok(output)
}

fn validate_schema(schema_version: &str) -> Result<(), ConnectorError> {
    if schema_version == BROKER_DIAGNOSTICS_SCHEMA_VERSION {
        return Ok(());
    }
    Err(ConnectorError::capability(
        ConnectorErrorCode::UnsupportedSchemaMajor,
        "read-only auth diagnostics schema is unsupported",
    ))
}

fn millis_to_datetime(value: u64) -> Option<DateTime<Utc>> {
    let seconds = i64::try_from(value / 1000).ok()?;
    let nanos = u32::try_from(value % 1000).ok()?.saturating_mul(1_000_000);
    DateTime::from_timestamp(seconds, nanos)
}

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::core::broker::AuthSecurityDiagnostics;
    use rocketmq_admin_core::core::broker::BrokerDiagnostics;
    use rocketmq_admin_core::core::broker::RocksDbMaintenanceDiagnostics;
    use rocketmq_admin_core::core::broker::TieredDispatchDiagnostics;

    use super::*;

    #[test]
    fn auth_projection_reports_rotation_as_explicitly_unsupported() {
        let output = project(QueryBrokerDiagnosticsResult {
            schema_version: BROKER_DIAGNOSTICS_SCHEMA_VERSION.to_owned(),
            observed_at_millis: 1_700_000_000_000,
            brokers: vec![BrokerDiagnostics {
                broker_name: "broker-a".to_owned(),
                broker_id: 0,
                observed_at_millis: None,
                coverage: BrokerDiagnosticsCoverage::Available,
                readiness: None,
                config: None,
                store_health: None,
                recovery: None,
                background_index_rebuild: None,
                rocksdb: RocksDbMaintenanceDiagnostics {
                    supported: false,
                    maintenance_running: None,
                    message_maintenance_running: None,
                },
                tiered: TieredDispatchDiagnostics {
                    configured: false,
                    dispatch_ready: None,
                    minimum_pinned_wal_segment: None,
                },
                auth: AuthSecurityDiagnostics {
                    supported: true,
                    authentication_enabled: Some(true),
                    authorization_enabled: Some(true),
                    acl_file_watch_enabled: Some(true),
                    acl_generation: Some(3),
                    acl_reload_attempts: Some(2),
                    acl_reload_successes: Some(2),
                    acl_reload_failures: Some(0),
                    acl_reload_skipped: Some(0),
                    credential_rotation_supported: false,
                },
                warnings: Vec::new(),
            }],
            unavailable_brokers: 0,
            partial: false,
        })
        .expect("supported diagnostics schema");
        assert_eq!(output.content["brokers"][0]["acl_generation"], 3);
        assert_eq!(
            output.content["brokers"][0]["credential_rotation"]["coverage"],
            "not_production_verified"
        );
    }
}
