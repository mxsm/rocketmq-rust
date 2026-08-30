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
use rocketmq_admin_core::core::broker::BrokerDiagnostics;
use rocketmq_admin_core::core::broker::BrokerDiagnosticsCoverage;
use rocketmq_admin_core::core::broker::QueryBrokerDiagnosticsResult;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::HaStatusProjection;
use serde_json::Value;
use serde_json::json;

use super::common::SourceOutput;
use crate::ConnectorError;
use crate::ConnectorErrorCode;

pub(super) fn project(result: QueryBrokerDiagnosticsResult) -> Result<SourceOutput, ConnectorError> {
    validate_schema(&result.schema_version)?;
    let observed_at = millis_to_datetime(result.observed_at_millis).unwrap_or_else(Utc::now);
    let unsupported = result
        .brokers
        .iter()
        .filter(|broker| broker.coverage == BrokerDiagnosticsCoverage::Unsupported)
        .count();
    let partial_brokers = result
        .brokers
        .iter()
        .filter(|broker| broker.coverage == BrokerDiagnosticsCoverage::Partial)
        .count();
    let brokers = result.brokers.into_iter().map(project_broker).collect::<Vec<_>>();
    let mut output = SourceOutput::available(
        json!({
            "schema_version": "rocketmq.sre-broker-store-evidence.v1",
            "observed_at": observed_at,
            "summary": {
                "observed_brokers": brokers.len(),
                "unavailable_brokers": result.unavailable_brokers,
                "unsupported_brokers": unsupported,
                "partial_brokers": partial_brokers
            },
            "brokers": brokers
        }),
        observed_at,
    )
    .with_exposure(EvidenceExposure::AdminRpc);
    if result.partial || result.unavailable_brokers > 0 || unsupported > 0 || partial_brokers > 0 {
        output.partial = true;
        output.coverage = if brokers_are_all_unsupported(unsupported, output.content["brokers"].as_array()) {
            CoverageStatus::NotProductionVerified
        } else {
            CoverageStatus::Partial
        };
        if result.unavailable_brokers > 0 {
            output.warnings.push("broker_diagnostics_source_unavailable".to_owned());
        }
        if unsupported > 0 {
            output
                .warnings
                .push("broker_diagnostics_contract_unsupported".to_owned());
        }
        if partial_brokers > 0 {
            output.warnings.push("broker_diagnostics_fields_missing".to_owned());
        }
    }
    Ok(output)
}

fn validate_schema(schema_version: &str) -> Result<(), ConnectorError> {
    if schema_version == BROKER_DIAGNOSTICS_SCHEMA_VERSION {
        return Ok(());
    }
    Err(ConnectorError::capability(
        ConnectorErrorCode::UnsupportedSchemaMajor,
        "read-only broker diagnostics schema is unsupported",
    ))
}

fn project_broker(broker: BrokerDiagnostics) -> Value {
    let ha = HaStatusProjection {
        supported: broker.ha.supported,
        role: broker.ha.role,
        master_epoch: broker.ha.master_epoch,
        sync_state_set_epoch: broker.ha.sync_state_set_epoch,
        sync_state_set_size: broker.ha.sync_state_set_size,
        max_replica_lag_bytes: broker.ha.max_replica_lag_bytes,
        ack_policy: broker.ha.ack_policy,
        required_ack_count: broker.ha.required_ack_count,
        decision_code: broker.ha.decision_code,
    };
    json!({
        "broker_name": broker.broker_name,
        "broker_id": broker.broker_id,
        "observed_at": broker.observed_at_millis.and_then(millis_to_datetime),
        "coverage": broker.coverage,
        "readiness": broker.readiness,
        "config": broker.config,
        "store_health": broker.store_health,
        "ha": ha,
        "recovery": broker.recovery,
        "background_index_rebuild": broker.background_index_rebuild,
        "rocksdb": broker.rocksdb,
        "tiered": broker.tiered,
        "auth": broker.auth,
        "warnings": broker.warnings
    })
}

fn millis_to_datetime(value: u64) -> Option<DateTime<Utc>> {
    let seconds = i64::try_from(value / 1000).ok()?;
    let nanos = u32::try_from(value % 1000).ok()?.saturating_mul(1_000_000);
    DateTime::from_timestamp(seconds, nanos)
}

fn brokers_are_all_unsupported(unsupported: usize, brokers: Option<&Vec<Value>>) -> bool {
    brokers.is_some_and(|brokers| !brokers.is_empty() && brokers.len() == unsupported)
}

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::core::broker::HaDiagnostics;
    use rocketmq_admin_core::core::broker::RocksDbMaintenanceDiagnostics;
    use rocketmq_admin_core::core::broker::TieredDispatchDiagnostics;

    use super::*;

    #[test]
    fn unsupported_broker_is_explicit_and_contains_no_runtime_kv_table() {
        let output = project(QueryBrokerDiagnosticsResult {
            schema_version: BROKER_DIAGNOSTICS_SCHEMA_VERSION.to_owned(),
            observed_at_millis: 1_700_000_000_000,
            brokers: vec![BrokerDiagnostics {
                broker_name: "broker-a".to_owned(),
                broker_id: 0,
                observed_at_millis: None,
                coverage: BrokerDiagnosticsCoverage::Unsupported,
                readiness: None,
                config: None,
                store_health: None,
                ha: HaDiagnostics::default(),
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
                auth: rocketmq_admin_core::core::broker::AuthSecurityDiagnostics {
                    supported: false,
                    authentication_enabled: None,
                    authorization_enabled: None,
                    acl_file_watch_enabled: None,
                    acl_generation: None,
                    acl_reload_attempts: None,
                    acl_reload_successes: None,
                    acl_reload_failures: None,
                    acl_reload_skipped: None,
                    credential_rotation_supported: false,
                },
                warnings: vec!["broker_diagnostics_contract_unsupported".to_owned()],
            }],
            unavailable_brokers: 0,
            partial: true,
        })
        .expect("supported diagnostics schema");

        assert_eq!(output.coverage, CoverageStatus::NotProductionVerified);
        assert_eq!(output.exposure, EvidenceExposure::AdminRpc);
        assert_eq!(output.content["brokers"][0]["coverage"], "unsupported");
        assert!(output.content.get("table").is_none());
    }

    #[test]
    fn unknown_admin_diagnostics_schema_fails_closed() {
        let error = project(QueryBrokerDiagnosticsResult {
            schema_version: "rocketmq.admin-broker-diagnostics.v2".to_owned(),
            observed_at_millis: 1_700_000_000_000,
            brokers: Vec::new(),
            unavailable_brokers: 0,
            partial: false,
        })
        .expect_err("unknown major must fail closed");

        assert_eq!(error.code, ConnectorErrorCode::UnsupportedSchemaMajor);
    }

    #[test]
    fn ha_projection_exposes_only_bounded_non_sensitive_status() {
        let output = project(QueryBrokerDiagnosticsResult {
            schema_version: BROKER_DIAGNOSTICS_SCHEMA_VERSION.to_owned(),
            observed_at_millis: 1_700_000_000_000,
            brokers: vec![BrokerDiagnostics {
                broker_name: "broker-a".to_owned(),
                broker_id: 1,
                observed_at_millis: Some(1_700_000_000_000),
                coverage: BrokerDiagnosticsCoverage::Available,
                readiness: None,
                config: None,
                store_health: None,
                ha: HaDiagnostics {
                    supported: true,
                    role: Some("master".to_owned()),
                    master_epoch: Some(7),
                    sync_state_set_epoch: Some(4),
                    sync_state_set_size: Some(3),
                    max_replica_lag_bytes: Some(128),
                    ack_policy: Some("all_in_sync_set".to_owned()),
                    required_ack_count: None,
                    decision_code: Some("not_observed".to_owned()),
                },
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
                auth: rocketmq_admin_core::core::broker::AuthSecurityDiagnostics {
                    supported: false,
                    authentication_enabled: None,
                    authorization_enabled: None,
                    acl_file_watch_enabled: None,
                    acl_generation: None,
                    acl_reload_attempts: None,
                    acl_reload_successes: None,
                    acl_reload_failures: None,
                    acl_reload_skipped: None,
                    credential_rotation_supported: false,
                },
                warnings: Vec::new(),
            }],
            unavailable_brokers: 0,
            partial: false,
        })
        .expect("supported diagnostics schema");

        let ha = &output.content["brokers"][0]["ha"];
        assert_eq!(ha["master_epoch"], 7);
        assert_eq!(ha["sync_state_set_size"], 3);
        assert_eq!(ha["max_replica_lag_bytes"], 128);
        assert!(ha.get("address").is_none());
        assert!(ha.get("config").is_none());
    }
}
