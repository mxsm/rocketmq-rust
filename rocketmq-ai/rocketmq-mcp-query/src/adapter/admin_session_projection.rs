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

pub(super) fn map_broker_diagnostics(
    row: &rocketmq_admin_core::core::broker::BrokerDiagnostics,
) -> crate::tools::broker_tools::BrokerDiagnosticsRow {
    use crate::tools::broker_tools as output;
    use rocketmq_admin_core::core::broker as admin;

    let (broker_role, broker_role_sanitized) = row
        .config
        .as_ref()
        .map(|value| project_broker_role(&value.broker_role))
        .unzip();
    let (store_type, store_type_sanitized) = row
        .config
        .as_ref()
        .map(|value| project_store_type(&value.store_type))
        .unzip();
    let (ha_role, ha_role_sanitized) = project_ha_role(row.ha.role.as_deref());
    let (ack_policy, ack_policy_sanitized) = project_ha_ack_policy(row.ha.ack_policy.as_deref());
    let (decision_code, decision_code_sanitized) = project_ha_decision_code(row.ha.decision_code.as_deref());
    let (background_state, background_state_sanitized) = row
        .background_index_rebuild
        .as_ref()
        .map(|value| project_background_index_rebuild_state(&value.state))
        .unzip();
    let mut warnings = row
        .warnings
        .iter()
        .filter(|warning| {
            matches!(
                warning.as_str(),
                "broker_diagnostics_fields_missing" | "broker_diagnostics_contract_unsupported"
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    if broker_role_sanitized.unwrap_or(false)
        || store_type_sanitized.unwrap_or(false)
        || ha_role_sanitized
        || ack_policy_sanitized
        || decision_code_sanitized
        || background_state_sanitized.unwrap_or(false)
        || warnings.len() != row.warnings.len()
    {
        warnings.push("broker_diagnostics_value_sanitized".to_string());
    }
    warnings.sort();
    warnings.dedup();

    output::BrokerDiagnosticsRow {
        broker_name: row.broker_name.clone(),
        broker_id: row.broker_id,
        observed_at_millis: row.observed_at_millis,
        coverage: match row.coverage {
            admin::BrokerDiagnosticsCoverage::Available => output::BrokerDiagnosticsCoverage::Available,
            admin::BrokerDiagnosticsCoverage::Partial => output::BrokerDiagnosticsCoverage::Partial,
            admin::BrokerDiagnosticsCoverage::Unsupported => output::BrokerDiagnosticsCoverage::Unsupported,
        },
        readiness: row.readiness.as_ref().map(|value| output::BrokerReadinessDiagnostics {
            ready: value.ready,
            active: value.active,
            shutdown: value.shutdown,
            registration_accepting: value.registration_accepting,
            registration_configured: value.registration_configured,
        }),
        config: row.config.as_ref().map(|value| output::BrokerRuntimeConfigSummary {
            generation: value.generation,
            broker_role: broker_role.unwrap_or(output::BrokerRole::Unknown),
            store_type: store_type.unwrap_or(output::BrokerStoreType::Unknown),
            timer_wheel_enabled: value.timer_wheel_enabled,
            transient_store_pool_enabled: value.transient_store_pool_enabled,
            tiered_store_configured: value.tiered_store_configured,
        }),
        store_health: row.store_health.as_ref().map(|value| output::StoreHealthDiagnostics {
            writeable: value.writeable,
            last_flush_error: value.last_flush_error,
            os_page_cache_busy: value.os_page_cache_busy,
            transient_store_pool_deficient: value.transient_store_pool_deficient,
            dispatch_behind_bytes: value.dispatch_behind_bytes,
            shutdown: value.shutdown,
            ha_pending_request_count: value.ha_pending_request_count,
            ha_pending_oldest_wait_millis: value.ha_pending_oldest_wait_millis,
            sync_flush: output::SyncFlushDiagnostics {
                queue_depth: value.sync_flush.queue_depth,
                timeout_total: value.sync_flush.timeout_total,
                oldest_wait_millis: value.sync_flush.oldest_wait_millis,
            },
        }),
        ha: output::HaDiagnostics {
            supported: row.ha.supported,
            role: ha_role,
            master_epoch: row.ha.master_epoch,
            sync_state_set_epoch: row.ha.sync_state_set_epoch,
            sync_state_set_size: row.ha.sync_state_set_size,
            max_replica_lag_bytes: row.ha.max_replica_lag_bytes,
            ack_policy,
            required_ack_count: row.ha.required_ack_count,
            decision_code,
        },
        recovery: row.recovery.as_ref().map(|value| output::RecoveryDiagnostics {
            available: value.available,
            total_duration_millis: value.total_duration_millis,
            phase_count: value.phase_count,
            failed_phase_count: value.failed_phase_count,
            fallback_phase_count: value.fallback_phase_count,
            fallback_reason_present: value.fallback_reason_present,
            scanned_bytes: value.scanned_bytes,
            recovered_messages: value.recovered_messages,
            invalid_messages: value.invalid_messages,
            truncated_files: value.truncated_files,
            index_files_removed: value.index_files_removed,
            index_files_rebuilt: value.index_files_rebuilt,
        }),
        background_index_rebuild: row.background_index_rebuild.as_ref().map(|value| {
            output::BackgroundIndexRebuildDiagnostics {
                state: background_state.unwrap_or(output::BackgroundIndexRebuildState::Unknown),
                effective_enabled: value.effective_enabled,
                gray_mode: value.gray_mode,
                current_safe_offset: value.current_safe_offset,
                target_offset: value.target_offset,
                backlog_bytes: value.backlog_bytes,
                rebuilt_bytes: value.rebuilt_bytes,
                rebuilt_messages: value.rebuilt_messages,
                failure_count: value.failure_count,
                bytes_per_second: value.bytes_per_second,
            }
        }),
        rocksdb: output::RocksDbMaintenanceDiagnostics {
            supported: row.rocksdb.supported,
            maintenance_running: row.rocksdb.maintenance_running,
            message_maintenance_running: row.rocksdb.message_maintenance_running,
        },
        tiered: output::TieredDispatchDiagnostics {
            configured: row.tiered.configured,
            dispatch_ready: row.tiered.dispatch_ready,
            minimum_pinned_wal_segment: row.tiered.minimum_pinned_wal_segment,
        },
        auth: output::AuthSecurityDiagnostics {
            supported: row.auth.supported,
            authentication_enabled: row.auth.authentication_enabled,
            authorization_enabled: row.auth.authorization_enabled,
            acl_file_watch_enabled: row.auth.acl_file_watch_enabled,
            acl_generation: row.auth.acl_generation,
            acl_reload_attempts: row.auth.acl_reload_attempts,
            acl_reload_successes: row.auth.acl_reload_successes,
            acl_reload_failures: row.auth.acl_reload_failures,
            acl_reload_skipped: row.auth.acl_reload_skipped,
            credential_rotation_supported: row.auth.credential_rotation_supported,
        },
        warnings,
    }
}

pub(super) fn bounded_proxy_operation_id(operation_id: Option<String>) -> (Option<String>, Vec<String>) {
    let (operation_id, sanitized) = safe_operation_id(operation_id);
    let warnings = sanitized
        .then_some("proxy_operation_id_sanitized".to_string())
        .into_iter()
        .collect();
    (operation_id, warnings)
}

pub(super) fn safe_operation_id(operation_id: Option<String>) -> (Option<String>, bool) {
    match operation_id {
        Some(value) if safe_runtime_token(&value, 128) => (Some(value), false),
        Some(_) => (None, true),
        None => (None, false),
    }
}

fn project_broker_role(value: &str) -> (crate::tools::broker_tools::BrokerRole, bool) {
    use crate::tools::broker_tools::BrokerRole;

    match value {
        "ASYNC_MASTER" => (BrokerRole::AsyncMaster, false),
        "SYNC_MASTER" => (BrokerRole::SyncMaster, false),
        "SLAVE" => (BrokerRole::Slave, false),
        _ => (BrokerRole::Unknown, true),
    }
}

fn project_store_type(value: &str) -> (crate::tools::broker_tools::BrokerStoreType, bool) {
    use crate::tools::broker_tools::BrokerStoreType;

    match value {
        "LocalFile" => (BrokerStoreType::LocalFile, false),
        "RocksDB" => (BrokerStoreType::RocksDb, false),
        _ => (BrokerStoreType::Unknown, true),
    }
}

fn project_ha_role(value: Option<&str>) -> (Option<crate::tools::broker_tools::HaRole>, bool) {
    use crate::tools::broker_tools::HaRole;

    match value {
        Some("master") => (Some(HaRole::Master), false),
        Some("replica") => (Some(HaRole::Replica), false),
        Some(_) => (None, true),
        None => (None, false),
    }
}

fn project_ha_ack_policy(value: Option<&str>) -> (Option<crate::tools::broker_tools::HaAckPolicy>, bool) {
    use crate::tools::broker_tools::HaAckPolicy;

    match value {
        Some("all_in_sync_set") => (Some(HaAckPolicy::AllInSyncSet), false),
        Some("local_durable") => (Some(HaAckPolicy::LocalDurable), false),
        Some("replica_count") => (Some(HaAckPolicy::ReplicaCount), false),
        Some(_) => (None, true),
        None => (None, false),
    }
}

fn project_ha_decision_code(value: Option<&str>) -> (Option<crate::tools::broker_tools::HaDecisionCode>, bool) {
    use crate::tools::broker_tools::HaDecisionCode;

    match value {
        Some("waiting_for_replica_progress") => (Some(HaDecisionCode::WaitingForReplicaProgress), false),
        Some("not_observed") => (Some(HaDecisionCode::NotObserved), false),
        Some(_) => (None, true),
        None => (None, false),
    }
}

fn project_background_index_rebuild_state(
    value: &str,
) -> (crate::tools::broker_tools::BackgroundIndexRebuildState, bool) {
    use crate::tools::broker_tools::BackgroundIndexRebuildState;

    match value {
        "idle" => (BackgroundIndexRebuildState::Idle, false),
        "running" => (BackgroundIndexRebuildState::Running, false),
        "paused" => (BackgroundIndexRebuildState::Paused, false),
        "completed" => (BackgroundIndexRebuildState::Completed, false),
        "retrying" => (BackgroundIndexRebuildState::Retrying, false),
        "failed" => (BackgroundIndexRebuildState::Failed, false),
        "shutdown" => (BackgroundIndexRebuildState::Shutdown, false),
        _ => (BackgroundIndexRebuildState::Unknown, true),
    }
}

fn safe_runtime_token(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::broker_tools as output;
    use rocketmq_admin_core::core::broker as admin;

    #[test]
    fn mcp_projection_drops_operation_ids_that_can_encode_topology_or_secrets() {
        assert_eq!(
            safe_operation_id(Some("operation-123".to_string())),
            (Some("operation-123".to_string()), false)
        );
        assert_eq!(safe_operation_id(Some("incident/123.op".to_string())), (None, true));
        assert_eq!(
            bounded_proxy_operation_id(Some("https://private-proxy.internal/token".to_string())),
            (None, vec!["proxy_operation_id_sanitized".to_string()])
        );
    }

    #[test]
    fn mcp_projection_replaces_non_allowlisted_diagnostic_tokens() {
        assert_eq!(
            project_broker_role("ASYNC_MASTER"),
            (crate::tools::broker_tools::BrokerRole::AsyncMaster, false)
        );
        assert_eq!(
            project_broker_role("supersecret123"),
            (crate::tools::broker_tools::BrokerRole::Unknown, true)
        );
    }

    #[test]
    fn diagnostic_projection_downgrades_unknown_backend_classifications() {
        let row = admin::BrokerDiagnostics {
            broker_name: "broker-a".to_string(),
            broker_id: 0,
            observed_at_millis: Some(42),
            coverage: admin::BrokerDiagnosticsCoverage::Available,
            readiness: None,
            config: Some(admin::BrokerConfigSummary {
                generation: 7,
                broker_role: "private-role".to_string(),
                store_type: "private-store".to_string(),
                timer_wheel_enabled: false,
                transient_store_pool_enabled: false,
                tiered_store_configured: false,
            }),
            store_health: None,
            ha: admin::HaDiagnostics {
                supported: true,
                role: Some("private-role".to_string()),
                ack_policy: Some("private-policy".to_string()),
                decision_code: Some("private-decision".to_string()),
                ..Default::default()
            },
            recovery: None,
            background_index_rebuild: Some(admin::BackgroundIndexRebuildDiagnostics {
                state: "private-state".to_string(),
                effective_enabled: false,
                gray_mode: false,
                current_safe_offset: 0,
                target_offset: 0,
                backlog_bytes: 0,
                rebuilt_bytes: 0,
                rebuilt_messages: 0,
                failure_count: 0,
                bytes_per_second: 0,
            }),
            rocksdb: admin::RocksDbMaintenanceDiagnostics {
                supported: false,
                maintenance_running: None,
                message_maintenance_running: None,
            },
            tiered: admin::TieredDispatchDiagnostics {
                configured: false,
                dispatch_ready: None,
                minimum_pinned_wal_segment: None,
            },
            auth: admin::AuthSecurityDiagnostics {
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
        };

        let projected = map_broker_diagnostics(&row);
        let config = projected
            .config
            .as_ref()
            .expect("config projection should remain present");
        assert_eq!(config.broker_role, output::BrokerRole::Unknown);
        assert_eq!(config.store_type, output::BrokerStoreType::Unknown);
        assert_eq!(projected.ha.role, None);
        assert_eq!(projected.ha.ack_policy, None);
        assert_eq!(projected.ha.decision_code, None);
        assert_eq!(
            projected
                .background_index_rebuild
                .as_ref()
                .expect("background rebuild projection should remain present")
                .state,
            output::BackgroundIndexRebuildState::Unknown
        );
        assert_eq!(projected.warnings, vec!["broker_diagnostics_value_sanitized"]);
        assert!(!serde_json::to_string(&projected)
            .expect("projected diagnostics should serialize")
            .contains("private-"));
    }
}
