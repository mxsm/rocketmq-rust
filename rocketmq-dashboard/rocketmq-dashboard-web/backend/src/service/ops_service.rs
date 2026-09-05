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

use crate::model::StorageStatusReason;
use crate::model::StorageStatusView;
use crate::persistence::DashboardPersistence;
use crate::persistence::StorageStatus;

/// Reads the safe, authenticated storage status view.
pub async fn storage_status(persistence: &DashboardPersistence) -> StorageStatusView {
    let health = persistence.storage_health().await;
    let reason = storage_status_reason(health.status);
    StorageStatusView {
        backend: health.backend,
        mode: health.mode,
        status: health.status,
        reason,
        schema_or_format_version: health.schema_version,
        observation_started_at: persistence.observation_started_at(),
        last_successful_write_at: health.last_successful_write_at,
        safe_available_bytes: health.available_bytes,
        pool_size: health.pool_size,
        idle_connections: health.idle_connections,
    }
}

fn storage_status_reason(status: StorageStatus) -> Option<StorageStatusReason> {
    match status {
        StorageStatus::Available => None,
        StorageStatus::Degraded => Some(StorageStatusReason::CapacityBelowReserve),
        StorageStatus::Unavailable => Some(StorageStatusReason::ProbeFailed),
    }
}

#[cfg(test)]
mod tests {
    use super::storage_status;
    use super::storage_status_reason;
    use crate::config::SqlPoolConfig;
    use crate::config::StorageConfig;
    use crate::model::StorageBackend;
    use crate::model::StorageStatusReason;
    use crate::persistence::DashboardPersistence;
    use crate::persistence::StorageStatus;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use tempfile::tempdir;

    #[test]
    fn authenticated_storage_view_has_no_location_or_connection_details() {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("ops-service-test"))
            .expect("runtime configuration is valid")
            .build()
            .unwrap();
        let directory = tempdir().unwrap();
        let persistence = owner.block_on(async {
            DashboardPersistence::initialize(
                &StorageConfig {
                    backend: StorageBackend::File,
                    data_path: directory.path().join("operator-private-directory"),
                    database_url: None,
                    pool: SqlPoolConfig::default(),
                },
                owner.root_context().component("persistence"),
            )
            .await
            .unwrap()
        });

        let view = owner.block_on(storage_status(&persistence));
        let json = serde_json::to_string(&view).unwrap();
        assert_eq!(view.status, StorageStatus::Available);
        assert_eq!(view.reason, None);
        assert!(view.observation_started_at > 0);
        assert!(!json.contains("operator-private-directory"));
        assert!(!json.contains("databaseUrl"));
        owner.shutdown_runtime_blocking().unwrap();
    }

    #[test]
    fn non_healthy_statuses_have_only_bounded_reasons() {
        assert_eq!(
            storage_status_reason(StorageStatus::Degraded),
            Some(StorageStatusReason::CapacityBelowReserve)
        );
        assert_eq!(
            storage_status_reason(StorageStatus::Unavailable),
            Some(StorageStatusReason::ProbeFailed)
        );
    }
}
