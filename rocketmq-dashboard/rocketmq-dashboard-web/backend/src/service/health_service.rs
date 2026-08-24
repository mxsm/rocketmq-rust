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
use crate::model::DashboardHistoryHealth;
use crate::model::HealthStatus;
use crate::model::SessionAuditCleanupHealth;
use crate::persistence::DashboardPersistence;
use crate::persistence::StorageHealth;
use crate::persistence::StorageStatus;

pub fn liveness_status() -> HealthStatus {
    HealthStatus {
        status: "UP".to_string(),
        storage: None,
        history: None,
        session_audit_cleanup: None,
    }
}

pub async fn readiness_status(
    persistence: &DashboardPersistence,
    history: DashboardHistoryHealth,
    session_audit_cleanup: SessionAuditCleanupHealth,
) -> HealthStatus {
    let mut status = readiness_status_from_storage(persistence.storage_health().await);
    status.history = Some(history);
    if session_audit_cleanup.connectivity != "available" {
        status.status = "DOWN".to_string();
    }
    status.session_audit_cleanup = Some(session_audit_cleanup);
    status
}

pub fn readiness_status_from_storage(storage: StorageHealth) -> HealthStatus {
    HealthStatus {
        status: if storage.status == StorageStatus::Available {
            "UP".to_string()
        } else {
            "DOWN".to_string()
        },
        storage: Some(storage),
        history: None,
        session_audit_cleanup: None,
    }
}

#[cfg(test)]
mod tests {
    use super::readiness_status_from_storage;
    use crate::model::StorageBackend;
    use crate::persistence::StorageHealth;
    use crate::persistence::StorageMode;
    use crate::persistence::StorageStatus;

    #[test]
    fn unavailable_storage_makes_readiness_down() {
        let status = readiness_status_from_storage(StorageHealth {
            backend: StorageBackend::Postgres,
            mode: StorageMode::MultiNode,
            status: StorageStatus::Unavailable,
            schema_version: None,
            last_successful_write_at: None,
            available_bytes: None,
            pool_size: None,
            idle_connections: None,
        });

        assert_eq!(status.status, "DOWN");
        assert_eq!(
            status.storage.expect("storage status").status,
            StorageStatus::Unavailable
        );
    }

    #[test]
    fn degraded_storage_makes_readiness_down() {
        let status = readiness_status_from_storage(StorageHealth {
            backend: StorageBackend::File,
            mode: StorageMode::SingleNode,
            status: StorageStatus::Degraded,
            schema_version: Some(2),
            last_successful_write_at: None,
            available_bytes: Some(1),
            pool_size: None,
            idle_connections: None,
        });

        assert_eq!(status.status, "DOWN");
    }
}
