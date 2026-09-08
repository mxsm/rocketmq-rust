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

use crate::error::DashboardError;
use crate::model::StorageBackend;
use crate::model::StorageStatusView;
use crate::persistence::StorageStatus;
use crate::persistence::error::PersistenceError;
use rocketmq_observability::DashboardStorageBackend;
use rocketmq_observability::DashboardStorageFailureLabel;
use rocketmq_observability::DashboardStorageMetricsRecorder;
use rocketmq_observability::DashboardStorageOperation;
use rocketmq_observability::DashboardStorageOperationResult;
use rocketmq_observability::DashboardStorageResult;
use rocketmq_observability::TelemetryHandle;
use std::time::Duration;

/// Dashboard adapter for the instance-owned observability recorder.
#[derive(Clone)]
pub struct StorageMetrics {
    inner: DashboardStorageMetricsRecorder,
}

impl StorageMetrics {
    /// Creates a recorder from the application's injected telemetry handle.
    pub fn from_handle(handle: &TelemetryHandle) -> Self {
        Self {
            inner: DashboardStorageMetricsRecorder::from_handle(handle),
        }
    }

    pub fn record_status(&self, view: &StorageStatusView) {
        let backend = storage_backend(view.backend);
        self.inner.record_status(backend, storage_result(view.status));
        self.inner.record_state(
            backend,
            view.safe_available_bytes,
            view.pool_size,
            view.idle_connections,
        );
    }

    /// Records a persistence operation using a fixed metric catalog.
    pub fn record_dashboard_operation<T>(
        &self,
        backend: StorageBackend,
        operation: DashboardStorageOperation,
        result: &Result<T, DashboardError>,
        elapsed: Duration,
    ) {
        let (result, failure) = match result {
            Ok(_) => (DashboardStorageOperationResult::Success, None),
            Err(error) => (
                DashboardStorageOperationResult::Failure,
                Some(storage_failure_label(error)),
            ),
        };
        self.inner
            .record_operation(storage_backend(backend), operation, result, failure, elapsed);
    }

    /// Records a direct persistence result without exposing its source error.
    pub fn record_persistence_operation<T>(
        &self,
        backend: StorageBackend,
        operation: DashboardStorageOperation,
        result: &Result<T, PersistenceError>,
        elapsed: Duration,
    ) {
        let (result, failure) = match result {
            Ok(_) => (DashboardStorageOperationResult::Success, None),
            Err(error) => (
                DashboardStorageOperationResult::Failure,
                Some(persistence_failure_label(error)),
            ),
        };
        self.inner
            .record_operation(storage_backend(backend), operation, result, failure, elapsed);
    }
}

fn storage_backend(backend: StorageBackend) -> DashboardStorageBackend {
    match backend {
        StorageBackend::File => DashboardStorageBackend::File,
        StorageBackend::Sqlite => DashboardStorageBackend::Sqlite,
        StorageBackend::MySql => DashboardStorageBackend::MySql,
        StorageBackend::Postgres => DashboardStorageBackend::Postgres,
    }
}

fn storage_result(status: StorageStatus) -> DashboardStorageResult {
    match status {
        StorageStatus::Available => DashboardStorageResult::Available,
        StorageStatus::Degraded => DashboardStorageResult::Degraded,
        StorageStatus::Unavailable => DashboardStorageResult::Unavailable,
    }
}

fn storage_failure_label(error: &DashboardError) -> DashboardStorageFailureLabel {
    match error {
        DashboardError::Storage(error) => persistence_failure_label(error),
        _ => DashboardStorageFailureLabel::Other,
    }
}

fn persistence_failure_label(error: &PersistenceError) -> DashboardStorageFailureLabel {
    match error {
        PersistenceError::Capacity => DashboardStorageFailureLabel::Capacity,
        PersistenceError::ConnectionUnavailable | PersistenceError::LockUnavailable => {
            DashboardStorageFailureLabel::Connection
        }
        PersistenceError::Timeout => DashboardStorageFailureLabel::Timeout,
        PersistenceError::Conflict => DashboardStorageFailureLabel::Conflict,
        _ => DashboardStorageFailureLabel::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::storage_backend;
    use super::storage_failure_label;
    use super::storage_result;
    use crate::error::DashboardError;
    use crate::model::StorageBackend;
    use crate::persistence::StorageStatus;
    use crate::persistence::error::PersistenceError;
    use rocketmq_observability::DashboardStorageBackend;
    use rocketmq_observability::DashboardStorageFailureLabel;
    use rocketmq_observability::DashboardStorageResult;

    #[test]
    fn status_labels_are_only_fixed_values() {
        assert_eq!(storage_backend(StorageBackend::MySql), DashboardStorageBackend::MySql);
        assert_eq!(
            storage_result(StorageStatus::Degraded),
            DashboardStorageResult::Degraded
        );
        assert_eq!(
            storage_failure_label(&DashboardError::Storage(PersistenceError::Timeout)),
            DashboardStorageFailureLabel::Timeout
        );
    }
}
