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

//! Native local-store evidence and History settings through the established runtime owner.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rocketmq_dashboard_common::{ConnectionScope, EnvironmentId, HistoryRetention};

use super::{AppServices, DesktopBackend, config_ui_error};
use crate::{
    infrastructure::{
        config_store::{FoundationFlags, HISTORY_MAX_POINTS_CAP, HISTORY_MAX_SERIES_CAP, HISTORY_MAX_TOTAL_POINTS_CAP},
        history_health::{CollectionStatus, HistoryCollectionHealth},
        local_storage_status::LocalFileStatus,
    },
    state::{UiError, UiErrorCode},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistorySettings {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub retention: HistoryRetention,
}

impl From<FoundationFlags> for HistorySettings {
    fn from(flags: FoundationFlags) -> Self {
        Self {
            enabled: flags.history_enabled,
            interval_seconds: flags.history_interval_seconds,
            retention: HistoryRetention {
                max_points_per_series: flags.history_max_points_per_series,
                max_series: flags.history_max_series,
                max_total_points: flags.history_max_total_points,
            },
        }
    }
}

impl HistorySettings {
    pub fn validate(self) -> Result<Self, UiError> {
        if Instant::now()
            .checked_add(Duration::from_secs(self.interval_seconds))
            .is_none()
        {
            return Err(UiError::new(
                "History interval is outside the supported clock range.",
                UiErrorCode::Validation,
                false,
            ));
        }
        if self.retention.max_points_per_series > HISTORY_MAX_POINTS_CAP
            || self.retention.max_series > HISTORY_MAX_SERIES_CAP
            || self.retention.max_total_points > HISTORY_MAX_TOTAL_POINTS_CAP
        {
            return Err(UiError::new(
                "History retention exceeds the supported per-series, series-count or total-point limit.",
                UiErrorCode::Validation,
                false,
            ));
        }
        Ok(self)
    }

    pub(super) fn apply(self, flags: &mut FoundationFlags) {
        flags.history_enabled = self.enabled;
        flags.history_interval_seconds = self.interval_seconds;
        flags.history_max_points_per_series = self.retention.max_points_per_series;
        flags.history_max_series = self.retention.max_series;
        flags.history_max_total_points = self.retention.max_total_points;
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LocalRuntimeStatus {
    pub configuration: LocalFileStatus,
    pub history: LocalFileStatus,
    pub environment_id: EnvironmentId,
    pub configuration_revision: u64,
    pub scope: ConnectionScope,
    pub collection: HistoryCollectionHealth,
    pub collector_running: bool,
    pub native_storage: super::storage_setup::StorageSetupSnapshot,
    pub collection_status: Option<CollectionStatus>,
}

impl AppServices {
    pub(crate) async fn local_runtime_status(&self) -> Result<LocalRuntimeStatus, UiError> {
        let backend = Arc::clone(&self.backend);
        let work = async move { backend.local_runtime_status().await };
        match &self.runtime_bridge {
            Some(runtime) => runtime.run("gpui-local-storage-status", work).await,
            None => work.await,
        }
    }
}

impl DesktopBackend {
    pub(super) async fn inspect_local_runtime(&self) -> Result<LocalRuntimeStatus, UiError> {
        let _update = self.update_gate.lock().await;
        let snapshot = self.store.environment_snapshot().map_err(config_ui_error)?;
        let configuration = self.store.inspect_storage().await;
        let history = self.history_store.inspect_storage().await;
        let collection = self.history_store.collection_health().map_err(config_ui_error)?;
        let native_storage = self.store.storage_setup_snapshot().await.into();
        let collection_status = super::storage_setup::now_epoch_ms().map(|now| collection.status(now));
        if self.store.environment_snapshot().map_err(config_ui_error)? != snapshot {
            return Err(UiError::new(
                "The environment changed while local storage status was checked. Refresh status.",
                UiErrorCode::Configuration,
                true,
            ));
        }
        let collector_running = self
            .history_lifecycle
            .lock()
            .as_ref()
            .is_some_and(|lifecycle| lifecycle.is_running());
        Ok(LocalRuntimeStatus {
            configuration,
            history,
            environment_id: snapshot.environment_id,
            configuration_revision: snapshot.connection.revision,
            scope: snapshot.connection.scope,
            collection,
            collector_running,
            native_storage,
            collection_status,
        })
    }
}

#[cfg(test)]
#[path = "local_storage_tests.rs"]
pub(crate) mod tests;
