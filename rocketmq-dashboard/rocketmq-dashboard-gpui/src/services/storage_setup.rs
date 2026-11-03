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

//! Safe setup projections; admitted writes use the existing application mutation owner.

use std::{path::PathBuf, sync::Arc};

use rocketmq_dashboard_storage::{StorageEngine, StorageStatus};

use super::{AppServices, config_ui_error};
pub(crate) use crate::infrastructure::config_store::{SelectionSource, WorkspaceSetupAction};
use crate::{
    infrastructure::config_store::NativeStorageSetup,
    state::{UiError, UiErrorCode},
};

#[derive(Clone, Debug)]
pub(crate) struct StorageSetupSnapshot {
    pub current_engine: Option<StorageEngine>,
    pub source: SelectionSource,
    pub saved_engine: Option<StorageEngine>,
    pub selection_error: Option<UiError>,
    pub selection_path: PathBuf,
    pub legacy_config_path: PathBuf,
    pub status: Result<StorageStatus, UiError>,
    pub checked_at_epoch_ms: Option<u64>,
}

impl From<NativeStorageSetup> for StorageSetupSnapshot {
    fn from(value: NativeStorageSetup) -> Self {
        let (saved_engine, selection_error) = match value.saved {
            Ok(value) => (value.map(|value| value.engine), None),
            Err(error) => (None, Some(config_ui_error(error))),
        };
        Self {
            current_engine: value.current_engine.ok(),
            source: value.source,
            saved_engine,
            selection_error,
            selection_path: value.selection_path,
            legacy_config_path: value.legacy_config_path,
            status: value.status.map_err(config_ui_error),
            checked_at_epoch_ms: now_epoch_ms(),
        }
    }
}

pub(super) fn now_epoch_ms() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}

pub(super) fn unavailable() -> UiError {
    UiError::new(
        "Native storage setup is unavailable for this backend.",
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}

impl AppServices {
    pub(crate) async fn storage_setup_snapshot(&self) -> Result<StorageSetupSnapshot, UiError> {
        let backend = Arc::clone(&self.backend);
        let work = async move { backend.storage_setup_snapshot().await };
        match &self.runtime_bridge {
            Some(runtime) => runtime.run("gpui-storage-setup-status", work).await,
            None => work.await,
        }
    }

    pub(crate) async fn save_storage_engine(&self, engine: StorageEngine) -> Result<(), UiError> {
        let backend = Arc::clone(&self.backend);
        let work = async move { backend.save_storage_engine(engine).await };
        match &self.runtime_bridge {
            Some(runtime) => runtime.run_owned("gpui-storage-selection-save", work).await,
            None => work.await,
        }
    }

    pub(crate) async fn complete_workspace_setup(&self, action: WorkspaceSetupAction) -> Result<(), UiError> {
        let backend = Arc::clone(&self.backend);
        let work = async move { backend.complete_workspace_setup(action).await };
        match &self.runtime_bridge {
            Some(runtime) => runtime.run_owned("gpui-workspace-setup", work).await,
            None => work.await,
        }
    }
}

#[cfg(test)]
#[path = "storage_setup_test_support.rs"]
pub(crate) mod test_support;
