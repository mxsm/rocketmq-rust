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

//! Explicit setup of the already selected authority, separate from next-launch preferences.

use rocketmq_dashboard_storage::{ImportSnapshot, StorageSelection, StorageStatus};

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SelectionSource {
    Default,
    Saved,
    Environment,
    Injected,
}

pub(super) struct FrozenSelection {
    pub(super) engine: Result<StorageEngine, ConfigStoreFailure>,
    pub(super) source: SelectionSource,
}

pub(crate) struct NativeStorageSetup {
    pub current_engine: Result<StorageEngine, ConfigStoreFailure>,
    pub source: SelectionSource,
    pub saved: Result<Option<StorageSelection>, ConfigStoreFailure>,
    pub selection_path: PathBuf,
    pub legacy_config_path: PathBuf,
    pub status: Result<StorageStatus, ConfigStoreFailure>,
}

#[derive(Clone, Debug)]
pub(crate) enum WorkspaceSetupAction {
    Initialize,
    ImportLegacy { config_path: PathBuf },
}

pub(super) fn engine_override_from_environment() -> Result<Option<StorageEngine>, ConfigStoreFailure> {
    match std::env::var(STORAGE_ENGINE_ENV) {
        Ok(value) => match value.as_str() {
            "file" => Ok(Some(StorageEngine::File)),
            "sqlite" => Ok(Some(StorageEngine::Sqlite)),
            "mysql" => Ok(Some(StorageEngine::MySql)),
            "postgres" => Ok(Some(StorageEngine::Postgres)),
            _ => Err(ConfigStoreFailure::validation(ConfigValidation::StorageEngine)),
        },
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(ConfigStoreFailure::validation(ConfigValidation::StorageEngine)),
    }
}

impl DesktopConfigStore {
    pub(super) fn selection_path(&self) -> PathBuf {
        let mut path = self.path.as_os_str().to_os_string();
        path.push(".storage-selection.json");
        PathBuf::from(path)
    }

    pub(super) async fn frozen_selection(&self) -> &FrozenSelection {
        self.selection
            .get_or_init(|| async {
                if let Some(storage) = self.native.get() {
                    return FrozenSelection {
                        engine: Ok(storage.engine()),
                        source: SelectionSource::Injected,
                    };
                }
                if !self.from_environment {
                    return FrozenSelection {
                        engine: Ok(StorageEngine::File),
                        source: SelectionSource::Injected,
                    };
                }
                match self.engine_override.clone() {
                    Ok(Some(engine)) => FrozenSelection {
                        engine: Ok(engine),
                        source: SelectionSource::Environment,
                    },
                    Err(error) => FrozenSelection {
                        engine: Err(error),
                        source: SelectionSource::Environment,
                    },
                    Ok(None) => match StorageSelection::load(self.selection_path(), self.context.clone()).await {
                        Ok(saved) => FrozenSelection {
                            engine: Ok(saved.as_ref().map_or(StorageEngine::File, |value| value.engine)),
                            source: if saved.is_some() {
                                SelectionSource::Saved
                            } else {
                                SelectionSource::Default
                            },
                        },
                        Err(error) => FrozenSelection {
                            engine: Err(error.into()),
                            source: SelectionSource::Saved,
                        },
                    },
                }
            })
            .await
    }

    pub(crate) async fn storage_setup_snapshot(&self) -> NativeStorageSetup {
        let _guard = self.io_gate.lock().await;
        let selection = self.frozen_selection().await;
        let status = match self.native_storage().await {
            Ok(storage) => storage.status().await.map_err(ConfigStoreFailure::from),
            Err(error) => Err(error),
        };
        NativeStorageSetup {
            current_engine: selection.engine.clone(),
            source: selection.source,
            saved: StorageSelection::load(self.selection_path(), self.context.clone())
                .await
                .map_err(ConfigStoreFailure::from),
            selection_path: self.selection_path(),
            legacy_config_path: self.path.clone(),
            status,
        }
    }

    pub(crate) async fn save_storage_engine(&self, engine: StorageEngine) -> Result<(), ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(ConfigStoreFailure::rejected(ConfigStoreRejection::Closed));
        }
        // Pin even an unsuccessful opening before writing next-launch preferences.
        if self.frozen_selection().await.source == SelectionSource::Environment {
            return Err(ConfigStoreFailure::validation(ConfigValidation::StorageEngineOverride));
        }
        if !engine.is_compiled() {
            return Err(ConfigStoreFailure::validation(
                ConfigValidation::StorageDriverUnavailable,
            ));
        }
        StorageSelection { engine }
            .save(self.selection_path(), self.context.clone())
            .await
            .map_err(ConfigStoreFailure::from)
    }

    pub(crate) async fn complete_workspace_setup(
        &self,
        action: WorkspaceSetupAction,
    ) -> Result<(), ConfigStoreFailure> {
        let _guard = self.io_gate.lock().await;
        let storage = self.native_storage().await?;
        // A fresh authoritative check controls admission; the transaction also rejects a racing initializer.
        if storage.engine() == StorageEngine::File || storage.load_desktop().await?.is_some() {
            return Err(ConfigStoreFailure::stale_revision());
        }
        let snapshot = match action {
            WorkspaceSetupAction::Initialize => {
                let document = DesktopDocument::from_legacy(DesktopConfig::default(), Vec::new())?;
                storage.initialize(document).await?
            }
            WorkspaceSetupAction::ImportLegacy { config_path } => {
                let candidate = ImportSnapshot::read_files(config_path, self.context.clone()).await?;
                storage.import(candidate).await?
            }
        };
        self.publish(snapshot.document)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "storage_setup_tests.rs"]
mod tests;
