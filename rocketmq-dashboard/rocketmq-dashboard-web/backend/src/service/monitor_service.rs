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
use crate::model::ConsumerMonitorConfig;
use crate::model::ConsumerMonitorMutationResult;
use crate::model::ConsumerMonitorUpsertRequest;
use crate::model::ConsumerMonitorView;
use crate::state::AppState;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct MonitorStore {
    path: PathBuf,
    entries: RwLock<BTreeMap<String, ConsumerMonitorConfig>>,
}

impl MonitorStore {
    pub fn new(path: PathBuf) -> Self {
        let entries = load_entries(&path).unwrap_or_default();
        Self {
            path,
            entries: RwLock::new(entries),
        }
    }

    async fn list(&self) -> Vec<ConsumerMonitorView> {
        self.entries
            .read()
            .await
            .iter()
            .map(|(consumer_group, config)| ConsumerMonitorView {
                consumer_group: consumer_group.clone(),
                min_count: config.min_count,
                max_diff_total: config.max_diff_total,
            })
            .collect()
    }

    async fn upsert(&self, request: ConsumerMonitorUpsertRequest) -> Result<ConsumerMonitorView, DashboardError> {
        let consumer_group = request.consumer_group.trim().to_string();
        if consumer_group.is_empty() {
            return Err(DashboardError::Validation("Consumer group is required".to_string()));
        }
        if request.min_count < 0 {
            return Err(DashboardError::Validation(
                "minCount must be greater than or equal to 0".to_string(),
            ));
        }
        if request.max_diff_total < 0 {
            return Err(DashboardError::Validation(
                "maxDiffTotal must be greater than or equal to 0".to_string(),
            ));
        }

        let mut entries = self.entries.write().await;
        entries.insert(
            consumer_group.clone(),
            ConsumerMonitorConfig {
                min_count: request.min_count,
                max_diff_total: request.max_diff_total,
            },
        );
        save_entries(&self.path, &entries)?;

        Ok(ConsumerMonitorView {
            consumer_group,
            min_count: request.min_count,
            max_diff_total: request.max_diff_total,
        })
    }

    async fn delete(&self, consumer_group: &str) -> Result<bool, DashboardError> {
        let consumer_group = consumer_group.trim();
        if consumer_group.is_empty() {
            return Err(DashboardError::Validation("Consumer group is required".to_string()));
        }

        let mut entries = self.entries.write().await;
        let removed = entries.remove(consumer_group).is_some();
        save_entries(&self.path, &entries)?;
        Ok(removed)
    }
}

pub async fn list_consumer_monitors(state: &AppState) -> Result<Vec<ConsumerMonitorView>, DashboardError> {
    Ok(state.monitor_store.list().await)
}

pub async fn create_or_update_consumer_monitor(
    state: &AppState,
    request: ConsumerMonitorUpsertRequest,
) -> Result<ConsumerMonitorMutationResult, DashboardError> {
    let item = state.monitor_store.upsert(request).await?;
    Ok(ConsumerMonitorMutationResult {
        message: format!("Consumer monitor {} saved", item.consumer_group),
        item: Some(item),
    })
}

pub async fn delete_consumer_monitor(
    state: &AppState,
    consumer_group: &str,
) -> Result<ConsumerMonitorMutationResult, DashboardError> {
    let removed = state.monitor_store.delete(consumer_group).await?;
    Ok(ConsumerMonitorMutationResult {
        message: if removed {
            format!("Consumer monitor {consumer_group} deleted")
        } else {
            format!("Consumer monitor {consumer_group} did not exist")
        },
        item: None,
    })
}

fn load_entries(path: &PathBuf) -> Result<BTreeMap<String, ConsumerMonitorConfig>, DashboardError> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let content = fs::read_to_string(path)
        .map_err(|error| DashboardError::Config(format!("Failed to read monitor config file: {error}")))?;
    if content.trim().is_empty() {
        return Ok(BTreeMap::new());
    }
    serde_json::from_str(&content)
        .map_err(|error| DashboardError::Config(format!("Failed to parse monitor config file: {error}")))
}

fn save_entries(path: &PathBuf, entries: &BTreeMap<String, ConsumerMonitorConfig>) -> Result<(), DashboardError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| DashboardError::Config(format!("Failed to create monitor config directory: {error}")))?;
    }
    let content = serde_json::to_string_pretty(entries)
        .map_err(|error| DashboardError::Internal(format!("Failed to serialize monitor config: {error}")))?;
    fs::write(path, content)
        .map_err(|error| DashboardError::Config(format!("Failed to write monitor config file: {error}")))
}

#[cfg(test)]
mod tests {
    use super::MonitorStore;
    use crate::model::ConsumerMonitorUpsertRequest;

    #[tokio::test]
    async fn monitor_store_round_trips_entries() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = MonitorStore::new(dir.path().join("monitor/consumer-monitor-config.json"));

        store
            .upsert(ConsumerMonitorUpsertRequest {
                consumer_group: "group-a".to_string(),
                min_count: 2,
                max_diff_total: 100,
            })
            .await
            .expect("upsert monitor");

        let reloaded = MonitorStore::new(dir.path().join("monitor/consumer-monitor-config.json"));
        let entries = reloaded.list().await;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].consumer_group, "group-a");
        assert_eq!(entries[0].min_count, 2);
        assert_eq!(entries[0].max_diff_total, 100);
    }
}
