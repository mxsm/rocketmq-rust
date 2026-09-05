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

//! Versioned, bounded persistence for real dashboard metric observations.

use std::{io, path::PathBuf, sync::Arc};

use rocketmq_dashboard_common::{
    HistoryMetricKind, HistoryPoint, HistoryRetention, filter_history_range, retain_history_points_bounded,
};
use rocketmq_runtime::ChildServiceContext;
use serde::{Deserialize, Serialize};

use super::config_store::{ConfigStoreError, write_json_atomically};

const HISTORY_SCHEMA_VERSION: u32 = 1;
const MAX_SERIES_IDENTITY_BYTES: usize = 1_024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HistoryDocument {
    schema_version: u32,
    points: Vec<HistoryPoint>,
}

impl Default for HistoryDocument {
    fn default() -> Self {
        Self {
            schema_version: HISTORY_SCHEMA_VERSION,
            points: Vec::new(),
        }
    }
}

/// JSON-backed metric history store.
pub struct HistoryStore {
    path: PathBuf,
    context: ChildServiceContext,
    gate: tokio::sync::Mutex<()>,
}

impl HistoryStore {
    /// Creates a store at an application-owned path.
    pub fn new(path: PathBuf, context: ChildServiceContext) -> Arc<Self> {
        Arc::new(Self {
            path,
            context,
            gate: tokio::sync::Mutex::new(()),
        })
    }

    /// Returns all retained observations in deterministic series/time order.
    pub async fn points(&self) -> Result<Vec<HistoryPoint>, ConfigStoreError> {
        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-history-points", move || {
                load_document(&path).map(|document| document.points)
            })
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?
    }

    /// Returns actual observations for one metric/series and time range without filling gaps.
    pub async fn query(
        &self,
        metric: HistoryMetricKind,
        series_identity: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, ConfigStoreError> {
        let points = self.points().await?;
        let selected = points
            .into_iter()
            .filter(|point| point.metric == metric && point.series_identity == series_identity)
            .collect::<Vec<_>>();
        Ok(filter_history_range(&selected, start_epoch_ms, end_epoch_ms))
    }

    /// Atomically appends one successful sample and applies every hard retention bound.
    ///
    /// An empty sample represents no successful observations and does not rewrite the document.
    pub async fn append_sample(
        &self,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<(), ConfigStoreError> {
        let sample = sample
            .into_iter()
            .filter(|point| {
                point.value.is_finite()
                    && !point.series_identity.trim().is_empty()
                    && point.series_identity.len() <= MAX_SERIES_IDENTITY_BYTES
            })
            .collect::<Vec<_>>();
        if sample.is_empty() {
            return Ok(());
        }

        let _guard = self.gate.lock().await;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-history-append-sample", move || {
                let mut document = load_document(&path)?;
                document.points.extend(sample);
                document.points = retain_history_points_bounded(document.points, retention);
                write_json_atomically(&path, &document)
            })
            .await
            .map_err(|error| ConfigStoreError::Runtime(error.to_string()))?
    }
}

fn load_document(path: &PathBuf) -> Result<HistoryDocument, ConfigStoreError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HistoryDocument::default()),
        Err(source) => {
            return Err(ConfigStoreError::Io {
                operation: "read history",
                path: path.clone(),
                source,
            });
        }
    };

    match serde_json::from_slice::<HistoryDocument>(&bytes) {
        Ok(document) if document.schema_version == HISTORY_SCHEMA_VERSION => Ok(document),
        Ok(document) => Err(ConfigStoreError::UnsupportedSchema {
            found: document.schema_version,
            supported: HISTORY_SCHEMA_VERSION,
        }),
        Err(document_error) => {
            // Delivery 02 persisted an array of lifecycle notes, never metrics. Accept that shape as
            // an empty metric history so existing installations upgrade without invented points.
            if serde_json::from_slice::<Vec<LegacyHistoryRecord>>(&bytes).is_ok() {
                return Ok(HistoryDocument::default());
            }
            Err(ConfigStoreError::InvalidDocument {
                path: path.clone(),
                summary: format!(
                    "{:?} at line {}, column {}",
                    document_error.classify(),
                    document_error.line(),
                    document_error.column()
                ),
            })
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyHistoryRecord {
    #[serde(rename = "id")]
    _id: String,
    #[serde(rename = "note")]
    _note: String,
}

#[cfg(test)]
mod tests {
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

    use super::*;

    fn point(series: &str, timestamp: u64, value: f64) -> HistoryPoint {
        HistoryPoint {
            metric: HistoryMetricKind::BrokerProduceTps,
            series_identity: series.into(),
            timestamp_epoch_ms: timestamp,
            value,
            source_revision: 7,
        }
    }

    fn retention(max_points_per_series: usize) -> HistoryRetention {
        HistoryRetention {
            max_points_per_series,
            max_series: 10,
            max_total_points: 10,
        }
    }

    fn with_store(test: impl FnOnce(Arc<HistoryStore>, &std::path::Path, &RuntimeOwner)) {
        let directory = tempfile::tempdir().expect("temp directory");
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("history-test", 1))
            .expect("test runtime configuration is valid")
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory"))
            .build()
            .expect("runtime");
        let path = directory.path().join("history.json");
        let store = HistoryStore::new(path.clone(), runtime.root_context().component("history"));
        test(store, &path, &runtime);
        runtime.shutdown_runtime_blocking().expect("owned shutdown");
    }

    #[test]
    fn successful_samples_are_atomic_bounded_and_queryable() {
        with_store(|store, path, runtime| {
            runtime.block_on(async {
                store
                    .append_sample(
                        vec![point("a", 1, 1.0), point("a", 2, 2.0), point("b", 1, 3.0)],
                        retention(1),
                    )
                    .await
                    .expect("append");
                assert_eq!(
                    store.points().await.expect("points"),
                    vec![point("a", 2, 2.0), point("b", 1, 3.0)]
                );
                assert_eq!(
                    store
                        .query(HistoryMetricKind::BrokerProduceTps, "a".into(), 0, 3)
                        .await
                        .expect("query"),
                    vec![point("a", 2, 2.0)]
                );
            });
            let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path).expect("file")).expect("json");
            assert_eq!(json["schemaVersion"], HISTORY_SCHEMA_VERSION);
        });
    }

    #[test]
    fn empty_or_invalid_sample_does_not_create_or_rewrite_history() {
        with_store(|store, path, runtime| {
            runtime.block_on(async {
                store.append_sample(Vec::new(), retention(10)).await.expect("empty");
                store
                    .append_sample(vec![point("a", 1, f64::NAN)], retention(10))
                    .await
                    .expect("invalid values are missing observations");
            });
            assert!(!path.exists());
        });
    }

    #[test]
    fn delivery_two_document_is_compatible_and_contains_no_invented_metric() {
        with_store(|store, path, runtime| {
            std::fs::write(path, br#"[{"id":"startup","note":"warming up"}]"#).expect("legacy history");
            assert!(runtime.block_on(store.points()).expect("compatible read").is_empty());
        });
    }

    #[test]
    fn persisted_document_obeys_global_series_and_total_point_caps() {
        with_store(|store, path, runtime| {
            runtime.block_on(async {
                store
                    .append_sample(
                        vec![
                            point("old", 1, 1.0),
                            point("middle", 2, 2.0),
                            point("new", 3, 3.0),
                            point("new", 4, 4.0),
                        ],
                        HistoryRetention {
                            max_points_per_series: 2,
                            max_series: 2,
                            max_total_points: 2,
                        },
                    )
                    .await
                    .expect("bounded append");
                assert_eq!(
                    store.points().await.expect("bounded points"),
                    vec![point("new", 3, 3.0), point("new", 4, 4.0)]
                );
            });
            let document: HistoryDocument =
                serde_json::from_slice(&std::fs::read(path).expect("file")).expect("bounded document");
            assert_eq!(document.points.len(), 2);
        });
    }
}
