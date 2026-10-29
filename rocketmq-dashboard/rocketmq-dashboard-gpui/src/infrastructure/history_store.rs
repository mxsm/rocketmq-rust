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

//! Versioned metric observations with explicit environment ownership.

use std::{collections::BTreeMap, io, path::PathBuf, sync::Arc};

use rocketmq_dashboard_common::{
    EnvironmentHistoryPoint, EnvironmentId, HistoryMetricKind, HistoryPoint, HistoryRetention, filter_history_range,
    retain_history_points_bounded,
};
use rocketmq_runtime::ChildServiceContext;
use serde::{Deserialize, Serialize};

use super::{
    config_store::{ConfigStoreFailure, DesktopConfigStore, write_json_atomically},
    storage_writer::StorageWriter,
};

const HISTORY_SCHEMA_VERSION: u32 = 2;
const MAX_SERIES_IDENTITY_BYTES: usize = 1_024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HistoryDocument {
    schema_version: u32,
    points: Vec<EnvironmentHistoryPoint>,
    legacy_notes: Vec<LegacyHistoryRecord>,
}

impl Default for HistoryDocument {
    fn default() -> Self {
        Self {
            schema_version: HISTORY_SCHEMA_VERSION,
            points: Vec::new(),
            legacy_notes: Vec::new(),
        }
    }
}

/// History shares the canonical store's process-lifetime directory writer in production.
pub struct HistoryStore {
    path: PathBuf,
    context: ChildServiceContext,
    gate: tokio::sync::Mutex<()>,
    owner: Option<Arc<DesktopConfigStore>>,
    writer: Arc<parking_lot::Mutex<Option<StorageWriter>>>,
}

impl HistoryStore {
    #[cfg(test)]
    pub(crate) async fn pause_queries(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.gate.lock().await
    }

    pub fn environment_snapshot(
        &self,
    ) -> Result<Option<rocketmq_dashboard_common::EnvironmentConnectionSnapshot>, ConfigStoreFailure> {
        self.owner
            .as_ref()
            .map(|owner| owner.environment_snapshot())
            .transpose()
    }
    #[cfg(test)]
    pub fn new(path: PathBuf, context: ChildServiceContext) -> Arc<Self> {
        Arc::new(Self {
            path,
            context,
            gate: tokio::sync::Mutex::new(()),
            owner: None,
            writer: Arc::new(parking_lot::Mutex::new(None)),
        })
    }

    pub fn with_config_store(path: PathBuf, context: ChildServiceContext, owner: Arc<DesktopConfigStore>) -> Arc<Self> {
        Arc::new(Self {
            path,
            context,
            gate: tokio::sync::Mutex::new(()),
            owner: Some(owner),
            writer: Arc::new(parking_lot::Mutex::new(None)),
        })
    }

    async fn ensure_writer(&self) -> Result<(), ConfigStoreFailure> {
        if let Some(owner) = &self.owner {
            owner.environment_snapshot()?;
            return Ok(());
        }
        let path = self.path.clone();
        let writer = Arc::clone(&self.writer);
        self.context
            .storage_io()
            .spawn_io("gpui-history-writer", move || {
                let mut writer = writer.lock();
                if writer.is_none() {
                    *writer = Some(StorageWriter::acquire(&path)?);
                }
                Ok::<_, ConfigStoreFailure>(())
            })
            .await
            .map_err(ConfigStoreFailure::runtime)?
    }

    pub async fn observations(&self) -> Result<Vec<EnvironmentHistoryPoint>, ConfigStoreFailure> {
        let _guard = self.gate.lock().await;
        self.ensure_writer().await?;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-history-points", move || {
                let (document, migrated) = load_document(&path)?;
                if migrated {
                    write_json_atomically(&path, &document)?;
                }
                Ok(document.points)
            })
            .await
            .map_err(ConfigStoreFailure::runtime)?
    }

    /// Compatibility projection. Environment-aware callers use `query_environment`.
    pub async fn points(&self) -> Result<Vec<HistoryPoint>, ConfigStoreFailure> {
        Ok(self
            .observations()
            .await?
            .into_iter()
            .map(|observation| observation.point)
            .collect())
    }

    /// Compatibility lookup of unassigned observations only.
    pub async fn query(
        &self,
        metric: HistoryMetricKind,
        series_identity: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, ConfigStoreFailure> {
        self.query_attribution(None, metric, series_identity, start_epoch_ms, end_epoch_ms)
            .await
    }

    /// Environment attribution is filtered before metric, series, or timestamp projection.
    pub async fn query_environment(
        &self,
        environment_id: EnvironmentId,
        metric: HistoryMetricKind,
        series_identity: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, ConfigStoreFailure> {
        self.query_attribution(
            Some(environment_id),
            metric,
            series_identity,
            start_epoch_ms,
            end_epoch_ms,
        )
        .await
    }

    async fn query_attribution(
        &self,
        environment_id: Option<EnvironmentId>,
        metric: HistoryMetricKind,
        series_identity: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, ConfigStoreFailure> {
        let points = self
            .observations()
            .await?
            .into_iter()
            .filter(|observation| observation.environment_id == environment_id)
            .map(|observation| observation.point)
            .filter(|point| point.metric == metric && point.series_identity == series_identity)
            .collect::<Vec<_>>();
        Ok(filter_history_range(&points, start_epoch_ms, end_epoch_ms))
    }

    pub async fn append_sample(
        &self,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<(), ConfigStoreFailure> {
        self.append_observations(
            sample
                .into_iter()
                .map(|point| EnvironmentHistoryPoint {
                    environment_id: None,
                    point,
                })
                .collect(),
            retention,
        )
        .await
    }

    /// The environment is captured with the provider generation before sampling starts.
    pub async fn append_environment_sample(
        &self,
        environment_id: EnvironmentId,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<(), ConfigStoreFailure> {
        self.append_observations(
            sample
                .into_iter()
                .map(|point| EnvironmentHistoryPoint {
                    environment_id: Some(environment_id.clone()),
                    point,
                })
                .collect(),
            retention,
        )
        .await
    }

    async fn append_observations(
        &self,
        sample: Vec<EnvironmentHistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<(), ConfigStoreFailure> {
        let sample = sample
            .into_iter()
            .filter(|observation| {
                let point = &observation.point;
                point.value.is_finite()
                    && !point.series_identity.trim().is_empty()
                    && point.series_identity.len() <= MAX_SERIES_IDENTITY_BYTES
            })
            .collect::<Vec<_>>();
        if sample.is_empty() {
            return Ok(());
        }
        let _guard = self.gate.lock().await;
        self.ensure_writer().await?;
        let path = self.path.clone();
        self.context
            .storage_io()
            .spawn_io("gpui-history-append-sample", move || {
                let (mut document, _) = load_document(&path)?;
                document.points.extend(sample);
                document.points = retain_observations(document.points, retention);
                write_json_atomically(&path, &document)
            })
            .await
            .map_err(ConfigStoreFailure::runtime)?
    }
}

fn retain_observations(
    points: Vec<EnvironmentHistoryPoint>,
    retention: HistoryRetention,
) -> Vec<EnvironmentHistoryPoint> {
    let mut environments: BTreeMap<Option<EnvironmentId>, Vec<HistoryPoint>> = BTreeMap::new();
    for observation in points {
        environments
            .entry(observation.environment_id)
            .or_default()
            .push(observation.point);
    }
    let mut retained = Vec::new();
    for (environment_id, points) in environments {
        for point in retain_history_points_bounded(points, retention) {
            retained.push(EnvironmentHistoryPoint {
                environment_id: environment_id.clone(),
                point,
            });
        }
    }
    let mut series = BTreeMap::new();
    for observation in &retained {
        series
            .entry((
                observation.environment_id.clone(),
                observation.point.metric,
                observation.point.series_identity.clone(),
            ))
            .and_modify(|newest: &mut u64| *newest = (*newest).max(observation.point.timestamp_epoch_ms))
            .or_insert(observation.point.timestamp_epoch_ms);
    }
    let mut ages = series
        .into_iter()
        .map(|(key, newest)| (newest, key))
        .collect::<Vec<_>>();
    ages.sort();
    let evicted = ages
        .into_iter()
        .rev()
        .skip(retention.max_series)
        .map(|(_, key)| key)
        .collect::<std::collections::BTreeSet<_>>();
    retained.retain(|observation| {
        !evicted.contains(&(
            observation.environment_id.clone(),
            observation.point.metric,
            observation.point.series_identity.clone(),
        ))
    });
    retained.sort_by(|left, right| {
        left.point
            .timestamp_epoch_ms
            .cmp(&right.point.timestamp_epoch_ms)
            .then(left.environment_id.cmp(&right.environment_id))
            .then(left.point.series_identity.cmp(&right.point.series_identity))
    });
    let evict = retained.len().saturating_sub(retention.max_total_points);
    retained.drain(..evict);
    retained.sort_by(|left, right| {
        left.environment_id
            .cmp(&right.environment_id)
            .then(left.point.metric.cmp(&right.point.metric))
            .then(left.point.series_identity.cmp(&right.point.series_identity))
            .then(left.point.timestamp_epoch_ms.cmp(&right.point.timestamp_epoch_ms))
    });
    retained
}

fn load_document(path: &PathBuf) -> Result<(HistoryDocument, bool), ConfigStoreFailure> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((HistoryDocument::default(), false)),
        Err(source) => return Err(ConfigStoreFailure::io("read history", path.clone(), source)),
    };
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|source| ConfigStoreFailure::invalid_document(path.clone(), source))?;
    if value.is_array() {
        let legacy_notes = serde_json::from_value(value)
            .map_err(|source| ConfigStoreFailure::invalid_document(path.clone(), source))?;
        return Ok((
            HistoryDocument {
                legacy_notes,
                ..HistoryDocument::default()
            },
            true,
        ));
    }
    let version = value
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    match version {
        1 => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields, rename_all = "camelCase")]
            struct LegacyMetricDocument {
                schema_version: u32,
                points: Vec<HistoryPoint>,
            }
            let legacy: LegacyMetricDocument = serde_json::from_value(value)
                .map_err(|source| ConfigStoreFailure::invalid_document(path.clone(), source))?;
            let _ = legacy.schema_version;
            Ok((
                HistoryDocument {
                    points: legacy
                        .points
                        .into_iter()
                        .map(|point| EnvironmentHistoryPoint {
                            environment_id: None,
                            point,
                        })
                        .collect(),
                    ..HistoryDocument::default()
                },
                true,
            ))
        }
        2 => serde_json::from_value(value)
            .map(|document| (document, false))
            .map_err(|source| ConfigStoreFailure::invalid_document(path.clone(), source)),
        version => Err(ConfigStoreFailure::unsupported_schema(
            u32::try_from(version).unwrap_or(u32::MAX),
            HISTORY_SCHEMA_VERSION,
        )),
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyHistoryRecord {
    id: String,
    note: String,
}

#[cfg(test)]
mod tests {
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

    use super::*;

    #[test]
    fn same_named_series_are_isolated_and_a_late_sample_keeps_its_original_environment() {
        with_store(|store, _, runtime| {
            let first = EnvironmentId::parse("production").unwrap();
            let second = EnvironmentId::parse("staging").unwrap();
            runtime.block_on(async {
                store
                    .append_environment_sample(first.clone(), vec![point("orders", 1, 10.0)], retention(10))
                    .await
                    .unwrap();
                store
                    .append_environment_sample(second.clone(), vec![point("orders", 2, 20.0)], retention(10))
                    .await
                    .unwrap();
                store
                    .append_environment_sample(first.clone(), vec![point("orders", 3, 11.0)], retention(10))
                    .await
                    .unwrap();
                assert_eq!(
                    store
                        .query_environment(first, HistoryMetricKind::BrokerProduceTps, "orders".into(), 0, 4)
                        .await
                        .unwrap(),
                    vec![point("orders", 1, 10.0), point("orders", 3, 11.0)]
                );
                assert_eq!(
                    store
                        .query_environment(second, HistoryMetricKind::BrokerProduceTps, "orders".into(), 0, 4)
                        .await
                        .unwrap(),
                    vec![point("orders", 2, 20.0)]
                );
            });
        });
    }

    #[test]
    fn legacy_metric_points_remain_unassigned_after_migration_and_restart() {
        with_store(|store, path, runtime| {
            let legacy_point = point("orders", 1, 9.0);
            let value = serde_json::json!({ "schemaVersion": 1, "points": [legacy_point.clone()] });
            std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
            runtime.block_on(async {
                assert_eq!(
                    store.observations().await.unwrap(),
                    vec![EnvironmentHistoryPoint {
                        environment_id: None,
                        point: legacy_point.clone()
                    }]
                );
                assert!(
                    store
                        .query_environment(
                            EnvironmentId::parse("production").unwrap(),
                            HistoryMetricKind::BrokerProduceTps,
                            "orders".into(),
                            0,
                            4
                        )
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert_eq!(store.observations().await.unwrap()[0].point, legacy_point);
            });
            let (migrated, needs_migration) = load_document(&path.to_path_buf()).unwrap();
            assert!(!needs_migration);
            assert_eq!(migrated.points[0].environment_id, None);
        });
    }

    #[test]
    fn unknown_history_version_and_unknown_fields_preserve_source() {
        for source in [
            br#"{"schemaVersion":99,"futurePoints":[1]}"#.as_slice(),
            br#"{"schemaVersion":1,"points":[],"futureField":true}"#.as_slice(),
        ] {
            with_store(|store, path, runtime| {
                std::fs::write(path, source).unwrap();
                assert!(runtime.block_on(store.observations()).is_err());
                assert!(
                    runtime
                        .block_on(store.append_sample(vec![point("orders", 2, 10.0)], retention(10)))
                        .is_err()
                );
                assert_eq!(std::fs::read(path).unwrap(), source);
            });
        }
    }

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
