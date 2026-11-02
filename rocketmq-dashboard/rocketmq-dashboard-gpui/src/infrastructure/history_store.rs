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

//! Native History and observed file evidence through the selected shared authority.

use super::{
    config_store::{ConfigStoreFailure, DesktopConfigStore},
    local_storage_status::{FileIoEvidence, FileReadState, LocalFileStatus, inspect_json},
};
use rocketmq_dashboard_common::{
    EnvironmentHistoryPoint, EnvironmentId, HistoryMetricKind, HistoryPoint, HistoryRetention, filter_history_range,
};
pub use rocketmq_dashboard_storage::HistoryAppendOutcome;
#[cfg(test)]
use rocketmq_dashboard_storage::{HISTORY_SCHEMA_VERSION, HistoryDocument};
use rocketmq_dashboard_storage::{HistoryReadOrigin, HistoryWriteOutcome, StorageEngine, StorageIoOperation};
use rocketmq_runtime::ChildServiceContext;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

#[path = "history_ownership.rs"]
mod ownership;
pub(crate) use ownership::HistoryCollectionLease;

pub struct HistoryStore {
    path: PathBuf,
    context: ChildServiceContext,
    gate: tokio::sync::Mutex<()>,
    owner: Arc<DesktopConfigStore>,
    collection: tokio::sync::Mutex<Option<HistoryCollectionLease>>,
    holder_id: String,
    health: parking_lot::RwLock<BTreeMap<Option<EnvironmentId>, super::history_health::HistoryCollectionHealth>>,
    io_evidence: Arc<parking_lot::RwLock<FileIoEvidence>>,
    #[cfg(test)]
    unassigned_fixture: bool,
}

impl HistoryStore {
    #[cfg(test)]
    pub(crate) async fn pause_queries(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.gate.lock().await
    }

    pub fn environment_snapshot(
        &self,
    ) -> Result<Option<rocketmq_dashboard_common::EnvironmentConnectionSnapshot>, ConfigStoreFailure> {
        #[cfg(test)]
        if self.unassigned_fixture {
            return Ok(None);
        }
        self.owner.environment_snapshot().map(Some)
    }

    pub(crate) async fn authoritative_environment_snapshot(
        &self,
    ) -> Result<Option<rocketmq_dashboard_common::EnvironmentConnectionSnapshot>, ConfigStoreFailure> {
        #[cfg(test)]
        if self.unassigned_fixture {
            return Ok(None);
        }
        self.owner.authoritative_environment_snapshot().await.map(Some)
    }

    #[cfg(test)]
    pub fn new(path: PathBuf, context: ChildServiceContext) -> Arc<Self> {
        let owner =
            DesktopConfigStore::with_history_path(path.with_file_name("config.json"), path.clone(), context.clone());
        let mut store = Self::compose(path, context, owner);
        store.unassigned_fixture = true;
        Arc::new(store)
    }

    pub fn with_config_store(path: PathBuf, context: ChildServiceContext, owner: Arc<DesktopConfigStore>) -> Arc<Self> {
        Arc::new(Self::compose(path, context, owner))
    }

    fn compose(path: PathBuf, context: ChildServiceContext, owner: Arc<DesktopConfigStore>) -> Self {
        Self {
            path,
            context,
            gate: tokio::sync::Mutex::new(()),
            owner,
            collection: tokio::sync::Mutex::new(None),
            holder_id: format!("gpui-history-{:032x}", rand::random::<u128>()),
            health: parking_lot::RwLock::new(BTreeMap::new()),
            io_evidence: Arc::new(parking_lot::RwLock::new(FileIoEvidence::default())),
            #[cfg(test)]
            unassigned_fixture: false,
        }
    }

    pub fn collection_health(&self) -> Result<super::history_health::HistoryCollectionHealth, ConfigStoreFailure> {
        let environment = self.environment_snapshot()?.map(|snapshot| snapshot.environment_id);
        let mut health = self.health.read().get(&environment).copied().unwrap_or_default();
        let settings = self.owner.history_settings().unwrap_or_default();
        health.enabled = settings.history_enabled;
        health.interval_seconds = settings.history_interval_seconds;
        Ok(health)
    }

    pub fn record_collection(
        &self,
        environment: Option<EnvironmentId>,
        side: super::history_health::CollectionSide,
        timestamp: u64,
        failed_resources: usize,
        attempt: super::history_health::CollectionAttempt,
    ) {
        let mut health = self.health.write();
        let health = health.entry(environment).or_default();
        let side = match side {
            super::history_health::CollectionSide::Topics => &mut health.topics,
            super::history_health::CollectionSide::Brokers => &mut health.brokers,
        };
        side.last_attempt = Some(attempt);
        side.last_attempt_epoch_ms = Some(timestamp);
        side.failed = matches!(
            attempt,
            super::history_health::CollectionAttempt::SamplingFailed
                | super::history_health::CollectionAttempt::PersistenceFailed
        );
        side.failed_resources = failed_resources;
        if attempt == super::history_health::CollectionAttempt::Persisted {
            side.last_success_epoch_ms = Some(timestamp);
        }
    }

    pub(crate) async fn inspect_storage(&self) -> LocalFileStatus {
        let _guard = self.gate.lock().await;
        let file = self
            .owner
            .native_storage()
            .await
            .is_ok_and(|store| store.engine() == StorageEngine::File);
        if file {
            let path = self.path.clone();
            let evidence = Arc::clone(&self.io_evidence);
            if self
                .context
                .storage_io()
                .spawn_io("gpui-history-status", move || {
                    inspect_json::<rocketmq_dashboard_storage::HistoryDocument>(
                        &path,
                        rocketmq_dashboard_storage::HISTORY_SCHEMA_VERSION,
                        &evidence,
                        |document| {
                            document.validate()?;
                            Ok(None)
                        },
                    );
                })
                .await
                .is_err()
            {
                self.io_evidence.write().record_read(FileReadState::Unknown, None, None);
            }
        } else {
            self.io_evidence.write().record_read(FileReadState::Unknown, None, None);
        }
        LocalFileStatus {
            path: self.path.clone(),
            writer_owned: self.owner.owns_writer(),
            evidence: *self.io_evidence.read(),
        }
    }

    pub async fn observations(&self) -> Result<Vec<EnvironmentHistoryPoint>, ConfigStoreFailure> {
        let _guard = self.gate.lock().await;
        let result = self.owner.native_storage().await?.read_history().await;
        match result {
            Ok(report) => {
                let mut evidence = self.io_evidence.write();
                evidence.record_read(
                    if report.origin == HistoryReadOrigin::Missing {
                        FileReadState::Missing
                    } else {
                        FileReadState::Readable
                    },
                    Some(report.document.schema_version),
                    None,
                );
                if report.origin == HistoryReadOrigin::Migrated {
                    evidence.record_write(true);
                }
                Ok(report.document.points)
            }
            Err(error) => {
                self.io_evidence.write().record_read(FileReadState::Failed, None, None);
                Err(error.into())
            }
        }
    }

    pub async fn points(&self) -> Result<Vec<HistoryPoint>, ConfigStoreFailure> {
        Ok(self
            .observations()
            .await?
            .into_iter()
            .map(|observation| observation.point)
            .collect())
    }

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
        let points: Vec<_> = self
            .observations()
            .await?
            .into_iter()
            .filter(|point| point.environment_id == environment_id)
            .map(|point| point.point)
            .filter(|point| point.metric == metric && point.series_identity == series_identity)
            .collect();
        Ok(filter_history_range(&points, start_epoch_ms, end_epoch_ms))
    }

    #[cfg(test)]
    pub async fn append_sample(
        &self,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<HistoryAppendOutcome, ConfigStoreFailure> {
        self.append_observations(None, sample, retention).await
    }

    #[cfg(test)]
    pub async fn append_environment_sample(
        &self,
        environment_id: EnvironmentId,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<HistoryAppendOutcome, ConfigStoreFailure> {
        self.append_observations(Some(environment_id), sample, retention).await
    }

    #[cfg(test)]
    async fn append_observations(
        &self,
        environment: Option<EnvironmentId>,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<HistoryAppendOutcome, ConfigStoreFailure> {
        let sample = valid_sample(sample);
        if sample.is_empty() {
            return Ok(HistoryAppendOutcome::NotRetained);
        }
        let lease = self
            .collection_lease(environment, std::time::Duration::from_secs(120))
            .await?
            .ok_or_else(ConfigStoreFailure::writer_unavailable)?;
        self.append_captured(lease, sample, retention).await
    }

    pub(crate) async fn append_captured(
        &self,
        lease: HistoryCollectionLease,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<HistoryAppendOutcome, ConfigStoreFailure> {
        let sample = valid_sample(sample);
        if sample.is_empty() {
            return Ok(HistoryAppendOutcome::NotRetained);
        }
        let _guard = self.gate.lock().await;
        let store = self.owner.native_storage().await?;
        let result = match lease {
            HistoryCollectionLease::Environment(lease) => store.append_history_observed(lease, sample, retention).await,
            HistoryCollectionLease::Unassigned(lease) => {
                store.append_unassigned_history_observed(lease, sample, retention).await
            }
        };
        match result {
            Ok(report) => {
                if store.engine() == StorageEngine::File && report.write == HistoryWriteOutcome::Committed {
                    self.io_evidence.write().record_write(true);
                }
                Ok(report.outcome)
            }
            Err(error) => {
                if error.io_operation() == Some(StorageIoOperation::Write) {
                    self.io_evidence.write().record_write(false);
                }
                Err(error.into())
            }
        }
    }
}

fn valid_sample(sample: Vec<HistoryPoint>) -> Vec<HistoryPoint> {
    sample
        .into_iter()
        .filter(|point| {
            point.value.is_finite() && !point.series_identity.trim().is_empty() && point.series_identity.len() <= 1_024
        })
        .collect()
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
                let authority = store.owner.native_storage().await.unwrap();
                let mut snapshot = authority.load_desktop().await.unwrap().unwrap();
                for id in [&first, &second] {
                    snapshot
                        .document
                        .profiles
                        .push(rocketmq_dashboard_storage::EnvironmentProfile::from_config(
                            id.clone(),
                            id.as_str().into(),
                            &rocketmq_dashboard_storage::DesktopConfig::default(),
                        ));
                }
                authority
                    .compare_exchange_desktop(snapshot.storage_revision, snapshot.document)
                    .await
                    .unwrap();
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
            let migrated: HistoryDocument = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            assert_eq!(migrated.schema_version, HISTORY_SCHEMA_VERSION);
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
