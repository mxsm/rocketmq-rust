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

//! Owned, cancellable collection of successful real metric samples.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use rocketmq_dashboard_common::{EnvironmentConnectionSnapshot, EnvironmentId, HistoryPoint, HistoryRetention};
use rocketmq_runtime::ChildServiceContext;
use tokio_util::sync::CancellationToken;

use crate::state::UiError;

use super::{
    config_store::ConfigStoreFailure,
    history_health::{CollectionAttempt, CollectionSide},
    history_store::{HistoryAppendOutcome, HistoryCollectionLease, HistoryStore},
};

/// Boxed sampling operation implemented by the Dashboard service boundary.
pub type HistorySampleFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<HistoryPoint>, UiError>> + Send + 'a>>;

pub struct EnvironmentHistorySample {
    pub environment_id: Option<EnvironmentId>,
    pub points: Vec<HistoryPoint>,
    pub failed_resources: usize,
}

pub type EnvironmentHistorySampleFuture<'a> =
    Pin<Box<dyn Future<Output = Result<EnvironmentHistorySample, UiError>> + Send + 'a>>;

/// Produces one set of real observations. Missing resources produce no points.
pub trait HistorySampler: Send + Sync {
    /// Samples current Topic metrics independently from Broker availability.
    fn sample_topics(&self) -> HistorySampleFuture<'_>;

    /// Samples current Broker metrics independently from Topic availability.
    fn sample_brokers(&self) -> HistorySampleFuture<'_>;

    fn sample_topics_scoped(
        &self,
        _scope: Option<EnvironmentConnectionSnapshot>,
    ) -> EnvironmentHistorySampleFuture<'_> {
        Box::pin(async {
            self.sample_topics().await.map(|points| EnvironmentHistorySample {
                failed_resources: 0,
                environment_id: None,
                points,
            })
        })
    }

    fn sample_brokers_scoped(
        &self,
        _scope: Option<EnvironmentConnectionSnapshot>,
    ) -> EnvironmentHistorySampleFuture<'_> {
        Box::pin(async {
            self.sample_brokers().await.map(|points| EnvironmentHistorySample {
                failed_resources: 0,
                environment_id: None,
                points,
            })
        })
    }
}

/// Owned collector lifecycle bound to the existing application runtime context.
pub struct HistoryLifecycle {
    task: Option<tokio::task::JoinHandle<()>>,
    cancellation: Option<CancellationToken>,
    interval_seconds: u64,
    retention: HistoryRetention,
}

impl HistoryLifecycle {
    /// Starts collection. An interval of zero explicitly disables the collector.
    pub fn start(
        context: &ChildServiceContext,
        interval_seconds: u64,
        retention: HistoryRetention,
        store: Arc<HistoryStore>,
        sampler: Arc<dyn HistorySampler>,
    ) -> Result<Self, ConfigStoreFailure> {
        if interval_seconds == 0 {
            return Ok(Self {
                task: None,
                cancellation: None,
                interval_seconds,
                retention,
            });
        }

        let cancellation = context.task_spawner().cancellation_token().child_token();
        let task_cancellation = cancellation.clone();
        let (_, task) = context
            .task_group()
            .spawn_service_with_handle("gpui-history-collector", async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        _ = task_cancellation.cancelled() => break,
                        _ = interval.tick() => {}
                    }
                    // A started sample owns its completion. Shutdown cancels the next iteration and
                    // awaits this operation instead of detaching or partially persisting a sample.
                    collect_with_lease(
                        &store,
                        sampler.as_ref(),
                        retention,
                        Duration::from_secs(interval_seconds.saturating_add(60).max(120)),
                    )
                    .await;
                }
                if store.release_collection().await.is_err() {
                    tracing::warn!(
                        outcome = "history_lease_release_failed",
                        "History ownership will expire at the authority"
                    );
                }
            })
            .map_err(ConfigStoreFailure::runtime)?;
        Ok(Self {
            task: Some(task),
            cancellation: Some(cancellation),
            interval_seconds,
            retention,
        })
    }

    /// Returns whether collection is enabled and owned by the runtime.
    pub fn is_started(&self) -> bool {
        self.task.is_some()
    }

    pub fn is_running(&self) -> bool {
        self.task.as_ref().is_some_and(|task| !task.is_finished())
    }

    #[cfg(test)]
    pub(crate) async fn finish_for_test(&mut self) {
        self.cancellation.as_ref().expect("started collector").cancel();
        // Keep the completed handle, as happens when a worker exits before reconciliation.
        while !self.task.as_ref().expect("owned task").is_finished() {
            tokio::task::yield_now().await;
        }
    }

    /// Returns whether the running collector already uses the requested bounded settings.
    pub fn matches_settings(&self, interval_seconds: u64, retention: HistoryRetention) -> bool {
        self.interval_seconds == interval_seconds && self.retention == retention
    }

    /// Cancels the next collection iteration and awaits all owned work.
    pub async fn stop(&mut self) -> Result<(), ConfigStoreFailure> {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.await.map_err(ConfigStoreFailure::runtime)
    }
}

impl Drop for HistoryLifecycle {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
    }
}

#[cfg(test)]
pub(in crate::infrastructure) async fn collect_once(
    store: &HistoryStore,
    sampler: &dyn HistorySampler,
    retention: HistoryRetention,
) {
    collect_with_lease(store, sampler, retention, Duration::from_secs(120)).await;
}

async fn collect_with_lease(
    store: &HistoryStore,
    sampler: &dyn HistorySampler,
    retention: HistoryRetention,
    ttl: Duration,
) {
    let scope = match store.authoritative_environment_snapshot().await {
        Ok(snapshot) => snapshot,
        Err(_) => {
            let environment = store
                .environment_snapshot()
                .ok()
                .flatten()
                .map(|snapshot| snapshot.environment_id);
            for side in [CollectionSide::Topics, CollectionSide::Brokers] {
                store.record_collection(
                    environment.clone(),
                    side,
                    epoch_ms(),
                    0,
                    CollectionAttempt::PersistenceFailed,
                );
            }
            return;
        }
    };
    let environment = scope.as_ref().map(|snapshot| snapshot.environment_id.clone());
    let lease = match store.collection_lease(environment.clone(), ttl).await {
        Ok(Some(lease)) => lease,
        result => {
            let attempt = if result.is_ok() {
                CollectionAttempt::OwnershipUnavailable
            } else {
                CollectionAttempt::PersistenceFailed
            };
            for side in [CollectionSide::Topics, CollectionSide::Brokers] {
                store.record_collection(environment.clone(), side, epoch_ms(), 0, attempt);
            }
            return;
        }
    };
    tokio::join!(
        sample_and_persist(
            store,
            CollectionSide::Topics,
            lease.clone(),
            sampler.sample_topics_scoped(scope.clone()),
            retention
        ),
        sample_and_persist(
            store,
            CollectionSide::Brokers,
            lease,
            sampler.sample_brokers_scoped(scope),
            retention
        ),
    );
}

async fn sample_and_persist(
    store: &HistoryStore,
    side: CollectionSide,
    lease: HistoryCollectionLease,
    sample: EnvironmentHistorySampleFuture<'_>,
    retention: HistoryRetention,
) {
    let environment = lease.environment().cloned();
    persist_captured(store, side, environment, lease, sample.await, retention).await;
}

#[cfg(test)]
async fn persist_side(
    store: &HistoryStore,
    side: CollectionSide,
    environment: Option<EnvironmentId>,
    result: Result<EnvironmentHistorySample, UiError>,
    retention: HistoryRetention,
) {
    let lease = store
        .collection_lease(environment.clone(), Duration::from_secs(120))
        .await
        .unwrap()
        .unwrap();
    persist_captured(store, side, environment, lease, result, retention).await;
}

async fn persist_captured(
    store: &HistoryStore,
    side: CollectionSide,
    environment: Option<EnvironmentId>,
    lease: HistoryCollectionLease,
    result: Result<EnvironmentHistorySample, UiError>,
    retention: HistoryRetention,
) {
    match result {
        Ok(sample) if sample.points.is_empty() => {
            store.record_collection(
                sample.environment_id.or(environment),
                side,
                epoch_ms(),
                sample.failed_resources,
                CollectionAttempt::NoObservations,
            );
        }
        Ok(sample) => {
            let sample_environment = sample.environment_id.clone();
            let failed_resources = sample.failed_resources;
            let result = if sample.environment_id.as_ref() == lease.environment() {
                store.append_captured(lease, sample.points, retention).await
            } else {
                Err(ConfigStoreFailure::stale_revision())
            };
            store.record_collection(
                sample_environment,
                side,
                epoch_ms(),
                failed_resources,
                match result {
                    Ok(HistoryAppendOutcome::Retained) => CollectionAttempt::Persisted,
                    Ok(HistoryAppendOutcome::NotRetained) => CollectionAttempt::NotRetained,
                    Err(_) => CollectionAttempt::PersistenceFailed,
                },
            );
            if result.is_err() {
                tracing::warn!(
                    history_side = side.label(),
                    outcome = "persist_failed",
                    "History sample was not persisted"
                );
            }
        }
        Err(_) => {
            store.record_collection(environment, side, epoch_ms(), 1, CollectionAttempt::SamplingFailed);
            tracing::warn!(
                history_side = side.label(),
                outcome = "sample_failed",
                "History sample was unavailable"
            );
        }
    }
}

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rocketmq_dashboard_common::HistoryMetricKind;
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

    use crate::state::UiErrorCode;

    use super::*;

    struct CapturedEnvironmentSampler {
        configuration: Arc<crate::infrastructure::config_store::DesktopConfigStore>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl HistorySampler for CapturedEnvironmentSampler {
        fn sample_topics(&self) -> HistorySampleFuture<'_> {
            Box::pin(std::future::ready(Ok(Vec::new())))
        }

        fn sample_brokers(&self) -> HistorySampleFuture<'_> {
            self.sample_topics()
        }

        fn sample_topics_scoped(
            &self,
            scope: Option<EnvironmentConnectionSnapshot>,
        ) -> EnvironmentHistorySampleFuture<'_> {
            Box::pin(async move {
                let snapshot = scope.expect("captured environment");
                assert_eq!(snapshot, self.configuration.environment_snapshot().unwrap());
                self.entered.notify_one();
                self.release.notified().await;
                Ok(EnvironmentHistorySample {
                    failed_resources: 0,
                    environment_id: Some(snapshot.environment_id),
                    points: vec![HistoryPoint {
                        metric: HistoryMetricKind::TopicMessages,
                        series_identity: "orders".into(),
                        timestamp_epoch_ms: 1,
                        value: 3.0,
                        source_revision: snapshot.connection.revision,
                    }],
                })
            })
        }
    }

    #[test]
    fn environment_change_during_collection_cannot_reassign_the_started_sample() {
        use rocketmq_dashboard_common::{OperationActor, OperationResource};

        use crate::infrastructure::{config_store::DesktopConfigStore, environment_store::EnvironmentChange};

        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let context = runtime.root_context().component("collection");
        let configuration = DesktopConfigStore::new(directory.path().join("config.json"), context.clone());
        let history = HistoryStore::with_config_store(
            directory.path().join("history.json"),
            context,
            Arc::clone(&configuration),
        );
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let sampler = CapturedEnvironmentSampler {
            configuration: Arc::clone(&configuration),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        };
        runtime.block_on(async {
            let config = configuration.load().await.unwrap();
            let first = configuration.environments().unwrap().active_id;
            let collecting = collect_once(&history, &sampler, retention(10));
            tokio::pin!(collecting);
            tokio::select! {
                _ = entered.notified() => {},
                _ = &mut collecting => panic!("collection completed before release"),
            }
            let context = configuration
                .operation_context(
                    config.revision,
                    OperationActor::LocalDesktop,
                    OperationResource::Configuration,
                )
                .unwrap();
            configuration
                .change_environment(EnvironmentChange::Create { name: "Staging".into() }, context)
                .await
                .unwrap();
            let second = configuration.environments().unwrap().active_id;
            release.notify_one();
            collecting.await;
            assert_eq!(
                history
                    .query_environment(first, HistoryMetricKind::TopicMessages, "orders".into(), 0, 2)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert!(
                history
                    .query_environment(second, HistoryMetricKind::TopicMessages, "orders".into(), 0, 2)
                    .await
                    .unwrap()
                    .is_empty()
            );
        });
        runtime.shutdown_runtime_blocking().unwrap();
    }

    struct FailedSampler;

    impl HistorySampler for FailedSampler {
        fn sample_topics(&self) -> HistorySampleFuture<'_> {
            Box::pin(std::future::ready(Err(UiError::new(
                "Unable to sample dashboard metrics.",
                UiErrorCode::Connection,
                true,
            ))))
        }

        fn sample_brokers(&self) -> HistorySampleFuture<'_> {
            self.sample_topics()
        }
    }

    struct BlockingSampler {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        samples: AtomicUsize,
    }

    impl HistorySampler for BlockingSampler {
        fn sample_topics(&self) -> HistorySampleFuture<'_> {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                let sequence = self.samples.fetch_add(1, Ordering::SeqCst) as u64;
                Ok(vec![HistoryPoint {
                    metric: HistoryMetricKind::TopicMessages,
                    series_identity: "orders".into(),
                    timestamp_epoch_ms: sequence,
                    value: sequence as f64,
                    source_revision: 2,
                }])
            })
        }

        fn sample_brokers(&self) -> HistorySampleFuture<'_> {
            Box::pin(std::future::ready(Ok(Vec::new())))
        }
    }

    struct BrokerOnlySampler;

    impl HistorySampler for BrokerOnlySampler {
        fn sample_topics(&self) -> HistorySampleFuture<'_> {
            FailedSampler.sample_topics()
        }

        fn sample_brokers(&self) -> HistorySampleFuture<'_> {
            Box::pin(std::future::ready(Ok(vec![HistoryPoint {
                metric: HistoryMetricKind::BrokerProduceTps,
                series_identity: "broker".into(),
                timestamp_epoch_ms: 1,
                value: 2.0,
                source_revision: 2,
            }])))
        }
    }

    fn retention(max_points_per_series: usize) -> HistoryRetention {
        HistoryRetention {
            max_points_per_series,
            max_series: 10,
            max_total_points: 100,
        }
    }

    fn runtime() -> RuntimeOwner {
        RuntimeOwner::plan(RuntimeConfig::for_parallelism("history-collector-test", 2))
            .expect("test runtime configuration is valid")
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory"))
            .build()
            .expect("runtime")
    }

    #[test]
    fn failed_sampling_never_writes_history() {
        let directory = tempfile::tempdir().expect("directory");
        let runtime = runtime();
        let store = HistoryStore::new(
            directory.path().join("history.json"),
            runtime.root_context().component("history"),
        );
        runtime.block_on(collect_once(&store, &FailedSampler, retention(10)));
        assert!(!directory.path().join("history.json").exists());
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }

    #[test]
    fn one_failed_side_does_not_discard_the_other_successful_side() {
        let directory = tempfile::tempdir().expect("directory");
        let runtime = runtime();
        let store = HistoryStore::new(
            directory.path().join("history.json"),
            runtime.root_context().component("history"),
        );
        runtime.block_on(collect_once(&store, &BrokerOnlySampler, retention(10)));
        assert_eq!(runtime.block_on(store.points()).expect("broker points").len(), 1);
        let mut health = store.collection_health().unwrap();
        assert!(health.topics.failed);
        assert!(health.topics.last_success_epoch_ms.is_none());
        assert!(health.brokers.last_success_epoch_ms.is_some());
        health.enabled = true;
        let latest = health.latest_success().unwrap();
        assert_eq!(
            health.status(latest),
            super::super::history_health::CollectionStatus::Partial
        );
        runtime.block_on(collect_once(&store, &FailedSampler, retention(10)));
        assert_eq!(store.collection_health().unwrap().latest_success(), Some(latest));
        assert_eq!(runtime.block_on(store.points()).unwrap().len(), 1);
        assert_eq!(
            health.status(latest + 180_001),
            super::super::history_health::CollectionStatus::Stale
        );
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }

    #[test]
    fn disabled_collector_has_no_task_and_started_sample_is_awaited_on_cancel() {
        let directory = tempfile::tempdir().expect("directory");
        let runtime = runtime();
        let context = runtime.root_context().component("history");
        let store = HistoryStore::new(directory.path().join("history.json"), context.clone());
        let disabled = HistoryLifecycle::start(&context, 0, retention(10), Arc::clone(&store), Arc::new(FailedSampler))
            .expect("disabled lifecycle");
        assert!(!disabled.is_started());
        assert!(disabled.matches_settings(0, retention(10)));

        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let sampler = Arc::new(BlockingSampler {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            samples: AtomicUsize::new(0),
        });
        let mut lifecycle = HistoryLifecycle::start(&context, 60, retention(10), Arc::clone(&store), sampler)
            .expect("started lifecycle");
        assert!(lifecycle.matches_settings(60, retention(10)));
        assert!(!lifecycle.matches_settings(30, retention(10)));
        assert!(!lifecycle.matches_settings(60, retention(20)));
        runtime.block_on(async {
            entered.notified().await;
            let stop = lifecycle.stop();
            let release_sample = async {
                release.notify_one();
            };
            let (stopped, ()) = tokio::join!(stop, release_sample);
            stopped.expect("collector stopped");
            assert_eq!(store.points().await.expect("retained point").len(), 1);
        });
        runtime.shutdown_runtime_blocking().expect("shutdown");
    }

    struct EmptySampler;
    impl HistorySampler for EmptySampler {
        fn sample_topics(&self) -> HistorySampleFuture<'_> {
            Box::pin(std::future::ready(Ok(Vec::new())))
        }
        fn sample_brokers(&self) -> HistorySampleFuture<'_> {
            self.sample_topics()
        }
    }

    #[test]
    fn collection_evidence_separates_sampling_persistence_empty_and_recovered_samples() {
        use crate::infrastructure::local_storage_status::{FileReadState, FileWriteState};
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let path = directory.path().join("history.json");
        let store = HistoryStore::new(path.clone(), runtime.root_context().component("history"));
        runtime.block_on(collect_once(&store, &FailedSampler, retention(10)));
        assert_eq!(
            store.collection_health().unwrap().brokers.last_attempt,
            Some(CollectionAttempt::SamplingFailed)
        );
        assert!(!path.exists());
        std::fs::create_dir(&path).unwrap();
        runtime.block_on(collect_once(&store, &BrokerOnlySampler, retention(10)));
        assert_eq!(
            store.collection_health().unwrap().brokers.last_attempt,
            Some(CollectionAttempt::PersistenceFailed)
        );
        let failed = runtime.block_on(store.inspect_storage());
        assert_eq!(failed.evidence.read, FileReadState::Failed);
        assert_eq!(
            failed.evidence.last_write,
            FileWriteState::NotObserved,
            "a failed read before append is not an attempted write"
        );
        std::fs::remove_dir(&path).unwrap();
        runtime.block_on(collect_once(&store, &BrokerOnlySampler, retention(10)));
        let persisted = store.collection_health().unwrap().brokers;
        assert_eq!(persisted.last_attempt, Some(CollectionAttempt::Persisted));
        assert!(persisted.last_success_epoch_ms.is_some());
        let recovered = runtime.block_on(store.inspect_storage());
        assert_eq!(recovered.evidence.read, FileReadState::Readable);
        assert_eq!(recovered.evidence.last_write, FileWriteState::Succeeded);
        runtime.block_on(collect_once(&store, &EmptySampler, retention(10)));
        let empty = store.collection_health().unwrap().brokers;
        assert_eq!(empty.last_attempt, Some(CollectionAttempt::NoObservations));
        assert!(!empty.failed);
        assert_eq!(empty.last_success_epoch_ms, persisted.last_success_epoch_ms);
        runtime.shutdown_runtime_blocking().unwrap();
    }

    #[test]
    fn readable_history_retains_actual_atomic_write_failure_evidence() {
        use crate::infrastructure::local_storage_status::{FileReadState, FileWriteState};
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        // The data filename is valid on NTFS/ext4; its atomic candidate exceeds the component limit.
        let path = directory.path().join(format!("{}.json", "h".repeat(245)));
        std::fs::write(&path, br#"{"schemaVersion":2,"points":[],"legacyNotes":[]}"#).unwrap();
        let store = HistoryStore::new(path, runtime.root_context().component("history"));
        runtime.block_on(collect_once(&store, &BrokerOnlySampler, retention(10)));
        assert_eq!(
            store.collection_health().unwrap().brokers.last_attempt,
            Some(CollectionAttempt::PersistenceFailed)
        );
        let status = runtime.block_on(store.inspect_storage());
        assert_eq!(status.evidence.read, FileReadState::Readable);
        assert_eq!(status.evidence.last_write, FileWriteState::Failed);
        assert!(status.evidence.last_successful_write_epoch_ms.is_none());
        assert!(runtime.block_on(store.points()).unwrap().is_empty());
        assert_eq!(
            runtime.block_on(store.inspect_storage()).evidence.last_write,
            FileWriteState::Failed,
            "a successful read must not erase write-failure evidence"
        );
        runtime.shutdown_runtime_blocking().unwrap();
    }

    #[test]
    fn zero_retention_does_not_report_a_stored_sample_but_records_the_empty_file_write() {
        use crate::infrastructure::local_storage_status::{FileReadState, FileWriteState};
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        for (index, limits) in [
            HistoryRetention {
                max_points_per_series: 0,
                ..retention(10)
            },
            HistoryRetention {
                max_series: 0,
                ..retention(10)
            },
            HistoryRetention {
                max_total_points: 0,
                ..retention(10)
            },
        ]
        .into_iter()
        .enumerate()
        {
            let store = HistoryStore::new(
                directory.path().join(format!("history-{index}.json")),
                runtime.root_context().component("history"),
            );
            runtime.block_on(collect_once(&store, &BrokerOnlySampler, retention(10)));
            let successful_sample = store.collection_health().unwrap().brokers.last_success_epoch_ms;
            assert!(successful_sample.is_some());
            runtime.block_on(collect_once(&store, &BrokerOnlySampler, limits));
            let health = store.collection_health().unwrap().brokers;
            assert_eq!(health.last_attempt, Some(CollectionAttempt::NotRetained));
            assert_eq!(health.last_success_epoch_ms, successful_sample);
            assert!(!health.failed);
            assert!(runtime.block_on(store.points()).unwrap().is_empty());
            let status = runtime.block_on(store.inspect_storage());
            assert_eq!(status.evidence.read, FileReadState::Readable);
            assert_eq!(status.evidence.last_write, FileWriteState::Succeeded);
            assert!(status.evidence.last_successful_write_epoch_ms.is_some());
        }
        runtime.shutdown_runtime_blocking().unwrap();
    }

    #[test]
    fn rejected_and_evicted_samples_do_not_advance_last_stored_sample() {
        use crate::infrastructure::local_storage_status::FileWriteState;
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let store = HistoryStore::new(
            directory.path().join("history.json"),
            runtime.root_context().component("history"),
        );
        let sample = |timestamp, value| {
            Ok(EnvironmentHistorySample {
                environment_id: None,
                failed_resources: 0,
                points: vec![HistoryPoint {
                    metric: HistoryMetricKind::BrokerCount,
                    series_identity: "registered".into(),
                    timestamp_epoch_ms: timestamp,
                    value,
                    source_revision: 0,
                }],
            })
        };
        runtime.block_on(persist_side(
            &store,
            CollectionSide::Brokers,
            None,
            sample(1, f64::NAN),
            retention(1),
        ));
        let rejected = store.collection_health().unwrap().brokers;
        assert_eq!(rejected.last_attempt, Some(CollectionAttempt::NotRetained));
        assert!(rejected.last_success_epoch_ms.is_none());
        let untouched = runtime.block_on(store.inspect_storage());
        assert_eq!(untouched.evidence.last_write, FileWriteState::NotObserved);
        assert!(!untouched.path.exists());
        runtime.block_on(persist_side(
            &store,
            CollectionSide::Brokers,
            None,
            sample(100, 2.0),
            retention(1),
        ));
        let success = store.collection_health().unwrap().brokers.last_success_epoch_ms;
        assert!(success.is_some());
        runtime.block_on(persist_side(
            &store,
            CollectionSide::Brokers,
            None,
            sample(1, 1.0),
            retention(1),
        ));
        let evicted = store.collection_health().unwrap().brokers;
        assert_eq!(evicted.last_attempt, Some(CollectionAttempt::NotRetained));
        assert_eq!(evicted.last_success_epoch_ms, success);
        let retained = runtime.block_on(store.points()).unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].timestamp_epoch_ms, 100);
        runtime.shutdown_runtime_blocking().unwrap();
    }
}
