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

//! Actual native adapters against two independent database clients, without a Web backend.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rocketmq_dashboard_common::{
    AuditAcceptedOperation, AuditAction, AuditFacts, AuditFilter, AuditOutcome, AuditQuery,
    EnvironmentConnectionSnapshot, HistoryMetricKind, HistoryPoint, HistoryRetention, MonitorMutationOutcome,
    MonitorRuleChange, MonitorRuleMutation, MonitorThresholds, OperationActor, OperationResource,
};
use rocketmq_dashboard_storage::{
    DashboardStore, DatabaseConnection, DesktopDocument, StorageEngine, StorageErrorKind, StorageOptions,
};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::{
    audit_store::AuditStore,
    config_store::{DesktopConfig, DesktopConfigStore},
    history_collector::{
        EnvironmentHistorySample, EnvironmentHistorySampleFuture, HistorySampleFuture, HistorySampler, collect_once,
    },
    history_health::CollectionAttempt,
    history_store::HistoryStore,
    local_storage_status::FileWriteState,
    monitor_store::MonitorStore,
};

struct CountingSampler {
    calls: AtomicUsize,
    series: String,
    timestamp: u64,
}

impl CountingSampler {
    fn sample(
        &self,
        scope: Option<EnvironmentConnectionSnapshot>,
        metric: HistoryMetricKind,
    ) -> EnvironmentHistorySampleFuture<'_> {
        // Count construction, not merely polling: a standby client must not start any RPC work.
        self.calls.fetch_add(1, Ordering::SeqCst);
        let scope = scope.expect("native environment scope captured before ownership");
        Box::pin(std::future::ready(Ok(EnvironmentHistorySample {
            environment_id: Some(scope.environment_id),
            points: vec![HistoryPoint {
                metric,
                series_identity: self.series.clone(),
                timestamp_epoch_ms: self.timestamp,
                value: 3.0,
                source_revision: scope.connection.revision,
            }],
            failed_resources: 0,
        })))
    }
}

impl HistorySampler for CountingSampler {
    fn sample_topics(&self) -> HistorySampleFuture<'_> {
        panic!("native collector must capture its scope");
    }
    fn sample_brokers(&self) -> HistorySampleFuture<'_> {
        panic!("native collector must capture its scope");
    }
    fn sample_topics_scoped(&self, scope: Option<EnvironmentConnectionSnapshot>) -> EnvironmentHistorySampleFuture<'_> {
        self.sample(scope, HistoryMetricKind::TopicMessages)
    }
    fn sample_brokers_scoped(
        &self,
        scope: Option<EnvironmentConnectionSnapshot>,
    ) -> EnvironmentHistorySampleFuture<'_> {
        self.sample(scope, HistoryMetricKind::BrokerProduceTps)
    }
}

fn exercise_native_adapters(engine: StorageEngine, url: String) {
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("gpui-native-adapters", 2))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(512 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    runtime.block_on(async {
        let context = runtime.root_context().component("native-adapters");
        let options = || StorageOptions::Database {
            engine,
            connection: DatabaseConnection::new(url.clone()),
        };
        let first = Arc::new(DashboardStore::open(options(), context.clone()).await.unwrap());
        let second = Arc::new(DashboardStore::open(options(), context.clone()).await.unwrap());
        let path = directory.path().join("config.json");
        let a = DesktopConfigStore::with_native_storage(path.clone(), context.clone(), Arc::clone(&first));
        let b = DesktopConfigStore::with_native_storage(path.clone(), context.clone(), Arc::clone(&second));
        if first.load_desktop().await.unwrap().is_none() {
            assert!(
                a.load().await.is_err(),
                "empty SQL must request explicit initialization"
            );
            assert!(!path.exists(), "empty SQL must never fall back to a local file");
            first
                .initialize(DesktopDocument::from_legacy(DesktopConfig::default(), Vec::new()).unwrap())
                .await
                .unwrap();
        }
        let original = a.load().await.unwrap();
        assert_eq!(b.load().await.unwrap(), original);
        assert!(Arc::ptr_eq(&first, &a.native_storage().await.unwrap()));
        assert!(Arc::ptr_eq(&second, &b.native_storage().await.unwrap()));
        assert!(!a.owns_writer(), "native SQL does not claim the file directory writer");
        let mut changes = a.configuration_changes();
        changes.borrow_and_update();
        let suffix = rand::random::<u64>();
        let actor = format!("native-adapter-{suffix}");
        let group = format!("native-workers-{suffix}");
        let accepted = AuditAcceptedOperation {
            context: b
                .operation_context(
                    original.revision,
                    OperationActor::LocalOperator {
                        username: actor.clone(),
                    },
                    OperationResource::Topic(group.clone()),
                )
                .unwrap(),
            action: AuditAction::TopicCreated,
            accepted_at_epoch_ms: now_ms(),
            topic_fields: Default::default(),
            consumer_fields: Default::default(),
        };
        let audit_a = AuditStore::new(Arc::clone(&a));
        let audit_b = AuditStore::new(Arc::clone(&b));
        let before_batch = first.load_desktop().await.unwrap().unwrap();
        assert!(
            audit_b
                .accept_batch(vec![accepted.clone(), accepted.clone()])
                .await
                .is_err()
        );
        assert_eq!(
            first.load_desktop().await.unwrap().unwrap(),
            before_batch,
            "failed batch admission must not orphan an earlier entry"
        );
        let stale = first.load_desktop().await.unwrap().unwrap();
        audit_b.accept(accepted.clone()).await.unwrap();
        assert!(audit_a.pending().await.unwrap().contains(&accepted));
        assert!(
            !changes.has_changed().unwrap(),
            "journal-only writes cannot invalidate the provider"
        );
        assert_eq!(
            first
                .compare_exchange_desktop(stale.storage_revision, stale.document)
                .await
                .unwrap_err()
                .kind(),
            StorageErrorKind::Conflict
        );

        let monitor_a = MonitorStore::with_config_store(Arc::clone(&a));
        let monitor_b = MonitorStore::with_config_store(Arc::clone(&b));
        let monitor_context = b
            .operation_context(
                original.revision,
                OperationActor::LocalOperator {
                    username: actor.clone(),
                },
                OperationResource::Monitor(group.clone()),
            )
            .unwrap();
        let request = MonitorRuleMutation {
            environment_id: monitor_context.environment_id.clone(),
            consumer_group: group.clone(),
            expected_revision: 0,
            change: MonitorRuleChange::Save {
                thresholds: MonitorThresholds {
                    min_count: 1,
                    max_diff_total: 300,
                },
                enabled: true,
                adopt_legacy_id: None,
            },
        };
        assert!(matches!(
            monitor_b
                .commit(request.clone(), monitor_context.clone())
                .await
                .unwrap(),
            MonitorMutationOutcome::Saved(_)
        ));
        assert!(matches!(
            monitor_a
                .commit(
                    request,
                    a.operation_context(
                        original.revision,
                        OperationActor::System,
                        OperationResource::Monitor(group.clone())
                    )
                    .unwrap()
                )
                .await
                .unwrap(),
            MonitorMutationOutcome::Conflict { .. }
        ));
        let observed = first.load_desktop().await.unwrap().unwrap();
        assert!(
            observed
                .document
                .monitors
                .rules
                .iter()
                .any(|rule| rule.rule.consumer_group == group)
        );
        let scope = a.environment_snapshot().unwrap();
        assert!(
            monitor_a
                .snapshot(scope.clone())
                .await
                .unwrap()
                .rules
                .iter()
                .any(|rule| rule.consumer_group == group)
        );
        assert_eq!(a.load().await.unwrap().revision, original.revision);
        assert!(!changes.has_changed().unwrap());

        let mut next = b.load().await.unwrap();
        next.nameservers.push(format!("native-{suffix}:9876"));
        let saved = b.save_next(next).await.unwrap();
        assert!(
            a.save_next(original).await.is_err(),
            "a stale configuration cannot overwrite a peer commit"
        );
        assert_eq!(a.load().await.unwrap(), saved);
        assert_eq!(
            a.inspect_storage().await.evidence.last_write,
            FileWriteState::NotObserved
        );
        assert!(changes.has_changed().unwrap());
        assert_eq!(changes.borrow_and_update().as_ref().unwrap().revision, saved.revision);
        audit_a
            .finish(
                accepted.clone(),
                AuditOutcome::Succeeded,
                AuditFacts::default(),
                now_ms(),
            )
            .await
            .unwrap();
        let terminal_revision = first.load_desktop().await.unwrap().unwrap().storage_revision;
        audit_b
            .finish(
                accepted.clone(),
                AuditOutcome::Succeeded,
                AuditFacts::default(),
                now_ms(),
            )
            .await
            .unwrap();
        assert_eq!(
            first.load_desktop().await.unwrap().unwrap().storage_revision,
            terminal_revision,
            "terminal retry must be idempotent"
        );
        assert!(
            audit_a.accept(accepted.clone()).await.is_err(),
            "new admission must reject the old configuration context"
        );
        let page = audit_b
            .query(AuditQuery {
                filter: AuditFilter {
                    start_epoch_ms: now_ms().saturating_sub(60_000),
                    end_epoch_ms: now_ms().saturating_add(1),
                    actor: Some(OperationActor::LocalOperator { username: actor }),
                    action: None,
                    outcome: None,
                    environment_id: None,
                },
                limit: 100,
                cursor: None,
            })
            .await
            .unwrap();
        assert!(
            page.events
                .iter()
                .any(|event| event.operation.context.request_id == accepted.context.request_id)
        );
        assert!(
            page.events
                .iter()
                .any(|event| event.operation.context.request_id == monitor_context.request_id)
        );
        assert!(
            audit_b
                .pending()
                .await
                .unwrap()
                .iter()
                .all(|operation| operation.context.request_id != accepted.context.request_id)
        );

        let history_a =
            HistoryStore::with_config_store(path.with_file_name("history.json"), context.clone(), Arc::clone(&a));
        let history_b = HistoryStore::with_config_store(path.with_file_name("history.json"), context, Arc::clone(&b));
        let scope = a.environment_snapshot().unwrap();
        assert!(
            history_a
                .collection_lease(Some(scope.environment_id.clone()), Duration::from_secs(120))
                .await
                .unwrap()
                .is_some()
        );
        let sampler = CountingSampler {
            calls: AtomicUsize::new(0),
            series: group.clone(),
            timestamp: now_ms(),
        };
        let retention = HistoryRetention {
            max_points_per_series: 50,
            max_series: 100,
            max_total_points: 1000,
        };
        collect_once(&history_b, &sampler, retention).await;
        assert_eq!(
            sampler.calls.load(Ordering::SeqCst),
            0,
            "a peer-owned collection must not construct sampler RPC futures"
        );
        let standby = history_b.collection_health().unwrap();
        assert_eq!(
            standby.topics.last_attempt,
            Some(CollectionAttempt::OwnershipUnavailable)
        );
        assert_eq!(
            standby.brokers.last_attempt,
            Some(CollectionAttempt::OwnershipUnavailable)
        );
        history_a.release_collection().await.unwrap();
        collect_once(&history_b, &sampler, retention).await;
        assert_eq!(sampler.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            history_a
                .query_environment(
                    scope.environment_id.clone(),
                    HistoryMetricKind::TopicMessages,
                    group.clone(),
                    sampler.timestamp,
                    sampler.timestamp
                )
                .await
                .unwrap()
                .len(),
            1
        );
        history_a
            .append_sample(
                vec![HistoryPoint {
                    metric: HistoryMetricKind::TopicMessages,
                    series_identity: group.clone(),
                    timestamp_epoch_ms: sampler.timestamp + 1,
                    value: 5.0,
                    source_revision: saved.revision,
                }],
                retention,
            )
            .await
            .unwrap();
        assert_eq!(
            history_a
                .query_environment(
                    scope.environment_id,
                    HistoryMetricKind::TopicMessages,
                    group,
                    sampler.timestamp,
                    sampler.timestamp + 1
                )
                .await
                .unwrap()
                .len(),
            1,
            "legacy unassigned points remain outside the active environment chart"
        );
        assert!(
            history_b
                .observations()
                .await
                .unwrap()
                .iter()
                .any(|point| point.environment_id.is_none() && point.point.timestamp_epoch_ms == sampler.timestamp + 1)
        );
        assert_eq!(
            history_b.inspect_storage().await.evidence.last_write,
            FileWriteState::NotObserved
        );
        history_a.release_collection().await.unwrap();
        history_b.release_collection().await.unwrap();
        a.close().await.unwrap();
        assert!(a.load().await.is_err());
        collect_once(&history_a, &sampler, retention).await;
        assert_eq!(
            sampler.calls.load(Ordering::SeqCst),
            2,
            "an unavailable authority prevents sampling"
        );
        assert_eq!(
            history_a.collection_health().unwrap().topics.last_attempt,
            Some(CollectionAttempt::PersistenceFailed)
        );
        assert_eq!(
            b.load().await.unwrap(),
            saved,
            "closing one pool must not close a peer authority"
        );
        b.close().await.unwrap();
        assert!(!path.exists(), "native SQL adapters must never create a file fallback");
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

fn now_ms() -> u64 {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap()
}

#[cfg(feature = "storage-sqlite")]
#[test]
fn sqlite_native_adapters() {
    let directory = tempfile::tempdir().unwrap();
    exercise_native_adapters(
        StorageEngine::Sqlite,
        format!(
            "sqlite://{}",
            directory
                .path()
                .join("native.sqlite3")
                .to_string_lossy()
                .replace('\\', "/")
        ),
    );
}

#[cfg(feature = "storage-mysql")]
#[test]
#[ignore = "requires isolated ROCKETMQ_DASHBOARD_NATIVE_MYSQL_TEST_URL"]
fn mysql_native_adapters() {
    exercise_native_adapters(
        StorageEngine::MySql,
        std::env::var("ROCKETMQ_DASHBOARD_NATIVE_MYSQL_TEST_URL").expect("dedicated native MySQL fixture"),
    );
}

#[cfg(feature = "storage-postgres")]
#[test]
#[ignore = "requires isolated ROCKETMQ_DASHBOARD_NATIVE_POSTGRES_TEST_URL"]
fn postgres_native_adapters() {
    exercise_native_adapters(
        StorageEngine::Postgres,
        std::env::var("ROCKETMQ_DASHBOARD_NATIVE_POSTGRES_TEST_URL").expect("dedicated native PostgreSQL fixture"),
    );
}
