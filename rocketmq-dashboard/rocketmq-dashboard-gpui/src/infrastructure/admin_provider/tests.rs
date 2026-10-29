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

//! Provider lifecycle, concurrency, redaction, and revision tests.

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use rocketmq_admin_core::core::{
    AdminResult,
    broker::{
        BrokerConfigPatchResult, PatchBrokerConfigRequest, QueryBrokerConfigGenerationRequest,
        QueryBrokerConfigGenerationResult,
    },
    dashboard::{
        DashboardBrokerConfig, DashboardBrokerInfo, DashboardBrokerList, DashboardBrokerRuntime, DashboardBrokerTarget,
        DashboardConsumerGroup, DashboardConsumerList, DashboardProducerInfo, DashboardTopicInfo, DashboardTopicList,
        DashboardTopicStats,
    },
    security::AdminCredentials,
};
use rocketmq_dashboard_common::{CredentialSourceKind, TransportSettings};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::infrastructure::{
    admin_session::{DashboardMutationSession, DashboardQuerySession, DashboardSessionFactory, SessionFuture},
    auth_state::MapEnvironment,
};

struct FixedClock;

impl HealthClock for FixedClock {
    fn now_epoch_ms(&self) -> Option<u64> {
        Some(42)
    }
}

#[derive(Default)]
struct FakeControls {
    dashboard_consumer_summary:
        std::sync::Mutex<Option<rocketmq_admin_core::core::dashboard_metrics::DashboardConsumerSummary>>,
    dashboard_topics: std::sync::Mutex<Option<Vec<String>>>,
    dashboard_rate_calls: std::sync::Mutex<Vec<Vec<String>>>,
    dashboard_stats_calls: std::sync::Mutex<Vec<String>>,
    query_active: AtomicUsize,
    query_max: AtomicUsize,
    mutation_active: AtomicUsize,
    mutation_max: AtomicUsize,
    query_shutdowns: AtomicUsize,
    mutation_shutdowns: AtomicUsize,
    query_creates: AtomicUsize,
    mutation_creates: AtomicUsize,
    topic_sends: std::sync::Mutex<Vec<(bool, usize)>>,
    topic_field_requests: std::sync::Mutex<Vec<rocketmq_admin_core::core::topic::TopicConfigFieldsPatchRequest>>,
    topic_field_report: std::sync::Mutex<Option<rocketmq_admin_core::core::topic::TopicConfigFieldsPatchReport>>,
}

struct FakeFactory {
    controls: Arc<FakeControls>,
    health: Result<(), AdminError>,
    query_barrier: Option<Arc<tokio::sync::Barrier>>,
    patch_outcome: BrokerConfigPatchResult,
}

impl DashboardSessionFactory for FakeFactory {
    fn create_query(
        &self,
        _snapshot: ConnectionSnapshot,
        _credentials: Option<AdminCredentials>,
    ) -> SessionFuture<'_, AdminResult<Box<dyn DashboardQuerySession>>> {
        self.controls.query_creates.fetch_add(1, Ordering::SeqCst);
        let session = FakeQuerySession {
            controls: Arc::clone(&self.controls),
            health: self.health.clone(),
            barrier: self.query_barrier.clone(),
        };
        let session: Box<dyn DashboardQuerySession> = Box::new(session);
        Box::pin(std::future::ready(Ok(session)))
    }

    fn create_mutation(
        &self,
        _snapshot: ConnectionSnapshot,
        _credentials: Option<AdminCredentials>,
    ) -> SessionFuture<'_, AdminResult<Box<dyn DashboardMutationSession>>> {
        self.controls.mutation_creates.fetch_add(1, Ordering::SeqCst);
        let session = FakeMutationSession {
            controls: Arc::clone(&self.controls),
            patch_outcome: self.patch_outcome,
        };
        let session: Box<dyn DashboardMutationSession> = Box::new(session);
        Box::pin(std::future::ready(Ok(session)))
    }
}

struct FakeQuerySession {
    controls: Arc<FakeControls>,
    health: Result<(), AdminError>,
    barrier: Option<Arc<tokio::sync::Barrier>>,
}

impl FakeQuerySession {
    fn enter_query(&self) -> usize {
        let active = self.controls.query_active.fetch_add(1, Ordering::SeqCst) + 1;
        self.controls.query_max.fetch_max(active, Ordering::SeqCst);
        active
    }

    fn leave_query(&self) {
        self.controls.query_active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl DashboardQuerySession for FakeQuerySession {
    fn topic_catalog(&self) -> SessionFuture<'_, AdminResult<rocketmq_admin_core::core::topic::DetailedTopicCatalog>> {
        use rocketmq_admin_core::core::topic::{
            DetailedTopicCatalog, DetailedTopicCatalogItem, TopicBrokerTarget, TopicInspectionCompleteness,
        };
        Box::pin(std::future::ready(Ok(DetailedTopicCatalog {
            items: vec![DetailedTopicCatalogItem {
                topic: "orders".into(),
                category: "APPLICATION".into(),
                message_type: Some("NORMAL".into()),
                clusters: vec!["cluster-a".into()],
                brokers: vec!["broker-a".into()],
                read_queue_count: Some(8),
                write_queue_count: Some(8),
                perm: Some(6),
                order: Some(false),
                system_topic: false,
            }],
            targets: Vec::new(),
            broker_targets: vec![TopicBrokerTarget {
                cluster_name: "cluster-a".into(),
                broker_name: "broker-a".into(),
                broker_addr: "broker-a:10911".into(),
            }],
            completeness: TopicInspectionCompleteness::Complete,
            failures: Vec::new(),
        })))
    }
    fn health(&self) -> SessionFuture<'_, AdminResult<()>> {
        Box::pin(async move {
            self.enter_query();
            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            self.leave_query();
            self.health.clone()
        })
    }

    fn list_topics(&self) -> SessionFuture<'_, AdminResult<DashboardTopicList>> {
        if let Some(names) = self.controls.dashboard_topics.lock().unwrap().clone() {
            return Box::pin(std::future::ready(Ok(DashboardTopicList {
                items: names
                    .into_iter()
                    .map(|topic| DashboardTopicInfo {
                        topic,
                        category: "NORMAL".into(),
                        broker_name: None,
                        read_queue_count: 0,
                        write_queue_count: 0,
                        perm: 0,
                    })
                    .collect(),
            })));
        }
        Box::pin(std::future::ready(Ok(DashboardTopicList {
            items: vec![DashboardTopicInfo {
                topic: "private-topic".into(),
                broker_name: Some("raw-topic-broker".into()),
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                category: "raw-topic-category".into(),
            }],
        })))
    }

    fn consumer_summary(
        &self,
    ) -> SessionFuture<'_, AdminResult<rocketmq_admin_core::core::dashboard_metrics::DashboardConsumerSummary>> {
        Box::pin(std::future::ready(Ok(self
            .controls
            .dashboard_consumer_summary
            .lock()
            .unwrap()
            .unwrap_or(
                rocketmq_admin_core::core::dashboard_metrics::DashboardConsumerSummary {
                    group_count: Some(1),
                    complete_backlog: None,
                    failed_targets: 1,
                },
            ))))
    }

    fn topic_rates<'a>(
        &'a self,
        topics: &'a [String],
    ) -> SessionFuture<'a, AdminResult<Vec<rocketmq_admin_core::core::dashboard_metrics::DashboardTopicRates>>> {
        self.controls.dashboard_rate_calls.lock().unwrap().push(topics.to_vec());
        Box::pin(std::future::ready(Ok(topics
            .iter()
            .map(
                |topic| rocketmq_admin_core::core::dashboard_metrics::DashboardTopicRates {
                    topic: topic.clone(),
                    produced_messages_24h: Some(if topic == "a-000" { 9_000 } else { 1 }),
                    produce_tps: if topic == "a-001" { None } else { Some(0.5) },
                    consume_tps: Some(0.0),
                    failed_targets: usize::from(topic == "a-001"),
                },
            )
            .collect())))
    }

    fn detailed_topic_stats<'a>(
        &'a self,
        topic: &'a str,
    ) -> SessionFuture<'a, AdminResult<rocketmq_admin_core::core::topic::DetailedTopicStats>> {
        self.controls
            .dashboard_stats_calls
            .lock()
            .unwrap()
            .push(topic.to_string());
        if topic == "a-002" {
            return Box::pin(std::future::ready(Err(AdminError::operation_failed(
                "topic_stats",
                "private backend body",
            ))));
        }
        Box::pin(std::future::ready(Ok(
            rocketmq_admin_core::core::topic::DetailedTopicStats {
                stats: rocketmq_admin_core::core::topic::TopicStats {
                    topic: topic.to_string(),
                    total_message_count: if topic == "a-000" { 50 } else { 0 },
                    queue_count: 1,
                    offsets: Vec::new(),
                },
                completeness: rocketmq_admin_core::core::topic::TopicInspectionCompleteness::Complete,
                failures: Vec::new(),
            },
        )))
    }

    fn topic_stats<'a>(&'a self, topic: &'a str) -> SessionFuture<'a, AdminResult<DashboardTopicStats>> {
        Box::pin(std::future::ready(Ok(DashboardTopicStats {
            topic: topic.into(),
            queue_count: 1,
            total_min_offset: 0,
            total_max_offset: 1,
        })))
    }

    fn list_consumers(&self) -> SessionFuture<'_, AdminResult<DashboardConsumerList>> {
        Box::pin(std::future::ready(Ok(DashboardConsumerList {
            items: vec![DashboardConsumerGroup {
                group: "private-consumer".into(),
                consume_type: "PUSH".into(),
                message_model: "CLUSTERING".into(),
                client_count: 1,
                diff_total: 9,
            }],
        })))
    }

    fn list_producers(&self) -> SessionFuture<'_, AdminResult<Vec<DashboardProducerInfo>>> {
        Box::pin(std::future::ready(Ok(vec![DashboardProducerInfo {
            topic: "private-topic".into(),
            producer_group: "private-producer".into(),
            connection_count: 3,
            broker_names: Vec::new(),
        }])))
    }

    fn list_brokers(&self) -> SessionFuture<'_, AdminResult<DashboardBrokerList>> {
        Box::pin(std::future::ready(Ok(DashboardBrokerList {
            clusters: vec!["cluster-a".into()],
            items: vec![DashboardBrokerInfo {
                cluster_name: "cluster-a".into(),
                broker_name: "broker-a".into(),
                broker_id: 0,
                address: "broker-a:10911".into(),
                role: "MASTER".into(),
                version: String::new(),
                produce_tps: 0.0,
                consume_tps: 0.0,
                runtime_entries: BTreeMap::from([
                    ("accessKey".into(), "raw-access-secret".into()),
                    ("tlsPrivateKey".into(), "raw-tls-key".into()),
                ]),
                runtime_error: Some("backend body token=raw-body-token".into()),
            }],
        })))
    }

    fn broker_runtime<'a>(
        &'a self,
        target: &'a DashboardBrokerTarget,
    ) -> SessionFuture<'a, AdminResult<DashboardBrokerRuntime>> {
        Box::pin(std::future::ready(Ok(DashboardBrokerRuntime {
            broker_name: target.broker_name.clone(),
            address: target.broker_addr.clone().unwrap_or_default(),
            entries: BTreeMap::from([
                ("brokerVersionDesc".into(), "V5_3_0".into()),
                ("accessKey".into(), "runtime-secret".into()),
                ("sslKeyPath".into(), "runtime-tls-key".into()),
            ]),
        })))
    }

    fn broker_config<'a>(
        &'a self,
        target: &'a DashboardBrokerTarget,
    ) -> SessionFuture<'a, AdminResult<DashboardBrokerConfig>> {
        Box::pin(std::future::ready(Ok(DashboardBrokerConfig {
            broker_name: target.broker_name.clone(),
            address: target.broker_addr.clone().unwrap_or_default(),
            entries: BTreeMap::from([
                ("flushDiskType".into(), "ASYNC_FLUSH".into()),
                ("secretKey".into(), "config-secret".into()),
                ("tlsCertPath".into(), "config-tls".into()),
            ]),
        })))
    }

    fn shutdown(self: Box<Self>) -> SessionFuture<'static, ()> {
        self.controls.query_shutdowns.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(()))
    }
}

struct FakeMutationSession {
    controls: Arc<FakeControls>,
    patch_outcome: BrokerConfigPatchResult,
}

impl FakeMutationSession {
    async fn exercise(&self) {
        let active = self.controls.mutation_active.fetch_add(1, Ordering::SeqCst) + 1;
        self.controls.mutation_max.fetch_max(active, Ordering::SeqCst);
        tokio::task::yield_now().await;
        self.controls.mutation_active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl DashboardMutationSession for FakeMutationSession {
    fn send_topic_message<'a>(
        &'a mut self,
        request: &'a rocketmq_admin_core::core::topic::TopicSendRequest,
    ) -> SessionFuture<'a, AdminResult<rocketmq_admin_core::core::topic::TopicSendResult>> {
        Box::pin(async move {
            self.controls
                .topic_sends
                .lock()
                .unwrap()
                .push((request.trace_enabled, request.message_body.len()));
            Ok(rocketmq_admin_core::core::topic::TopicSendResult {
                topic: request.topic.clone(),
                send_status: "SEND_OK".into(),
                message_id: None,
                broker_name: None,
                queue_id: None,
                queue_offset: 0,
                transaction_id: None,
                region_id: None,
                local_transaction_state: None,
            })
        })
    }

    fn patch_topic_fields<'a>(
        &'a mut self,
        request: &'a rocketmq_admin_core::core::topic::TopicConfigFieldsPatchRequest,
    ) -> SessionFuture<'a, AdminResult<rocketmq_admin_core::core::topic::TopicConfigFieldsPatchReport>> {
        use rocketmq_admin_core::core::topic::{TopicConfigFieldsPatchReport, TopicConfigFieldsTargetOutcome};
        Box::pin(async move {
            self.controls.topic_field_requests.lock().unwrap().push(request.clone());
            if let Some(report) = self.controls.topic_field_report.lock().unwrap().take() {
                return Ok(report);
            }
            Ok(TopicConfigFieldsPatchReport {
                targets: request
                    .targets()
                    .iter()
                    .map(|target| TopicConfigFieldsTargetOutcome {
                        target: target.clone(),
                        applied: true,
                        failure: None,
                        retryable: false,
                        outcome_unknown: false,
                        latest: None,
                    })
                    .collect(),
                order_reconciled: Some(true),
            })
        })
    }
    fn query_config_generation<'a>(
        &'a mut self,
        _request: &'a QueryBrokerConfigGenerationRequest,
    ) -> SessionFuture<'a, AdminResult<QueryBrokerConfigGenerationResult>> {
        Box::pin(async move {
            self.exercise().await;
            Ok(QueryBrokerConfigGenerationResult { generation: 7 })
        })
    }

    fn patch_config_if_generation<'a>(
        &'a mut self,
        _request: &'a PatchBrokerConfigRequest,
    ) -> SessionFuture<'a, AdminResult<BrokerConfigPatchResult>> {
        Box::pin(async move {
            self.exercise().await;
            Ok(self.patch_outcome)
        })
    }

    fn shutdown(self: Box<Self>) -> SessionFuture<'static, ()> {
        self.controls.mutation_shutdowns.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(()))
    }
}

fn runtime() -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig::for_parallelism("gpui-provider-test", 2))
        .expect("test runtime configuration is valid")
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory limit"))
        .build()
        .expect("test runtime")
}

fn snapshot(revision: u64, scope: ConnectionScope) -> ConnectionSnapshot {
    ConnectionSnapshot {
        revision,
        nameserver: Some("localhost:9876".into()),
        proxy: (scope == ConnectionScope::Proxy).then(|| "localhost:8080".into()),
        scope,
        transport: TransportSettings::default(),
        credential_source: CredentialSourceKind::None,
    }
}

fn provider(
    runtime: &RuntimeOwner,
    controls: Arc<FakeControls>,
    health: Result<(), AdminError>,
    query_barrier: Option<Arc<tokio::sync::Barrier>>,
    patch_outcome: BrokerConfigPatchResult,
) -> Arc<GpuiAdminProvider> {
    GpuiAdminProvider::with_factory(
        runtime.root_context().component("provider"),
        Arc::new(FakeFactory {
            controls,
            health,
            query_barrier,
            patch_outcome,
        }),
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        Arc::new(FixedClock),
    )
}

fn applied() -> BrokerConfigPatchResult {
    BrokerConfigPatchResult::Applied {
        previous_generation: 7,
        generation: 8,
    }
}

fn safe_applied() -> SafeConfigPatchOutcome {
    SafeConfigPatchOutcome::Applied {
        previous_generation: 7,
        generation: 8,
    }
}

#[test]
fn topic_trace_and_changed_fields_reach_the_mutation_session() {
    use rocketmq_dashboard_common::{
        TopicConfigFieldsPatch, TopicConfigTargetView, TopicIdentity, TopicMessageType, TopicPermission,
        TopicTargetIdentity,
    };
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        for trace_enabled in [false, true] {
            let receipt = provider
                .send_topic_message(
                    1,
                    SafeTopicSendRequest {
                        topic: TopicIdentity::parse("orders").unwrap(),
                        key: String::new(),
                        tag: String::new(),
                        body: "ephemeral body".into(),
                        trace_enabled,
                    },
                )
                .await
                .unwrap();
            assert!(receipt.delivered);
        }
        let result = provider
            .patch_topic_fields(
                1,
                SafeTopicFieldsPatchRequest {
                    topic: TopicIdentity::parse("orders").unwrap(),
                    targets: vec![TopicConfigTargetView {
                        target: TopicTargetIdentity::parse("cluster-a", "broker-a", "broker-a:10911").unwrap(),
                        version: 7,
                        read_queue_count: 8,
                        write_queue_count: 8,
                        permission: TopicPermission::parse(6).ok(),
                        ordered: false,
                        message_type: TopicMessageType::Normal,
                    }],
                    patch: TopicConfigFieldsPatch {
                        permission: TopicPermission::parse(7).ok(),
                        ordered: Some(true),
                        message_type: Some(TopicMessageType::Transaction),
                        ..Default::default()
                    },
                },
            )
            .await
            .unwrap();
        assert!(result.outcome.is_complete_success());
        assert_eq!(*controls.topic_sends.lock().unwrap(), [(false, 14), (true, 14)]);
        {
            let requests = controls.topic_field_requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].targets()[0].expected_version, 7);
            let patch = requests[0].patch();
            assert_eq!((patch.read_queue_nums, patch.write_queue_nums), (None, None));
            assert_eq!(patch.perm, Some(7));
            assert_eq!(patch.order, Some(true));
            assert_eq!(
                patch.message_type,
                Some(rocketmq_admin_core::core::supervised_mutation::TopicMessageType::Transaction)
            );
        }
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn topic_persistence_failures_do_not_become_rebase_candidates() {
    use rocketmq_admin_core::core::{
        supervised_mutation::{MutationFailureCode, TopicMessageType as AdminMessageType, TopicReplacement},
        topic::{
            TopicConfigFieldsPatchReport, TopicConfigFieldsState, TopicConfigFieldsTarget,
            TopicConfigFieldsTargetOutcome,
        },
    };
    use rocketmq_dashboard_common::{
        TopicConfigFieldsPatch, TopicConfigTargetView, TopicFailureCode, TopicIdentity, TopicMessageType,
        TopicPermission, TopicTargetIdentity,
    };
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        for unknown in [false, true] {
            *controls.topic_field_report.lock().unwrap() = Some(TopicConfigFieldsPatchReport {
                targets: vec![TopicConfigFieldsTargetOutcome {
                    target: TopicConfigFieldsTarget {
                        cluster_name: "cluster-a".into(),
                        broker_name: "broker-a".into(),
                        broker_address: "broker-a:10911".into(),
                        expected_version: 7,
                    },
                    applied: false,
                    failure: Some(MutationFailureCode::PersistenceFailed),
                    retryable: false,
                    outcome_unknown: unknown,
                    latest: Some(TopicConfigFieldsState {
                        version: Some(7),
                        configuration: Some(TopicReplacement {
                            read_queue_nums: 8,
                            write_queue_nums: 8,
                            perm: 6,
                            order: false,
                            message_type: AdminMessageType::Normal,
                        }),
                    }),
                }],
                order_reconciled: None,
            });
            let result = provider
                .patch_topic_fields(
                    1,
                    SafeTopicFieldsPatchRequest {
                        topic: TopicIdentity::parse("orders").unwrap(),
                        targets: vec![TopicConfigTargetView {
                            target: TopicTargetIdentity::parse("cluster-a", "broker-a", "broker-a:10911").unwrap(),
                            version: 7,
                            read_queue_count: 8,
                            write_queue_count: 8,
                            permission: TopicPermission::parse(6).ok(),
                            ordered: false,
                            message_type: TopicMessageType::Normal,
                        }],
                        patch: TopicConfigFieldsPatch {
                            read_queue_count: Some(12),
                            ..Default::default()
                        },
                    },
                )
                .await
                .unwrap();
            assert!(
                result.conflicts.is_empty(),
                "dirty or uncertain persistence cannot enable Keep/Adopt version rebase actions"
            );
            assert_eq!(result.outcome.applied_count(), 0);
            assert_eq!(result.outcome.failed_count(), 1);
            assert_eq!(
                result.outcome.targets[0].failure,
                Some(if unknown {
                    TopicFailureCode::Unconfirmed
                } else {
                    TopicFailureCode::Unavailable
                })
            );
            assert!(!result.outcome.targets[0].retryable);
        }
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

fn patch_request() -> SafeConfigPatchRequest {
    SafeConfigPatchRequest {
        address: "broker-a:10911".into(),
        expected_generation: 7,
        entries: [("flushInterval".into(), "2000".into())].into_iter().collect(),
    }
}

#[test]
fn health_uses_real_session_result_and_proxy_health_is_unknown() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, Arc::clone(&controls), Ok(()), None, applied());
    runtime.block_on(async {
        provider
            .switch(snapshot(1, ConnectionScope::NameServer))
            .await
            .expect("switch");
        let health = provider.check_health().await.expect("health");
        assert_eq!(health.availability, EndpointAvailability::Available);
        assert_eq!(health.checked_at_epoch_ms, Some(42));
        provider
            .switch(snapshot(2, ConnectionScope::Proxy))
            .await
            .expect("switch");
        let health = provider.check_health().await.expect("proxy health");
        assert_eq!(health.availability, EndpointAvailability::Unknown);
        provider.shutdown().await;
    });
    assert_eq!(controls.query_shutdowns.load(Ordering::SeqCst), 2);
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn backend_error_body_is_redacted_and_cancellation_wins() {
    let runtime = runtime();
    let provider = provider(
        &runtime,
        Arc::new(FakeControls::default()),
        Err(AdminError::operation_failed("list_topics", "access-value secret-value")),
        None,
        applied(),
    );
    runtime.block_on(async {
        provider
            .switch(snapshot(1, ConnectionScope::NameServer))
            .await
            .expect("switch");
        let health = provider.check_health().await.expect("failed health result");
        let summary = health.failure_summary.expect("failure summary");
        assert!(!summary.contains("access-value"));
        assert!(!summary.contains("secret-value"));

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = provider
            .check_health_with_cancellation(cancellation)
            .await
            .expect_err("cancelled");
        assert_eq!(error.code(), ProviderFailureCode::Cancelled);
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn operational_provider_error_retains_admin_source_without_rendering_backend_text() {
    let failure = map_admin_error(AdminError::operation_failed(
        "list_topics",
        "password=private-password token=private-token",
    ));
    let error = failure.into_operational().expect("backend failures are operational");

    assert!(std::error::Error::source(&error).is_some());
    assert!(
        std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<AdminError>())
            .is_some()
    );
    for projection in [format!("{error}"), format!("{error:?}")] {
        assert!(!projection.contains("private-password"));
        assert!(!projection.contains("private-token"));
    }
}

#[test]
fn dashboard_responses_cross_the_provider_boundary_only_as_allowlisted_erased_safe_dtos() {
    let runtime = runtime();
    let provider = provider(&runtime, Arc::new(FakeControls::default()), Ok(()), None, applied());
    runtime.block_on(async {
        provider
            .switch(snapshot(1, ConnectionScope::NameServer))
            .await
            .expect("switch");
        let inventory = provider.list_brokers(1).await.expect("inventory");
        let topics = provider.list_topics(1).await.expect("topics");
        let stats = provider
            .topic_stats(1, "private-topic".into())
            .await
            .expect("Topic stats");
        let consumers = provider.list_consumers(1).await.expect("consumers");
        let producers = provider.list_producers(1).await.expect("producers");
        let target = SafeBrokerTarget {
            broker_name: "broker-a".into(),
            address: "broker-a:10911".into(),
        };
        let runtime = provider.broker_runtime(1, target.clone()).await.expect("runtime");
        let config = provider.broker_config(1, target).await.expect("config");

        assert_eq!(topics.topics, ["private-topic"]);
        assert_eq!(stats.total_max_offset, 1);
        assert_eq!(consumers.group_count, 1);
        assert_eq!(producers.distinct_group_count, 1);
        let combined = format!("{inventory:?}{runtime:?}{config:?}{topics:?}{stats:?}{consumers:?}{producers:?}");
        for raw in [
            "raw-access-secret",
            "raw-tls-key",
            "raw-body-token",
            "runtime-secret",
            "runtime-tls-key",
            "config-secret",
            "config-tls",
            "private-topic",
            "raw-topic-broker",
            "raw-topic-category",
            "private-consumer",
            "private-producer",
        ] {
            assert!(!combined.contains(raw));
        }
        let secret = runtime
            .entries
            .iter()
            .find(|entry| entry.key == "accessKey")
            .expect("sensitive runtime entry");
        assert_eq!(secret.display_value(), "<redacted>");
        assert_eq!(secret.copy_value(), None);
        assert_eq!(config.entries["secretKey"], "<redacted>");
        assert_eq!(config.entries["tlsCertPath"], "<redacted>");
        assert!(
            !serde_json::to_string(&runtime.entries)
                .expect("safe runtime serde")
                .contains("runtime-secret")
        );
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn query_calls_share_one_session_and_run_concurrently() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let provider = provider(&runtime, Arc::clone(&controls), Ok(()), Some(barrier), applied());
    runtime.block_on(async {
        provider
            .switch(snapshot(1, ConnectionScope::NameServer))
            .await
            .expect("switch");
        let (left, right) = tokio::join!(provider.check_health(), provider.check_health());
        left.expect("left");
        right.expect("right");
        provider.shutdown().await;
    });
    assert_eq!(controls.query_creates.load(Ordering::SeqCst), 1);
    assert_eq!(controls.query_max.load(Ordering::SeqCst), 2);
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn mutation_session_is_lazy_reused_and_serialized() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, Arc::clone(&controls), Ok(()), None, applied());
    runtime.block_on(async {
        provider
            .switch(snapshot(1, ConnectionScope::NameServer))
            .await
            .expect("switch");
        let (left, right) = tokio::join!(
            provider.patch_config_if_generation(1, patch_request()),
            provider.patch_config_if_generation(1, patch_request())
        );
        assert_eq!(left.expect("left"), safe_applied());
        assert_eq!(right.expect("right"), safe_applied());
        provider.shutdown().await;
    });
    assert_eq!(controls.mutation_creates.load(Ordering::SeqCst), 1);
    assert_eq!(controls.mutation_max.load(Ordering::SeqCst), 1);
    assert_eq!(controls.mutation_shutdowns.load(Ordering::SeqCst), 1);
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn switch_shuts_both_sessions_and_rejects_stale_revision() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, Arc::clone(&controls), Ok(()), None, applied());
    runtime.block_on(async {
        provider
            .switch(snapshot(2, ConnectionScope::NameServer))
            .await
            .expect("first");
        provider
            .query_config_generation(2, "broker-a:10911".into())
            .await
            .expect("generation");
        provider
            .switch(snapshot(3, ConnectionScope::NameServer))
            .await
            .expect("second");
        assert_eq!(controls.query_shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(controls.mutation_shutdowns.load(Ordering::SeqCst), 1);
        let stale = provider
            .switch(snapshot(2, ConnectionScope::NameServer))
            .await
            .expect_err("stale");
        assert_eq!(stale.code(), ProviderFailureCode::StaleRevision);
        provider.shutdown().await;
    });
    assert_eq!(controls.query_shutdowns.load(Ordering::SeqCst), 2);
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn generation_conflict_is_returned_without_an_overwrite_retry() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let conflict = BrokerConfigPatchResult::GenerationConflict {
        expected_generation: 7,
        actual_generation: 9,
    };
    let provider = provider(&runtime, Arc::clone(&controls), Ok(()), None, conflict);
    runtime.block_on(async {
        provider
            .switch(snapshot(1, ConnectionScope::NameServer))
            .await
            .expect("switch");
        let result = provider
            .patch_config_if_generation(1, patch_request())
            .await
            .expect("typed outcome");
        assert_eq!(
            result,
            SafeConfigPatchOutcome::GenerationConflict {
                expected_generation: 7,
                actual_generation: 9,
            }
        );
        provider.shutdown().await;
    });
    assert_eq!(controls.mutation_creates.load(Ordering::SeqCst), 1);
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn dashboard_sampling_bounds_calls_filters_application_topics_and_keeps_partial_observations() {
    use crate::{
        infrastructure::{history_collector::HistorySampler, history_store::HistoryStore},
        services::dashboard::DashboardService,
    };
    use rocketmq_dashboard_common::{HistoryMetricKind, Observed};
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let mut names = (0..205).map(|index| format!("a-{index:03}")).collect::<Vec<_>>();
    names.extend([
        "%RETRY%orders".into(),
        "%DLQ%orders".into(),
        "SCHEDULE_TOPIC_XXXX".into(),
        "rmq_sys_private".into(),
    ]);
    *controls.dashboard_topics.lock().unwrap() = Some(names);
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    let directory = tempfile::tempdir().unwrap();
    let context = runtime.root_context().component("history");
    let history = HistoryStore::new(directory.path().join("history.json"), context.clone());
    let service = DashboardService::new(provider.clone(), history, context);
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        let load = service.topic_current(1).await.unwrap();
        assert_eq!(load.total_topics, 209);
        assert_eq!(load.application_topics, 205);
        assert_eq!(load.omitted_topics, 5);
        assert_eq!(controls.dashboard_rate_calls.lock().unwrap()[0].len(), 200);
        assert_eq!(controls.dashboard_stats_calls.lock().unwrap().len(), 20);
        assert_eq!(load.items.len(), 20);
        assert_eq!(load.items[0].topic, "a-000");
        assert_eq!(load.items[0].produce_tps, Observed::Observed(0.5));
        assert_eq!(load.items[0].consume_tps, Observed::Observed(0.0));
        assert!(load.failed_topics >= 2);
        assert!(load.failures.iter().any(|failure| failure.topic == "a-002"
            && failure.stage == rocketmq_dashboard_common::TopicCurrentStage::Offsets));
        assert!(
            load.failures.iter().any(|failure| failure.topic == "a-001"
                && failure.stage == rocketmq_dashboard_common::TopicCurrentStage::Rates)
        );
        assert_eq!(
            load.items
                .iter()
                .find(|item| item.topic == "a-002")
                .unwrap()
                .total_messages,
            Observed::Unknown
        );
        assert!(
            load.items
                .iter()
                .any(|item| item.total_messages == Observed::Observed(0))
        );
        let points = service.sample_topics().await.unwrap();
        assert!(
            points
                .iter()
                .any(|point| point.metric == HistoryMetricKind::TopicCount && point.value == 209.0)
        );
        assert!(
            points
                .iter()
                .any(|point| point.series_identity == "a-000" && point.value == 50.0)
        );
        assert!(!points.iter().any(|point| point.series_identity == "a-002"));
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn dashboard_overview_keeps_incomplete_backlog_unknown_and_preserves_real_zero() {
    use crate::{infrastructure::history_store::HistoryStore, services::dashboard::DashboardService};
    use rocketmq_admin_core::core::dashboard_metrics::DashboardConsumerSummary;
    use rocketmq_dashboard_common::Observed;
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    let directory = tempfile::tempdir().unwrap();
    let context = runtime.root_context().component("history");
    let service = DashboardService::new(
        provider.clone(),
        HistoryStore::new(directory.path().join("history.json"), context.clone()),
        context,
    );
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        for value in [0, -2, 9] {
            *controls.dashboard_consumer_summary.lock().unwrap() = Some(DashboardConsumerSummary {
                group_count: Some(2),
                complete_backlog: Some(value),
                failed_targets: 0,
            });
            let load = service.overview(1).await.unwrap();
            assert_eq!(load.overview.consumer_backlog, Observed::Observed(value));
            assert_eq!(load.overview.consumer_group_count, Observed::Observed(2));
        }
        *controls.dashboard_consumer_summary.lock().unwrap() = Some(DashboardConsumerSummary {
            group_count: None,
            complete_backlog: None,
            failed_targets: 2,
        });
        let load = service.overview(1).await.unwrap();
        assert_eq!(load.overview.consumer_backlog, Observed::Unknown);
        assert_eq!(load.overview.consumer_group_count, Observed::Unknown);
        assert!(load.has_warning());
        assert_eq!(load.overview.topic_count, Observed::Observed(1));
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().unwrap();
}
