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

#[path = "acl_tests.rs"]
mod acl_tests;
#[path = "dlq_tests.rs"]
mod dlq_tests;

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
    dashboard::{DashboardMessage, DashboardMessageList, DashboardMessageQuery},
    message::{DirectConsumeRequest, DirectConsumeResult, MessageLookupRequest, MessageRecord},
    security::AdminCredentials,
};
use rocketmq_dashboard_common::{CredentialSourceKind, TransportSettings};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
#[path = "message_trace_tests.rs"]
mod trace_tests;
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
    acl_reads: std::sync::Mutex<Vec<(String, bool)>>,
    acl_users: std::sync::Mutex<BTreeMap<String, Vec<serde_json::Value>>>,
    acl_policies: std::sync::Mutex<BTreeMap<String, Vec<serde_json::Value>>>,
    acl_inventory: std::sync::Mutex<Option<DashboardBrokerList>>,
    acl_writes: std::sync::Mutex<Vec<(String, rocketmq_admin_core::core::acl_workspace::AclExactWrite)>>,
    acl_write_errors: std::sync::Mutex<BTreeMap<String, AdminError>>,
    trace_queries: std::sync::Mutex<Vec<rocketmq_admin_core::core::message_trace::MessageTraceLookupRequest>>,
    trace_result: std::sync::Mutex<Option<rocketmq_admin_core::core::message_trace::MessageTraceReadResult>>,
    message_queries: std::sync::Mutex<Vec<DashboardMessageQuery>>,
    message_lookups: std::sync::Mutex<Vec<MessageLookupRequest>>,
    message_consumes: std::sync::Mutex<Vec<DirectConsumeRequest>>,
    message_results: std::sync::Mutex<Option<DashboardMessageList>>,
    message_scan_records: std::sync::Mutex<Option<Vec<DashboardMessage>>>,
    message_record: std::sync::Mutex<Option<MessageRecord>>,
    message_records: std::sync::Mutex<std::collections::VecDeque<MessageRecord>>,
    message_consume_result: std::sync::Mutex<Option<DirectConsumeResult>>,
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
    fn acl_documents<'a>(
        &'a self,
        address: &'a str,
        kind: rocketmq_admin_core::core::acl_workspace::AclReadKind,
    ) -> SessionFuture<'a, AdminResult<rocketmq_admin_core::core::acl_workspace::AclDocuments>> {
        let users = matches!(kind, rocketmq_admin_core::core::acl_workspace::AclReadKind::Users);
        self.controls.acl_reads.lock().unwrap().push((address.into(), users));
        let records = if users {
            &self.controls.acl_users
        } else {
            &self.controls.acl_policies
        };
        Box::pin(std::future::ready(Ok(
            rocketmq_admin_core::core::acl_workspace::AclDocuments::new(
                records.lock().unwrap().get(address).cloned().unwrap_or_default(),
            ),
        )))
    }
    fn message_trace<'a>(
        &'a self,
        request: &'a rocketmq_admin_core::core::message_trace::MessageTraceLookupRequest,
    ) -> SessionFuture<'a, AdminResult<rocketmq_admin_core::core::message_trace::MessageTraceReadResult>> {
        self.controls.trace_queries.lock().unwrap().push(request.clone());
        Box::pin(std::future::ready(
            self.controls
                .trace_result
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| AdminError::unavailable("trace", "test response missing")),
        ))
    }
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

    fn query_messages<'a>(
        &'a self,
        request: &'a DashboardMessageQuery,
    ) -> SessionFuture<'a, AdminResult<DashboardMessageList>> {
        self.controls.message_queries.lock().unwrap().push(request.clone());
        if let Some(records) = self.controls.message_scan_records.lock().unwrap().as_ref() {
            // A source-backed page exercises the provider's inclusive Topic-query contract rather
            // than supplying a response that is independent of the requested window and page.
            let size = request.page_size.unwrap_or(20) as usize;
            let items = records
                .iter()
                .filter(|record| {
                    request.topic.as_ref() == Some(&record.topic)
                        && request.begin.is_none_or(|begin| record.store_timestamp >= begin)
                        && request.end.is_none_or(|end| record.store_timestamp <= end)
                })
                .skip(request.page_num.unwrap_or(1).saturating_sub(1) as usize * size)
                .take(size)
                .cloned()
                .collect::<Vec<_>>();
            return Box::pin(std::future::ready(Ok(DashboardMessageList {
                total: items.len(),
                items,
            })));
        }
        Box::pin(std::future::ready(Ok(self
            .controls
            .message_results
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default())))
    }
    fn lookup_message<'a>(
        &'a self,
        request: &'a MessageLookupRequest,
    ) -> SessionFuture<'a, AdminResult<MessageRecord>> {
        self.controls.message_lookups.lock().unwrap().push(request.clone());
        Box::pin(std::future::ready(
            self.controls
                .message_records
                .lock()
                .unwrap()
                .pop_front()
                .or_else(|| self.controls.message_record.lock().unwrap().clone())
                .ok_or_else(|| AdminError::not_found("message", "missing test response")),
        ))
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
        if let Some(list) = self.controls.acl_inventory.lock().unwrap().clone() {
            return Box::pin(std::future::ready(Ok(list)));
        }
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
    fn apply_acl<'a>(
        &'a mut self,
        address: &'a str,
        request: &'a rocketmq_admin_core::core::acl_workspace::AclExactWrite,
    ) -> SessionFuture<'a, AdminResult<()>> {
        self.controls
            .acl_writes
            .lock()
            .unwrap()
            .push((address.into(), request.clone()));
        Box::pin(std::future::ready(
            self.controls
                .acl_write_errors
                .lock()
                .unwrap()
                .get(address)
                .cloned()
                .map_or(Ok(()), Err),
        ))
    }
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

    fn consume_message_directly<'a>(
        &'a mut self,
        request: &'a DirectConsumeRequest,
    ) -> SessionFuture<'a, AdminResult<DirectConsumeResult>> {
        self.controls.message_consumes.lock().unwrap().push(request.clone());
        Box::pin(std::future::ready(
            self.controls
                .message_consume_result
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| AdminError::operation_failed("consume", "missing test response")),
        ))
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

fn raw_message(topic: &str, id: &str) -> DashboardMessage {
    DashboardMessage {
        topic: topic.into(),
        message_id: id.into(),
        keys: Some("private-key".into()),
        tags: Some("private-tag".into()),
        born_timestamp: 10,
        store_timestamp: 20,
        born_host: "private-host".into(),
        store_host: "private-host".into(),
        queue_id: 2,
        queue_offset: 33,
        store_size: 100,
        reconsume_times: 0,
        body_crc: 0,
        sys_flag: 0,
        flag: 0,
        prepared_transaction_offset: 0,
        body: b"private-body".to_vec(),
        properties: BTreeMap::from([("private-property".into(), "private-value".into())]),
    }
}
fn raw_record(topic: &str, id: &str) -> MessageRecord {
    let message = raw_message(topic, id);
    MessageRecord {
        topic: message.topic,
        message_id: message.message_id,
        unique_message_id: Some("unique-id".into()),
        keys: message.keys,
        tags: message.tags,
        born_timestamp: message.born_timestamp,
        store_timestamp: message.store_timestamp,
        born_host: message.born_host,
        store_host: message.store_host,
        queue_id: message.queue_id,
        queue_offset: message.queue_offset,
        store_size: message.store_size,
        reconsume_times: 0,
        body_crc: 0,
        sys_flag: 0,
        flag: 0,
        prepared_transaction_offset: 0,
        body: message.body,
        properties: message.properties,
    }
}

#[test]
fn message_provider_maps_each_mode_preserves_scan_paging_and_discards_search_payloads() {
    use rocketmq_dashboard_common::{MessagePageExtent, MessageSearchQuery};
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(5, ConnectionScope::NameServer)).await.unwrap();
        *controls.message_results.lock().unwrap() = Some(DashboardMessageList {
            items: vec![raw_message("orders", "physical-id")],
            total: 999,
        });
        let query = MessageSearchQuery::TopicRange {
            topic: "orders".into(),
            begin: 100,
            end: 200,
            page: 2,
            page_size: 1,
        };
        let page = provider.query_messages(5, query).await.unwrap();
        assert_eq!(page.extent, MessagePageExtent::MoreMayExist);
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].queue_offset, 33);
        assert_eq!(page.rows[0].tags.as_ref().unwrap().text, "private-tag");
        assert_eq!(page.rows[0].keys.as_ref().unwrap().text, "private-key");
        assert_eq!(page.rows[0].born_host.text, "private-host");
        assert_eq!(page.rows[0].store_host.text, "private-host");
        let debug = format!("{page:?}");
        for secret in [
            "private-body",
            "private-key",
            "private-tag",
            "private-property",
            "private-value",
            "private-host",
        ] {
            assert!(!debug.contains(secret));
        }
        assert_eq!(
            controls.message_queries.lock().unwrap()[0],
            DashboardMessageQuery {
                topic: Some("orders".into()),
                begin: Some(100),
                end: Some(200),
                page_num: Some(2),
                page_size: Some(1),
                ..Default::default()
            }
        );
        provider
            .query_messages(
                5,
                MessageSearchQuery::Key {
                    topic: "orders".into(),
                    key: "key-a".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            controls.message_queries.lock().unwrap()[1],
            DashboardMessageQuery {
                topic: Some("orders".into()),
                key: Some("key-a".into()),
                ..Default::default()
            }
        );
        *controls.message_record.lock().unwrap() = Some(raw_record("orders", "physical-id"));
        let page = provider
            .query_messages(
                5,
                MessageSearchQuery::MessageId {
                    topic: "orders".into(),
                    message_id: "unique-id".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(page.rows[0].identity.message_id, "physical-id");
        assert_eq!(controls.message_lookups.lock().unwrap()[0].message_id, "unique-id");
        *controls.message_results.lock().unwrap() = Some(DashboardMessageList::default());
        let empty = provider
            .query_messages(
                5,
                MessageSearchQuery::TopicRange {
                    topic: "orders".into(),
                    begin: 100,
                    end: 200,
                    page: 3,
                    page_size: 1,
                },
            )
            .await
            .unwrap();
        assert!(empty.rows.is_empty());
        assert_eq!(empty.extent, MessagePageExtent::End);
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}

#[test]
fn message_provider_rejects_wrong_topic_or_detail_id_and_controls_consume_result_and_remark() {
    use rocketmq_dashboard_common::{MessageConsumeCode, MessageConsumeCommand, MessageIdentity, MessageSearchQuery};
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(8, ConnectionScope::NameServer)).await.unwrap();
        *controls.message_record.lock().unwrap() = Some(raw_record("other", "physical-id"));
        assert!(
            provider
                .query_messages(
                    8,
                    MessageSearchQuery::MessageId {
                        topic: "orders".into(),
                        message_id: "physical-id".into()
                    }
                )
                .await
                .is_err()
        );
        let identity = MessageIdentity {
            topic: "orders".into(),
            message_id: "physical-id".into(),
        };
        *controls.message_record.lock().unwrap() = Some(raw_record("orders", "wrong-id"));
        assert!(provider.message_payload(8, identity.clone()).await.is_err());
        *controls.message_record.lock().unwrap() = Some(raw_record("orders", "physical-id"));
        let payload = provider.message_payload(8, identity.clone()).await.unwrap();
        assert_eq!(payload.body(), b"private-body");
        *controls.message_consume_result.lock().unwrap() = Some(DirectConsumeResult {
            success: true,
            consume_result: Some("CR_LATER".into()),
            remark: Some("private-server-remark private-body".into()),
        });
        let receipt = provider
            .consume_message(
                8,
                MessageConsumeCommand {
                    identity,
                    consumer_group: "group-a".into(),
                    client_id: Some("client-a".into()),
                    scope: ConnectionScope::NameServer,
                },
            )
            .await
            .unwrap();
        assert_eq!(receipt.code, MessageConsumeCode::Later);
        assert!(!receipt.is_success());
        assert!(receipt.remark_returned);
        assert!(!format!("{receipt:?} {}", receipt.remark()).contains("private-server-remark"));
        let calls = controls.message_consumes.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].topic, "orders");
        assert_eq!(calls[0].message_id, "physical-id");
        assert_eq!(calls[0].consumer_group, "group-a");
        assert_eq!(calls[0].client_id.as_deref(), Some("client-a"));
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}

#[test]
fn message_provider_bounds_real_tags_keys_and_hosts_without_cloning_arbitrary_properties() {
    use rocketmq_dashboard_common::MessageSearchQuery;
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    let mut raw = raw_message("orders", "id");
    raw.keys = Some("private-key".repeat(500));
    raw.tags = Some("长标签".repeat(500));
    raw.born_host = "private-host".repeat(500);
    raw.store_host = raw.born_host.clone();
    *controls.message_results.lock().unwrap() = Some(DashboardMessageList {
        items: vec![raw],
        total: 1,
    });
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        let page = provider
            .query_messages(
                1,
                MessageSearchQuery::Key {
                    topic: "orders".into(),
                    key: "key".into(),
                },
            )
            .await
            .unwrap();
        let row = &page.rows[0];
        for preview in [
            row.keys.as_ref().unwrap(),
            row.tags.as_ref().unwrap(),
            &row.born_host,
            &row.store_host,
        ] {
            assert!(preview.truncated);
            assert!(preview.text.len() <= 1024);
            assert!(preview.display().ends_with('…'));
        }
        for secret in [
            "private-body",
            "private-property",
            "private-value",
            "private-key",
            "private-host",
            "长标签",
        ] {
            assert!(!format!("{row:?}").contains(secret));
        }
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
