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
        PatchBrokerConfigOutcome, PatchBrokerConfigRequest, QueryBrokerConfigGenerationRequest,
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
    query_active: AtomicUsize,
    query_max: AtomicUsize,
    mutation_active: AtomicUsize,
    mutation_max: AtomicUsize,
    query_shutdowns: AtomicUsize,
    mutation_shutdowns: AtomicUsize,
    query_creates: AtomicUsize,
    mutation_creates: AtomicUsize,
}

struct FakeFactory {
    controls: Arc<FakeControls>,
    health: Result<(), AdminError>,
    query_barrier: Option<Arc<tokio::sync::Barrier>>,
    patch_outcome: PatchBrokerConfigOutcome,
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
    patch_outcome: PatchBrokerConfigOutcome,
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
    ) -> SessionFuture<'a, AdminResult<PatchBrokerConfigOutcome>> {
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
    patch_outcome: PatchBrokerConfigOutcome,
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

fn applied() -> PatchBrokerConfigOutcome {
    PatchBrokerConfigOutcome::Applied {
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
        Err(AdminError::backend("list_topics", "access-value secret-value")),
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
        assert_eq!(error.code(), ProviderErrorCode::Cancelled);
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().expect("shutdown");
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
        assert_eq!(stale.code(), ProviderErrorCode::StaleRevision);
        provider.shutdown().await;
    });
    assert_eq!(controls.query_shutdowns.load(Ordering::SeqCst), 2);
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn generation_conflict_is_returned_without_an_overwrite_retry() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let conflict = PatchBrokerConfigOutcome::GenerationConflict {
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
