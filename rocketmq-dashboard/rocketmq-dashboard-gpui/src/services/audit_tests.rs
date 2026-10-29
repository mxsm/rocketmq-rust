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

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use rocketmq_dashboard_common::{
    AuditEvent, AuditFilter, BrokerConfigPatch, BrokerIdentity, ConsumerAclClassification, ConsumerConfigEntries,
    ConsumerCreateCommand, ConsumerFailureCode, ConsumerFailureStage, ConsumerIdentity, ConsumerInventory,
    ConsumerMutationGuarantee, ConsumerMutationKind, ConsumerObservationState, ConsumerPartialOutcome,
    ConsumerTargetIdentity, ConsumerTargetOutcome, TopicFailureCode, TopicFailureStage, TopicIdentity,
    TopicMutationGuarantee, TopicMutationKind, TopicPartialOutcome, TopicTargetOutcome,
};
use rocketmq_runtime::{ChildServiceContext, ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::{
    infrastructure::{
        audit_store::AuditStore,
        auth_state::{LOGIN_PASSWORD_ENV, LOGIN_USERNAME_ENV, MapEnvironment},
    },
    services::{
        brokers::BrokerConfigMutationResult,
        consumers::{ConsumerRequestScope, test_support::FakeConsumerBackend},
        dashboard_broker::test_support::FakeDashboardBrokerOperations,
        topics::{TopicMutationResult, TopicRequestScope, TopicSendCommand, test_support::FakeTopicBackend},
    },
};

struct Fixture {
    directory: tempfile::TempDir,
    runtime: RuntimeOwner,
    store: Arc<DesktopConfigStore>,
    auth: Arc<DesktopAuthState>,
    services: AppServices,
    mutations: ChildServiceContext,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("audit-service-test", 1))
            .unwrap()
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
            .build()
            .unwrap();
        let store = DesktopConfigStore::new(
            directory.path().join("config.json"),
            runtime.root_context().component("storage"),
        );
        let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([
            (LOGIN_USERNAME_ENV, "operator"),
            (LOGIN_PASSWORD_ENV, "private-password"),
        ])));
        let mutations = runtime.root_context().component("mutations");
        let services = AppServices::desktop_with_fake_provider(
            Arc::clone(&store),
            Arc::clone(&auth),
            mutations.clone(),
            runtime.root_context().component("history"),
            runtime.root_context().component("monitor"),
        );
        runtime.block_on(services.bootstrap()).unwrap();
        Self {
            directory,
            runtime,
            store,
            auth,
            services,
            mutations,
        }
    }

    fn events(&self) -> Vec<AuditEvent> {
        self.services
            .query_audit(AuditQuery {
                filter: AuditFilter::recent(now_epoch_ms().unwrap()),
                limit: 200,
                cursor: None,
            })
            .unwrap()
            .events
    }
}

fn broker() -> BrokerIdentity {
    BrokerIdentity {
        cluster: "cluster-a".into(),
        broker_name: "broker-a".into(),
        broker_id: 0,
        address: "127.0.0.1:10911".into(),
    }
}

fn patch() -> BrokerConfigPatch {
    BrokerConfigPatch::new(
        broker(),
        4,
        BTreeMap::from([("flushDiskType".into(), "private-config-value".into())]),
    )
}

fn applied_broker() -> BrokerConfigMutationResult {
    BrokerConfigMutationResult::AppliedReloadFailed {
        previous_generation: 4,
        reported_generation: 5,
        invalidations: Vec::new(),
        error: UiError::new("Authoritative refresh failed.", UiErrorCode::Connection, true),
    }
}

#[test]
fn audit_dropped_waiter_signout_and_owned_shutdown_preserve_the_accepted_actor_and_terminal() {
    let mut fixture = Fixture::new();
    fixture
        .runtime
        .block_on(fixture.services.authenticate("operator", "private-password"))
        .unwrap();
    let backend = Arc::new(FakeDashboardBrokerOperations::default());
    backend.queue_patch(Ok(applied_broker()));
    let (started, release) = backend.hold_next_patch();
    fixture.services = fixture.services.with_dashboard_broker_operations(backend.clone());
    let original_environment = fixture.store.environments().unwrap().active_id;
    fixture.runtime.block_on(async {
        let mut waiter = Box::pin(fixture.services.patch_broker_config(0, patch()));
        tokio::select! {
            result = &mut waiter => panic!("operation finished before release: {result:?}"),
            result = started => result.unwrap(),
        }
        drop(waiter);
        fixture.services.sign_out().await.unwrap();
        assert!(!fixture.auth.session().is_authenticated());
        release.send(()).unwrap();
        let report = fixture.mutations.task_group().shutdown(Duration::from_secs(5)).await;
        assert!(report.is_healthy(), "owned jobs must finish before storage is stopped");
    });
    let events = fixture.events();
    let broker_event = events
        .iter()
        .find(|event| event.operation.action == AuditAction::BrokerUpdated)
        .unwrap();
    assert_eq!(
        broker_event.operation.context.actor,
        OperationActor::LocalOperator {
            username: "operator".into()
        }
    );
    assert_eq!(broker_event.operation.context.environment_id, original_environment);
    assert_eq!(broker_event.outcome, AuditOutcome::Succeeded);
    assert!(broker_event.facts.refresh_failed);
    assert_eq!(backend.calls().patches.len(), 1);
    let file = std::fs::read_to_string(fixture.store.path()).unwrap();
    for secret in [
        "private-password",
        "private-config-value",
        "Authoritative refresh failed",
    ] {
        assert!(!file.contains(secret));
    }
    assert!(fixture.runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}

#[test]
fn audit_business_success_survives_terminal_write_failure_and_retry_never_replays_broker() {
    let mut fixture = Fixture::new();
    let backend = Arc::new(FakeDashboardBrokerOperations::default());
    backend.queue_patch(Ok(applied_broker()));
    let (started, release) = backend.hold_next_patch();
    fixture.services = fixture.services.with_dashboard_broker_operations(backend.clone());
    fixture.runtime.block_on(async {
        let mut waiter = Box::pin(fixture.services.patch_broker_config(0, patch()));
        tokio::select! {
            result = &mut waiter => panic!("operation finished before release: {result:?}"),
            result = started => result.unwrap(),
        }
        let backup = fixture.directory.path().join("saved-config.json");
        std::fs::rename(fixture.store.path(), &backup).unwrap();
        std::fs::create_dir(fixture.store.path()).unwrap();
        release.send(()).unwrap();
        assert!(matches!(
            waiter.await.unwrap(),
            BrokerConfigMutationResult::AppliedReloadFailed { .. }
        ));
        assert!(fixture.services.audit_write_warning());
        assert_eq!(fixture.services.audit_pending().unwrap().len(), 1);
        fixture.services.retry_audit_writes().await.unwrap();
        assert!(fixture.services.audit_write_warning(), "a failed retry remains visible");
        std::fs::remove_dir(fixture.store.path()).unwrap();
        std::fs::rename(backup, fixture.store.path()).unwrap();
        fixture.services.retry_audit_writes().await.unwrap();
        fixture.services.retry_audit_writes().await.unwrap();
        assert!(!fixture.services.audit_write_warning());
        assert!(fixture.services.audit_pending().unwrap().is_empty());
    });
    assert_eq!(backend.calls().patches.len(), 1);
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome, AuditOutcome::Succeeded);
    assert!(events[0].facts.refresh_failed);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_owned_shutdown_timeout_leaves_a_durable_unknown_request_without_replay() {
    let fixture = Fixture::new();
    let executed = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&executed);
    fixture.runtime.block_on(async {
        let operation = fixture.services.accept_operation(0, AuditAction::MessageConsumed, OperationResource::Message("opaque-id".into())).unwrap();
        let (started, observed_start) = tokio::sync::oneshot::channel();
        let mut waiter = Box::pin(fixture.services.run_mutation("held-mutation", operation, async move {
            observed.fetch_add(1, Ordering::AcqRel);
            started.send(()).unwrap();
            std::future::pending::<Result<(), UiError>>().await
        }));
        tokio::select! { result = &mut waiter => panic!("unexpected completion: {result:?}"), result = observed_start => result.unwrap() }
        drop(waiter);
        let report = fixture.mutations.task_group().shutdown(Duration::ZERO).await;
        assert!(report.timed_out > 0);
        assert_eq!(fixture.services.audit_pending().unwrap().len(), 1);
        assert!(fixture.events().is_empty());
    });
    let bytes: serde_json::Value = serde_json::from_slice(&std::fs::read(fixture.store.path()).unwrap()).unwrap();
    assert_eq!(bytes["audit"]["pending"].as_array().unwrap().len(), 1);
    assert!(bytes["audit"]["events"].as_array().unwrap().is_empty());
    assert_eq!(executed.load(Ordering::Acquire), 1);
    let _ = fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_topic_send_and_consumer_partial_use_real_service_hooks_and_never_persist_payloads() {
    let mut fixture = Fixture::new();
    let topics = Arc::new(FakeTopicBackend::default());
    topics.queue_send(Ok(()));
    let consumers = Arc::new(FakeConsumerBackend::default());
    let group = ConsumerIdentity::parse("orders-consumer").unwrap();
    let target = ConsumerTargetIdentity::parse("cluster-a", "broker-a", "127.0.0.1:10911").unwrap();
    consumers.queue_create(Ok(ConsumerPartialOutcome {
        group: group.clone(),
        kind: ConsumerMutationKind::Create,
        guarantee: ConsumerMutationGuarantee::PreflightBestEffort,
        reload_failed: false,
        targets: vec![
            ConsumerTargetOutcome {
                target: "applied".into(),
                stage: ConsumerFailureStage::Mutation,
                applied: true,
                failure: None,
                retryable: false,
            },
            ConsumerTargetOutcome {
                target: "unknown".into(),
                stage: ConsumerFailureStage::Mutation,
                applied: false,
                failure: Some(ConsumerFailureCode::Unavailable),
                retryable: false,
            },
        ],
    }));
    consumers.queue_inventory(Ok(ConsumerInventory {
        groups: Vec::new(),
        targets: vec![target.clone()],
        observation: ConsumerObservationState::Complete,
        failures: Vec::new(),
        capabilities: rocketmq_dashboard_common::ConsumerCapabilities::for_scope(
            rocketmq_dashboard_common::ConnectionScope::NameServer,
        ),
    }));
    fixture.services = fixture
        .services
        .with_topic_backend(topics.clone())
        .with_consumer_backend(consumers.clone());
    fixture.runtime.block_on(async {
        fixture
            .services
            .send_topic_message(
                TopicRequestScope { revision: 0, epoch: 1 },
                TopicSendCommand::new(
                    TopicIdentity::parse("orders").unwrap(),
                    "private-key".into(),
                    "private-tag".into(),
                    "private-body".into(),
                    true,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let result = fixture
            .services
            .create_consumer(
                ConsumerRequestScope { revision: 0, epoch: 1 },
                ConsumerCreateCommand {
                    group,
                    targets: vec![target],
                    entries: ConsumerConfigEntries {
                        retry_max_times: 16,
                        retry_queue_nums: 1,
                        consume_timeout_minutes: 15,
                    },
                    authorization: ConsumerAclClassification::Authorized,
                },
            )
            .await
            .unwrap();
        assert_eq!(result.audit().0, AuditOutcome::Partial);
    });
    let events = fixture.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].outcome, AuditOutcome::Partial);
    assert_eq!(events[1].operation.action, AuditAction::TopicMessageSent);
    let file = std::fs::read_to_string(fixture.store.path()).unwrap();
    for secret in ["private-key", "private-tag", "private-body"] {
        assert!(!file.contains(secret));
    }
    assert_eq!(topics.calls().send.len(), 1);
    assert_eq!(consumers.calls().create.len(), 1);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_domain_projection_distinguishes_rejection_unknown_partial_and_refresh_failure() {
    let topic = TopicIdentity::parse("orders").unwrap();
    let mut outcome = TopicPartialOutcome {
        topic,
        kind: TopicMutationKind::Create,
        guarantee: TopicMutationGuarantee::PreflightBestEffort,
        reload_failed: false,
        targets: vec![TopicTargetOutcome {
            target: "broker-a".into(),
            stage: TopicFailureStage::Mutation,
            applied: false,
            failure: Some(TopicFailureCode::Conflict),
            retryable: false,
        }],
    };
    assert_eq!(
        TopicMutationResult::Rejected(outcome.clone()).audit().0,
        AuditOutcome::Rejected
    );
    outcome.targets[0].failure = Some(TopicFailureCode::Unavailable);
    assert_eq!(
        TopicMutationResult::Rejected(outcome.clone()).audit().0,
        AuditOutcome::Unknown
    );
    outcome.targets[0].applied = true;
    outcome.targets[0].failure = None;
    outcome.reload_failed = true;
    let (result, facts) = TopicMutationResult::AppliedReloadFailed {
        outcome,
        invalidations: Vec::new(),
        error: UiError::new("Refresh failed.", UiErrorCode::Connection, true),
    }
    .audit();
    assert_eq!(result, AuditOutcome::Succeeded);
    assert!(facts.refresh_failed);
}

#[test]
fn audit_query_access_follows_local_auth_and_failed_login_does_not_persist_the_supplied_identity() {
    let fixture = Fixture::new();
    fixture.runtime.block_on(async {
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        fixture
            .services
            .mutate_with_progress(crate::services::ConfigMutation::SetAuthEnabled(true), progress)
            .await
            .unwrap();
        assert!(
            fixture
                .services
                .query_audit(AuditQuery {
                    filter: AuditFilter::recent(now_epoch_ms().unwrap()),
                    limit: 10,
                    cursor: None
                })
                .is_err()
        );
        assert!(
            fixture
                .services
                .authenticate("untrusted-form-user", "wrong-password")
                .await
                .is_err()
        );
        fixture
            .services
            .authenticate("operator", "private-password")
            .await
            .unwrap();
        assert!(!fixture.events().is_empty());
    });
    let file = std::fs::read_to_string(fixture.store.path()).unwrap();
    assert!(!file.contains("untrusted-form-user"));
    assert!(!file.contains("wrong-password"));
    assert!(!file.contains("private-password"));
    assert!(
        AuditStore::new(Arc::clone(&fixture.store))
            .pending()
            .unwrap()
            .is_empty()
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_login_retains_the_frozen_actor_and_records_only_the_verified_session_identity() {
    let fixture = Fixture::new();
    let environment = fixture.store.environments().unwrap().active_id;
    fixture.runtime.block_on(async {
        assert!(
            fixture
                .services
                .authenticate("unverified-form-user", "wrong-password")
                .await
                .is_err()
        );
        fixture
            .services
            .authenticate("operator", "private-password")
            .await
            .unwrap();
    });
    let events = fixture.events();
    let successful = events
        .iter()
        .find(|event| event.operation.action == AuditAction::LocalLogin && event.outcome == AuditOutcome::Succeeded)
        .unwrap();
    assert_eq!(successful.operation.context.actor, OperationActor::LocalDesktop);
    assert_eq!(successful.operation.context.environment_id, environment);
    assert_eq!(successful.operation.context.configuration_revision, 0);
    assert_eq!(
        successful.facts.authenticated_actor,
        Some(OperationActor::LocalOperator {
            username: "operator".into()
        })
    );
    let rejected = events
        .iter()
        .find(|event| event.outcome == AuditOutcome::Rejected)
        .unwrap();
    assert!(rejected.facts.authenticated_actor.is_none());
    assert_ne!(
        successful.operation.context.request_id,
        rejected.operation.context.request_id
    );
    let file = std::fs::read_to_string(fixture.store.path()).unwrap();
    for forbidden in ["unverified-form-user", "wrong-password", "private-password"] {
        assert!(!file.contains(forbidden));
    }
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_saturated_terminal_queue_still_signs_out_and_retries_only_the_reserved_logout_fact() {
    let fixture = Fixture::new();
    fixture.auth.authenticate("operator", "private-password").unwrap();
    fixture.runtime.block_on(async {
        let audit = fixture.services.audit.as_ref().unwrap();
        let mut writes = Vec::new();
        for index in 0..crate::infrastructure::audit_store::MAX_PENDING {
            let operation = audit
                .capture(
                    0,
                    AuditAction::BrokerUpdated,
                    OperationResource::Broker(format!("broker-{index}")),
                )
                .unwrap();
            audit.store.accept(operation.clone()).await.unwrap();
            writes.push(TerminalWrite {
                operation,
                outcome: AuditOutcome::Succeeded,
                facts: AuditFacts::default(),
                timestamp_epoch_ms: now_epoch_ms().unwrap(),
            });
        }
        let backup = fixture.directory.path().join("saved-config.json");
        std::fs::rename(fixture.store.path(), &backup).unwrap();
        std::fs::create_dir(fixture.store.path()).unwrap();
        for write in writes {
            audit.complete(write).await;
        }
        assert_eq!(
            audit.failed_writes.lock().len(),
            crate::infrastructure::audit_store::MAX_PENDING
        );
        fixture.services.sign_out().await.unwrap();
        assert!(!fixture.auth.session().is_authenticated());
        assert!(fixture.services.audit_write_warning());
        let logout_id = audit
            .failed_logout
            .lock()
            .as_ref()
            .unwrap()
            .write
            .operation
            .context
            .request_id
            .clone();
        fixture.services.sign_out().await.unwrap();
        assert_eq!(
            audit
                .failed_logout
                .lock()
                .as_ref()
                .unwrap()
                .write
                .operation
                .context
                .request_id,
            logout_id
        );
        assert_eq!(
            audit.failed_writes.lock().len(),
            crate::infrastructure::audit_store::MAX_PENDING
        );
        std::fs::remove_dir(fixture.store.path()).unwrap();
        std::fs::rename(backup, fixture.store.path()).unwrap();
        // A persistence retry must not execute sign_out again against a later session.
        fixture.auth.authenticate("operator", "private-password").unwrap();
        fixture.services.retry_audit_writes().await.unwrap();
        assert!(fixture.auth.session().is_authenticated());
        assert!(audit.failed_writes.lock().is_empty());
        assert!(audit.failed_logout.lock().is_none());
        assert!(!fixture.services.audit_write_warning());
    });
    let events = fixture.events();
    let logouts: Vec<_> = events
        .iter()
        .filter(|event| event.operation.action == AuditAction::LocalLogout)
        .collect();
    assert_eq!(logouts.len(), 1);
    assert_eq!(logouts[0].outcome, AuditOutcome::Succeeded);
    assert_eq!(
        logouts[0].operation.context.actor,
        OperationActor::LocalOperator {
            username: "operator".into()
        }
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn audit_retry_button_enters_the_owned_runtime_from_the_gpui_executor(cx: &mut gpui::TestAppContext) {
    use gpui::{AppContext as _, Modifiers};
    use gpui_component::Root;

    cx.update(gpui_component::init);
    let fixture = Fixture::new();
    fixture.runtime.block_on(async {
        let audit = fixture.services.audit.as_ref().unwrap();
        let operation = audit
            .capture(
                0,
                AuditAction::BrokerUpdated,
                OperationResource::Broker("broker-a".into()),
            )
            .unwrap();
        audit.store.accept(operation.clone()).await.unwrap();
        let backup = fixture.directory.path().join("saved-config.json");
        std::fs::rename(fixture.store.path(), &backup).unwrap();
        std::fs::create_dir(fixture.store.path()).unwrap();
        audit
            .complete(TerminalWrite {
                operation,
                outcome: AuditOutcome::Succeeded,
                facts: AuditFacts::default(),
                timestamp_epoch_ms: now_epoch_ms().unwrap(),
            })
            .await;
        std::fs::remove_dir(fixture.store.path()).unwrap();
        std::fs::rename(backup, fixture.store.path()).unwrap();
    });
    assert!(fixture.services.audit_write_warning());
    let (completed, completion) = std::sync::mpsc::channel();
    let services = fixture.services.clone().with_runtime_completion(completed);
    let (_root, cx) = cx.add_window_view(move |window, cx| {
        let dashboard = cx.new(|cx| crate::app::RocketmqDashboard::with_services(window, services, cx));
        Root::new(dashboard, window, cx)
    });
    cx.run_until_parked();
    assert_eq!(
        completion.recv_timeout(Duration::from_secs(5)).unwrap(),
        "gpui-bootstrap"
    );
    cx.run_until_parked();
    let bounds = cx
        .debug_bounds("retry-audit-write")
        .expect("persistent warning offers audit-only retry");
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
    assert_eq!(
        completion.recv_timeout(Duration::from_secs(5)).unwrap(),
        "gpui-audit-retry"
    );
    cx.run_until_parked();
    assert!(!fixture.services.audit_write_warning());
    assert_eq!(fixture.events().len(), 1);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
