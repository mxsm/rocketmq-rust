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

use super::idempotency::admit_cache;
use super::idempotency::complete_cache;
use super::idempotency::CacheAdmission;
use super::idempotency::IdempotencyIdentity;
use super::idempotency::IdempotencyKey;
use super::*;
use std::collections::BTreeSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::audit::MemoryAuditSink;
use crate::audit::ReliableAuditSink;
use crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION;

#[derive(Clone, Copy)]
enum Behavior {
    Success,
    Partial,
    VerificationFailed,
    Block,
    Panic,
}

#[derive(Default)]
struct Counters {
    opens: AtomicUsize,
    runs: AtomicUsize,
    shutdowns: AtomicUsize,
}

struct FakeFactory {
    behavior: Behavior,
    counters: Arc<Counters>,
    gate: Arc<tokio::sync::Notify>,
}

impl UpsertSessionFactory for FakeFactory {
    fn open<'a>(
        &'a self,
        _cluster: &'a ClusterName,
    ) -> RuntimeFuture<'a, Result<Box<dyn UpsertSession>, ControlError>> {
        Box::pin(async move {
            self.counters.opens.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FakeSession {
                behavior: self.behavior,
                counters: self.counters.clone(),
                gate: self.gate.clone(),
            }) as Box<dyn UpsertSession>)
        })
    }
}

struct FakeSession {
    behavior: Behavior,
    counters: Arc<Counters>,
    gate: Arc<tokio::sync::Notify>,
}

impl UpsertSession for FakeSession {
    fn run<'a>(&'a mut self, request: UpsertRequest) -> RuntimeFuture<'a, Result<UpsertResponse, ControlError>> {
        Box::pin(async move {
            self.counters.runs.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                Behavior::Block => self.gate.notified().await,
                Behavior::Panic => panic!("synthetic adapter panic"),
                Behavior::Success | Behavior::Partial | Behavior::VerificationFailed => {}
            }
            let UpsertRequest::Topic(args) = request else {
                return Err(ControlError::execution_failed());
            };
            let (status, targets) = match self.behavior {
                Behavior::Partial => (tools::MutationStatus::Partial, Vec::new()),
                Behavior::VerificationFailed => (
                    tools::MutationStatus::Failed,
                    vec![tools::MutationTarget {
                        target: tools::LogicalMutationTarget {
                            broker_name: "broker-a".to_owned(),
                        },
                        before: tools::VisibleState::Unknown,
                        requested: args.replacement.clone(),
                        after: None,
                        applied: true,
                        changed: true,
                        persistence: tools::PersistenceState::Persisted,
                        verification: tools::VerificationState::Failed,
                        failure: Some(tools::FailureCode::VerificationFailed),
                        retryable: true,
                    }],
                ),
                Behavior::Success | Behavior::Block | Behavior::Panic => (tools::MutationStatus::Applied, Vec::new()),
            };
            Ok(UpsertResponse::Topic(topic_response(
                &args,
                if args.dry_run {
                    tools::MutationMode::DryRun
                } else {
                    tools::MutationMode::Execute
                },
                status,
                BTreeMap::new(),
                None,
                targets,
                Vec::new(),
            )))
        })
    }

    fn shutdown(&mut self) -> RuntimeFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            self.counters.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

fn request(key: Option<&str>) -> UpsertRequest {
    UpsertRequest::Topic(tools::UpsertTopicArgs {
        schema_version: MUTATION_ARGUMENTS_SCHEMA_VERSION.to_owned(),
        cluster: "cluster-a".to_owned(),
        topic: "orders".to_owned(),
        broker_names: vec!["broker-b".to_owned(), "broker-a".to_owned()],
        replacement: tools::TopicReplacement {
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            order: false,
            message_type: tools::TopicMessageType::Normal,
        },
        dry_run: false,
        confirm: true,
        reason: Some("planned operation".to_owned()),
        request_key: key.map(ToOwned::to_owned),
    })
}

fn principal(subject: &str) -> Principal {
    Principal {
        subject: subject.to_owned(),
        scopes: BTreeSet::from(["rocketmq:write".to_owned()]),
        allowed_operations: BTreeSet::from([ControlOperation::TopicUpsert]),
        allowed_clusters: BTreeSet::from([ClusterName::try_new("cluster-a").unwrap()]),
    }
}

#[test]
fn stage_c_idempotency_identity_covers_operation_resource_and_full_payload() {
    let principal = principal("alice");
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    let common = || {
        (
            MUTATION_ARGUMENTS_SCHEMA_VERSION.to_owned(),
            "cluster-a".to_owned(),
            Some("planned operation".to_owned()),
            Some("request-1234".to_owned()),
        )
    };

    let (schema_version, cluster_name, reason, request_key) = common();
    let offset = MutationToolRequest::ConsumerOffset(tools::ResetConsumerOffsetArgs {
        schema_version,
        cluster: cluster_name,
        topic: "orders".to_owned(),
        consumer_group: "workers".to_owned(),
        timestamp: "2026-08-30T00:00:00Z".to_owned(),
        force: false,
        dry_run: false,
        confirm: true,
        reason,
        request_key,
    });
    let offset_identity = IdempotencyIdentity::from_request(&principal, &cluster, &offset).unwrap();
    let offset_key = offset_identity.key.as_ref().expect("explicit offset key");
    assert_eq!(offset_key.operation, ControlOperation::ConsumerOffsetReset);
    assert_eq!(offset_key.targets, ["orders", "workers"]);

    let (schema_version, cluster_name, reason, request_key) = common();
    let broker = MutationToolRequest::BrokerConfig(tools::PatchBrokerConfigArgs {
        schema_version,
        cluster: cluster_name,
        broker_name: "broker-a".to_owned(),
        properties: tools::BrokerConfigProperties {
            trace_topic_enable: Some("true".to_owned()),
            ..tools::BrokerConfigProperties::default()
        },
        dry_run: false,
        confirm: true,
        reason,
        request_key,
    });
    let broker_identity = IdempotencyIdentity::from_request(&principal, &cluster, &broker).unwrap();
    let broker_key = broker_identity.key.as_ref().expect("explicit broker key");
    assert_eq!(broker_key.operation, ControlOperation::BrokerConfigPatch);
    assert_eq!(broker_key.targets, ["broker-a"]);

    let (schema_version, cluster_name, reason, request_key) = common();
    let request_mode = MutationToolRequest::ConsumerRequestMode(tools::SetConsumerRequestModeArgs {
        schema_version,
        cluster: cluster_name,
        topic: "orders".to_owned(),
        consumer_group: "workers".to_owned(),
        mode: tools::ConsumerRequestMode::Pop,
        pop_share_queue_num: 4,
        timeout_millis: 12_000,
        dry_run: false,
        confirm: true,
        reason,
        request_key,
    });
    let request_mode_identity = IdempotencyIdentity::from_request(&principal, &cluster, &request_mode).unwrap();
    let request_mode_key = request_mode_identity.key.as_ref().expect("explicit request-mode key");
    assert_eq!(request_mode_key.operation, ControlOperation::ConsumerRequestMode);
    assert_eq!(request_mode_key.targets, ["orders", "workers"]);

    assert_ne!(offset_identity.payload, broker_identity.payload);
    assert_ne!(broker_identity.payload, request_mode_identity.payload);
    let mut changed_timeout = request_mode.clone();
    let MutationToolRequest::ConsumerRequestMode(args) = &mut changed_timeout else {
        unreachable!();
    };
    args.timeout_millis += 1;
    assert_ne!(
        request_mode_identity.payload,
        IdempotencyIdentity::from_request(&principal, &cluster, &changed_timeout)
            .unwrap()
            .payload
    );
}

fn runtime(
    behavior: Behavior,
    timeout: Duration,
) -> (
    ToolRuntime,
    Arc<Counters>,
    Arc<MemoryAuditSink>,
    Arc<tokio::sync::Notify>,
) {
    let counters = Arc::new(Counters::default());
    let sink = Arc::new(MemoryAuditSink::new(32, 4096));
    let gate = Arc::new(tokio::sync::Notify::new());
    let context = rocketmq_runtime::RuntimeContext::from_current("control-upsert-test");
    let owner = context.service_context("control-upsert-test").task_group().clone();
    (
        ToolRuntime::new(
            AuditTrail::new(sink.clone()),
            Arc::new(FakeFactory {
                behavior,
                counters: counters.clone(),
                gate: gate.clone(),
            }),
            timeout,
            owner,
        ),
        counters,
        sink,
        gate,
    )
}

fn authorized() -> AuthorizedMutation {
    AuthorizedMutation::synthetic(
        ControlOperation::TopicUpsert,
        ClusterName::try_new("cluster-a").unwrap(),
    )
}

#[tokio::test]
async fn completed_partial_and_verification_failures_are_cached_with_exact_audit() {
    for behavior in [Behavior::Success, Behavior::Partial, Behavior::VerificationFailed] {
        let (runtime, counters, sink, _) = runtime(behavior, Duration::from_secs(1));
        let first = runtime
            .execute(
                &principal("alice"),
                &authorized(),
                request(Some("request-1234")),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let second = runtime
            .execute(
                &principal("alice"),
                &authorized(),
                request(Some("request-1234")),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&second).unwrap(),
            serde_json::to_value(&first).unwrap()
        );
        let (expected_result, expected_code) = match behavior {
            Behavior::Success => (crate::audit::AuditResult::Applied, None),
            Behavior::Partial => (
                crate::audit::AuditResult::Partial,
                Some(crate::error::ControlErrorCode::PartialApply),
            ),
            Behavior::VerificationFailed => (
                crate::audit::AuditResult::Failed,
                Some(crate::error::ControlErrorCode::VerificationFailed),
            ),
            Behavior::Block | Behavior::Panic => unreachable!(),
        };
        assert_eq!(first.is_error(), !matches!(behavior, Behavior::Success));
        for response in [&first, &second] {
            let UpsertResponse::Topic(response) = response else {
                unreachable!();
            };
            assert_eq!(response.error_code, expected_code);
        }
        assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
        assert_eq!(counters.runs.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
        let records = sink.records().await.unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[1].result, expected_result);
        assert_eq!(records[3].result, expected_result);
        assert_eq!(records[1].error_code, expected_code);
        assert_eq!(records[3].error_code, expected_code);
    }
}

#[tokio::test]
async fn request_key_collision_and_principal_isolation_are_fail_closed() {
    let (runtime, counters, _, _) = runtime(Behavior::Success, Duration::from_secs(1));
    runtime
        .execute(
            &principal("alice"),
            &authorized(),
            request(Some("request-1234")),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let mut changed = request(Some("request-1234"));
    let UpsertRequest::Topic(args) = &mut changed else {
        unreachable!();
    };
    args.replacement.write_queue_nums = 9;
    assert_eq!(
        runtime
            .execute(&principal("alice"), &authorized(), changed, CancellationToken::new())
            .await
            .unwrap_err()
            .code(),
        crate::error::ControlErrorCode::InvalidArgument
    );
    runtime
        .execute(
            &principal("bob"),
            &authorized(),
            request(Some("request-1234")),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(counters.opens.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn concurrent_followers_share_one_session_and_adapter_panics_still_shutdown() {
    let (blocked_runtime, counters, _, gate) = runtime(Behavior::Block, Duration::from_secs(1));
    let first = tokio::spawn({
        let runtime = blocked_runtime.clone();
        async move {
            runtime
                .execute(
                    &principal("alice"),
                    &authorized(),
                    request(Some("request-1234")),
                    CancellationToken::new(),
                )
                .await
        }
    });
    while counters.runs.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let second = tokio::spawn({
        let runtime = blocked_runtime.clone();
        async move {
            runtime
                .execute(
                    &principal("alice"),
                    &authorized(),
                    request(Some("request-1234")),
                    CancellationToken::new(),
                )
                .await
        }
    });
    tokio::task::yield_now().await;
    gate.notify_waiters();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);

    let (runtime, counters, _, _) = runtime(Behavior::Panic, Duration::from_secs(1));
    assert_eq!(
        runtime
            .execute(
                &principal("alice"),
                &authorized(),
                request(None),
                CancellationToken::new()
            )
            .await
            .unwrap_err()
            .code(),
        crate::error::ControlErrorCode::ExecutionFailed
    );
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_a_follower_does_not_cancel_or_evict_the_leader() {
    let (runtime, counters, sink, gate) = runtime(Behavior::Block, Duration::from_secs(2));
    let leader = tokio::spawn({
        let runtime = runtime.clone();
        async move {
            runtime
                .execute(
                    &principal("alice"),
                    &authorized(),
                    request(Some("request-1234")),
                    CancellationToken::new(),
                )
                .await
        }
    });
    while counters.runs.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }

    let cancellation = CancellationToken::new();
    let follower = tokio::spawn({
        let runtime = runtime.clone();
        let cancellation = cancellation.clone();
        async move {
            runtime
                .execute(
                    &principal("alice"),
                    &authorized(),
                    request(Some("request-1234")),
                    cancellation,
                )
                .await
        }
    });
    while sink.records().await.unwrap().len() < 2 {
        tokio::task::yield_now().await;
    }
    cancellation.cancel();
    assert_eq!(
        follower.await.unwrap().unwrap_err().code(),
        crate::error::ControlErrorCode::Cancelled
    );
    assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 0);

    gate.notify_waiters();
    leader.await.unwrap().unwrap();
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    runtime
        .execute(
            &principal("alice"),
            &authorized(),
            request(Some("request-1234")),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
    assert_eq!(sink.records().await.unwrap().len(), 6);
}

#[tokio::test(start_paused = true)]
async fn timing_out_a_follower_does_not_cancel_or_evict_the_leader() {
    let (runtime, counters, sink, gate) = runtime(Behavior::Block, Duration::from_secs(2));
    let leader = tokio::spawn({
        let runtime = runtime.clone();
        async move {
            runtime
                .execute(
                    &principal("alice"),
                    &authorized(),
                    request(Some("request-1234")),
                    CancellationToken::new(),
                )
                .await
        }
    });
    while counters.runs.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }

    let mut follower_runtime = runtime.clone();
    follower_runtime.operation_timeout = Duration::from_millis(5);
    let follower = tokio::spawn(async move {
        follower_runtime
            .execute(
                &principal("alice"),
                &authorized(),
                request(Some("request-1234")),
                CancellationToken::new(),
            )
            .await
    });
    while sink.records().await.unwrap().len() < 2 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_millis(6)).await;
    assert_eq!(
        follower.await.unwrap().unwrap_err().code(),
        crate::error::ControlErrorCode::Timeout
    );
    assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 0);

    gate.notify_waiters();
    leader.await.unwrap().unwrap();
    runtime
        .execute(
            &principal("alice"),
            &authorized(),
            request(Some("request-1234")),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(sink.records().await.unwrap().len(), 6);
}

#[tokio::test]
async fn explicit_key_capacity_rejection_is_stable_before_audit_or_session() {
    let (runtime, counters, sink, _) = runtime(Behavior::Success, Duration::from_secs(1));
    for index in 0..IDEMPOTENCY_CAPACITY {
        let key = IdempotencyKey {
            principal: "blocked".to_owned(),
            operation: ControlOperation::TopicUpsert,
            cluster: ClusterName::try_new("cluster-a").unwrap(),
            targets: vec!["broker-a".to_owned()],
            request_key: format!("blocked-{index:08}"),
        };
        assert!(matches!(
            admit_cache(&runtime.idempotency, Some(&key), "blocked").await.unwrap(),
            CacheAdmission::Leader
        ));
    }

    let principal = principal("alice");
    let authorized = authorized();
    let first = runtime.execute(
        &principal,
        &authorized,
        request(Some("request-4097")),
        CancellationToken::new(),
    );
    let second = runtime.execute(
        &principal,
        &authorized,
        request(Some("request-4097")),
        CancellationToken::new(),
    );
    let (first, second) = tokio::join!(first, second);
    for result in [first, second] {
        assert_eq!(
            result.unwrap_err().code(),
            crate::error::ControlErrorCode::OperationUnavailable
        );
    }
    assert_eq!(runtime.idempotency.lock().await.entries.len(), IDEMPOTENCY_CAPACITY);
    assert_eq!(counters.opens.load(Ordering::SeqCst), 0);
    assert!(sink.records().await.unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn idempotency_ttl_expires_and_capacity_evicts_oldest_completed_entry() {
    let (runtime, counters, _, _) = runtime(Behavior::Success, Duration::from_secs(1));
    runtime
        .execute(
            &principal("alice"),
            &authorized(),
            request(Some("request-1234")),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    tokio::time::advance(IDEMPOTENCY_TTL + Duration::from_millis(1)).await;
    runtime
        .execute(
            &principal("alice"),
            &authorized(),
            request(Some("request-1234")),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(counters.opens.load(Ordering::SeqCst), 2);

    let cache = Mutex::new(IdempotencyState::default());
    let UpsertRequest::Topic(sample_args) = request(None) else {
        unreachable!();
    };
    let result = Ok(UpsertResponse::Topic(topic_response(
        &sample_args,
        tools::MutationMode::DryRun,
        tools::MutationStatus::Planned,
        BTreeMap::new(),
        None,
        Vec::new(),
        Vec::new(),
    )));
    for index in 0..=IDEMPOTENCY_CAPACITY {
        let key = IdempotencyKey {
            principal: "alice".to_owned(),
            operation: ControlOperation::TopicUpsert,
            cluster: ClusterName::try_new("cluster-a").unwrap(),
            targets: vec!["broker-a".to_owned()],
            request_key: format!("request-{index:08}"),
        };
        assert!(matches!(
            admit_cache(&cache, Some(&key), "payload").await.unwrap(),
            CacheAdmission::Leader
        ));
        complete_cache(&cache, key, "payload".to_owned(), result.clone()).await;
    }
    let state = cache.lock().await;
    assert_eq!(state.entries.len(), IDEMPOTENCY_CAPACITY);
    assert!(!state.entries.keys().any(|key| key.request_key == "request-00000000"));
}

#[tokio::test]
async fn cancellation_timeout_and_caller_drop_preserve_shutdown_and_terminal_audit() {
    let (cancel_runtime, counters, sink, _) = runtime(Behavior::Block, Duration::from_secs(1));
    let cancellation = CancellationToken::new();
    let task = tokio::spawn({
        let runtime = cancel_runtime.clone();
        let task_cancellation = cancellation.clone();
        async move {
            runtime
                .execute(&principal("alice"), &authorized(), request(None), task_cancellation)
                .await
        }
    });
    while counters.runs.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    cancellation.cancel();
    assert_eq!(
        task.await.unwrap().unwrap_err().code(),
        crate::error::ControlErrorCode::Cancelled
    );
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(sink.records().await.unwrap().len(), 2);

    let (timeout_runtime, counters, _, _) = runtime(Behavior::Block, Duration::from_millis(5));
    assert_eq!(
        timeout_runtime
            .execute(
                &principal("alice"),
                &authorized(),
                request(None),
                CancellationToken::new()
            )
            .await
            .unwrap_err()
            .code(),
        crate::error::ControlErrorCode::Timeout
    );
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);

    let (runtime, counters, sink, gate) = runtime(Behavior::Block, Duration::from_secs(1));
    let caller = tokio::spawn({
        let runtime = runtime.clone();
        async move {
            runtime
                .execute(
                    &principal("alice"),
                    &authorized(),
                    request(None),
                    CancellationToken::new(),
                )
                .await
        }
    });
    while counters.runs.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    caller.abort();
    gate.notify_waiters();
    tokio::time::timeout(Duration::from_secs(1), async {
        while counters.shutdowns.load(Ordering::SeqCst) == 0 || sink.records().await.unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn failed_postread_preserves_applied_and_persisted_target_truth() {
    let UpsertRequest::Topic(args) = request(None) else {
        unreachable!();
    };
    let before = BTreeMap::from([("broker-a".to_owned(), tools::VisibleState::Absent)]);
    let outcome = admin::MetadataMutationOutcome {
        targets: vec![admin::MetadataMutationTargetOutcome {
            broker_name: "broker-a".to_owned(),
            expected_state: admin::ExpectedState::Absent,
            resulting_state: Some(admin::ExpectedState::Present { version: 1 }),
            applied: true,
            changed: true,
            persistence: admin::MutationPersistenceState::Persisted,
            verification: admin::MutationVerificationState::NotPerformed,
            failure: None,
            retryable: false,
        }],
        failures: Vec::new(),
        order_reconciled: Some(true),
    };
    let response = topic_executed(&args, before, outcome, None);
    assert_eq!(response.status, tools::MutationStatus::Failed);
    assert_eq!(
        response.error_code,
        Some(crate::error::ControlErrorCode::VerificationFailed)
    );
    assert!(response.is_error());
    assert_eq!(response.targets[0].after, None);
    assert!(response.targets[0].applied);
    assert!(response.targets[0].changed);
    assert_eq!(response.targets[0].persistence, tools::PersistenceState::Persisted);
    assert_eq!(response.targets[0].verification, tools::VerificationState::Failed);
    assert_eq!(
        response.targets[0].failure,
        Some(tools::FailureCode::VerificationFailed)
    );
}

#[test]
fn topic_and_group_dry_runs_report_all_mixed_and_zero_success() {
    let UpsertRequest::Topic(topic_args) = request(None) else {
        unreachable!();
    };
    let group_args = tools::UpsertConsumerGroupArgs {
        schema_version: MUTATION_ARGUMENTS_SCHEMA_VERSION.to_owned(),
        cluster: "cluster-a".to_owned(),
        consumer_group: "orders_consumers".to_owned(),
        broker_names: vec!["broker-a".to_owned(), "broker-b".to_owned()],
        replacement: tools::ConsumerGroupReplacement {
            consume_enable: true,
            consume_from_min_enable: false,
            consume_broadcast_enable: false,
            consume_message_orderly: false,
            retry_queue_nums: 1,
            retry_max_times: 16,
            broker_id: 0,
            which_broker_when_consume_slowly: 1,
            notify_consumer_ids_changed_enable: true,
            group_sys_flag: 0,
            consume_timeout_minute: 15,
        },
        dry_run: true,
        confirm: false,
        reason: None,
        request_key: None,
    };
    let topic_before = BTreeMap::from([
        ("broker-a".to_owned(), tools::VisibleState::Absent),
        ("broker-b".to_owned(), tools::VisibleState::Absent),
    ]);
    let group_before = BTreeMap::from([
        ("broker-a".to_owned(), tools::VisibleState::Absent),
        ("broker-b".to_owned(), tools::VisibleState::Absent),
    ]);
    let failure = |broker_name: &str| admin::MutationTargetFailure {
        broker_name: broker_name.to_owned(),
        queue_id: None,
        code: admin::MutationFailureCode::Unavailable,
        retryable: true,
    };
    for (failures, expected) in [
        (Vec::new(), tools::MutationStatus::Planned),
        (vec![failure("broker-a")], tools::MutationStatus::Partial),
        (
            vec![failure("broker-a"), failure("broker-b")],
            tools::MutationStatus::Failed,
        ),
    ] {
        assert_eq!(
            topic_dry_run(&topic_args, topic_before.clone(), &failures).status,
            expected
        );
        assert_eq!(
            group_dry_run(&group_args, group_before.clone(), &failures).status,
            expected
        );
    }
}

#[test]
fn response_mapping_preserves_unchanged_conflict_partial_and_persistence_states() {
    let replacement = tools::TopicReplacement {
        read_queue_nums: 8,
        write_queue_nums: 8,
        perm: 6,
        order: false,
        message_type: tools::TopicMessageType::Normal,
    };
    let state = tools::VisibleState::Present {
        version: 2,
        value: replacement.clone(),
    };
    let mapped = |outcomes: Vec<admin::MetadataMutationTargetOutcome>, brokers: &[&str]| {
        let before = brokers
            .iter()
            .map(|broker| ((*broker).to_owned(), state.clone()))
            .collect();
        let after = brokers
            .iter()
            .map(|broker| ((*broker).to_owned(), state.clone()))
            .collect();
        let outcomes = outcomes
            .into_iter()
            .map(|outcome| (outcome.broker_name.clone(), outcome))
            .collect();
        let targets = build_executed_targets(before, after, outcomes, BTreeMap::new(), replacement.clone());
        (execution_status(&targets), targets)
    };
    let outcome =
        |broker: &str,
         applied: bool,
         changed: bool,
         persistence: admin::MutationPersistenceState,
         failure: Option<admin::MutationFailureCode>| admin::MetadataMutationTargetOutcome {
            broker_name: broker.to_owned(),
            expected_state: admin::ExpectedState::Present { version: 1 },
            resulting_state: Some(admin::ExpectedState::Present { version: 2 }),
            applied,
            changed,
            persistence,
            verification: admin::MutationVerificationState::NotPerformed,
            failure,
            retryable: false,
        };

    let (status, targets) = mapped(
        vec![outcome(
            "broker-a",
            true,
            false,
            admin::MutationPersistenceState::NotRequired,
            None,
        )],
        &["broker-a"],
    );
    assert_eq!(status, tools::MutationStatus::Applied);
    assert_eq!(targets[0].verification, tools::VerificationState::Verified);

    let (status, _) = mapped(
        vec![outcome(
            "broker-a",
            false,
            false,
            admin::MutationPersistenceState::NotRequired,
            Some(admin::MutationFailureCode::Conflict),
        )],
        &["broker-a"],
    );
    assert_eq!(status, tools::MutationStatus::Conflict);

    let (status, targets) = mapped(
        vec![outcome(
            "broker-a",
            true,
            true,
            admin::MutationPersistenceState::Failed,
            Some(admin::MutationFailureCode::PersistenceFailed),
        )],
        &["broker-a"],
    );
    assert_eq!(status, tools::MutationStatus::Failed);
    assert_eq!(targets[0].persistence, tools::PersistenceState::Failed);

    let (status, targets) = mapped(
        vec![
            outcome("broker-a", true, true, admin::MutationPersistenceState::Persisted, None),
            outcome(
                "broker-b",
                false,
                false,
                admin::MutationPersistenceState::NotRequired,
                Some(admin::MutationFailureCode::Conflict),
            ),
        ],
        &["broker-b", "broker-a"],
    );
    assert_eq!(status, tools::MutationStatus::Partial);
    assert_eq!(
        targets
            .iter()
            .map(|target| target.target.broker_name.as_str())
            .collect::<Vec<_>>(),
        vec!["broker-a", "broker-b"]
    );
}
