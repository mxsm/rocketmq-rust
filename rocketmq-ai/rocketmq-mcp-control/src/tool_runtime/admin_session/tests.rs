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

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use super::*;

#[derive(Default)]
struct Evidence {
    offset_executes: AtomicUsize,
    offset_verification_failure: AtomicBool,
    broker_executes: AtomicUsize,
    request_mode_executes: AtomicUsize,
    request_mode_verification_failure: AtomicBool,
    request_mode_mixed_postread: AtomicBool,
    request_mode_timeout: AtomicU64,
    shutdowns: AtomicUsize,
}

struct OffsetPlan {
    rows: Vec<admin::OffsetResetPreviewRow>,
    failures: Vec<admin::MutationTargetFailure>,
}

struct BrokerPlan {
    targets: Vec<admin::BrokerMutationConfigTarget>,
    failures: Vec<admin::MutationTargetFailure>,
}

struct RequestModePlan {
    targets: Vec<(String, Option<admin::RequestModeValue>)>,
    failures: Vec<admin::MutationTargetFailure>,
}

struct ProductionPathBackend {
    evidence: Arc<Evidence>,
}

fn unused<T: Send>() -> RuntimeFuture<'static, Result<T, ControlError>> {
    Box::pin(async { Err(ControlError::operation_unavailable()) })
}

impl SupervisedMutationBackend for ProductionPathBackend {
    type TopicPlan = ();
    type GroupPlan = ();
    type OffsetPlan = OffsetPlan;
    type BrokerPlan = BrokerPlan;
    type RequestModePlan = RequestModePlan;

    fn preflight_topic<'a>(
        &'a mut self,
        _request: &'a admin::TopicMutationPreflightRequest,
        _broker_names: &'a [String],
    ) -> RuntimeFuture<'a, Result<Self::TopicPlan, ControlError>> {
        unused()
    }

    fn topic_targets(_plan: &Self::TopicPlan) -> Vec<admin::MetadataPreflightTarget<admin::TopicReplacement>> {
        Vec::new()
    }

    fn topic_failures(_plan: &Self::TopicPlan) -> &[admin::MutationTargetFailure] {
        &[]
    }

    fn execute_topic<'a>(
        &'a mut self,
        _plan: &'a Self::TopicPlan,
    ) -> RuntimeFuture<'a, Result<admin::MetadataMutationOutcome, ControlError>> {
        unused()
    }

    fn preflight_group<'a>(
        &'a mut self,
        _request: &'a admin::SubscriptionGroupMutationPreflightRequest,
        _broker_names: &'a [String],
    ) -> RuntimeFuture<'a, Result<Self::GroupPlan, ControlError>> {
        unused()
    }

    fn group_targets(
        _plan: &Self::GroupPlan,
    ) -> Vec<admin::MetadataPreflightTarget<admin::SubscriptionGroupReplacement>> {
        Vec::new()
    }

    fn group_failures(_plan: &Self::GroupPlan) -> &[admin::MutationTargetFailure] {
        &[]
    }

    fn execute_group<'a>(
        &'a mut self,
        _plan: &'a Self::GroupPlan,
    ) -> RuntimeFuture<'a, Result<admin::MetadataMutationOutcome, ControlError>> {
        unused()
    }

    fn preview_offset<'a>(
        &'a mut self,
        _request: &'a admin::OffsetResetPreviewRequest,
    ) -> RuntimeFuture<'a, Result<Self::OffsetPlan, ControlError>> {
        Box::pin(async {
            Ok(OffsetPlan {
                rows: vec![admin::OffsetResetPreviewRow {
                    broker_name: "broker-a".to_owned(),
                    queue_id: 0,
                    current_offset: 9,
                    planned_offset: 4,
                    delta: -5,
                    changed: true,
                }],
                failures: vec![admin::MutationTargetFailure {
                    broker_name: "broker-0-preview-failed".to_owned(),
                    queue_id: None,
                    code: admin::MutationFailureCode::Unavailable,
                    retryable: true,
                }],
            })
        })
    }

    fn offset_rows(plan: &Self::OffsetPlan) -> Vec<admin::OffsetResetPreviewRow> {
        plan.rows.clone()
    }

    fn offset_failures(plan: &Self::OffsetPlan) -> &[admin::MutationTargetFailure] {
        &plan.failures
    }

    fn execute_offset<'a>(
        &'a mut self,
        _plan: &'a Self::OffsetPlan,
    ) -> RuntimeFuture<'a, Result<admin::OffsetResetOutcome, ControlError>> {
        self.evidence.offset_executes.fetch_add(1, Ordering::SeqCst);
        let verification_failure = self.evidence.offset_verification_failure.load(Ordering::SeqCst);
        Box::pin(async move {
            Ok(admin::OffsetResetOutcome {
                targets: vec![admin::OffsetResetTargetOutcome {
                    broker_name: "broker-a".to_owned(),
                    queue_id: 0,
                    expected_offset: 9,
                    planned_offset: 4,
                    observed_offset: (!verification_failure).then_some(4),
                    applied: true,
                    changed: true,
                    failure: verification_failure.then_some(admin::MutationFailureCode::VerificationFailed),
                    retryable: verification_failure,
                }],
                failures: vec![admin::MutationTargetFailure {
                    broker_name: "broker-0-preview-failed".to_owned(),
                    queue_id: None,
                    code: admin::MutationFailureCode::Unavailable,
                    retryable: true,
                }],
            })
        })
    }

    fn preflight_broker<'a>(
        &'a mut self,
        _cluster: &'a str,
        broker_name: &'a str,
    ) -> RuntimeFuture<'a, Result<Self::BrokerPlan, ControlError>> {
        let broker_name = broker_name.to_owned();
        Box::pin(async move {
            Ok(BrokerPlan {
                targets: vec![admin::BrokerMutationConfigTarget {
                    broker_name,
                    state: admin::BrokerMutationConfigState {
                        generation: 7,
                        auto_create_topic_enable: true,
                        auto_create_subscription_group: true,
                        broker_permission: 6,
                        default_topic_queue_nums: 8,
                        message_index_enable: true,
                        trace_topic_enable: false,
                    },
                }],
                failures: Vec::new(),
            })
        })
    }

    fn broker_targets(plan: &Self::BrokerPlan) -> Vec<admin::BrokerMutationConfigTarget> {
        plan.targets.clone()
    }

    fn broker_failures(plan: &Self::BrokerPlan) -> &[admin::MutationTargetFailure] {
        &plan.failures
    }

    fn execute_broker<'a>(
        &'a mut self,
        _plan: &'a Self::BrokerPlan,
        patch: admin::BrokerMutationConfigPatch,
    ) -> RuntimeFuture<'a, Result<admin::BrokerMutationConfigOutcome, ControlError>> {
        self.evidence.broker_executes.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            assert_eq!(patch.trace_topic_enable, Some(true));
            Ok(admin::BrokerMutationConfigOutcome {
                targets: vec![admin::BrokerMutationConfigTargetOutcome {
                    broker_name: "broker-a".to_owned(),
                    before: admin::BrokerMutationConfigState {
                        generation: 7,
                        auto_create_topic_enable: true,
                        auto_create_subscription_group: true,
                        broker_permission: 6,
                        default_topic_queue_nums: 8,
                        message_index_enable: true,
                        trace_topic_enable: false,
                    },
                    after: None,
                    applied: true,
                    changed: true,
                    persistence: admin::MutationPersistenceState::Persisted,
                    verification: admin::MutationVerificationState::Failed,
                    failure: Some(admin::MutationFailureCode::VerificationFailed),
                    retryable: true,
                }],
                failures: Vec::new(),
            })
        })
    }

    fn preflight_request_mode<'a>(
        &'a mut self,
        request: &'a admin::RequestModePreflightRequest,
    ) -> RuntimeFuture<'a, Result<Self::RequestModePlan, ControlError>> {
        let replacement = request.replacement;
        let mixed_postread = self.evidence.request_mode_mixed_postread.load(Ordering::SeqCst);
        Box::pin(async move {
            assert_eq!(replacement.mode, admin::RequestMode::Pop);
            let mut targets = vec![(
                "broker-a".to_owned(),
                Some(admin::RequestModeValue {
                    mode: admin::RequestMode::Pull,
                    pop_share_queue_num: 0,
                }),
            )];
            if mixed_postread {
                targets.insert(
                    0,
                    (
                        "broker-b".to_owned(),
                        Some(admin::RequestModeValue {
                            mode: admin::RequestMode::Pull,
                            pop_share_queue_num: 0,
                        }),
                    ),
                );
            }
            Ok(RequestModePlan {
                targets,
                failures: Vec::new(),
            })
        })
    }

    fn request_mode_targets(plan: &Self::RequestModePlan) -> Vec<(String, Option<admin::RequestModeValue>)> {
        plan.targets.clone()
    }

    fn request_mode_failures(plan: &Self::RequestModePlan) -> &[admin::MutationTargetFailure] {
        &plan.failures
    }

    fn execute_request_mode<'a>(
        &'a mut self,
        _plan: &'a Self::RequestModePlan,
        timeout_millis: u64,
    ) -> RuntimeFuture<'a, Result<admin::RequestModeMutationOutcome, ControlError>> {
        self.evidence.request_mode_executes.fetch_add(1, Ordering::SeqCst);
        self.evidence
            .request_mode_timeout
            .store(timeout_millis, Ordering::SeqCst);
        let verification_failure = self.evidence.request_mode_verification_failure.load(Ordering::SeqCst);
        let mixed_postread = self.evidence.request_mode_mixed_postread.load(Ordering::SeqCst);
        Box::pin(async move {
            let successful = admin::RequestModeTargetOutcome {
                broker_name: "broker-a".to_owned(),
                expected: Some(admin::RequestModeValue {
                    mode: admin::RequestMode::Pull,
                    pop_share_queue_num: 0,
                }),
                current: Some(admin::RequestModeValue {
                    mode: admin::RequestMode::Pop,
                    pop_share_queue_num: 4,
                }),
                applied: true,
                changed: true,
                persistence: admin::MutationPersistenceState::Persisted,
                verification: admin::MutationVerificationState::Verified,
                failure: None,
                retryable: false,
            };
            if mixed_postread {
                return Ok(admin::RequestModeMutationOutcome {
                    targets: vec![
                        admin::RequestModeTargetOutcome {
                            broker_name: "broker-b".to_owned(),
                            expected: Some(admin::RequestModeValue {
                                mode: admin::RequestMode::Pull,
                                pop_share_queue_num: 0,
                            }),
                            current: None,
                            applied: true,
                            changed: true,
                            persistence: admin::MutationPersistenceState::Persisted,
                            verification: admin::MutationVerificationState::Failed,
                            failure: Some(admin::MutationFailureCode::VerificationFailed),
                            retryable: true,
                        },
                        successful,
                    ],
                    failures: Vec::new(),
                });
            }
            Ok(admin::RequestModeMutationOutcome {
                targets: vec![if verification_failure {
                    admin::RequestModeTargetOutcome {
                        current: None,
                        verification: admin::MutationVerificationState::Failed,
                        failure: Some(admin::MutationFailureCode::VerificationFailed),
                        retryable: true,
                        ..successful
                    }
                } else {
                    successful
                }],
                failures: Vec::new(),
            })
        })
    }

    fn shutdown(&mut self) -> RuntimeFuture<'_, Result<(), ControlError>> {
        self.evidence.shutdowns.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

fn common() -> (String, String, bool, bool, Option<String>, Option<String>) {
    (
        crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION.to_owned(),
        "cluster-a".to_owned(),
        false,
        true,
        Some("approved operation".to_owned()),
        None,
    )
}

#[tokio::test]
async fn production_session_orchestrates_all_stage_c_operations_and_preserves_truth() {
    let evidence = Arc::new(Evidence::default());
    let mut session = AdminMutationToolSession::new(ProductionPathBackend {
        evidence: Arc::clone(&evidence),
    });
    let (schema_version, cluster, dry_run, confirm, reason, request_key) = common();
    let offset = session
        .run(MutationToolRequest::ConsumerOffset(tools::ResetConsumerOffsetArgs {
            schema_version,
            cluster,
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            timestamp: "2026-08-30T08:00:00+08:00".to_owned(),
            force: false,
            dry_run,
            confirm,
            reason,
            request_key,
        }))
        .await
        .expect("offset through production session");
    let MutationToolResponse::ConsumerOffset(offset) = offset else {
        panic!("unexpected response variant");
    };
    assert_eq!(offset.status, tools::MutationStatus::Partial);
    assert_eq!(offset.error_code, Some(crate::error::ControlErrorCode::PartialApply));
    assert_eq!(offset.targets.len(), 2);
    assert!(offset.targets.iter().any(|target| {
        target.broker_name == "broker-0-preview-failed"
            && target.queue_id.is_none()
            && target.failure == Some(tools::FailureCode::Unavailable)
    }));
    assert_eq!(offset.targets[0].broker_name, "broker-0-preview-failed");
    assert_eq!(
        offset
            .targets
            .iter()
            .find(|target| target.queue_id == Some(0))
            .and_then(|target| target.after),
        Some(4)
    );

    let (schema_version, cluster, dry_run, confirm, reason, request_key) = common();
    let broker = session
        .run(MutationToolRequest::BrokerConfig(tools::PatchBrokerConfigArgs {
            schema_version,
            cluster,
            broker_name: "broker-a".to_owned(),
            properties: tools::BrokerConfigProperties {
                trace_topic_enable: Some("true".to_owned()),
                ..tools::BrokerConfigProperties::default()
            },
            dry_run,
            confirm,
            reason,
            request_key,
        }))
        .await
        .expect("broker through production session");
    let MutationToolResponse::BrokerConfig(broker) = broker else {
        panic!("unexpected response variant");
    };
    assert_eq!(broker.status, tools::MutationStatus::Failed);
    assert_eq!(
        broker.error_code,
        Some(crate::error::ControlErrorCode::VerificationFailed)
    );
    assert!(broker.after.is_none());
    assert!(broker.targets[0].applied);
    assert!(broker.targets[0].after.is_none());
    assert_eq!(broker.targets[0].failure, Some(tools::FailureCode::VerificationFailed));

    let (schema_version, cluster, dry_run, confirm, reason, request_key) = common();
    let request_mode = session
        .run(MutationToolRequest::ConsumerRequestMode(
            tools::SetConsumerRequestModeArgs {
                schema_version,
                cluster,
                topic: "orders".to_owned(),
                consumer_group: "orders-consumer".to_owned(),
                mode: tools::ConsumerRequestMode::Pop,
                pop_share_queue_num: 4,
                timeout_millis: 12_345,
                dry_run,
                confirm,
                reason,
                request_key,
            },
        ))
        .await
        .expect("request mode through production session");
    let MutationToolResponse::ConsumerRequestMode(request_mode) = request_mode else {
        panic!("unexpected response variant");
    };
    assert_eq!(request_mode.status, tools::MutationStatus::Applied);
    assert_eq!(request_mode.error_code, None);
    assert_eq!(
        request_mode.targets[0].after.expect("postread").mode,
        tools::ConsumerRequestMode::Pop
    );
    assert_eq!(evidence.offset_executes.load(Ordering::SeqCst), 1);
    assert_eq!(evidence.broker_executes.load(Ordering::SeqCst), 1);
    assert_eq!(evidence.request_mode_executes.load(Ordering::SeqCst), 1);
    assert_eq!(evidence.request_mode_timeout.load(Ordering::SeqCst), 12_345);

    session.shutdown().await.expect("session shutdown");
    assert_eq!(evidence.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn production_session_maps_stage_c_postread_errors_as_verification_failures() {
    let evidence = Arc::new(Evidence::default());
    evidence.offset_verification_failure.store(true, Ordering::SeqCst);
    evidence.request_mode_verification_failure.store(true, Ordering::SeqCst);
    let mut session = AdminMutationToolSession::new(ProductionPathBackend {
        evidence: Arc::clone(&evidence),
    });

    let (schema_version, cluster, dry_run, confirm, reason, request_key) = common();
    let offset = session
        .run(MutationToolRequest::ConsumerOffset(tools::ResetConsumerOffsetArgs {
            schema_version,
            cluster,
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            timestamp: "2026-08-30T08:00:00+08:00".to_owned(),
            force: false,
            dry_run,
            confirm,
            reason,
            request_key,
        }))
        .await
        .expect("offset through production session");
    let MutationToolResponse::ConsumerOffset(offset) = offset else {
        panic!("unexpected response variant");
    };
    let offset_target = offset
        .targets
        .iter()
        .find(|target| target.queue_id == Some(0))
        .expect("offset queue target");
    assert!(offset_target.applied);
    assert!(offset_target.changed);
    assert_eq!(offset_target.after, None);
    assert_eq!(offset_target.failure, Some(tools::FailureCode::VerificationFailed));
    assert!(offset_target.retryable);
    assert!(offset.after.is_none());

    let (schema_version, cluster, dry_run, confirm, reason, request_key) = common();
    let request_mode = session
        .run(MutationToolRequest::ConsumerRequestMode(
            tools::SetConsumerRequestModeArgs {
                schema_version,
                cluster,
                topic: "orders".to_owned(),
                consumer_group: "orders-consumer".to_owned(),
                mode: tools::ConsumerRequestMode::Pop,
                pop_share_queue_num: 4,
                timeout_millis: 12_345,
                dry_run,
                confirm,
                reason,
                request_key,
            },
        ))
        .await
        .expect("request mode through production session");
    let MutationToolResponse::ConsumerRequestMode(request_mode) = request_mode else {
        panic!("unexpected response variant");
    };
    let request_target = &request_mode.targets[0];
    assert!(request_target.applied);
    assert!(request_target.changed);
    assert_eq!(request_target.after, None);
    assert_eq!(request_target.verification, tools::VerificationState::Failed);
    assert_eq!(request_target.failure, Some(tools::FailureCode::VerificationFailed));
    assert!(request_target.retryable);
    assert!(request_mode.after.is_none());

    session.shutdown().await.expect("session shutdown");
    assert_eq!(evidence.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn production_session_preserves_sorted_mixed_request_mode_postread_truth() {
    let evidence = Arc::new(Evidence::default());
    evidence.request_mode_mixed_postread.store(true, Ordering::SeqCst);
    let mut session = AdminMutationToolSession::new(ProductionPathBackend {
        evidence: Arc::clone(&evidence),
    });
    let (schema_version, cluster, dry_run, confirm, reason, request_key) = common();
    let response = session
        .run(MutationToolRequest::ConsumerRequestMode(
            tools::SetConsumerRequestModeArgs {
                schema_version,
                cluster,
                topic: "orders".to_owned(),
                consumer_group: "orders-consumer".to_owned(),
                mode: tools::ConsumerRequestMode::Pop,
                pop_share_queue_num: 4,
                timeout_millis: 12_345,
                dry_run,
                confirm,
                reason,
                request_key,
            },
        ))
        .await
        .expect("mixed request mode through production session");
    let MutationToolResponse::ConsumerRequestMode(response) = response else {
        panic!("unexpected response variant");
    };

    assert_eq!(response.status, tools::MutationStatus::Partial);
    assert_eq!(response.error_code, Some(crate::error::ControlErrorCode::PartialApply));
    assert_eq!(response.targets.len(), 2);
    assert_eq!(
        response
            .targets
            .iter()
            .map(|target| target.broker_name.as_str())
            .collect::<Vec<_>>(),
        ["broker-a", "broker-b"]
    );
    let successful = &response.targets[0];
    assert!(successful.applied);
    assert!(successful.changed);
    assert_eq!(successful.verification, tools::VerificationState::Verified);
    assert_eq!(successful.failure, None);
    assert!(!successful.retryable);
    assert_eq!(
        successful.after.expect("successful postread").mode,
        tools::ConsumerRequestMode::Pop
    );
    let failed = &response.targets[1];
    assert!(failed.applied);
    assert!(failed.changed);
    assert_eq!(failed.after, None);
    assert_eq!(failed.verification, tools::VerificationState::Failed);
    assert_eq!(failed.failure, Some(tools::FailureCode::VerificationFailed));
    assert!(failed.retryable);

    let after = response.after.expect("at least one broker has postread truth");
    assert_eq!(
        after.keys().map(String::as_str).collect::<Vec<_>>(),
        ["broker-a", "broker-b"]
    );
    assert_eq!(
        after.get("broker-a").and_then(|value| *value),
        Some(tools::RequestModeValue {
            mode: tools::ConsumerRequestMode::Pop,
            pop_share_queue_num: 4,
        })
    );
    assert_eq!(after.get("broker-b"), Some(&None));
    assert_eq!(evidence.request_mode_executes.load(Ordering::SeqCst), 1);
    assert_eq!(evidence.request_mode_timeout.load(Ordering::SeqCst), 12_345);

    session.shutdown().await.expect("session shutdown");
    assert_eq!(evidence.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn production_session_dry_runs_all_stage_c_operations_without_execute() {
    let evidence = Arc::new(Evidence::default());
    let mut session = AdminMutationToolSession::new(ProductionPathBackend {
        evidence: Arc::clone(&evidence),
    });
    let (schema_version, cluster, _, confirm, reason, request_key) = common();
    let offset = session
        .run(MutationToolRequest::ConsumerOffset(tools::ResetConsumerOffsetArgs {
            schema_version,
            cluster,
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            timestamp: "2026-08-30T08:00:00+08:00".to_owned(),
            force: false,
            dry_run: true,
            confirm,
            reason,
            request_key,
        }))
        .await
        .expect("offset dry run through production session");
    let MutationToolResponse::ConsumerOffset(offset) = offset else {
        panic!("unexpected response variant");
    };
    assert_eq!(offset.mode, tools::MutationMode::DryRun);
    assert_eq!(offset.status, tools::MutationStatus::Partial);
    assert_eq!(offset.error_code, Some(crate::error::ControlErrorCode::PartialApply));

    let (schema_version, cluster, _, confirm, reason, request_key) = common();
    let broker = session
        .run(MutationToolRequest::BrokerConfig(tools::PatchBrokerConfigArgs {
            schema_version,
            cluster,
            broker_name: "broker-a".to_owned(),
            properties: tools::BrokerConfigProperties {
                trace_topic_enable: Some("true".to_owned()),
                ..tools::BrokerConfigProperties::default()
            },
            dry_run: true,
            confirm,
            reason,
            request_key,
        }))
        .await
        .expect("broker dry run through production session");
    let MutationToolResponse::BrokerConfig(broker) = broker else {
        panic!("unexpected response variant");
    };
    assert_eq!(broker.mode, tools::MutationMode::DryRun);
    assert_eq!(broker.status, tools::MutationStatus::Planned);
    assert_eq!(broker.error_code, None);

    let (schema_version, cluster, _, confirm, reason, request_key) = common();
    let request_mode = session
        .run(MutationToolRequest::ConsumerRequestMode(
            tools::SetConsumerRequestModeArgs {
                schema_version,
                cluster,
                topic: "orders".to_owned(),
                consumer_group: "orders-consumer".to_owned(),
                mode: tools::ConsumerRequestMode::Pop,
                pop_share_queue_num: 4,
                timeout_millis: 12_345,
                dry_run: true,
                confirm,
                reason,
                request_key,
            },
        ))
        .await
        .expect("request mode dry run through production session");
    let MutationToolResponse::ConsumerRequestMode(request_mode) = request_mode else {
        panic!("unexpected response variant");
    };
    assert_eq!(request_mode.mode, tools::MutationMode::DryRun);
    assert_eq!(request_mode.status, tools::MutationStatus::Planned);
    assert_eq!(request_mode.error_code, None);
    assert_eq!(evidence.offset_executes.load(Ordering::SeqCst), 0);
    assert_eq!(evidence.broker_executes.load(Ordering::SeqCst), 0);
    assert_eq!(evidence.request_mode_executes.load(Ordering::SeqCst), 0);
    assert_eq!(evidence.request_mode_timeout.load(Ordering::SeqCst), 0);

    session.shutdown().await.expect("session shutdown");
    assert_eq!(evidence.shutdowns.load(Ordering::SeqCst), 1);
}
