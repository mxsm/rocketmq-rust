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

use super::{test_support::*, *};
use std::time::Duration;

#[test]
fn acl_known_rejections_and_mixed_results_persist_without_unconfirmed_targets() {
    let fixture = Fixture::new();
    fixture.runtime.block_on(async {
        fixture
            .services
            .authenticate("operator", "private-password")
            .await
            .unwrap();
        for failure in [AclFailure::AccessDenied, AclFailure::Invalid, AclFailure::Unavailable] {
            fixture.backend.results.lock().unwrap().push_back(AclWriteResult {
                targets: vec![AclWriteTarget {
                    target: target(),
                    state: AclWriteState::Rejected(failure),
                }],
            });
            fixture.services.mutate_acl(0, command()).await.unwrap();
        }
        let mut second = target();
        second.broker_id = 1;
        second.address = "broker-a:20911".into();
        let mut command = command();
        command.scope = AclScope::confirm(&[target(), second.clone()], "cluster-a", None).unwrap();
        command.targets.push(second.clone());
        fixture.backend.results.lock().unwrap().push_back(AclWriteResult {
            targets: vec![
                AclWriteTarget {
                    target: target(),
                    state: AclWriteState::Applied,
                },
                AclWriteTarget {
                    target: second,
                    state: AclWriteState::Rejected(AclFailure::AccessDenied),
                },
            ],
        });
        fixture.services.mutate_acl(0, command).await.unwrap();
        let events = fixture.events();
        let events = events
            .iter()
            .filter(|event| event.operation.action == AuditAction::AclUserCreated)
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 4);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.outcome == AuditOutcome::Rejected)
                .count(),
            3
        );
        let partial = events
            .iter()
            .find(|event| event.outcome == AuditOutcome::Partial)
            .unwrap();
        assert_eq!(partial.facts.applied_targets, 1);
        assert!(
            events
                .iter()
                .all(|event| event.facts.unconfirmed_targets == 0 && event.facts.failure_code.is_none())
        );
        assert_eq!(fixture.backend.writes.lock().unwrap().len(), 4);
    });
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn acl_owned_write_survives_waiter_drop_and_signout_with_partial_unknown_audit_and_no_secret_payload() {
    let fixture = Fixture::new();
    fixture.runtime.block_on(async{
        fixture.services.authenticate("operator","private-password").await.unwrap();let mut second=target();second.broker_id=1;second.address="broker-a:20911".into();let scope=AclScope::confirm(&[target(),second.clone()],"cluster-a",None).unwrap();let mut command=command();command.scope=scope;command.targets.push(second.clone());fixture.backend.results.lock().unwrap().push_back(AclWriteResult{targets:vec![AclWriteTarget{target:target(),state:AclWriteState::Applied},AclWriteTarget{target:second,state:AclWriteState::Unknown}]});
        let(started,release,_)=fixture.backend.hold_write();let mut waiter=Box::pin(fixture.services.mutate_acl(0,command));tokio::select!{result=&mut waiter=>panic!("write completed before gate: {result:?}"),result=started=>result.unwrap()};let pending=fixture.services.audit_pending().unwrap().pop().unwrap();drop(waiter);fixture.services.sign_out().await.unwrap();release.send(()).unwrap();assert!(fixture.mutations.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
        let events=fixture.events();let event=events.iter().find(|event|event.operation.action==AuditAction::AclUserCreated).unwrap();assert_eq!(event.operation,pending);assert_eq!(event.operation.context.environment_id,fixture.store.environment_snapshot().unwrap().environment_id);assert_eq!(event.operation.context.actor,OperationActor::LocalOperator{username:"operator".into()});assert_eq!(event.outcome,AuditOutcome::Partial);assert_eq!((event.facts.applied_targets,event.facts.unconfirmed_targets),(1,1));let json=serde_json::to_string(event).unwrap();assert!(!json.contains("explicit-secret")&&!json.contains("password")&&!json.contains("userType"));assert_eq!(fixture.backend.writes.lock().unwrap().len(),1);assert!(fixture.services.audit_pending().unwrap().is_empty());
    });
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
#[test]
fn acl_read_drop_cancels_query_and_auth_and_stale_revision_reject_before_backend() {
    let fixture = Fixture::new();
    fixture.runtime.block_on(async {
        let request = AclRequest {
            revision: 0,
            epoch: 1,
            scope: scope(),
        };
        let (started, _release, done) = fixture.backend.hold_query();
        let mut query = Box::pin(fixture.services.acl_users(request));
        tokio::select! {result=&mut query=>panic!("query completed early: {result:?}"),result=started=>result.unwrap()};
        drop(query);
        tokio::time::timeout(Duration::from_secs(5), done)
            .await
            .unwrap()
            .unwrap();
        assert!(fixture.events().is_empty());
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        fixture
            .services
            .mutate_with_progress(super::super::ConfigMutation::SetAuthEnabled(true), progress)
            .await
            .unwrap();
        let revision = fixture.services.connection_state().config.revision;
        let before = fixture.backend.user_calls.lock().unwrap().len();
        assert_eq!(
            fixture
                .services
                .acl_users(AclRequest {
                    revision: 0,
                    epoch: 2,
                    scope: scope()
                })
                .await
                .unwrap_err()
                .code(),
            UiErrorCode::Validation
        );
        assert_eq!(
            fixture
                .services
                .acl_users(AclRequest {
                    revision,
                    epoch: 3,
                    scope: scope()
                })
                .await
                .unwrap_err()
                .code(),
            UiErrorCode::Authentication
        );
        assert_eq!(
            fixture
                .services
                .mutate_acl(revision, command())
                .await
                .unwrap_err()
                .code(),
            UiErrorCode::Authentication
        );
        assert_eq!(fixture.backend.user_calls.lock().unwrap().len(), before);
        assert!(fixture.backend.writes.lock().unwrap().is_empty());
        fixture
            .services
            .authenticate("operator", "private-password")
            .await
            .unwrap();
        assert!(
            fixture
                .services
                .acl_users(AclRequest {
                    revision,
                    epoch: 4,
                    scope: scope()
                })
                .await
                .is_ok()
        );
    });
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
