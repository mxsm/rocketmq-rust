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

use super::*;
use rocketmq_admin_core::core::acl_workspace::AclExactWrite;
use rocketmq_dashboard_common::*;

fn target() -> BrokerIdentity {
    BrokerIdentity {
        cluster: "cluster-a".into(),
        broker_name: "broker-a".into(),
        broker_id: 0,
        address: "broker-a:10911".into(),
    }
}
fn scope() -> AclScope {
    AclScope::confirm(&[target()], "cluster-a", Some("broker-a")).unwrap()
}
fn user() -> serde_json::Value {
    serde_json::json!({"username":"alice","password":"backend-private","userType":"Normal","userStatus":"enable"})
}
fn policy() -> serde_json::Value {
    serde_json::json!({"subject":"User:alice","policies":[{"policyType":"Custom","entries":[{"resource":"Topic:orders","actions":["Pub"],"sourceIps":["127.0.0.1"],"decision":"Allow"},{"resource":"Topic:untouched","actions":["Sub"],"sourceIps":[],"decision":"Allow"}]}]})
}
#[test]
fn acl_policy_create_aliases_never_write_or_bypass_existing_canonical_records() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    let mut existing = policy();
    existing["futureField"] = true.into();
    controls
        .acl_policies
        .lock()
        .unwrap()
        .insert(target().address, vec![existing]);
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        for (subject, resource, action) in [
            ("alice", "Topic:orders", "Pub"),
            ("user:alice", "Topic:orders", "Pub"),
            ("User:alice", "topic:orders", "Pub"),
            ("User:alice", "Topic:orders**", "Pub"),
            ("User:alice", "Topic:orders", "Any"),
        ] {
            assert!(
                provider
                    .mutate_acl(
                        1,
                        AclCommand {
                            scope: scope(),
                            targets: vec![target()],
                            change: AclChange::SavePolicy {
                                create: true,
                                subject: subject.into(),
                                policy_type: "Custom".into(),
                                entry: AclPolicyEntry {
                                    resource: resource.into(),
                                    actions: vec![action.into()],
                                    source_ips: vec![],
                                    decision: "Allow".into()
                                }
                            }
                        }
                    )
                    .await
                    .is_err()
            );
        }
        assert!(controls.acl_reads.lock().unwrap().is_empty());
        let result = provider
            .mutate_acl(
                1,
                AclCommand {
                    scope: scope(),
                    targets: vec![target()],
                    change: AclChange::SavePolicy {
                        create: true,
                        subject: "User:alice".into(),
                        policy_type: "Custom".into(),
                        entry: AclPolicyEntry {
                            resource: "Topic:orders".into(),
                            actions: vec!["Pub".into()],
                            source_ips: vec![],
                            decision: "Allow".into(),
                        },
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(result.targets[0].state, AclWriteState::Rejected(AclFailure::Invalid));
        assert!(controls.acl_writes.lock().unwrap().is_empty());
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().unwrap();
}
#[test]
fn acl_provider_exact_reads_user_password_intent_and_structured_policy_writes_reach_sessions() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async{
        provider.switch(snapshot(1,ConnectionScope::NameServer)).await.unwrap();
        let create=AclCommand{scope:scope(),targets:vec![target()],change:AclChange::SaveUser{create:true,username:"alice".into(),password:Some(AclSecret::new("explicit-private".into())),user_type:"Normal".into(),status:"disable".into()}};
        assert_eq!(provider.mutate_acl(1,create).await.unwrap().targets[0].state,AclWriteState::Applied);
        controls.acl_users.lock().unwrap().insert(target().address,vec![user()]);
        let users=provider.acl_users(1,scope()).await.unwrap();assert_eq!(users.items[0].password.as_ref().unwrap().expose(),"backend-private");
        provider.mutate_acl(1,AclCommand{scope:scope(),targets:vec![target()],change:AclChange::SaveUser{create:false,username:"alice".into(),password:None,user_type:"Super".into(),status:"enable".into()}}).await.unwrap();
        controls.acl_policies.lock().unwrap().insert(target().address,vec![policy()]);
        let entry=AclPolicyEntry{resource:"Topic:orders".into(),actions:vec!["Pub".into(),"Sub".into()],source_ips:vec!["10.0.0.0/8".into()],decision:"Deny".into()};
        provider.mutate_acl(1,AclCommand{scope:scope(),targets:vec![target()],change:AclChange::SavePolicy{create:false,subject:"User:alice".into(),policy_type:"Custom".into(),entry:entry.clone()}}).await.unwrap();
        provider.mutate_acl(1,AclCommand{scope:scope(),targets:vec![target()],change:AclChange::DeletePolicyEntry{subject:"User:alice".into(),policy_type:"Custom".into(),resource:"Topic:orders".into()}}).await.unwrap();
        provider.mutate_acl(1,AclCommand{scope:scope(),targets:vec![target()],change:AclChange::DeleteUser{username:"alice".into()}}).await.unwrap();
        { let calls=controls.acl_writes.lock().unwrap();assert_eq!(calls.len(),5);assert!(calls.iter().all(|(address,_)|address=="broker-a:10911"));
        assert!(matches!(&calls[0].1,AclExactWrite::User{password:Some(password),status,..}if password=="explicit-private"&&status=="disable"));
        assert!(matches!(&calls[1].1,AclExactWrite::User{password:None,user_type,..}if user_type=="Super"));
        assert!(matches!(&calls[2].1,AclExactWrite::Policy{entry,..}if entry.resource=="Topic:orders"&&entry.actions==["Pub","Sub"]&&entry.source_ips==["10.0.0.0/8"]&&entry.decision=="Deny"));
        assert!(matches!(&calls[3].1,AclExactWrite::DeleteEntry{resource,policy_type,..}if resource=="Topic:orders"&&policy_type=="Custom"));
        } provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().unwrap();
}
#[test]
fn acl_provider_rejects_stale_scope_before_acl_reads_and_keeps_unknown_policy_read_only() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        let mut wrong = target();
        wrong.address = "changed:10911".into();
        let stale_scope = AclScope::confirm(&[wrong], "cluster-a", Some("broker-a")).unwrap();
        assert!(provider.acl_users(1, stale_scope).await.is_err());
        assert!(controls.acl_reads.lock().unwrap().is_empty());
        let mut unknown = policy();
        unknown["policies"][0]["entries"][0]["futureField"] = true.into();
        controls
            .acl_policies
            .lock()
            .unwrap()
            .insert(target().address, vec![unknown]);
        let records = provider.acl_policies(1, scope()).await.unwrap();
        assert!(records.items[0].read_only);
        let result = provider
            .mutate_acl(
                1,
                AclCommand {
                    scope: scope(),
                    targets: vec![target()],
                    change: AclChange::DeletePolicyEntry {
                        subject: "User:alice".into(),
                        policy_type: "Custom".into(),
                        resource: "Topic:orders".into(),
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(result.targets[0].state, AclWriteState::Rejected(AclFailure::Invalid));
        assert!(controls.acl_writes.lock().unwrap().is_empty());
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn acl_provider_multiple_targets_report_applied_and_unknown_without_retrying() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    let mut second = target();
    second.broker_id = 1;
    second.address = "broker-a:20911".into();
    let targets = vec![target(), second.clone()];
    *controls.acl_inventory.lock().unwrap() = Some(DashboardBrokerList {
        clusters: vec!["cluster-a".into()],
        items: targets
            .iter()
            .map(|target| DashboardBrokerInfo {
                cluster_name: target.cluster.clone(),
                broker_name: target.broker_name.clone(),
                broker_id: target.broker_id,
                address: target.address.clone(),
                role: "UNKNOWN".into(),
                version: String::new(),
                produce_tps: 0.,
                consume_tps: 0.,
                runtime_entries: BTreeMap::new(),
                runtime_error: None,
            })
            .collect(),
    });
    controls.acl_write_errors.lock().unwrap().insert(
        second.address.clone(),
        AdminError::operation_failed("acl", "private transport failure"),
    );
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        let result = provider
            .mutate_acl(
                1,
                AclCommand {
                    scope: AclScope::confirm(&targets, "cluster-a", None).unwrap(),
                    targets: targets.clone(),
                    change: AclChange::SaveUser {
                        create: true,
                        username: "alice".into(),
                        password: Some(AclSecret::new("test-password".into())),
                        user_type: "Normal".into(),
                        status: "enable".into(),
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(result.targets[0].state, AclWriteState::Applied);
        assert_eq!(result.targets[1].state, AclWriteState::Unknown);
        {
            let calls = controls.acl_writes.lock().unwrap();
            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].0, targets[0].address);
            assert_eq!(calls[1].0, second.address);
        }
        assert!(!format!("{result:?}").contains("private transport failure"));
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn acl_provider_retained_wire_codes_distinguish_confirmed_rejection_from_unknown() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        for (code, expected) in [
            (16, AclWriteState::Rejected(AclFailure::AccessDenied)),
            (3, AclWriteState::Rejected(AclFailure::Unavailable)),
            (1, AclWriteState::Unknown),
        ] {
            controls.acl_write_errors.lock().unwrap().insert(
                target().address,
                AdminError::from_error(
                    "acl_workspace_write",
                    rocketmq_error::Error::new(&rocketmq_error::BROKER_OPERATION_FAILED).with_context(
                        rocketmq_error::ErrorContext::new().with_i64(rocketmq_error::fields::BROKER_CODE, code),
                    ),
                ),
            );
            let result = provider
                .mutate_acl(
                    1,
                    AclCommand {
                        scope: scope(),
                        targets: vec![target()],
                        change: AclChange::SaveUser {
                            create: true,
                            username: "alice".into(),
                            password: Some(AclSecret::new("explicit-private".into())),
                            user_type: "Normal".into(),
                            status: "enable".into(),
                        },
                    },
                )
                .await
                .unwrap();
            assert_eq!(result.targets[0].state, expected);
        }
        assert_eq!(controls.acl_writes.lock().unwrap().len(), 3);
        provider.shutdown().await;
    });
    runtime.shutdown_runtime_blocking().unwrap();
}
