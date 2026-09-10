// Copyright 2023 The RocketMQ Rust Authors
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

use super::service::*;
use super::types::*;
use crate::error::{DashboardError, DashboardResult};
use rocketmq_admin_core::core::dashboard::*;
use std::cell::RefCell;

struct Fake {
    calls: RefCell<Vec<&'static str>>,
    reject: bool,
    read_fails: bool,
}
impl AclAccess for Fake {
    async fn brokers(&self) -> DashboardResult<Vec<DashboardBrokerInfo>> {
        self.calls.borrow_mut().push("brokers");
        Ok(vec![DashboardBrokerInfo {
            cluster_name: "cluster".into(),
            broker_name: "broker-a".into(),
            broker_id: 0,
            address: "localhost:10911".into(),
            role: "MASTER".into(),
            version: String::new(),
            produce_tps: 0.0,
            consume_tps: 0.0,
            runtime_entries: Default::default(),
            runtime_error: None,
        }])
    }
    async fn users(&self, query: &DashboardAclQuery) -> DashboardResult<Vec<DashboardAclUser>> {
        self.calls.borrow_mut().push("users");
        assert_eq!(query.selector.broker_name.as_deref(), Some("broker-a"));
        if self.read_fails {
            return Err(DashboardError::Authentication("rejected".into()));
        }
        Ok(vec![DashboardAclUser {
            broker_name: "broker-a".into(),
            broker_addr: "localhost:10911".into(),
            username: "alice".into(),
            user_type: Some("normal".into()),
            user_status: Some("enable".into()),
        }])
    }
    async fn create_user(&self, _: &DashboardAclUserMutationRequest) -> DashboardResult<()> {
        self.calls.borrow_mut().push("write");
        if self.reject {
            Err(DashboardError::Authentication("rejected".into()))
        } else {
            Ok(())
        }
    }
    async fn update_user(&self, request: &DashboardAclUserMutationRequest) -> DashboardResult<()> {
        self.create_user(request).await
    }
    async fn delete_user(&self, _: &TargetSelector, _: &str) -> DashboardResult<()> {
        Ok(())
    }
}
fn request() -> AclUserChange {
    AclUserChange {
        scope: AclScope {
            cluster_name: "cluster".into(),
            broker_name: "broker-a".into(),
            broker_addr: "localhost:10911".into(),
        },
        username: "alice".into(),
        password: "secret-sentinel".into(),
        user_type: AclUserType::Normal,
        user_status: Some(AclUserStatus::Enable),
    }
}
#[tokio::test]
async fn acl_success_rejection_and_read_failure_preserve_write_truth() {
    for (reject, read_fails) in [(false, false), (true, false), (false, true)] {
        let admin = Fake {
            calls: RefCell::new(vec![]),
            reject,
            read_fails,
        };
        let result = change_user(&admin, request(), AclUserOperation::Create).await;
        if reject {
            assert!(result.is_err());
            assert_eq!(*admin.calls.borrow(), ["brokers", "write"]);
        } else {
            let result = result.unwrap();
            assert!(result.success);
            assert_eq!(result.users.is_none(), read_fails);
            assert_eq!(*admin.calls.borrow(), ["brokers", "write", "users"]);
            assert!(!serde_json::to_string(&result).unwrap().contains("secret-sentinel"));
        }
    }
}
#[tokio::test]
async fn blank_password_and_stale_scope_never_write() {
    let admin = Fake {
        calls: RefCell::new(vec![]),
        reject: false,
        read_fails: false,
    };
    let mut empty = request();
    empty.password.clear();
    assert!(change_user(&admin, empty, AclUserOperation::Update).await.is_err());
    assert!(admin.calls.borrow().is_empty());
    let mut stale = request();
    stale.scope.broker_addr = "elsewhere:10911".into();
    assert!(change_user(&admin, stale, AclUserOperation::Create).await.is_err());
    assert_eq!(*admin.calls.borrow(), ["brokers"]);
    let mut missing = request();
    missing.user_status = None;
    assert!(missing.to_core(AclUserOperation::Create).is_ok());
    assert!(missing.to_core(AclUserOperation::Update).is_err());
    assert!(!format!("{:?}", request().to_core(AclUserOperation::Create).unwrap()).contains("secret-sentinel"));
}

#[test]
#[ignore = "requires the isolated deploy/dev/acl Docker fixture"]
fn local_acl_user_create_update_delete_round_trip() {
    use rocketmq_admin_core::client_adapter::{AdminBuilder, ClientRuntime, ClientRuntimeConfig, TelemetryHandle};
    use rocketmq_admin_core::core::security::AdminCredentials;
    use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("tauri-acl-users-smoke"))
        .unwrap()
        .build()
        .unwrap();
    let runtime = ClientRuntime::try_new(
        owner.root_context().component("client"),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )
    .unwrap();
    let result = owner.block_on(async {
        let mut session = AdminBuilder::new(runtime.clone())
            .namesrv_addr("127.0.0.1:29876")
            .vip_channel_enabled(false)
            .credentials(AdminCredentials::try_new("tauri-dev-admin", "tauri-dev-secret", None).unwrap())
            .admin_group(format!("tauri-acl-user-smoke-{}", uuid::Uuid::new_v4()))
            .build_and_start()
            .await
            .unwrap();
        let scope = AclScope {
            cluster_name: "TauriAclDebugCluster".into(),
            broker_name: "tauri-acl-broker".into(),
            broker_addr: "127.0.0.1:22911".into(),
        };
        let username = format!("tauri-smoke-{}", uuid::Uuid::new_v4().simple());
        let request = AclUserChange {
            scope: scope.clone(),
            username: username.clone(),
            password: "public-fixture-test-secret".into(),
            user_type: AclUserType::Normal,
            user_status: Some(AclUserStatus::Enable),
        };
        let checked: DashboardResult<()> = async {
            let created = change_user(&session, request.clone(), AclUserOperation::Create).await?;
            if !created
                .users
                .as_ref()
                .is_some_and(|users| users.iter().any(|user| user.username == username))
            {
                return Err(DashboardError::Internal("Created ACL user was not read back"));
            }
            let updated = change_user(
                &session,
                AclUserChange {
                    user_status: Some(AclUserStatus::Disable),
                    ..request
                },
                AclUserOperation::Update,
            )
            .await?;
            if !updated.users.as_ref().is_some_and(|users| {
                users.iter().any(|user| {
                    user.username == username
                        && user
                            .user_status
                            .as_deref()
                            .is_some_and(|status| status.eq_ignore_ascii_case("disable"))
                })
            }) {
                return Err(DashboardError::Internal("Updated ACL status was not read back"));
            }
            Ok(())
        }
        .await;
        let deleted = session.dashboard_delete_acl_user(&scope.selector(), &username).await;
        let users = list_users(&session, &scope).await;
        session.shutdown().await;
        runtime.shutdown().await;
        checked?;
        deleted?;
        if users?.iter().any(|user| user.username == username) {
            return Err(DashboardError::Internal("Deleted ACL user still exists"));
        }
        Ok(())
    });
    owner.shutdown_runtime_blocking().unwrap();
    result.unwrap();
}
