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
use crate::infrastructure::{
    auth_state::{DesktopAuthState, LOGIN_PASSWORD_ENV, LOGIN_USERNAME_ENV, MapEnvironment},
    config_store::DesktopConfigStore,
};
use rocketmq_runtime::{ChildServiceContext, ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};
use std::{collections::VecDeque, sync::Mutex};

type Gate = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
);
#[derive(Default)]
pub(crate) struct FakeAclBackend {
    pub users: Mutex<Vec<AclUser>>,
    pub policies: Mutex<Vec<AclPolicy>>,
    pub user_calls: Mutex<Vec<AclRequest>>,
    pub policy_calls: Mutex<Vec<AclRequest>>,
    pub writes: Mutex<Vec<AclCommand>>,
    pub results: Mutex<VecDeque<AclWriteResult>>,
    query_gate: Mutex<Option<Gate>>,
    write_gate: Mutex<Option<Gate>>,
}
impl FakeAclBackend {
    pub fn hold_query(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        hold(&self.query_gate)
    }
    pub fn hold_write(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        hold(&self.write_gate)
    }
}
fn hold(
    slot: &Mutex<Option<Gate>>,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (started, observed) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let (finished, done) = tokio::sync::oneshot::channel();
    *slot.lock().unwrap() = Some((started, gate, finished));
    (observed, release, done)
}
struct Finish(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for Finish {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
async fn wait(gate: Option<Gate>) {
    if let Some((started, gate, finished)) = gate {
        let _done = Finish(Some(finished));
        let _ = started.send(());
        let _ = gate.await;
    }
}
impl AclBackend for FakeAclBackend {
    fn users(&self, request: AclRequest) -> ServiceFuture<'_, Result<AclRead<AclUser>, UiError>> {
        self.user_calls.lock().unwrap().push(request.clone());
        let items = self
            .users
            .lock()
            .unwrap()
            .iter()
            .filter(|user| request.scope.targets().contains(&user.target))
            .cloned()
            .collect();
        let gate = self.query_gate.lock().unwrap().take();
        Box::pin(async move {
            wait(gate).await;
            Ok(AclRead {
                items,
                ..Default::default()
            })
        })
    }
    fn policies(&self, request: AclRequest) -> ServiceFuture<'_, Result<AclRead<AclPolicy>, UiError>> {
        self.policy_calls.lock().unwrap().push(request.clone());
        let items = self
            .policies
            .lock()
            .unwrap()
            .iter()
            .filter(|policy| request.scope.targets().contains(&policy.target))
            .cloned()
            .collect();
        Box::pin(std::future::ready(Ok(AclRead {
            items,
            ..Default::default()
        })))
    }
    fn mutate(&self, _revision: u64, command: AclCommand) -> ServiceFuture<'_, Result<AclWriteResult, UiError>> {
        self.writes.lock().unwrap().push(command.clone());
        let gate = self.write_gate.lock().unwrap().take();
        let result = self
            .results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| AclWriteResult {
                targets: command
                    .targets
                    .iter()
                    .map(|target| AclWriteTarget {
                        target: target.clone(),
                        state: AclWriteState::Applied,
                    })
                    .collect(),
            });
        Box::pin(async move {
            wait(gate).await;
            for target in &result.targets {
                if target.state != AclWriteState::Applied {
                    continue;
                }
                match &command.change {
                    AclChange::SaveUser {
                        username,
                        password,
                        user_type,
                        status,
                        ..
                    } => {
                        let mut users = self.users.lock().unwrap();
                        let previous = users
                            .iter()
                            .find(|user| user.target == target.target && user.username == *username)
                            .and_then(|user| user.password.clone());
                        users.retain(|user| user.target != target.target || user.username != *username);
                        users.push(AclUser {
                            target: target.target.clone(),
                            username: username.clone(),
                            password: password.clone().or(previous),
                            user_type: Some(user_type.clone()),
                            status: Some(status.clone()),
                            read_only: false,
                        });
                    }
                    AclChange::DeleteUser { username } => self
                        .users
                        .lock()
                        .unwrap()
                        .retain(|user| user.target != target.target || user.username != *username),
                    AclChange::SavePolicy {
                        subject,
                        policy_type,
                        entry,
                        ..
                    } => {
                        let mut policies = self.policies.lock().unwrap();
                        if let Some(policy) = policies.iter_mut().find(|policy| {
                            policy.target == target.target
                                && policy.subject == *subject
                                && policy.policy_type == *policy_type
                        }) {
                            policy.entries.retain(|existing| existing.resource != entry.resource);
                            policy.entries.push(entry.clone());
                        } else {
                            policies.push(AclPolicy {
                                target: target.target.clone(),
                                subject: subject.clone(),
                                policy_type: policy_type.clone(),
                                entries: vec![entry.clone()],
                                read_only: false,
                            });
                        }
                    }
                    AclChange::DeletePolicyEntry {
                        subject,
                        policy_type,
                        resource,
                    } => {
                        let mut policies = self.policies.lock().unwrap();
                        for policy in policies.iter_mut().filter(|policy| {
                            policy.target == target.target
                                && policy.subject == *subject
                                && policy.policy_type == *policy_type
                        }) {
                            policy.entries.retain(|entry| entry.resource != *resource);
                        }
                        policies.retain(|policy| !policy.entries.is_empty());
                    }
                }
            }
            Ok(result)
        })
    }
}
pub(crate) fn target() -> BrokerIdentity {
    BrokerIdentity {
        cluster: "cluster-a".into(),
        broker_name: "broker-a".into(),
        broker_id: 0,
        address: "broker-a:10911".into(),
    }
}
pub(crate) fn scope() -> AclScope {
    AclScope::confirm(&[target()], "cluster-a", Some("broker-a")).unwrap()
}
pub(crate) fn user() -> AclUser {
    AclUser {
        target: target(),
        username: "alice".into(),
        password: Some(AclSecret::new("backend-secret".into())),
        user_type: Some("Normal".into()),
        status: Some("enable".into()),
        read_only: false,
    }
}
pub(crate) fn command() -> AclCommand {
    AclCommand {
        scope: scope(),
        targets: vec![target()],
        change: AclChange::SaveUser {
            create: true,
            username: "alice".into(),
            password: Some(AclSecret::new("explicit-secret".into())),
            user_type: "Normal".into(),
            status: "enable".into(),
        },
    }
}
pub(crate) fn inventory() -> Vec<BrokerInventoryItem> {
    vec![BrokerInventoryItem {
        identity: target(),
        role: BrokerRole::Master,
        version: Observed::Unknown,
        availability: EndpointAvailability::Unknown,
        produce_tps: Observed::Unknown,
        consume_tps: Observed::Unknown,
    }]
}
pub(crate) struct Fixture {
    pub runtime: RuntimeOwner,
    pub services: AppServices,
    pub backend: Arc<FakeAclBackend>,
    pub mutations: ChildServiceContext,
    pub store: Arc<DesktopConfigStore>,
    _directory: tempfile::TempDir,
}
impl Fixture {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("acl-workspace-test", 1))
            .unwrap()
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
            .build()
            .unwrap();
        let store = DesktopConfigStore::new(
            directory.path().join("config.json"),
            runtime.root_context().component("storage"),
        );
        let mutations = runtime.root_context().component("mutations");
        let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([
            (LOGIN_USERNAME_ENV, "operator"),
            (LOGIN_PASSWORD_ENV, "private-password"),
        ])));
        let backend = Arc::new(FakeAclBackend::default());
        let broker = Arc::new(super::super::dashboard_broker::test_support::FakeDashboardBrokerOperations::default());
        broker.queue_inventory(Ok(inventory()));
        let services = AppServices::desktop_with_fake_provider(
            store.clone(),
            auth,
            mutations.clone(),
            runtime.root_context().component("history"),
            runtime.root_context().component("monitor"),
        )
        .with_acl_backend(backend.clone())
        .with_dashboard_broker_operations(broker);
        runtime.block_on(services.bootstrap()).unwrap();
        Self {
            runtime,
            services,
            backend,
            mutations,
            store,
            _directory: directory,
        }
    }
    pub fn events(&self) -> Vec<AuditEvent> {
        self.services
            .query_audit(AuditQuery {
                filter: AuditFilter::recent(super::super::audit::now_epoch_ms().unwrap()),
                limit: 200,
                cursor: None,
            })
            .unwrap()
            .events
    }
}
