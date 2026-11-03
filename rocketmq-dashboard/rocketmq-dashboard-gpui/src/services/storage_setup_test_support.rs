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

use super::super::{
    ApplicationBackend, ConfigMutation, ConfigProgressSender, ConfigUpdate, GlobalConnectionState, ServiceFuture,
    SessionState, StartupSnapshot,
};
use super::*;
use parking_lot::Mutex;
use rocketmq_dashboard_common::{EndpointHealth, OperationContext};

pub(crate) struct FakeStorageSetup {
    pub snapshot: Mutex<StorageSetupSnapshot>,
    pub actions: Mutex<Vec<WorkspaceSetupAction>>,
    pub saved: Mutex<Vec<StorageEngine>>,
    pub failure: Mutex<Option<UiError>>,
    pub held: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub admitted: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl FakeStorageSetup {
    pub fn services(initialized: bool) -> (AppServices, Arc<Self>) {
        let backend = Arc::new(Self {
            snapshot: Mutex::new(StorageSetupSnapshot {
                current_engine: Some(StorageEngine::Sqlite),
                source: SelectionSource::Saved,
                saved_engine: Some(StorageEngine::Sqlite),
                selection_error: None,
                selection_path: "legacy/storage-selection.json".into(),
                legacy_config_path: "legacy/config.json".into(),
                status: Ok(StorageStatus {
                    engine: StorageEngine::Sqlite,
                    schema_version: 1,
                    document_schema_version: initialized.then_some(4),
                    initialized,
                    last_successful_write_epoch_ms: None,
                    pool_connections: Some(1),
                    pool_idle_connections: Some(1),
                }),
                checked_at_epoch_ms: Some(1_700_000_000_000),
            }),
            actions: Mutex::new(Vec::new()),
            saved: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
            held: Mutex::new(None),
            admitted: Mutex::new(None),
        });
        let services = AppServices {
            backend: backend.clone(),
            ..AppServices::default()
        };
        (services, backend)
    }
}

impl ApplicationBackend for FakeStorageSetup {
    fn storage_setup_snapshot(&self) -> ServiceFuture<'_, Result<StorageSetupSnapshot, UiError>> {
        Box::pin(std::future::ready(Ok(self.snapshot.lock().clone())))
    }
    fn save_storage_engine(&self, engine: StorageEngine) -> ServiceFuture<'_, Result<(), UiError>> {
        self.saved.lock().push(engine);
        self.snapshot.lock().saved_engine = Some(engine);
        Box::pin(std::future::ready(Ok(())))
    }
    fn complete_workspace_setup(&self, action: WorkspaceSetupAction) -> ServiceFuture<'_, Result<(), UiError>> {
        self.actions.lock().push(action);
        if let Some(admitted) = self.admitted.lock().take() {
            let _ = admitted.send(());
        }
        let held = self.held.lock().take();
        Box::pin(async move {
            if let Some(held) = held {
                let _ = held.await;
            }
            if let Some(error) = self.failure.lock().take() {
                return Err(error);
            }
            if let Ok(status) = &mut self.snapshot.lock().status {
                status.initialized = true;
                status.document_schema_version = Some(4);
            }
            Ok(())
        })
    }
    fn bootstrap(&self) -> ServiceFuture<'_, Result<StartupSnapshot, UiError>> {
        let result = if self
            .snapshot
            .lock()
            .status
            .as_ref()
            .is_ok_and(|status| status.initialized)
        {
            Ok(StartupSnapshot {
                configuration_revision: 0,
                login_required: true,
                has_valid_session: false,
            })
        } else {
            Err(UiError::new(
                "Workspace setup required",
                UiErrorCode::Configuration,
                true,
            ))
        };
        Box::pin(std::future::ready(result))
    }
    fn authenticate<'a>(&'a self, _: &'a str, _: &'a str) -> ServiceFuture<'a, Result<SessionState, UiError>> {
        Box::pin(std::future::ready(Err(unavailable())))
    }
    fn sign_out(&self) -> ServiceFuture<'_, Result<(), UiError>> {
        Box::pin(std::future::ready(Ok(())))
    }
    fn mutate(
        &self,
        _: ConfigMutation,
        _: ConfigProgressSender,
        _: Option<OperationContext>,
    ) -> ServiceFuture<'_, Result<ConfigUpdate, UiError>> {
        Box::pin(std::future::ready(Err(unavailable())))
    }
    fn check_all_nameservers(&self) -> ServiceFuture<'_, Result<Vec<EndpointHealth>, UiError>> {
        Box::pin(std::future::ready(Ok(Vec::new())))
    }
    fn connection_state(&self) -> GlobalConnectionState {
        GlobalConnectionState::default()
    }
    fn open_config_location(&self) -> ServiceFuture<'_, Result<(), UiError>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

#[test]
fn admitted_workspace_setup_finishes_after_its_ui_receiver_is_dropped() {
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("workspace-ownership", 2))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    let (mut services, backend) = FakeStorageSetup::services(false);
    services.runtime_bridge = Some(super::super::RuntimeBridge {
        context: runtime.root_context().component("setup"),
        completion: None,
    });
    let (done, completion) = std::sync::mpsc::channel();
    services = services.with_runtime_completion(done);
    let (release, held) = tokio::sync::oneshot::channel();
    let (admitted, accepted) = tokio::sync::oneshot::channel();
    *backend.held.lock() = Some(held);
    *backend.admitted.lock() = Some(admitted);
    runtime.block_on(async {
        let mut delivery = Box::pin(services.complete_workspace_setup(WorkspaceSetupAction::Initialize));
        tokio::select! {
            result = &mut delivery => panic!("held setup finished early: {result:?}"),
            result = accepted => result.unwrap(),
        }
        drop(delivery);
        release.send(()).unwrap();
    });
    assert_eq!(
        completion.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),
        "gpui-workspace-setup"
    );
    assert!(backend.snapshot.lock().status.as_ref().unwrap().initialized);
    runtime.shutdown_runtime_blocking().unwrap();
}
