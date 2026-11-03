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

use rocketmq_runtime::{ChildServiceContext, ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::{
    infrastructure::{
        auth_state::{DesktopAuthState, MapEnvironment},
        config_store::DesktopConfigStore,
        local_storage_status::{FileReadState, FileWriteState},
    },
    services::ConfigMutation,
};

pub(crate) struct Fixture {
    _directory: tempfile::TempDir,
    pub runtime: RuntimeOwner,
    pub store: Arc<DesktopConfigStore>,
    pub services: AppServices,
    pub history: ChildServiceContext,
}

impl Fixture {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("ops-storage-test", 1))
            .unwrap()
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
            .build()
            .unwrap();
        let store = DesktopConfigStore::new(
            directory.path().join("config.json"),
            runtime.root_context().component("config"),
        );
        let history = runtime.root_context().component("history");
        let services = AppServices::desktop_with_fake_provider(
            store.clone(),
            DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
            runtime.root_context().component("services"),
            history.clone(),
            runtime.root_context().component("monitor"),
        );
        runtime.block_on(services.bootstrap()).unwrap();
        Self {
            _directory: directory,
            runtime,
            store,
            services,
            history,
        }
    }

    pub fn mutate(&self, mutation: ConfigMutation) -> Result<super::super::ConfigUpdate, UiError> {
        let (progress, _events) = tokio::sync::mpsc::unbounded_channel();
        self.runtime
            .block_on(self.services.mutate_with_progress(mutation, progress))
    }

    pub fn status(&self) -> LocalRuntimeStatus {
        self.runtime.block_on(self.services.local_runtime_status()).unwrap()
    }
}

#[test]
fn current_file_read_failure_and_recovery_are_independent_of_prior_successful_writes() {
    let fixture = Fixture::new();
    let initial = fixture.status();
    assert_eq!(initial.configuration.evidence.read, FileReadState::Readable);
    assert_eq!(initial.configuration.evidence.last_write, FileWriteState::Succeeded);
    assert!(initial.configuration.writer_owned && initial.history.writer_owned);
    assert_eq!(initial.history.evidence.read, FileReadState::Missing);
    assert_eq!(initial.history.evidence.last_write, FileWriteState::NotObserved);
    assert!(
        !initial.history.path.exists(),
        "status must not create a History file or probe writes"
    );
    let original = std::fs::read(fixture.store.path()).unwrap();
    std::fs::write(fixture.store.path(), b"{invalid").unwrap();
    let failed = fixture.status();
    assert_eq!(failed.configuration.evidence.read, FileReadState::Failed);
    assert!(failed.configuration.evidence.schema_version.is_none());
    assert_eq!(failed.configuration.evidence.last_write, FileWriteState::Succeeded);
    assert_eq!(
        failed.configuration.evidence.last_successful_write_epoch_ms,
        initial.configuration.evidence.last_successful_write_epoch_ms
    );
    std::fs::write(fixture.store.path(), original).unwrap();
    let recovered = fixture.status();
    assert_eq!(recovered.configuration.evidence.read, FileReadState::Readable);
    assert_eq!(recovered.configuration.evidence.file_revision, Some(0));
    assert_eq!(
        recovered.configuration.evidence.last_successful_write_epoch_ms,
        initial.configuration.evidence.last_successful_write_epoch_ms
    );
    assert!(!recovered.history.path.exists());
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn history_settings_cas_retention_replacement_and_disable_keep_one_owned_collector() {
    let fixture = Fixture::new();
    let settings = HistorySettings {
        enabled: true,
        interval_seconds: 60,
        retention: HistoryRetention {
            max_points_per_series: 10,
            max_series: 4,
            max_total_points: 40,
        },
    };
    fixture
        .mutate(ConfigMutation::SaveHistorySettings {
            expected_revision: 0,
            settings,
        })
        .unwrap();
    assert_eq!(fixture.history.task_group().task_count(), 1);
    assert!(fixture.status().collector_running);
    let stale = fixture.mutate(ConfigMutation::SaveHistorySettings {
        expected_revision: 0,
        settings: HistorySettings {
            interval_seconds: 30,
            ..settings
        },
    });
    assert!(stale.is_err());
    assert_eq!(
        fixture
            .services
            .connection_state()
            .config
            .foundations
            .history_interval_seconds,
        60
    );
    assert_eq!(fixture.services.connection_state().config.revision, 1);
    assert_eq!(fixture.history.task_group().task_count(), 1);
    fixture
        .mutate(ConfigMutation::SaveHistorySettings {
            expected_revision: 1,
            settings,
        })
        .unwrap();
    assert_eq!(
        fixture.history.task_group().task_count(),
        1,
        "saving unchanged settings must reuse the collector"
    );
    let changed = HistorySettings {
        retention: HistoryRetention {
            max_total_points: 20,
            ..settings.retention
        },
        ..settings
    };
    fixture
        .mutate(ConfigMutation::SaveHistorySettings {
            expected_revision: 2,
            settings: changed,
        })
        .unwrap();
    assert_eq!(
        fixture.history.task_group().task_count(),
        1,
        "replacement must await the prior owned collector"
    );
    fixture
        .mutate(ConfigMutation::SaveHistorySettings {
            expected_revision: 3,
            settings: HistorySettings {
                interval_seconds: 0,
                ..changed
            },
        })
        .unwrap();
    let disabled = fixture.status();
    assert_eq!(fixture.history.task_group().task_count(), 0);
    assert!(!disabled.collector_running && disabled.collection.enabled && disabled.collection.interval_seconds == 0);
    let revision = fixture.services.connection_state().config.revision;
    let rejected = fixture.mutate(ConfigMutation::SaveHistorySettings {
        expected_revision: revision,
        settings: HistorySettings {
            retention: HistoryRetention {
                max_series: HISTORY_MAX_SERIES_CAP + 1,
                ..changed.retention
            },
            ..changed
        },
    });
    assert!(rejected.is_err());
    assert_eq!(fixture.services.connection_state().config.revision, revision);
    assert_eq!(fixture.history.task_group().task_count(), 0);
    fixture.mutate(ConfigMutation::AddProxy("proxy:8080".into())).unwrap();
    fixture
        .mutate(ConfigMutation::SwitchProxy("proxy:8080".into()))
        .unwrap();
    assert_eq!(fixture.status().scope, ConnectionScope::Proxy);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn reconciliation_replaces_a_finished_worker_even_when_settings_match() {
    use crate::{
        infrastructure::{
            history_collector::{HistoryLifecycle, HistorySampleFuture, HistorySampler},
            history_store::HistoryStore,
            monitor_store::MonitorStore,
        },
        services::{GlobalConnectionState, ProductFakeConnectionProvider},
    };

    struct EmptySampler;
    impl HistorySampler for EmptySampler {
        fn sample_topics(&self) -> HistorySampleFuture<'_> {
            Box::pin(std::future::ready(Ok(Vec::new())))
        }
        fn sample_brokers(&self) -> HistorySampleFuture<'_> {
            self.sample_topics()
        }
    }

    let fixture = Fixture::new();
    let history_store = HistoryStore::with_config_store(
        fixture.status().history.path,
        fixture.history.clone(),
        fixture.store.clone(),
    );
    let sampler: Arc<dyn HistorySampler> = Arc::new(EmptySampler);
    let foundations = FoundationFlags {
        history_enabled: true,
        history_interval_seconds: 60,
        ..fixture.services.connection_state().config.foundations
    };
    let retention = HistorySettings::from(foundations).retention;
    let mut finished =
        HistoryLifecycle::start(&fixture.history, 60, retention, history_store.clone(), sampler.clone()).unwrap();
    fixture.runtime.block_on(finished.finish_for_test());
    assert!(!finished.is_running());
    assert!(finished.matches_settings(60, retention));
    assert_eq!(fixture.history.task_group().task_count(), 0);
    let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([])));
    let host_context = fixture.runtime.root_context().component("recovery-host");
    let backend = DesktopBackend {
        store: fixture.store.clone(),
        provider: Arc::new(ProductFakeConnectionProvider),
        sessions: crate::services::sessions::SessionService::new(
            fixture.store.clone(),
            auth.clone(),
            host_context.clone(),
        ),
        auth,
        state: parking_lot::RwLock::new(GlobalConnectionState::default()),
        update_gate: tokio::sync::Mutex::new(()),
        history_context: fixture.history.clone(),
        monitor_context: fixture.runtime.root_context().component("recovery-monitor"),
        host_context,
        history_store,
        history_sampler: sampler,
        monitor_store: MonitorStore::with_config_store(fixture.store.clone()),
        history_lifecycle: parking_lot::Mutex::new(Some(finished)),
        monitor_lifecycle: parking_lot::Mutex::new(None),
    };
    fixture
        .runtime
        .block_on(backend.reconcile_foundations(foundations))
        .unwrap();
    assert!(backend.history_lifecycle.lock().as_ref().unwrap().is_running());
    assert_eq!(fixture.history.task_group().task_count(), 1);
    fixture
        .runtime
        .block_on(backend.reconcile_foundations(foundations))
        .unwrap();
    assert_eq!(fixture.history.task_group().task_count(), 1);
    fixture
        .runtime
        .block_on(backend.reconcile_foundations(FoundationFlags {
            history_enabled: false,
            ..foundations
        }))
        .unwrap();
    assert_eq!(fixture.history.task_group().task_count(), 0);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
