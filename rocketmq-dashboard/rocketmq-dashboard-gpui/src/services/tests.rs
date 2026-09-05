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
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
};

use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::infrastructure::auth_state::MapEnvironment;

struct FakeConnectionProvider {
    switch_results: parking_lot::Mutex<VecDeque<Result<(), ProviderError>>>,
    switch_gate: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    revision: AtomicU64,
    endpoint: parking_lot::Mutex<String>,
}

impl FakeConnectionProvider {
    fn new(results: impl IntoIterator<Item = Result<(), ProviderError>>) -> Arc<Self> {
        Arc::new(Self {
            switch_results: parking_lot::Mutex::new(results.into_iter().collect()),
            switch_gate: parking_lot::Mutex::new(None),
            revision: AtomicU64::new(0),
            endpoint: parking_lot::Mutex::new(String::new()),
        })
    }
}

impl ConnectionProvider for FakeConnectionProvider {
    fn switch(
        &self,
        snapshot: rocketmq_dashboard_common::ConnectionSnapshot,
    ) -> ServiceFuture<'_, Result<AdminSessionSummary, ProviderError>> {
        let result = self.switch_results.lock().pop_front().unwrap_or(Ok(()));
        let gate = self.switch_gate.lock().take();
        self.revision.store(snapshot.revision, Ordering::Release);
        *self.endpoint.lock() = snapshot.nameserver.unwrap_or_default();
        Box::pin(async move {
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            result.map(|()| AdminSessionSummary {
                revision: snapshot.revision,
                status: AdminSessionStatus::Connected,
                credential_source: snapshot.credential_source,
            })
        })
    }

    fn check_health(&self) -> ServiceFuture<'_, Result<EndpointHealth, ProviderError>> {
        let health = EndpointHealth {
            endpoint: self.endpoint.lock().clone(),
            revision: self.revision.load(Ordering::Acquire),
            availability: rocketmq_dashboard_common::EndpointAvailability::Available,
            checked_at_epoch_ms: Some(1),
            failure_summary: None,
        };
        Box::pin(std::future::ready(Ok(health)))
    }

    fn check_endpoints(
        &self,
        snapshots: Vec<rocketmq_dashboard_common::ConnectionSnapshot>,
    ) -> ServiceFuture<'_, Result<Vec<EndpointHealth>, ProviderError>> {
        Box::pin(std::future::ready(Ok(snapshots
            .into_iter()
            .map(|snapshot| EndpointHealth {
                endpoint: snapshot.nameserver.unwrap_or_default(),
                revision: snapshot.revision,
                availability: Default::default(),
                checked_at_epoch_ms: None,
                failure_summary: None,
            })
            .collect())))
    }
}

fn test_runtime(name: &'static str) -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig::for_parallelism(name, 1))
        .expect("test runtime configuration is valid")
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory limit"))
        .build()
        .expect("test runtime")
}

fn real_test_services(
    runtime: &RuntimeOwner,
    path: PathBuf,
    provider: Arc<dyn ConnectionProvider>,
) -> (Arc<DesktopConfigStore>, AppServices) {
    let store = DesktopConfigStore::new(path, runtime.root_context().component("config"));
    let services = AppServices::desktop_inner(
        Arc::clone(&store),
        provider,
        None,
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        runtime.root_context().component("services"),
        runtime.root_context().component("history"),
        runtime.root_context().component("monitor"),
    );
    (store, services)
}

#[test]
fn progress_exposes_connecting_before_provider_then_exact_order() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let runtime = test_runtime("gpui-progress-order");
    let provider = FakeConnectionProvider::new([Ok(())]);
    let (release, gate) = tokio::sync::oneshot::channel();
    *provider.switch_gate.lock() = Some(gate);
    let (store, services) = real_test_services(&runtime, directory.path().join("config.json"), provider);

    runtime.block_on(async {
        store.load().await.expect("initialize store revision");
        let (progress, mut updates) = tokio::sync::mpsc::unbounded_channel();
        let mut operation =
            Box::pin(services.mutate_with_progress(ConfigMutation::AddNameServer("localhost:9876".into()), progress));
        let first = tokio::select! {
            update = updates.recv() => update.expect("persisted progress"),
            result = &mut operation => panic!("operation completed before provider gate: {result:?}"),
        };
        assert_eq!(
            first,
            ConfigUpdated {
                revision: 1,
                phase: ConfigUpdatePhase::Persisted,
                route_transition: ConfigRouteTransition::None,
            }
        );
        assert_eq!(
            services.connection_state().session.status,
            AdminSessionStatus::Connecting
        );
        release.send(()).expect("release provider");
        operation.await.expect("mutation");
        let mut observed = vec![first];
        while let Some(update) = updates.recv().await {
            observed.push(update);
        }
        assert_eq!(
            observed,
            [
                ConfigUpdated {
                    revision: 1,
                    phase: ConfigUpdatePhase::Persisted,
                    route_transition: ConfigRouteTransition::None,
                },
                ConfigUpdated {
                    revision: 1,
                    phase: ConfigUpdatePhase::ProviderSwitched,
                    route_transition: ConfigRouteTransition::None,
                },
                ConfigUpdated {
                    revision: 1,
                    phase: ConfigUpdatePhase::Invalidated,
                    route_transition: ConfigRouteTransition::None,
                },
                ConfigUpdated {
                    revision: 1,
                    phase: ConfigUpdatePhase::HealthRefreshed,
                    route_transition: ConfigRouteTransition::None,
                },
                ConfigUpdated {
                    revision: 1,
                    phase: ConfigUpdatePhase::Completed,
                    route_transition: ConfigRouteTransition::None,
                },
            ]
        );
    });
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn unreachable_connection_projects_connecting_then_failed_with_real_health_semantics() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let runtime = test_runtime("gpui-progress-failure-projection");
    let provider = FakeConnectionProvider::new([Err(ProviderError::unavailable_for_test())]);
    let (release, gate) = tokio::sync::oneshot::channel();
    *provider.switch_gate.lock() = Some(gate);
    let (store, services) = real_test_services(&runtime, directory.path().join("config.json"), provider);

    runtime.block_on(async {
        store.load().await.expect("initialize store revision");
        let (progress, mut updates) = tokio::sync::mpsc::unbounded_channel();
        let mut operation =
            Box::pin(services.mutate_with_progress(ConfigMutation::AddNameServer("unreachable:9876".into()), progress));
        let persisted = tokio::select! {
            update = updates.recv() => update.expect("persisted progress"),
            result = &mut operation => panic!("operation completed before provider gate: {result:?}"),
        };
        assert_eq!(persisted.phase, ConfigUpdatePhase::Persisted);
        let connecting = crate::components::topbar::ConnectionSummary::from_state(
            &services.connection_state(),
            &SessionState::signed_out(),
        );
        assert_eq!(connecting.admin_session_label(), "Admin: Connecting");
        assert_eq!(connecting.health, "Health unknown");

        release.send(()).expect("release provider");
        let update = operation.await.expect("persisted connection warning");
        assert!(update.connection_warning.is_some());
        let failed = crate::components::topbar::ConnectionSummary::from_state(
            &services.connection_state(),
            &SessionState::signed_out(),
        );
        assert_eq!(failed.revision, 1);
        assert_eq!(failed.admin_session_label(), "Admin: Failed");
        assert_eq!(failed.health, "Unavailable");
    });
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn failed_transport_save_rolls_back_at_a_second_revision_with_ordered_progress() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let runtime = test_runtime("gpui-progress-rollback");
    let provider = FakeConnectionProvider::new([Err(ProviderError::unavailable_for_test()), Ok(())]);
    let (store, services) = real_test_services(&runtime, directory.path().join("config.json"), provider);

    runtime.block_on(async {
        store.load().await.expect("initialize store revision");
        let (progress, mut updates) = tokio::sync::mpsc::unbounded_channel();
        let error = services
            .mutate_with_progress(
                ConfigMutation::SaveTransport(TransportSettings {
                    use_tls: true,
                    use_vip_channel: true,
                }),
                progress,
            )
            .await
            .expect_err("provider failure triggers rollback");
        assert!(error.is_retryable());
        let final_state = services.connection_state();
        assert_eq!(final_state.config.revision, 2, "rollback error: {error}");
        let mut observed = Vec::new();
        while let Some(update) = updates.recv().await {
            observed.push((update.revision, update.phase));
        }
        assert_eq!(
            observed,
            [
                (1, ConfigUpdatePhase::Persisted),
                (1, ConfigUpdatePhase::ProviderSwitched),
                (1, ConfigUpdatePhase::Invalidated),
                (1, ConfigUpdatePhase::HealthRefreshed),
                (2, ConfigUpdatePhase::Persisted),
                (2, ConfigUpdatePhase::ProviderSwitched),
                (2, ConfigUpdatePhase::Invalidated),
                (2, ConfigUpdatePhase::HealthRefreshed),
                (2, ConfigUpdatePhase::RolledBack),
            ]
        );
        assert_eq!(final_state.config.transport, TransportSettings::default());
    });
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn configuration_and_global_debug_are_structurally_redacted() {
    let endpoint = "private-endpoint.example:9876";
    let config = DesktopConfig {
        nameservers: vec![endpoint.into()],
        current_nameserver: Some(endpoint.into()),
        proxies: vec!["private-proxy.example:8080".into()],
        current_proxy: Some("private-proxy.example:8080".into()),
        ..DesktopConfig::default()
    };
    let state = GlobalConnectionState {
        config: config.clone(),
        health: Some(EndpointHealth {
            endpoint: endpoint.into(),
            revision: 0,
            availability: Default::default(),
            checked_at_epoch_ms: None,
            failure_summary: Some("private-backend-value".into()),
        }),
        ..GlobalConnectionState::default()
    };
    let update = ConfigUpdate {
        config,
        connection_warning: Some(UiError::new(
            "private-diagnostic-value password-value access-value secret-value token-value",
            UiErrorCode::Connection,
            true,
        )),
    };
    let session = SessionState::for_username("session-value".into());
    let endpoint_mutation = ConfigMutation::RemoveNameServer {
        address: endpoint.into(),
        replacement: Some("replacement-private:9876".into()),
    };
    let proxy_mutation = ConfigMutation::SwitchProxy("private-proxy.example:8080".into());
    let transport_mutation = ConfigMutation::SaveTransport(TransportSettings {
        use_tls: true,
        use_vip_channel: true,
    });
    let summary = crate::components::topbar::ConnectionSummary::from_state(&state, &session);
    let debug = format!(
        "{state:?} {update:?} {session:?} {summary:?} {endpoint_mutation:?} {proxy_mutation:?} \
         {transport_mutation:?}"
    );

    for forbidden in [
        endpoint,
        "private-proxy.example:8080",
        "replacement-private:9876",
        "private-backend-value",
        "private-diagnostic-value",
        "password-value",
        "access-value",
        "secret-value",
        "token-value",
        "session-value",
    ] {
        assert!(!debug.contains(forbidden));
    }
}

#[test]
fn stale_health_is_discarded_after_a_persisted_revision_change() {
    let mut state = GlobalConnectionState::default();
    let config = DesktopConfig {
        revision: 2,
        ..DesktopConfig::default()
    };
    state.persisted(config);
    let accepted = state.apply_health(EndpointHealth {
        endpoint: "old:9876".into(),
        revision: 1,
        availability: Default::default(),
        checked_at_epoch_ms: None,
        failure_summary: None,
    });

    assert!(!accepted);
    assert!(state.health.is_none());
}

#[test]
fn config_propagation_updates_shell_before_invalidating_old_scope() {
    let mut state = GlobalConnectionState::default();
    let config = DesktopConfig {
        revision: 4,
        current_nameserver: Some("localhost:9876".into()),
        nameservers: vec!["localhost:9876".into()],
        ..DesktopConfig::default()
    };

    state.persisted(config);
    assert_eq!(state.config.revision, 4);
    assert_eq!(state.request_invalidation, 0);
    state.provider_switched(AdminSessionSummary {
        revision: 4,
        status: AdminSessionStatus::Connected,
        credential_source: CredentialSourceKind::None,
    });
    state.invalidate_old_scope();

    assert_eq!(state.session.status, AdminSessionStatus::Connected);
    assert_eq!(state.request_invalidation, 1);
    assert_eq!(state.sensitive_clear_generation, 1);

    state.provider_failed();
    assert_eq!(state.session.revision, 4);
    assert_eq!(state.session.status, AdminSessionStatus::Failed);
}

#[test]
fn endpoint_mutations_enforce_active_replacement_and_proxy_fallback() {
    let mut config = DesktopConfig {
        nameservers: vec!["one:9876".into(), "two:9876".into()],
        current_nameserver: Some("one:9876".into()),
        proxies: vec!["proxy:8080".into()],
        current_proxy: Some("proxy:8080".into()),
        scope: ConnectionScope::Proxy,
        ..DesktopConfig::default()
    };
    assert!(
        apply_mutation(
            &mut config,
            &ConfigMutation::RemoveNameServer {
                address: "one:9876".into(),
                replacement: None,
            },
        )
        .is_err()
    );
    apply_mutation(
        &mut config,
        &ConfigMutation::RemoveNameServer {
            address: "one:9876".into(),
            replacement: Some("two:9876".into()),
        },
    )
    .expect("replacement");
    apply_mutation(
        &mut config,
        &ConfigMutation::RemoveProxy {
            address: "proxy:8080".into(),
            replacement: None,
            fallback_to_nameserver: true,
        },
    )
    .expect("fallback");
    assert_eq!(config.scope, ConnectionScope::NameServer);
    assert_eq!(config.current_proxy, None);

    let mut nameserver_scope = DesktopConfig::default();
    apply_mutation(&mut nameserver_scope, &ConfigMutation::AddProxy("first:8080".into())).expect("first proxy");
    assert_eq!(nameserver_scope.scope, ConnectionScope::NameServer);
    apply_mutation(
        &mut nameserver_scope,
        &ConfigMutation::RemoveProxy {
            address: "first:8080".into(),
            replacement: None,
            fallback_to_nameserver: false,
        },
    )
    .expect("current but inactive proxy is removable");
    assert!(nameserver_scope.proxies.is_empty());
    assert!(nameserver_scope.current_proxy.is_none());
}

#[test]
fn only_an_actual_authentication_boundary_requests_shell_navigation() {
    let ordinary = [
        ConfigMutation::AddNameServer("private-nameserver:9876".into()),
        ConfigMutation::SwitchNameServer("private-nameserver:9876".into()),
        ConfigMutation::SaveTransport(TransportSettings {
            use_tls: true,
            use_vip_channel: true,
        }),
        ConfigMutation::AddProxy("private-proxy:8080".into()),
        ConfigMutation::SwitchProxy("private-proxy:8080".into()),
        ConfigMutation::SetCredentialSource(CredentialSourceKind::Environment),
        ConfigMutation::Reload,
    ];
    for mutation in ordinary {
        assert_eq!(mutation.route_transition(false, false), ConfigRouteTransition::None);
        assert_eq!(mutation.route_transition(true, true), ConfigRouteTransition::None);
    }
    assert_eq!(
        ConfigMutation::SetAuthEnabled(true).route_transition(false, true),
        ConfigRouteTransition::AuthenticationEnabled
    );
    assert_eq!(
        ConfigMutation::SetAuthEnabled(false).route_transition(true, false),
        ConfigRouteTransition::AuthenticationDisabled
    );
}

#[test]
fn auth_enable_preflight_failure_does_not_persist_or_advance_revision() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let runtime = test_runtime("gpui-auth-preflight");
    let provider = FakeConnectionProvider::new([]);
    let (store, services) = real_test_services(&runtime, directory.path().join("config.json"), provider);

    runtime.block_on(async {
        let initial = store.load().await.expect("initial config");
        let (progress, mut updates) = tokio::sync::mpsc::unbounded_channel();
        let error = services
            .mutate_with_progress(ConfigMutation::SetAuthEnabled(true), progress)
            .await
            .expect_err("missing environment must reject enable");
        assert!(error.is_retryable());
        assert!(updates.recv().await.is_none());
        let persisted = store.load().await.expect("unchanged config");
        assert_eq!(persisted, initial);
        assert!(!persisted.auth.enabled);
        assert_eq!(persisted.revision, 0);
    });
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn real_service_endpoint_mutations_require_explicit_replacement_but_allow_inactive_first_proxy_delete() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let runtime = test_runtime("gpui-endpoint-product");
    let provider = FakeConnectionProvider::new([]);
    let (store, services) = real_test_services(&runtime, directory.path().join("config.json"), provider);

    runtime.block_on(async {
        store.load().await.expect("initial config");
        for mutation in [
            ConfigMutation::AddNameServer("one:9876".into()),
            ConfigMutation::AddNameServer("two:9876".into()),
        ] {
            let (progress, _updates) = tokio::sync::mpsc::unbounded_channel();
            services
                .mutate_with_progress(mutation, progress)
                .await
                .expect("add nameserver");
        }
        let revision_before_rejection = services.connection_state().config.revision;
        let (progress, _updates) = tokio::sync::mpsc::unbounded_channel();
        services
            .mutate_with_progress(
                ConfigMutation::RemoveNameServer {
                    address: "one:9876".into(),
                    replacement: None,
                },
                progress,
            )
            .await
            .expect_err("current nameserver needs replacement");
        assert_eq!(services.connection_state().config.revision, revision_before_rejection);

        let (progress, _updates) = tokio::sync::mpsc::unbounded_channel();
        services
            .mutate_with_progress(
                ConfigMutation::RemoveNameServer {
                    address: "one:9876".into(),
                    replacement: Some("two:9876".into()),
                },
                progress,
            )
            .await
            .expect("explicit nameserver replacement");
        assert_eq!(
            services.connection_state().config.current_nameserver.as_deref(),
            Some("two:9876")
        );

        let (progress, _updates) = tokio::sync::mpsc::unbounded_channel();
        services
            .mutate_with_progress(ConfigMutation::AddProxy("first:8080".into()), progress)
            .await
            .expect("add first inactive proxy");
        assert_eq!(services.connection_state().config.scope, ConnectionScope::NameServer);
        let (progress, _updates) = tokio::sync::mpsc::unbounded_channel();
        services
            .mutate_with_progress(
                ConfigMutation::RemoveProxy {
                    address: "first:8080".into(),
                    replacement: None,
                    fallback_to_nameserver: false,
                },
                progress,
            )
            .await
            .expect("inactive current proxy may be deleted");
        assert!(services.connection_state().config.proxies.is_empty());
    });
    runtime.shutdown_runtime_blocking().expect("shutdown");
}

#[test]
fn reload_reconciles_history_and_monitor_lifecycles_in_both_directions() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let runtime = test_runtime("gpui-foundation-reconcile");
    let provider = FakeConnectionProvider::new([]);
    let store = DesktopConfigStore::new(
        directory.path().join("config.json"),
        runtime.root_context().component("config"),
    );
    let history_context = runtime.root_context().component("history");
    let monitor_context = runtime.root_context().component("monitor");
    let history_tasks = history_context.task_group().clone();
    let monitor_tasks = monitor_context.task_group().clone();
    let services = AppServices::desktop_inner(
        Arc::clone(&store),
        provider,
        None,
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        runtime.root_context().component("services"),
        history_context,
        monitor_context,
    );

    runtime.block_on(async {
        let initial = store.load().await.expect("initial config");
        store
            .save_next(DesktopConfig {
                foundations: crate::infrastructure::config_store::FoundationFlags {
                    history_enabled: true,
                    monitor_enabled: true,
                    ..Default::default()
                },
                ..initial
            })
            .await
            .expect("enable foundations externally");
        services.bootstrap().await.expect("bootstrap enabled foundations");
        assert_eq!(history_tasks.task_count(), 1);
        assert_eq!(monitor_tasks.task_count(), 1);

        let enabled = store.load().await.expect("enabled config");
        store
            .save_next(DesktopConfig {
                foundations: Default::default(),
                ..enabled
            })
            .await
            .expect("disable foundations externally");
        let (progress, _updates) = tokio::sync::mpsc::unbounded_channel();
        services
            .mutate_with_progress(ConfigMutation::Reload, progress)
            .await
            .expect("reload disabled foundations");
        assert_eq!(history_tasks.task_count(), 0);
        assert_eq!(monitor_tasks.task_count(), 0);

        let disabled = store.load().await.expect("disabled config");
        store
            .save_next(DesktopConfig {
                foundations: crate::infrastructure::config_store::FoundationFlags {
                    history_enabled: true,
                    monitor_enabled: true,
                    ..Default::default()
                },
                ..disabled
            })
            .await
            .expect("re-enable foundations externally");
        let (progress, _updates) = tokio::sync::mpsc::unbounded_channel();
        services
            .mutate_with_progress(ConfigMutation::Reload, progress)
            .await
            .expect("reload re-enabled foundations");
        assert_eq!(history_tasks.task_count(), 1);
        assert_eq!(monitor_tasks.task_count(), 1);
    });
    runtime.shutdown_runtime_blocking().expect("shutdown");
}
