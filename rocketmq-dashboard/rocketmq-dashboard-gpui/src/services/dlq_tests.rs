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
use crate::{
    infrastructure::{
        auth_state::{DesktopAuthState, MapEnvironment},
        config_store::DesktopConfigStore,
    },
    services::ConfigMutation,
};
use rocketmq_dashboard_common::{AuditFilter, AuditQuery};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};
use std::time::Duration;
fn scope() -> MessageRequestScope {
    MessageRequestScope { revision: 0, epoch: 1 }
}
fn runtime() -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig::for_parallelism("dlq-test", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap()
}
fn desktop(
    runtime: &RuntimeOwner,
    directory: &std::path::Path,
    backend: Arc<FakeDlqBackend>,
) -> (AppServices, Arc<DesktopConfigStore>) {
    let store = DesktopConfigStore::new(
        directory.join("config.json"),
        runtime.root_context().component("storage"),
    );
    (
        AppServices::desktop_with_fake_provider(
            store.clone(),
            DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
            runtime.root_context().component("application"),
            runtime.root_context().component("history"),
            runtime.root_context().component("monitor"),
        )
        .with_dlq_backend(backend),
        store,
    )
}
#[test]
fn dlq_group_inventory_requires_current_nameserver_scope_without_changing_proxy_consumer_queries() {
    use crate::services::consumers::test_support::FakeConsumerBackend;
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let backend = Arc::new(FakeConsumerBackend::default());
    let (services, _) = desktop(&runtime, directory.path(), Arc::new(FakeDlqBackend::default()));
    let services = services.with_consumer_backend(backend.clone());
    runtime.block_on(async {
        services.bootstrap().await.unwrap();
        let request = ConsumerRequestScope {
            revision: 0,
            epoch: 1,
            query_scope: ConsumerQueryScope::Configured,
        };
        backend.queue_inventory(Ok(group_inventory()));
        assert_eq!(services.dlq_group_inventory(request).await.unwrap().groups.len(), 1);
        let release = backend.hold_next_inventory();
        let started = backend.observe_next_inventory();
        backend.queue_inventory(Ok(group_inventory()));
        let mut pending = Box::pin(services.dlq_group_inventory(request));
        tokio::select! {
            result = &mut pending => panic!("discovery completed before release: {result:?}"),
            result = started => result.unwrap(),
        }
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        services
            .mutate_with_progress(ConfigMutation::AddProxy("proxy:8080".into()), progress.clone())
            .await
            .unwrap();
        services
            .mutate_with_progress(ConfigMutation::SwitchProxy("proxy:8080".into()), progress)
            .await
            .unwrap();
        release.send(()).unwrap();
        assert_eq!(pending.await.unwrap_err().code(), UiErrorCode::CapabilityUnavailable);
        let proxy_request = ConsumerRequestScope {
            revision: services.connection_state().config.revision,
            epoch: 2,
            ..request
        };
        assert_eq!(
            services.dlq_group_inventory(proxy_request).await.unwrap_err().code(),
            UiErrorCode::CapabilityUnavailable
        );
        assert_eq!(backend.calls().inventory.len(), 2);
        backend.queue_inventory(Ok(group_inventory()));
        services.consumer_inventory(proxy_request).await.unwrap();
        assert_eq!(
            backend.calls().inventory.len(),
            3,
            "ordinary Consumer Proxy queries remain available"
        );
        assert!(
            backend
                .calls()
                .inventory
                .iter()
                .all(|scope| scope.query_scope == ConsumerQueryScope::Configured)
        );
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
#[tokio::test]
async fn dlq_batch_deduplicates_and_retry_uses_only_confirmed_failures() {
    let backend = Arc::new(FakeDlqBackend::default());
    let services = AppServices::default().with_dlq_backend(backend.clone());
    let targets = (1..=5).map(|i| row(i).target.unwrap()).collect::<Vec<_>>();
    backend.consumes.lock().unwrap().extend(
        [
            DlqEntryState::Succeeded,
            DlqEntryState::BusinessFailure,
            DlqEntryState::CallFailed,
            DlqEntryState::Unknown,
            DlqEntryState::Unattempted,
        ]
        .map(attempt),
    );
    let mut requested = targets.clone();
    requested.push(targets[0].clone());
    let result = services.recover_dlq(scope(), requested).await.unwrap();
    assert_eq!(result.entries.len(), 5);
    assert_eq!(result.retryable_targets(), vec![targets[1].clone(), targets[2].clone()]);
    backend
        .consumes
        .lock()
        .unwrap()
        .extend([attempt(DlqEntryState::Succeeded), attempt(DlqEntryState::Succeeded)]);
    services.recover_dlq(scope(), result.retryable_targets()).await.unwrap();
    let calls = backend.calls().consumes;
    assert_eq!(calls.len(), 7);
    assert_eq!(calls[5].1, targets[1]);
    assert_eq!(calls[6].1, targets[2]);
    assert!(services.recover_dlq(scope(), Vec::new()).await.is_err());
    assert_eq!(backend.calls().consumes.len(), 7);
}
#[test]
fn dlq_entire_admitted_batch_survives_receiver_drop_and_keeps_original_environment_audit() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let backend = Arc::new(FakeDlqBackend::default());
    let (services, store) = desktop(&runtime, directory.path(), backend.clone());
    runtime.block_on(async {
        services.bootstrap().await.unwrap(); let original_environment = store.environment_snapshot().unwrap().environment_id;
        backend.consumes.lock().unwrap().extend([DlqEntryState::Succeeded, DlqEntryState::BusinessFailure, DlqEntryState::Unknown].map(attempt));
        let (started, release, _) = backend.hold_consume(); let targets = (1..=3).map(|i| row(i).target.unwrap()).collect();
        let mut batch = Box::pin(services.recover_dlq(scope(), targets));
        tokio::select! { result = &mut batch => panic!("batch finished before release: {result:?}"), result = started => result.unwrap() }
        drop(batch);
        let (progress, _) = tokio::sync::mpsc::unbounded_channel(); services.mutate_with_progress(ConfigMutation::CreateEnvironment("another environment".into()), progress).await.unwrap();
        assert_ne!(store.environment_snapshot().unwrap().environment_id, original_environment);
        release.send(()).unwrap();
        assert!(services.runtime_bridge.as_ref().unwrap().context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
        assert_eq!(backend.calls().consumes.len(), 3); assert!(services.audit_pending().unwrap().is_empty());
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        let events = services.query_audit(AuditQuery { filter: AuditFilter::recent(now), limit: 20, cursor: None }).unwrap().events.into_iter().filter(|event| event.operation.action == AuditAction::DlqResent).collect::<Vec<_>>();
        assert_eq!(events.len(), 3); let outcomes = events.iter().map(|event| event.outcome).collect::<Vec<_>>();
        assert!(outcomes.contains(&AuditOutcome::Succeeded) && outcomes.contains(&AuditOutcome::Failed) && outcomes.contains(&AuditOutcome::Unknown));
        assert!(events.iter().all(|event| event.operation.context.environment_id == original_environment && event.operation.context.configuration_revision == 0));
        let ids = events.iter().map(|event| &event.operation.context.request_id).collect::<BTreeSet<_>>(); assert_eq!(ids.len(), 3);
        let persisted = std::fs::read_to_string(store.path()).unwrap(); assert!(!persisted.contains("group-a"));
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}

#[test]
fn dlq_audit_intake_failure_keeps_unattempted_entries_and_sends_no_consume() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let backend = Arc::new(FakeDlqBackend::default());
    let (services, store) = desktop(&runtime, directory.path(), backend.clone());
    runtime.block_on(async {
        services.bootstrap().await.unwrap();
        // This task-owned path can no longer be read as a journal. Admission must fail before RPC.
        std::fs::remove_file(store.path()).unwrap();
        std::fs::create_dir(store.path()).unwrap();
        let result = services
            .recover_dlq(scope(), vec![row(1).target.unwrap(), row(2).target.unwrap()])
            .await
            .unwrap();
        assert_eq!(result.entries.len(), 2);
        assert!(
            result
                .entries
                .iter()
                .all(|entry| entry.attempt.state == DlqEntryState::Unattempted)
        );
        assert!(backend.calls().consumes.is_empty());
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
#[test]
fn dlq_query_is_cancellable_and_full_csv_write_is_owned_and_preserves_size_or_io_errors() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let backend = Arc::new(FakeDlqBackend::default());
    let (services, _) = desktop(&runtime, directory.path(), backend.clone());
    runtime.block_on(async {
        services.bootstrap().await.unwrap(); backend.queries.lock().unwrap().push_back(Ok(page())); let (started, _release, finished) = backend.hold_query();
        let mut read = Box::pin(services.query_dlq(scope(), query())); tokio::select! { result = &mut read => panic!("read completed early: {result:?}"), result = started => result.unwrap() } drop(read); finished.await.unwrap();
        let body = "完整内容".repeat(50_000); backend.exports.lock().unwrap().push_back(Ok(csv(body.clone())));
        let path = directory.path().join("page.csv"); let expected = page().rows.iter().map(|row| row.metadata.identity.clone()).collect::<Vec<_>>();
        services.export_dlq_page(scope(), query(), expected.clone(), path.clone()).await.unwrap(); assert!(std::fs::read_to_string(&path).unwrap().contains(&body));
        backend.exports.lock().unwrap().push_back(Err(UiError::new(rocketmq_dashboard_common::DlqCsvError::TooLarge.summary(), UiErrorCode::Validation, false)));
        let oversized = directory.path().join("too-large.csv"); assert!(services.export_dlq_page(scope(), query(), expected.clone(), oversized.clone()).await.is_err()); assert!(!oversized.exists());
        backend.exports.lock().unwrap().push_back(Ok(csv("private-body".into())));
        let failure = services.export_dlq_page(scope(), query(), expected, directory.path().join("missing-parent").join("page.csv")).await.unwrap_err(); assert_eq!(failure.code(), UiErrorCode::Configuration); assert!(!format!("{failure:?}").contains("private-body"));
        assert_eq!(backend.calls().exports.len(), 3); assert_eq!(backend.calls().queries.len(), 1);
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
