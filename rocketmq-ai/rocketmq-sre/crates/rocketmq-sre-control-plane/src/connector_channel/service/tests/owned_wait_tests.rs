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

use rocketmq_runtime::RuntimeContext;
use tokio::sync::oneshot;

use super::*;

#[tokio::test]
async fn an_already_cancelled_owner_never_admits_a_query() {
    let store = MemoryStore::default();
    let service = ConnectorChannelService::new(store.clone(), "test-token").unwrap();
    let tenant = TenantId::new();
    let cluster = ClusterId::new();
    let runtime = RuntimeContext::from_current("cancel-before-connector-read");
    let context = runtime.service_context("forecast");
    context.task_group().cancel();
    assert!(
        service
            .query_and_wait_owned(
                tenant,
                cluster,
                query(tenant, cluster),
                Utc::now() + chrono::Duration::seconds(20),
                context.task_group()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.state.lock().await.commands.is_empty());
}

#[tokio::test]
async fn pending_source_read_stops_with_its_owner_and_only_records_cancel_intent() {
    let store = MemoryStore::default();
    let service = ConnectorChannelService::new(store.clone(), "test-token").unwrap();
    let tenant = TenantId::new();
    let cluster = ClusterId::new();
    let session = ConnectorSessionId::new();
    service
        .register(&principal(), &registration(tenant, cluster, session))
        .await
        .unwrap();
    let mut commands = service.command_signal.subscribe();
    let runtime = RuntimeContext::from_current("cancel-pending-connector-read");
    let context = runtime.service_context("forecast");
    let group = context.task_group().clone();
    let worker = group.clone();
    let (finished, result) = oneshot::channel();
    group
        .spawn_service("source-wait", async move {
            let response = service
                .query_and_wait_owned(
                    tenant,
                    cluster,
                    query(tenant, cluster),
                    Utc::now() + chrono::Duration::seconds(20),
                    &worker,
                )
                .await
                .unwrap();
            finished.send(response).unwrap();
        })
        .unwrap();
    commands.changed().await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(3), group.shutdown(Duration::from_secs(2)))
        .await
        .unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
    assert!(
        result.await.unwrap().is_none(),
        "owner stop is not a fabricated deadline or remote result"
    );
    let state = store.state.lock().await;
    let log = &state.commands[&session];
    assert_eq!(log.len(), 2);
    assert!(matches!(&log[0], ConnectorCommand::Query { .. }));
    assert!(matches!(&log[1], ConnectorCommand::Cancel { .. }));
    assert_eq!(log[0].correlation_id(), log[1].correlation_id());
    assert!(
        state.responses.is_empty(),
        "cancel intent does not acknowledge remote completion"
    );
}

#[tokio::test]
async fn cancellation_drains_an_admitted_enqueue_before_cancelling_its_read_wait() {
    let gate = Arc::new(AdmissionGate::default());
    let store = MemoryStore {
        admission_gate: Some(gate.clone()),
        ..MemoryStore::default()
    };
    let service = ConnectorChannelService::new(store.clone(), "test-token").unwrap();
    let tenant = TenantId::new();
    let cluster = ClusterId::new();
    let session = ConnectorSessionId::new();
    service
        .register(&principal(), &registration(tenant, cluster, session))
        .await
        .unwrap();
    let runtime = RuntimeContext::from_current("drain-connector-admission");
    let context = runtime.service_context("forecast");
    let group = context.task_group().clone();
    let worker = group.clone();
    let (finished, mut result) = oneshot::channel();
    group
        .spawn_service("admission", async move {
            let response = service
                .query_and_wait_owned(
                    tenant,
                    cluster,
                    query(tenant, cluster),
                    Utc::now() + chrono::Duration::seconds(20),
                    &worker,
                )
                .await
                .unwrap();
            finished.send(response).unwrap();
        })
        .unwrap();
    gate.entered.notified().await;
    group.cancel();
    assert!(matches!(result.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
    gate.release.notify_one();
    let report = tokio::time::timeout(Duration::from_secs(3), group.shutdown(Duration::from_secs(2)))
        .await
        .unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
    assert!(result.await.unwrap().is_none());
    let state = store.state.lock().await;
    let log = &state.commands[&session];
    assert_eq!(
        log.len(),
        2,
        "the completed admission is followed only by its cancel intent"
    );
    assert_eq!(log[0].correlation_id(), log[1].correlation_id());
}
