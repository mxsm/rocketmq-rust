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

use super::{test_support::FakeMessageBackend, *};
use crate::infrastructure::{
    auth_state::{DesktopAuthState, MapEnvironment},
    config_store::DesktopConfigStore,
};
use rocketmq_dashboard_common::{
    AuditAction, AuditFilter, AuditOutcome, AuditQuery, MessageConsumeCode, MessageConsumeCommand,
    MessageConsumeReceipt, MessageIdentity, MessagePageExtent, MessageSearchPage, MessageSearchQuery,
};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};
use std::{sync::Arc, time::Duration};

fn identity() -> MessageIdentity {
    MessageIdentity {
        topic: "orders".into(),
        message_id: "7F00000100002A9F0000000000000001".into(),
    }
}
fn command() -> MessageConsumeCommand {
    MessageConsumeCommand {
        identity: identity(),
        consumer_group: "group-a".into(),
        client_id: Some("client-a".into()),
        scope: ConnectionScope::NameServer,
    }
}
fn scope() -> MessageRequestScope {
    MessageRequestScope { revision: 0, epoch: 1 }
}
fn empty() -> MessageSearchPage {
    MessageSearchPage {
        rows: Vec::new(),
        extent: MessagePageExtent::End,
    }
}
fn receipt(code: MessageConsumeCode) -> MessageConsumeReceipt {
    MessageConsumeReceipt {
        code,
        reported_success: code == MessageConsumeCode::Success,
        remark_returned: true,
    }
}

#[tokio::test]
async fn message_service_rejects_topicless_search_and_invalid_consume_before_backend() {
    let backend = Arc::new(FakeMessageBackend::default());
    let services = AppServices::default().with_message_backend(backend.clone());
    {
        for query in [
            MessageSearchQuery::Key {
                topic: String::new(),
                key: "key".into(),
            },
            MessageSearchQuery::MessageId {
                topic: String::new(),
                message_id: "id".into(),
            },
        ] {
            assert_eq!(
                services.query_messages(scope(), query).await.unwrap_err().code(),
                UiErrorCode::Validation
            );
        }
        let mut invalid = command();
        invalid.consumer_group.clear();
        assert_eq!(
            services.consume_message(scope(), invalid).await.unwrap_err().code(),
            UiErrorCode::Validation
        );
    }
    let calls = backend.calls();
    assert!(calls.query.is_empty() && calls.consume.is_empty());
}

#[test]
fn message_consume_audit_distinguishes_business_failure_and_transport_unknown() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("message-audit-test", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    let store = DesktopConfigStore::new(
        directory.path().join("config.json"),
        runtime.root_context().component("storage"),
    );
    let backend = Arc::new(FakeMessageBackend::default());
    let services = AppServices::desktop_with_fake_provider(
        store.clone(),
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        runtime.root_context().component("mutations"),
        runtime.root_context().component("history"),
        runtime.root_context().component("monitor"),
    )
    .with_message_backend(backend.clone());
    runtime.block_on(async {
        services.bootstrap().await.unwrap();
        backend.queue_consume(Ok(receipt(MessageConsumeCode::Later)));
        assert!(!services.consume_message(scope(), command()).await.unwrap().is_success());
        backend.queue_consume(Err(UiError::caused_by(
            "Direct consume is unconfirmed.",
            UiErrorCode::Connection,
            false,
            std::io::Error::other("private-body-private-remark"),
        )));
        assert_eq!(
            services.consume_message(scope(), command()).await.unwrap_err().code(),
            UiErrorCode::Connection
        );
        backend.queue_consume(Ok(receipt(MessageConsumeCode::Success)));
        assert!(services.consume_message(scope(), command()).await.unwrap().is_success());
    });
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let events = services
        .query_audit(AuditQuery {
            filter: AuditFilter::recent(now),
            limit: 20,
            cursor: None,
        })
        .unwrap()
        .events;
    let outcomes = events
        .iter()
        .filter(|event| event.operation.action == AuditAction::MessageConsumed)
        .map(|event| event.outcome)
        .collect::<Vec<_>>();
    assert!(outcomes.contains(&AuditOutcome::Failed));
    assert!(outcomes.contains(&AuditOutcome::Unknown));
    assert!(outcomes.contains(&AuditOutcome::Succeeded));
    assert_eq!(outcomes.len(), 3);
    let persisted = std::fs::read_to_string(store.path()).unwrap();
    for secret in ["private-body", "private-remark", "client-a", "group-a"] {
        assert!(!persisted.contains(secret));
    }
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}

#[test]
fn message_admitted_consume_survives_dropped_waiter_while_query_cancels() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("message-owner-test", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    let store = DesktopConfigStore::new(
        directory.path().join("config.json"),
        runtime.root_context().component("storage"),
    );
    let mutations = runtime.root_context().component("mutations");
    let backend = Arc::new(FakeMessageBackend::default());
    let services = AppServices::desktop_with_fake_provider(
        store.clone(),
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        mutations.clone(),
        runtime.root_context().component("history"),
        runtime.root_context().component("monitor"),
    )
    .with_message_backend(backend.clone());
    runtime.block_on(async {
        services.bootstrap().await.unwrap();
        backend.queue_query(Ok(empty()));let(started,_release,finished)=backend.hold_next_query();
        let mut query=Box::pin(services.query_messages(scope(),MessageSearchQuery::Key{topic:"orders".into(),key:"key".into()}));
        tokio::select!{result=&mut query=>panic!("query finished early: {result:?}"),result=started=>result.unwrap()}
        drop(query);finished.await.unwrap();
        backend.queue_consume(Ok(receipt(MessageConsumeCode::Success)));let(started,release,_finished)=backend.hold_next_consume();
        let mut consume=Box::pin(services.consume_message(scope(),command()));
        tokio::select!{result=&mut consume=>panic!("consume finished early: {result:?}"),result=started=>result.unwrap()}
        drop(consume);release.send(()).unwrap();
        let report=mutations.task_group().shutdown(Duration::from_secs(5)).await;assert!(report.is_healthy());
        assert!(services.audit_pending().unwrap().is_empty());
    });
    assert_eq!(backend.calls().consume.len(), 1);
    let persisted = std::fs::read_to_string(store.path()).unwrap();
    assert!(persisted.contains("message_consumed"));
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
