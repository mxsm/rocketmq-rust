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

use super::{
    test_support::{FakeMessageBackend, row, trace},
    *,
};
use crate::infrastructure::{
    auth_state::{DesktopAuthState, MapEnvironment},
    config_store::DesktopConfigStore,
};
use rocketmq_dashboard_common::{DEFAULT_TRACE_TOPIC, MessageTraceQuery};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

fn scope() -> MessageRequestScope {
    MessageRequestScope { revision: 0, epoch: 1 }
}
fn query() -> MessageTraceQuery {
    MessageTraceQuery {
        identity: row().identity,
        trace_topic: DEFAULT_TRACE_TOPIC.into(),
    }
}

#[tokio::test]
async fn trace_service_validates_identity_and_trace_topic_and_never_requests_payload_or_mutation() {
    let backend = Arc::new(FakeMessageBackend::default());
    let services = AppServices::default().with_message_backend(backend.clone());
    let mut invalid = query();
    invalid.trace_topic.clear();
    assert_eq!(
        services.message_trace(scope(), invalid).await.unwrap_err().code(),
        UiErrorCode::Validation
    );
    assert!(backend.calls().trace.is_empty());
    let expected = trace(row().identity);
    backend.queue_trace(Ok(expected.clone()));
    assert_eq!(services.message_trace(scope(), query()).await.unwrap(), expected);
    let mut wrong_identity = expected;
    wrong_identity.identity.topic = "different".into();
    backend.queue_trace(Ok(wrong_identity));
    assert_eq!(
        services.message_trace(scope(), query()).await.unwrap_err().code(),
        UiErrorCode::Validation
    );
    assert!(backend.calls().payload.is_empty());
    assert!(backend.calls().consume.is_empty());
}

#[test]
fn trace_owned_read_cancels_when_the_gpui_waiter_drops_and_shutdown_drains_it() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("trace-owner", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    let store = DesktopConfigStore::new(
        directory.path().join("config.json"),
        runtime.root_context().component("storage"),
    );
    let services = AppServices::desktop_with_fake_provider(
        store,
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        runtime.root_context().component("mutations"),
        runtime.root_context().component("history"),
        runtime.root_context().component("monitor"),
    );
    let backend = Arc::new(FakeMessageBackend::default());
    let services = services.with_message_backend(backend.clone());
    runtime.block_on(async {
        services.bootstrap().await.unwrap();
        backend.queue_trace(Ok(trace(row().identity)));
        let (started, _release, finished) = backend.hold_next_trace();
        let mut waiter = Box::pin(services.message_trace(scope(), query()));
        tokio::select! {
            value = &mut waiter => panic!("Trace read completed before release: {value:?}"),
            result = started => result.unwrap(),
        }
        drop(waiter);
        finished.await.unwrap();
        assert!(services.audit_pending().unwrap().is_empty());
    });
    assert_eq!(backend.calls().trace.len(), 1);
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
