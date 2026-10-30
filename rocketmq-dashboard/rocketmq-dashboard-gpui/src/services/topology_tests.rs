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
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};
fn runtime() -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig::for_parallelism("topology-test", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap()
}
#[tokio::test]
async fn topology_independent_failures_keep_other_layers_and_safe_codes_without_false_health() {
    let backend = Arc::new(FakeTopologyBackend::default());
    let services = AppServices::default().with_topology_backend(backend.clone());
    let context = services.topology_context();
    let first = services.topology_snapshot(context.clone()).await.unwrap();
    let topic_time = first.layers[&TopologySource::Topics].observed_at_ms;
    let mut cache = TopologyCache::default();
    cache.apply(
        context.clone(),
        first.layers.into_values().collect(),
        first.generated_at_ms,
    );
    backend.fail(
        TopologySource::Topics,
        UiError::new("private failure detail", UiErrorCode::Connection, true),
    );
    let partial = services.topology_snapshot(context.clone()).await.unwrap();
    assert_eq!(
        partial.layers[&TopologySource::Topics].code,
        Some(TopologyFailureCode::Connection)
    );
    assert!(
        partial.layers[&TopologySource::Brokers]
            .nodes
            .iter()
            .any(|node| node.traffic.produce_tps == Some(0.5))
    );
    assert!(!format!("{partial:?}").contains("private failure detail"));
    cache.apply(
        context.clone(),
        partial.layers.into_values().collect(),
        partial.generated_at_ms,
    );
    assert_eq!(
        cache.snapshot().unwrap().layers[&TopologySource::Topics].observed_at_ms,
        topic_time
    );
    for source in TopologySource::ALL
        .into_iter()
        .filter(|source| !matches!(source, TopologySource::Config | TopologySource::Nameservers))
    {
        backend.fail(source, UiError::new("unavailable", UiErrorCode::Connection, true));
    }
    let failed = services.topology_snapshot(context.clone()).await.unwrap();
    assert_eq!(
        failed.layers[&TopologySource::Nameservers].state,
        TopologySourceState::Complete
    );
    assert!(failed.all_remote_failed());
    let diff = cache.apply(context, failed.layers.into_values().collect(), failed.generated_at_ms);
    assert!(diff.removed.is_empty());
    assert!(
        cache
            .snapshot()
            .unwrap()
            .nodes()
            .iter()
            .any(|node| matches!(node.id, TopologyNodeId::Broker(_)))
    );
}
#[tokio::test(start_paused = true)]
async fn topology_refresh_modes_wait_the_actual_interval_after_each_completed_read() {
    for (mode, seconds) in [
        (TopologyRefresh::FiveSeconds, 5),
        (TopologyRefresh::TenSeconds, 10),
        (TopologyRefresh::ThirtySeconds, 30),
    ] {
        let backend = Arc::new(FakeTopologyBackend::default());
        let services = AppServices::default().with_topology_backend(backend.clone());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let context = services.topology_context();
        let mut watching = Box::pin(topology_loop(services, context, mode, sender));
        tokio::select! { result = &mut watching => panic!("loop ended: {result:?}"), result = receiver.recv() => { result.unwrap().unwrap(); } }
        tokio::time::advance(Duration::from_secs(seconds - 1)).await;
        tokio::select! { biased; result = &mut watching => panic!("loop ended: {result:?}"), _ = std::future::ready(()) => {} }
        assert_eq!(
            backend
                .calls()
                .iter()
                .filter(|request| request.source == TopologySource::Brokers)
                .count(),
            1
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::select! { result = &mut watching => panic!("loop ended: {result:?}"), result = receiver.recv() => { result.unwrap().unwrap(); } }
        assert_eq!(
            backend
                .calls()
                .iter()
                .filter(|request| request.source == TopologySource::Brokers)
                .count(),
            2
        );
        drop(watching);
    }
}
#[test]
fn topology_page_receiver_drop_cancels_inflight_reads_and_root_shutdown_awaits_the_owned_loop() {
    let runtime = runtime();
    let backend = Arc::new(FakeTopologyBackend::default());
    let services = AppServices::default()
        .with_topology_backend(backend.clone())
        .with_topology_runtime(runtime.root_context().component("application"));
    runtime.block_on(async {
        let (started, _release, finished) = backend.hold(TopologySource::Brokers);
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let mut watching = Box::pin(services.watch_topology(services.topology_context(), TopologyRefresh::FiveSeconds, sender));
        tokio::select! { result = &mut watching => panic!("loop ended before release: {result:?}"), result = started => result.unwrap() }
        drop(watching); finished.await.unwrap();
        let (started, _release, finished) = backend.hold(TopologySource::Brokers);
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let mut watching = Box::pin(services.watch_topology(services.topology_context(), TopologyRefresh::TenSeconds, sender));
        tokio::select! { result = &mut watching => panic!("loop ended before release: {result:?}"), result = started => result.unwrap() }
        assert!(services.runtime_bridge.as_ref().unwrap().context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
        finished.await.unwrap(); assert!(watching.await.is_err());
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
#[test]
fn topology_captures_environment_and_revision_and_never_falls_back_from_proxy_to_direct() {
    use crate::{
        infrastructure::{
            auth_state::{DesktopAuthState, MapEnvironment},
            config_store::DesktopConfigStore,
        },
        services::ConfigMutation,
    };
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeTopologyBackend::default());
    let services = AppServices::desktop_with_fake_provider(
        DesktopConfigStore::new(
            directory.path().join("config.json"),
            runtime.root_context().component("storage"),
        ),
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        runtime.root_context().component("application"),
        runtime.root_context().component("history"),
        runtime.root_context().component("monitor"),
    )
    .with_topology_backend(backend.clone());
    runtime.block_on(async {
        services.bootstrap().await.unwrap(); let original = services.topology_context();
        let (started, release, _) = backend.hold(TopologySource::Brokers); let mut read = Box::pin(services.topology_snapshot(original.clone()));
        tokio::select! { result = &mut read => panic!("read ended before release: {result:?}"), result = started => result.unwrap() }
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        services.mutate_with_progress(ConfigMutation::CreateEnvironment("other".into()), progress.clone()).await.unwrap();
        assert_ne!(services.topology_context().environment, original.environment); release.send(()).unwrap(); assert!(read.await.is_err());
        services.mutate_with_progress(ConfigMutation::AddProxy("proxy:8080".into()), progress.clone()).await.unwrap();
        services.mutate_with_progress(ConfigMutation::SwitchProxy("proxy:8080".into()), progress).await.unwrap();
        let calls = backend.calls().len(); let context = services.topology_context(); assert_eq!(context.scope, ConnectionScope::Proxy);
        let snapshot = services.topology_snapshot(context.clone()).await.unwrap();
        assert_eq!(snapshot.context, context); assert!(snapshot.all_remote_failed()); assert_eq!(backend.calls().len(), calls);
        assert!(snapshot.layers.values().filter(|layer| layer.source != TopologySource::Config).all(|layer| layer.code == Some(TopologyFailureCode::Unsupported)));
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
