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

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_error::RocketMQResult;
use rocketmq_runtime::RuntimeContext;
use tokio::net::TcpListener;

use super::*;
use crate::connection::Connection;
use crate::request_processor::default_request_processor::DefaultRequestProcessor;
use crate::runtime::config::client_config::TransportClientConfig;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;

use self::runtime_test_support::test_service_context;

#[cfg(test)]
mod runtime_test_support {
    use super::*;

    pub(super) fn test_service_context(name: &'static str) -> ChildServiceContext {
        RuntimeContext::from_current(name).service_context("remoting-client-service")
    }
}

#[derive(Default)]
struct CountingHook {
    before_count: AtomicUsize,
    after_count: AtomicUsize,
    after_observed_before_field: AtomicBool,
}

impl RPCHook for CountingHook {
    fn do_before_request(&self, _remote_addr: SocketAddr, request: &mut RemotingCommand) -> RocketMQResult<()> {
        self.before_count.fetch_add(1, Ordering::SeqCst);
        request.ensure_ext_fields_initialized();
        request.add_ext_field("hooked", "true");
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> RocketMQResult<()> {
        self.after_count.fetch_add(1, Ordering::SeqCst);
        self.after_observed_before_field.store(
            request
                .ext_fields()
                .and_then(|fields| fields.get("hooked"))
                .is_some_and(|value| value == "true"),
            Ordering::SeqCst,
        );
        response.ensure_ext_fields_initialized();
        response.add_ext_field("afterHook", "true");
        Ok(())
    }
}

#[tokio::test]
async fn is_use_tls_reflects_client_config() {
    let config = TransportClientConfig {
        tls: TlsConfig {
            enable: true,
            ..TlsConfig::default()
        },
        ..Default::default()
    };
    let client = TransportClient::build_for_test(
        Arc::new(config),
        DefaultRequestProcessor,
        test_service_context("remoting-client-tls-test"),
    );

    assert!(client.is_use_tls());
    assert!(client.tls_config().enable);
}

#[tokio::test]
async fn start_tracks_background_tasks_with_task_group() {
    let config = TransportClientConfig {
        connect: ConnectConfig {
            timeout: Duration::from_millis(10),
        },
        maintenance: MaintenanceConfig {
            idle_scan_interval: Some(Duration::from_millis(10)),
        },
        ..Default::default()
    };
    let client = Arc::new(TransportClient::build_for_test(
        Arc::new(config),
        DefaultRequestProcessor,
        test_service_context("remoting-client-background-test"),
    ));
    client.start().await.expect("start client background tasks");

    let task_group = client
        .background_task_group
        .lock()
        .as_ref()
        .cloned()
        .expect("background task group");
    assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
    assert_eq!(task_group.task_count(), 2);

    let repeated = client.start().await.expect("repeat client start");
    assert!(repeated.already_running);
    let repeated_start_group = client
        .background_task_group
        .lock()
        .as_ref()
        .cloned()
        .expect("background task group after repeated start");
    assert_eq!(repeated_start_group.id(), task_group.id());
    assert_eq!(repeated_start_group.task_count(), 2);

    client.shutdown();

    assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::ShutdownCompleted);
}

#[test]
fn nameserver_scan_interval_is_independent_from_connect_timeout() {
    let config = TransportClientConfig {
        connect: ConnectConfig {
            timeout: Duration::from_millis(7),
        },
        ..Default::default()
    };

    assert_ne!(
        TransportClient::<DefaultRequestProcessor>::NAMESERVER_SCAN_INTERVAL,
        config.connect.timeout
    );
    assert_eq!(
        TransportClient::<DefaultRequestProcessor>::NAMESERVER_SCAN_INTERVAL,
        Duration::from_secs(30)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_nameserver_updates_publish_complete_owned_snapshots() {
    let client = Arc::new(TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("remoting-client-update-test"),
    ));
    let first = client.clone();
    let second = client.clone();

    let first_updates = tokio::spawn(async move {
        for _ in 0..64 {
            first.update_name_server_address_list_sync(vec!["ns-a:9876".into(), "ns-b:9876".into()]);
            tokio::task::yield_now().await;
        }
    });
    let second_updates = tokio::spawn(async move {
        for _ in 0..64 {
            second.update_name_server_address_list_sync(vec!["ns-c:9876".into(), "ns-d:9876".into()]);
            tokio::task::yield_now().await;
        }
    });

    for _ in 0..128 {
        let snapshot = client.get_name_server_address_list();
        assert!(snapshot.is_empty() || snapshot.len() == 2);
        if let Some(first) = snapshot.first() {
            let first_group = first.as_str().starts_with("ns-a") || first.as_str().starts_with("ns-b");
            assert!(snapshot.iter().all(|address| {
                let address = address.as_str();
                if first_group {
                    address.starts_with("ns-a") || address.starts_with("ns-b")
                } else {
                    address.starts_with("ns-c") || address.starts_with("ns-d")
                }
            }));
        }
        tokio::task::yield_now().await;
    }

    first_updates.await.expect("first updater should finish");
    second_updates.await.expect("second updater should finish");
    assert_eq!(client.get_name_server_address_list().len(), 2);
    client.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_endpoint_readers_observe_one_generation() {
    let client = Arc::new(TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("endpoint-generation-reader-test"),
    ));
    let endpoint_a = NameServerEndpoint::legacy("ns-a:9876").expect("valid nameserver");
    let identity_a = endpoint_a.identity().clone();
    client.apply_name_server_endpoint_snapshot_sync(vec![endpoint_a.clone()], Duration::ZERO);
    let first_lease = client
        .endpoint_state
        .load()
        .lease_for(&identity_a)
        .expect("initial endpoint lease");
    assert!(client
        .endpoint_state
        .update_availability(&first_lease, &identity_a, true));
    assert!(client.endpoint_state.set_chosen(&first_lease));

    let updater = Arc::clone(&client);
    let updates = tokio::spawn(async move {
        for _ in 0..64 {
            updater.apply_name_server_endpoint_snapshot_sync(
                vec![
                    endpoint_a.clone(),
                    NameServerEndpoint::legacy("ns-b:9876").expect("valid nameserver"),
                ],
                Duration::ZERO,
            );
            tokio::task::yield_now().await;
            updater.apply_name_server_endpoint_snapshot_sync(
                vec![
                    endpoint_a.clone(),
                    NameServerEndpoint::legacy("ns-c:9876").expect("valid nameserver"),
                ],
                Duration::ZERO,
            );
            tokio::task::yield_now().await;
        }
    });

    for _ in 0..128 {
        let state = client.endpoint_state.load();
        let observed_lease = state.lease_for(&identity_a).expect("configured endpoint lease");
        assert!(observed_lease.same_generation(&first_lease));
        assert!(state.available().contains(&identity_a));
        assert_eq!(state.chosen(), Some(&identity_a));
        assert!((1..=2).contains(&state.endpoints().len()));
        assert!(state
            .endpoints()
            .iter()
            .all(|endpoint| state.lease_for(endpoint.identity()).is_some()));
        tokio::task::yield_now().await;
    }

    updates.await.expect("endpoint updates should complete");
    client.shutdown();
}

#[tokio::test]
async fn stale_failure_cannot_clear_newer_nameserver_choice() {
    let client = Arc::new(TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("stale-chosen-clear-test"),
    ));
    let endpoint_a = NameServerEndpoint::legacy("ns-a:9876").expect("valid nameserver");
    let endpoint_b = NameServerEndpoint::legacy("ns-b:9876").expect("valid nameserver");
    let endpoint_c = NameServerEndpoint::legacy("ns-c:9876").expect("valid nameserver");
    let identity_a = endpoint_a.identity().clone();
    let identity_c = endpoint_c.identity().clone();

    client.apply_name_server_endpoint_snapshot_sync(vec![endpoint_a.clone(), endpoint_b.clone()], Duration::ZERO);
    let stale_a_lease = client.endpoint_state.load().lease_for(&identity_a).expect("S1 A lease");
    assert!(client.endpoint_state.set_chosen(&stale_a_lease));

    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let entered_wait = entered.notified();
    tokio::pin!(entered_wait);
    entered_wait.as_mut().enable();
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let stale_store = Arc::clone(&client.endpoint_state);
    let entered_for_task = Arc::clone(&entered);
    let resume_for_task = Arc::clone(&resume);
    let stale_task = tokio::spawn(async move {
        entered_for_task.notify_one();
        resume_for_task.notified().await;
        let _ = result_tx.send(stale_store.clear_chosen_if_matches(&stale_a_lease));
    });
    entered_wait.await;

    client.apply_name_server_endpoint_snapshot_sync(vec![endpoint_a, endpoint_b, endpoint_c], Duration::ZERO);
    let c_lease = client.endpoint_state.load().lease_for(&identity_c).expect("S2 C lease");
    assert!(client.endpoint_state.set_chosen(&c_lease));
    resume.notify_one();

    assert!(!result_rx.await.expect("stale clear result"));
    stale_task.await.expect("stale failure task");
    let state = client.endpoint_state.load();
    assert_eq!(state.chosen(), Some(&identity_c));
    assert!(state
        .lease_for(&identity_c)
        .is_some_and(|lease| lease.same_generation(&c_lease)));
    client.shutdown();
}

#[tokio::test]
async fn stale_probe_result_cannot_update_replaced_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (release_server, server_release) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.expect("accept stale probe connection");
        server_release.await.expect("release probe server");
    });
    let client = Arc::new(TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("stale-probe-generation-test"),
    ));
    let endpoint = NameServerEndpoint::legacy(addr.to_string()).expect("valid nameserver");
    let identity = endpoint.identity().clone();
    client.apply_name_server_endpoint_snapshot_sync(vec![endpoint.clone()], Duration::ZERO);
    let stale_lease = client
        .endpoint_state
        .load()
        .lease_for(&identity)
        .expect("initial endpoint lease");
    let hook = EndpointCompletionTestHook::new();
    client.install_connect_completion_test_hook(hook.clone());
    let scan_client = Arc::clone(&client);
    let scan = tokio::spawn(async move {
        scan_client.scan_available_name_srv().await;
    });
    hook.wait_until_entered().await;

    client.apply_name_server_endpoint_snapshot_sync(Vec::new(), Duration::ZERO);
    client.apply_name_server_endpoint_snapshot_sync(vec![endpoint.clone()], Duration::ZERO);
    let readded_lease = client
        .endpoint_state
        .load()
        .lease_for(&identity)
        .expect("re-added endpoint lease");
    assert!(!readded_lease.same_generation(&stale_lease));
    client
        .nameserver_health
        .connection_admission_if_current(&identity, &readded_lease, || {
            client.endpoint_state.is_current(&readded_lease)
        });
    assert!(client
        .nameserver_health
        .owns_latency_for_test(&identity, &readded_lease));
    hook.release();
    scan.await.expect("stale scan should finish");

    assert!(!client.endpoint_state.load().available().contains(&identity));
    assert!(client
        .nameserver_health
        .owns_latency_for_test(&identity, &readded_lease));
    client.shutdown();
    release_server.send(()).expect("release probe server");
    server.await.expect("probe server task");
}

#[tokio::test]
async fn stale_connect_completion_cannot_register_into_new_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.expect("accept client");
        let _ = release_rx.await;
    });
    let client = Arc::new(TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("stale-connect-generation-test"),
    ));
    let endpoint = NameServerEndpoint::legacy(addr.to_string()).expect("valid nameserver");
    let identity = endpoint.identity().clone();
    client.apply_name_server_endpoint_snapshot_sync(vec![endpoint.clone()], Duration::ZERO);
    let stale_lease = client
        .endpoint_state
        .load()
        .lease_for(&identity)
        .expect("initial endpoint lease");
    let hook = EndpointCompletionTestHook::new();
    client.install_connect_completion_test_hook(hook.clone());
    let connect_client = Arc::clone(&client);
    let connect_identity = identity.clone();
    let connect_endpoint = endpoint.clone();
    let connect_lease = stale_lease.clone();
    let connect = tokio::spawn(async move {
        connect_client
            .create_client_for_nameserver_until(
                &connect_identity,
                connect_endpoint,
                connect_lease,
                RequestDeadline::after(Duration::from_secs(1)),
            )
            .await
    });
    hook.wait_until_entered().await;

    client.apply_name_server_endpoint_snapshot_sync(Vec::new(), Duration::ZERO);
    client.apply_name_server_endpoint_snapshot_sync(vec![endpoint], Duration::ZERO);
    let readded_lease = client
        .endpoint_state
        .load()
        .lease_for(&identity)
        .expect("re-added endpoint lease");
    hook.release();

    assert!(!client.can_commit_endpoint_lease(Some(&stale_lease)));
    assert!(connect
        .await
        .expect("connect flight task")
        .expect("stale connection completion")
        .is_none());
    assert!(!client
        .connection_registry
        .has_session_for_lease(&identity, &stale_lease));
    assert!(!client
        .connection_registry
        .has_session_for_lease(&identity, &readded_lease));
    client.shutdown();
    release_tx.send(()).expect("release server");
    server.await.expect("server task");
}

#[tokio::test]
async fn retired_generation_cleanup_preserves_readded_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (release_server, server_release) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.expect("accept nameserver session");
        server_release.await.expect("release retirement server");
    });
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("retired-generation-cleanup-test"),
    );
    let endpoint = NameServerEndpoint::legacy(addr.to_string()).expect("valid nameserver");
    let identity = endpoint.identity().clone();
    client.apply_name_server_endpoint_snapshot_sync(vec![endpoint.clone()], Duration::ZERO);
    let first_lease = client
        .endpoint_state
        .load()
        .lease_for(&identity)
        .expect("initial endpoint lease");
    let session = client
        .create_client_for_nameserver_until(
            &identity,
            endpoint.clone(),
            first_lease.clone(),
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("connect initial nameserver")
        .expect("initial nameserver session");
    assert!(client
        .connection_registry
        .has_session_for_lease(&identity, &first_lease));
    assert!(client.nameserver_health.owns_latency_for_test(&identity, &first_lease));
    drop(session);

    client.apply_name_server_endpoint_snapshot_sync(
        vec![
            endpoint.clone(),
            NameServerEndpoint::legacy("ns-b:9876").expect("valid nameserver"),
        ],
        Duration::ZERO,
    );
    let retained_lease = client
        .endpoint_state
        .load()
        .lease_for(&identity)
        .expect("retained endpoint lease");
    assert!(retained_lease.same_generation(&first_lease));

    client.apply_name_server_endpoint_snapshot_sync(
        vec![NameServerEndpoint::legacy("ns-b:9876").expect("valid nameserver")],
        Duration::ZERO,
    );
    assert!(!client
        .connection_registry
        .has_session_for_lease(&identity, &first_lease));
    assert!(!client.nameserver_health.owns_latency_for_test(&identity, &first_lease));
    client.shutdown();
    release_server.send(()).expect("release retirement server");
    server.await.expect("retirement server task");
}

#[tokio::test]
async fn direct_broker_session_survives_nameserver_retirement_at_same_address() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (release_server, server_release) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (_direct_socket, _) = listener.accept().await.expect("accept direct broker session");
        let (_nameserver_socket, _) = listener.accept().await.expect("accept nameserver session");
        server_release.await.expect("release shared-address server");
    });
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("direct-and-nameserver-scope-test"),
    );
    let endpoint = NameServerEndpoint::legacy(addr.to_string()).expect("valid nameserver");
    let identity = endpoint.identity().clone();
    let direct_session = client
        .create_client(&identity, Duration::from_secs(1))
        .await
        .expect("direct broker session");

    client.apply_name_server_endpoint_snapshot_sync(vec![endpoint.clone()], Duration::ZERO);
    let nameserver_lease = client
        .endpoint_state
        .load()
        .lease_for(&identity)
        .expect("nameserver endpoint lease");
    let nameserver_session = client
        .create_client_for_nameserver_until(
            &identity,
            endpoint,
            nameserver_lease.clone(),
            RequestDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("connect nameserver session")
        .expect("nameserver session");
    assert!(client.connection_registry.healthy_session(&identity, None).is_some());
    assert!(client
        .connection_registry
        .has_session_for_lease(&identity, &nameserver_lease));
    drop(nameserver_session);

    client.apply_name_server_endpoint_snapshot_sync(Vec::new(), Duration::ZERO);
    assert!(direct_session.connection().is_healthy());
    assert!(client.connection_registry.healthy_session(&identity, None).is_some());
    assert!(!client
        .connection_registry
        .has_session_for_lease(&identity, &nameserver_lease));

    drop(direct_session);
    client.shutdown();
    release_server.send(()).expect("release shared-address server");
    server.await.expect("shared-address server task");
}

#[tokio::test]
async fn service_context_parents_background_and_worker_tasks() {
    let context = RuntimeContext::from_current("remoting-default-client-parent-test");
    let service = context.service_context("remoting-client-service");
    let config = TransportClientConfig {
        connect: ConnectConfig {
            timeout: Duration::from_millis(10),
        },
        maintenance: MaintenanceConfig {
            idle_scan_interval: Some(Duration::from_millis(10)),
        },
        ..Default::default()
    };
    let client = Arc::new(TransportClient::build_for_test(
        Arc::new(config),
        DefaultRequestProcessor,
        service.clone(),
    ));
    client.start().await.expect("start client background tasks");
    client
        .spawn_worker_task("remoting.client.parent-test-worker", async {})
        .expect("worker task should spawn");

    let background_task_group = client
        .background_task_group
        .lock()
        .as_ref()
        .cloned()
        .expect("background task group");
    let worker_task_group = client
        .worker_task_group
        .lock()
        .as_ref()
        .cloned()
        .expect("worker task group");

    assert_eq!(background_task_group.parent_id(), Some(service.task_group().id()));
    assert_eq!(worker_task_group.parent_id(), Some(service.task_group().id()));

    client.shutdown();
    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn invoke_request_runs_outbound_rpc_hooks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");

    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut connection = Connection::new(socket);
        let request = connection
            .receive_command()
            .await
            .expect("request frame")
            .expect("request command");
        let hooked = request
            .ext_fields()
            .and_then(|fields| fields.get("hooked"))
            .map(|value| value.as_str());
        assert_eq!(hooked, Some("true"));

        let mut response = RemotingCommand::create_response_command_with_code(ResponseCode::Success);
        response.set_opaque_mut(request.opaque());
        connection.send_command(response).await.expect("send response");
    });

    let hook = Arc::new(CountingHook::default());
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("remoting-client-hook-test"),
    );
    client.register_rpc_hook(hook.clone());

    let target = CheetahString::from_string(addr.to_string());
    let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo);
    let response = client
        .invoke_request(Some(&target), request, 3_000)
        .await
        .expect("invoke request");

    assert_eq!(hook.before_count.load(Ordering::SeqCst), 1);
    assert_eq!(hook.after_count.load(Ordering::SeqCst), 1);
    assert!(hook.after_observed_before_field.load(Ordering::SeqCst));
    assert_eq!(
        response
            .ext_fields()
            .and_then(|fields| fields.get("afterHook"))
            .map(|value| value.as_str()),
        Some("true")
    );

    server.await.expect("server task");
    client.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_endpoint_burst_uses_one_lifecycle_owned_connect_flight() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut connection = Connection::new(socket);
        for _ in 0..32 {
            let request = connection
                .receive_command()
                .await
                .expect("request frame")
                .expect("request command");
            let mut response = RemotingCommand::create_response_command_with_code(ResponseCode::Success);
            response.set_opaque_mut(request.opaque());
            connection.send_command(response).await.expect("send response");
        }
    });
    let client = Arc::new(TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("remoting-client-singleflight-test"),
    ));
    let target = CheetahString::from_string(addr.to_string());
    let requests = (0..32).map(|opaque| {
        let client = client.clone();
        let target = target.clone();
        async move {
            client
                .invoke_request(
                    Some(&target),
                    RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo).set_opaque(opaque),
                    3_000,
                )
                .await
        }
    });

    let responses = tokio::time::timeout(Duration::from_secs(5), futures::future::join_all(requests))
        .await
        .expect("burst deadline");
    assert!(responses.into_iter().all(|response| response.is_ok()));
    assert_eq!(client.connect_attempts.load(Ordering::Relaxed), 1);
    assert_eq!(client.connection_registry.len(), 1);

    server.await.expect("server task");
    client.shutdown();
}

#[tokio::test]
async fn timed_out_request_retires_the_cached_session_before_the_next_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        let (first_socket, _) = listener.accept().await.expect("accept first client");
        let mut first = Connection::new(first_socket);
        let _ = first
            .receive_command()
            .await
            .expect("first request frame")
            .expect("first request");

        let (second_socket, _) = time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("timeout must force a reconnect")
            .expect("accept replacement client");
        let mut second = Connection::new(second_socket);
        let request = second
            .receive_command()
            .await
            .expect("replacement request frame")
            .expect("replacement request");
        second
            .send_command(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()),
            )
            .await
            .expect("send replacement response");
    });

    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("remoting-client-timeout-test"),
    );
    let target = CheetahString::from_string(addr.to_string());
    assert!(client
        .invoke_request(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            30,
        )
        .await
        .is_err());

    let response = client
        .invoke_request(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            500,
        )
        .await
        .expect("next request must use a new owner and connection");
    assert_eq!(response.code(), ResponseCode::Success.to_i32());

    server.await.expect("server task");
    client.shutdown();
}

#[tokio::test]
async fn registry_token_distinguishes_replacements_and_same_port_nameserver_identities() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.expect("accept first client");
        let (second, _) = listener.accept().await.expect("accept replacement client");
        let _connections = (first, second);
        let _ = release_rx.await;
    });

    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("remoting-client-registry-token-test"),
    );
    let target = CheetahString::from_string(addr.to_string());
    let first = client
        .create_client(&target, Duration::from_secs(1))
        .await
        .expect("first client");
    client.connection_registry.remove_session_by_identity(&target);
    let replacement = client
        .create_client(&target, Duration::from_secs(1))
        .await
        .expect("replacement client");
    client.connection_registry.remove_session_by_identity(&target);

    let first_identity = CheetahString::from_static_str("nameserver-a:9876");
    let replacement_identity = CheetahString::from_static_str("nameserver-b:9876");
    client
        .connection_registry
        .insert_session(first_identity.clone(), first.clone(), None, || true);
    client
        .connection_registry
        .insert_session(replacement_identity.clone(), replacement.clone(), None, || true);

    assert_eq!(client.session_cache_identity(None, &first), first_identity);
    assert_eq!(client.session_cache_identity(None, &replacement), replacement_identity);
    assert!(!client.remove_cached_session_if_matches(&replacement_identity, &first));
    assert!(client.connection_registry.contains(&replacement_identity));
    assert!(client.remove_cached_session_if_matches(&first_identity, &first));
    assert!(client.connection_registry.contains(&replacement_identity));

    client.shutdown();
    let _ = release_tx.send(());
    server.await.expect("server task");
}

#[tokio::test]
async fn invoke_oneway_waits_for_writer_completion() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();

    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut connection = Connection::new(socket);
        let request = time::timeout(Duration::from_secs(3), connection.receive_command())
            .await
            .expect("oneway request should arrive")
            .expect("request frame")
            .expect("request command");
        let hooked = request
            .ext_fields()
            .and_then(|fields| fields.get("hooked"))
            .map(|value| value.as_str() == "true")
            .unwrap_or(false);

        let _ = received_tx.send((request.code(), request.is_oneway_rpc(), hooked));
    });

    let hook = Arc::new(CountingHook::default());
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("remoting-client-oneway-test"),
    );
    client.register_rpc_hook(hook.clone());

    let target = CheetahString::from_string(addr.to_string());
    let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo);
    client
        .invoke_request_oneway(&target, request, 3_000)
        .await
        .expect("one-way send should complete");

    let (code, is_oneway, hooked) = time::timeout(Duration::from_secs(3), received_rx)
        .await
        .expect("server should receive oneway request")
        .expect("server should report received request");

    assert_eq!(code, RequestCode::GetBrokerClusterInfo.to_i32());
    assert!(is_oneway);
    assert!(hooked);
    assert_eq!(hook.before_count.load(Ordering::SeqCst), 1);
    assert_eq!(hook.after_count.load(Ordering::SeqCst), 0);

    server.await.expect("server task");
    client.shutdown();
}

#[tokio::test]
async fn explicit_broker_requests_do_not_update_nameserver_latency() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut connection = Connection::new(socket);
        let request = connection
            .receive_command()
            .await
            .expect("request frame")
            .expect("request command");
        connection
            .send_command(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()),
            )
            .await
            .expect("send response");
        connection
            .receive_command()
            .await
            .expect("oneway frame")
            .expect("oneway command");
    });
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("explicit-broker-latency-test"),
    );
    let target = CheetahString::from_string(addr.to_string());

    client
        .invoke_request(
            Some(&target),
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            3_000,
        )
        .await
        .expect("explicit Broker request");
    client
        .invoke_request_oneway(
            &target,
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            3_000,
        )
        .await
        .expect("explicit Broker oneway");

    assert_eq!(client.nameserver_health.latency_p99_for_test(&target), None);
    assert_eq!(client.nameserver_health.latency_error_count_for_test(&target), 0);
    server.await.expect("server task");
    client.shutdown();
}

#[tokio::test]
async fn nameserver_request_updates_latency_and_failover_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut connection = Connection::new(socket);
        let request = connection
            .receive_command()
            .await
            .expect("request frame")
            .expect("request command");
        connection
            .send_command(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()),
            )
            .await
            .expect("send response");
    });
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("nameserver-latency-test"),
    );
    let target = CheetahString::from_string(addr.to_string());
    client.update_name_server_address_list_sync(vec![target.clone()]);

    client
        .invoke_request(
            None,
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            3_000,
        )
        .await
        .expect("NameServer request");

    assert!(client.nameserver_health.latency_p99_for_test(&target).is_some());
    assert_eq!(client.nameserver_health.latency_error_count_for_test(&target), 0);
    server.await.expect("server task");
    client.shutdown();
}

#[tokio::test]
async fn unchanged_resolved_endpoint_reuses_connection_and_identity_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let socket_addr = listener.local_addr().expect("listener addr");
    let (release_server, server_release) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut connection = Connection::new(socket);
        for _ in 0..2 {
            let request = connection
                .receive_command()
                .await
                .expect("request frame")
                .expect("request command");
            connection
                .send_command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque()),
                )
                .await
                .expect("send response");
        }
        server_release.await.expect("release server connection");
    });
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("nameserver-unchanged-endpoint-test"),
    );
    let target = ConnectTarget::new(socket_addr, "namesrv.default.svc:9876").unwrap();
    let identity = target.identity();
    client.update_name_server_connect_targets_sync(vec![target.clone()], Duration::from_secs(1));

    for _ in 0..2 {
        client
            .invoke_request(
                None,
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                3_000,
            )
            .await
            .expect("NameServer request");
        client.update_name_server_connect_targets_sync(vec![target.clone()], Duration::from_secs(1));
    }

    assert_eq!(client.connect_attempts.load(Ordering::Relaxed), 1);
    assert!(client.connection_registry.contains(&identity));
    assert!(client.nameserver_health.latency_p99_for_test(&identity).is_some());
    let snapshot = client.snapshot();
    assert_eq!(snapshot.healthy_name_server_count, 1);
    assert_eq!(snapshot.probing_name_server_count, 0);
    assert_eq!(snapshot.draining_name_server_count, 0);
    assert_eq!(snapshot.circuit_open_name_server_count, 0);
    release_server.send(()).expect("release server");
    server.await.expect("server task");
    client.shutdown();
}

#[tokio::test]
async fn same_socket_with_new_authority_does_not_reuse_the_old_session() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let socket_addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut connection = Connection::new(socket);
            let request = connection
                .receive_command()
                .await
                .expect("request frame")
                .expect("request command");
            connection
                .send_command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque()),
                )
                .await
                .expect("send response");
        }
    });
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("nameserver-authority-isolation-test"),
    );
    let first = ConnectTarget::new(socket_addr, "namesrv-a.default.svc:9876").unwrap();
    let second = ConnectTarget::new(socket_addr, "namesrv-b.default.svc:9876").unwrap();
    client.update_name_server_connect_targets_sync(vec![first.clone()], Duration::from_secs(1));
    client
        .invoke_request(
            None,
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            3_000,
        )
        .await
        .expect("first authority request");

    client.update_name_server_connect_targets_sync(vec![second.clone()], Duration::from_secs(1));
    client
        .invoke_request(
            None,
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            3_000,
        )
        .await
        .expect("second authority request");

    assert_eq!(client.connect_attempts.load(Ordering::Relaxed), 2);
    assert!(!client.connection_registry.contains(&first.identity()));
    assert!(client.connection_registry.contains(&second.identity()));
    assert!(client
        .nameserver_health
        .latency_p99_for_test(&second.identity())
        .is_some());
    server.await.expect("server task");
    client.shutdown();
}

#[tokio::test]
async fn removed_endpoint_rejects_new_work_and_closes_after_drain_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let socket_addr = listener.local_addr().expect("listener addr");
    let (request_received, request_started) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut connection = Connection::new(socket);
        connection
            .receive_command()
            .await
            .expect("request frame")
            .expect("request command");
        request_received.send(()).expect("signal request");
        let trailing = time::timeout(Duration::from_secs(1), connection.receive_command())
            .await
            .expect("drain timeout should close the socket");
        assert!(trailing.is_none(), "drained connection should reach EOF");
    });
    let client = Arc::new(TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("nameserver-drain-timeout-test"),
    ));
    let target = ConnectTarget::new(socket_addr, "namesrv.default.svc:9876").unwrap();
    let identity = target.identity();
    client.update_name_server_connect_targets_sync(vec![target], Duration::from_millis(25));
    let request_client = client.clone();
    let request = tokio::spawn(async move {
        request_client
            .invoke_request(
                None,
                RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
                3_000,
            )
            .await
    });
    request_started.await.expect("request should reach server");

    client.update_name_server_connect_targets_sync(Vec::new(), Duration::from_millis(25));

    assert!(client.get_name_server_address_list().is_empty());
    assert!(!client.connection_registry.contains(&identity));
    assert!(time::timeout(Duration::from_secs(1), request)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    server.await.expect("server task");
    let report = client.shutdown_with_report(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{report:?}");
}

#[tokio::test]
async fn nameserver_connect_failure_tries_next_candidate_within_deadline() {
    let closed_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind closed endpoint");
    let closed_addr = closed_listener.local_addr().expect("closed endpoint address");
    drop(closed_listener);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind healthy endpoint");
    let healthy_addr = listener.local_addr().expect("healthy endpoint address");
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept fallback client");
        let mut connection = Connection::new(socket);
        let request = connection
            .receive_command()
            .await
            .expect("request frame")
            .expect("request command");
        connection
            .send_command(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque()),
            )
            .await
            .expect("send response");
    });

    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("nameserver-connect-failover-test"),
    );
    client.apply_name_server_endpoint_snapshot_sync(
        vec![
            NameServerEndpoint::legacy(closed_addr.to_string()).unwrap(),
            NameServerEndpoint::legacy(healthy_addr.to_string()).unwrap(),
        ],
        Duration::from_secs(30),
    );

    let response = client
        .invoke_request(
            None,
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            3_000,
        )
        .await
        .expect("second NameServer should satisfy the request");

    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    server.await.expect("fallback server task");
    client.shutdown();
}

#[tokio::test]
async fn request_failure_after_write_does_not_retry_another_nameserver() {
    let first_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind first endpoint");
    let first_addr = first_listener.local_addr().expect("first endpoint address");
    let second_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind second endpoint");
    let second_addr = second_listener.local_addr().expect("second endpoint address");
    let first_server = tokio::spawn(async move {
        let (socket, _) = first_listener.accept().await.expect("accept first client");
        let mut connection = Connection::new(socket);
        connection
            .receive_command()
            .await
            .expect("request frame")
            .expect("request command");
    });

    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("nameserver-no-write-retry-test"),
    );
    client.apply_name_server_endpoint_snapshot_sync(
        vec![
            NameServerEndpoint::legacy(first_addr.to_string()).unwrap(),
            NameServerEndpoint::legacy(second_addr.to_string()).unwrap(),
        ],
        Duration::from_secs(30),
    );

    assert!(client
        .invoke_request(
            None,
            RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo),
            1_000,
        )
        .await
        .is_err());
    assert!(
        time::timeout(Duration::from_millis(100), second_listener.accept())
            .await
            .is_err(),
        "the second NameServer must not receive a replay after request bytes were written"
    );

    first_server.await.expect("first server task");
    client.shutdown();
}

#[tokio::test]
async fn shutdown_with_report_closes_connection_table_clients() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.expect("accept client");
        time::sleep(Duration::from_secs(5)).await;
    });
    let client = TransportClient::build_for_test(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        test_service_context("remoting-client-shutdown-test"),
    );

    let target = CheetahString::from_string(addr.to_string());
    let created = client
        .create_client(&target, Duration::from_secs(3))
        .await
        .expect("client connection should be created");
    drop(created);
    assert_eq!(client.connection_registry.len(), 1);

    let report = client.shutdown_with_report(Duration::from_secs(1)).await;

    assert!(report.is_healthy(), "{report:?}");
    assert_eq!(report.connections.len(), 1);
    assert_eq!(report.connections[0].addr, target);
    assert!(client.connection_registry.is_empty());
    server.abort();
}

#[tokio::test]
async fn idle_scan_evicts_an_expired_persistent_session() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.expect("accept client");
        time::sleep(Duration::from_secs(5)).await;
    });
    let config = TransportClientConfig {
        maintenance: MaintenanceConfig {
            idle_scan_interval: Some(Duration::from_millis(1)),
        },
        ..TransportClientConfig::default()
    };
    let client = TransportClient::build_for_test(
        Arc::new(config),
        DefaultRequestProcessor,
        test_service_context("remoting-client-idle-eviction-test"),
    );
    let target = CheetahString::from_string(addr.to_string());
    let created = client
        .create_client(&target, Duration::from_secs(3))
        .await
        .expect("client connection should be created");
    created.set_last_used_millis_for_test(0);
    drop(created);

    client.scan_idle_connections();

    assert!(client.connection_registry.is_empty());
    client.shutdown();
    server.abort();
}
