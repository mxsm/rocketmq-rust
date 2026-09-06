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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::net::SocketAddr;

use super::*;
use crate::metrics::RequestType as _;
use crate::typ::Node;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_protocol::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_protocol::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_protocol::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::{RemotingCommandDefaults, RemotingCommandFactory};
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_transport::api::OutboundRequestOutcome;

#[test]
fn controller_manager_does_not_log_the_full_configuration() {
    let sources = [
        include_str!("../controller_manager.rs"),
        include_str!("lifecycle.rs"),
        include_str!("leadership.rs"),
    ];
    let full_config_log = ["Creating controller manager with config: ", "{:?}"].concat();

    assert!(sources.iter().all(|source| !source.contains(&full_config_log)));
}

fn test_telemetry_handle() -> TelemetryHandle {
    TelemetryHandle::noop()
}

async fn wait_until<F>(timeout: Duration, mut predicate: F, context: &str)
where
    F: FnMut() -> bool,
{
    let start = current_millis();
    loop {
        if predicate() {
            return;
        }
        assert!(
            current_millis().saturating_sub(start) < timeout.as_millis() as u64,
            "timed out waiting for {context}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

fn test_broker_session(id: u64) -> crate::controller::broker_heartbeat_manager::BrokerSession {
    crate::controller::broker_heartbeat_manager::BrokerSession::for_test(id, Arc::new(AtomicBool::new(false)))
}

fn reserve_controller_addresses() -> (SocketAddr, SocketAddr) {
    let remoting = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve remoting address");
    let raft = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve raft address");
    let addresses = (
        remoting.local_addr().expect("remoting address"),
        raft.local_addr().expect("raft address"),
    );
    drop((remoting, raft));
    addresses
}

fn test_service_context() -> ChildServiceContext {
    rocketmq_runtime::RuntimeContext::from_current("controller-manager-test").service_context("controller-manager")
}

fn test_notify_task(broker_id: u64) -> NotifyTask {
    let state = NotifyState::try_new(
        1,
        rocketmq_store_api::MasterEpoch::try_from(1).expect("test master epoch"),
        rocketmq_store_api::SyncStateSetEpoch::try_from(1).expect("test sync-state-set epoch"),
        Some("127.0.0.1:10911".to_string()),
    )
    .expect("test notify state");
    NotifyTask::new(
        NotifyKey {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_id,
        },
        state,
        CheetahString::from_static_str("127.0.0.1:10911"),
        Some(CheetahString::from_static_str("127.0.0.1:10911")),
        Vec::new(),
    )
}

#[tokio::test]
async fn inactive_broker_worker_observes_manager_cancellation() {
    let task_group = test_service_context()
        .component("inactive-broker-worker-cancellation")
        .task_group()
        .clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let started_task = started.clone();

    spawn_inactive_broker_worker(&task_group, async move {
        started_task.notify_one();
        std::future::pending::<()>().await;
    })
    .expect("spawn inactive broker worker");
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("inactive broker worker should start");

    let report = task_group.shutdown(Duration::from_secs(1)).await;

    assert!(report.is_healthy(), "shutdown report: {}", report.to_json());
    assert_eq!(report.cancelled, 1);
    assert_eq!(task_group.task_count(), 0);
}

#[tokio::test]
async fn manager_retains_its_injected_remoting_command_factory() {
    let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(667, SerializeType::ROCKETMQ));
    let config = ControllerConfig::default().with_node_info(1, reserve_controller_addresses().0);

    let manager = ControllerManager::new_with_security_and_remoting_command_factory(
        config,
        test_service_context(),
        test_telemetry_handle(),
        None,
        factory,
    )
    .await
    .expect("create manager with explicit command factory");

    assert_eq!(manager.remoting_command_factory(), factory);
    let response = manager
        .controller()
        .get_controller_metadata()
        .await
        .expect("query unstarted Controller")
        .expect("unstarted Controller response");
    assert_eq!(response.version(), 667);
    assert_eq!(response.serialize_type(), SerializeType::ROCKETMQ);
}

#[tokio::test]
async fn request_processor_uses_its_manager_command_factory() {
    let binary_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(668, SerializeType::ROCKETMQ));
    let json_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(669, SerializeType::JSON));
    let binary_manager = Arc::new(
        ControllerManager::new_with_remoting_command_factory(
            ControllerConfig::default().with_node_info(1, reserve_controller_addresses().0),
            test_service_context(),
            test_telemetry_handle(),
            binary_factory,
        )
        .await
        .expect("create binary manager"),
    );
    let json_manager = Arc::new(
        ControllerManager::new_with_remoting_command_factory(
            ControllerConfig::default().with_node_info(2, reserve_controller_addresses().0),
            test_service_context(),
            test_telemetry_handle(),
            json_factory,
        )
        .await
        .expect("create JSON manager"),
    );

    async fn unsupported_response(manager: Arc<ControllerManager>) -> RemotingCommand {
        let processor = ControllerRequestProcessor::new(manager);
        let mut request = RemotingCommand::create_remoting_command(-12345);
        let dispatch = processor.handle_request(test_broker_session(1), "transport-session-1", &mut request);
        processor
            .complete_request(None, dispatch)
            .await
            .expect("unsupported request dispatch")
    }

    let binary = unsupported_response(binary_manager).await;
    let json = unsupported_response(json_manager).await;

    assert_eq!(binary.code(), ResponseCode::RequestCodeNotSupported as i32);
    assert_eq!(binary.version(), 668);
    assert_eq!(binary.serialize_type(), SerializeType::ROCKETMQ);
    assert_eq!(json.code(), ResponseCode::RequestCodeNotSupported as i32);
    assert_eq!(json.version(), 669);
    assert_eq!(json.serialize_type(), SerializeType::JSON);
}

#[tokio::test]
async fn broker_heartbeat_admission_controls_raft_dispatch_and_capacity_response() {
    let manager = Arc::new(
        ControllerManager::new(
            ControllerConfig::default().with_node_info(1, reserve_controller_addresses().0),
            test_service_context(),
            test_telemetry_handle(),
        )
        .await
        .expect("create manager"),
    );
    let processor = ControllerRequestProcessor::new(manager.clone());
    assert_eq!(
        manager.heartbeat_manager().on_broker_session_heartbeat(
            "test-cluster",
            "broker-a",
            "127.0.0.1:10911",
            1,
            Some(60_000),
            test_broker_session(2),
            Some(2),
            Some(20),
            Some(19),
            None,
        ),
        crate::controller::broker_heartbeat_manager::BrokerHeartbeatAdmission::Accepted
    );

    let heartbeat_header = |broker_name: &'static str, broker_id: i64| BrokerHeartbeatRequestHeader {
        cluster_name: CheetahString::from_static_str("test-cluster"),
        broker_addr: CheetahString::from_static_str("127.0.0.1:10911"),
        broker_name: CheetahString::from_static_str(broker_name),
        broker_id: Some(broker_id),
        epoch: Some(99),
        max_offset: Some(999),
        confirm_offset: Some(998),
        store_ready: Some(true),
        heartbeat_timeout_mills: Some(60_000),
        election_priority: None,
    };
    let mut superseded_request =
        RemotingCommand::create_request_command(RequestCode::BrokerHeartbeat, heartbeat_header("broker-a", 1));
    superseded_request.make_custom_header_to_net();
    let superseded = processor
        .handle_request(test_broker_session(1), "superseded-session", &mut superseded_request)
        .await
        .expect("dispatch superseded heartbeat")
        .expect("superseded heartbeat response");
    assert_eq!(superseded.code(), ResponseCode::Success as i32);
    assert_eq!(
        superseded.remark().map(|remark| remark.as_str()),
        Some("Heart beat ignored from superseded session")
    );

    manager.heartbeat_manager().saturate_session_high_water_for_test();
    let mut capacity_request =
        RemotingCommand::create_request_command(RequestCode::BrokerHeartbeat, heartbeat_header("capacity-broker", 2));
    capacity_request.make_custom_header_to_net();
    let capacity = processor
        .handle_request(test_broker_session(3), "capacity-session", &mut capacity_request)
        .await
        .expect("dispatch capacity-rejected heartbeat")
        .expect("capacity rejection response");
    assert_eq!(capacity.code(), ResponseCode::SystemBusy as i32);
    assert_eq!(
        capacity.remark().map(|remark| remark.as_str()),
        Some("Broker heartbeat session capacity exceeded")
    );
    assert!(manager
        .heartbeat_manager()
        .get_broker_live_info("test-cluster", "capacity-broker", 2)
        .is_none());

    std::mem::forget(manager);
}

#[tokio::test]
async fn enabled_security_requires_an_injected_adapter() {
    let mut config = ControllerConfig::default().with_node_info(1, reserve_controller_addresses().0);
    config.authentication_enabled = true;

    let error = match ControllerManager::new(config, test_service_context(), test_telemetry_handle()).await {
        Ok(_) => panic!("security-enabled Controller must fail closed without an adapter"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("no ControllerSecurity adapter was injected"));
}

#[tokio::test]
async fn concurrent_initialize_is_serialized_and_manager_handles_do_not_form_a_cycle() {
    let config = ControllerConfig::default().with_node_info(1, reserve_controller_addresses().0);
    let manager = Arc::new(
        ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("create manager"),
    );
    assert!(!manager.is_initialized());
    assert!(!manager.is_running());

    let (first, second) = tokio::join!(manager.initialize(), manager.initialize());
    let results = [first.expect("first initialize"), second.expect("second initialize")];
    assert_eq!(results.into_iter().filter(|initialized| *initialized).count(), 1);
    assert!(manager.is_initialized());
    assert!(!manager.is_running());
    let processor = Arc::new(ControllerRequestProcessor::new(manager.clone()));
    let processor_clone = processor.clone();
    assert_eq!(Arc::strong_count(&processor), 2);
    let weak_manager = Arc::downgrade(&manager);
    drop(manager);

    assert!(weak_manager.upgrade().is_none());
    drop(processor_clone);
    drop(processor);
}

#[tokio::test]
async fn concurrent_start_waits_for_the_single_lifecycle_transition() {
    let (remoting_addr, raft_addr) = reserve_controller_addresses();
    let config = ControllerConfig::default()
        .with_node_info(1, remoting_addr)
        .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
        .with_storage_backend(crate::config::StorageBackendType::Memory);
    let manager = Arc::new(
        ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("create manager"),
    );
    manager.initialize().await.expect("initialize manager");

    let (first, second) = tokio::join!(manager.start(), manager.start());
    first.expect("first start");
    second.expect("second start");
    assert!(manager.is_running());

    let endpoint = CheetahString::from_string(remoting_addr.to_string());
    let request = RemotingCommand::create_remoting_command(-12_345).set_opaque(73);
    let response = match manager
        .remoting_client
        .transport_client()
        .invoke_request(Some(&endpoint), request, 3_000)
        .await
    {
        Ok(OutboundRequestOutcome::Response(response)) => response,
        Ok(OutboundRequestOutcome::Rejected(rejection)) => {
            panic!("production V2 Controller request was rejected: {rejection:?}")
        }
        Ok(OutboundRequestOutcome::Contract(contract)) => {
            panic!("production V2 Controller request contract failed: {contract:?}")
        }
        Err(error) => panic!("production V2 Controller request failed: {error:?}"),
    };
    assert_eq!(response.code(), ResponseCode::RequestCodeNotSupported as i32);
    assert_eq!(response.opaque(), 73);
    assert!(response.body().is_none());

    manager.shutdown().await.expect("shutdown manager");
    assert!(!manager.is_running());
    let restart_error = manager
        .start()
        .await
        .expect_err("a stopped controller must not restart");
    assert_eq!(
        restart_error.to_string(),
        "Runtime error: Controller manager cannot be restarted after shutdown or a failed startup"
    );
    std::mem::forget(manager);
}

#[tokio::test]
async fn startup_failure_cleanup_stops_owned_components() {
    let (remoting_addr, raft_addr) = reserve_controller_addresses();
    let config = ControllerConfig::default()
        .with_node_info(1, remoting_addr)
        .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
        .with_storage_backend(crate::config::StorageBackendType::Memory);
    let manager = Arc::new(
        ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("create manager"),
    );
    manager.initialize().await.expect("initialize manager");
    manager.start().await.expect("start manager before simulated failure");
    assert!(manager.is_running());
    assert_eq!(manager.heartbeat_manager.scan_task_count(), 1);

    let _lifecycle_guard = manager.lifecycle_lock.lock().await;
    let error = manager
        .cleanup_after_start_failure(ControllerError::runtime_error(
            "simulated failure after component startup",
        ))
        .await;

    assert!(error.to_string().contains("simulated failure after component startup"));
    assert!(!manager.is_running());
    assert_eq!(manager.heartbeat_manager.scan_task_count(), 0);
    assert!(manager.manager_task_group.lock().is_none());
    drop(_lifecycle_guard);
    manager
        .shutdown()
        .await
        .expect("idempotent shutdown after startup cleanup");
}

#[tokio::test]
async fn occupied_remoting_listener_fails_startup_and_cleans_up_owned_components() {
    let occupied = std::net::TcpListener::bind("0.0.0.0:0").expect("occupy remoting listener");
    let remoting_addr = std::net::SocketAddr::from((
        std::net::Ipv4Addr::LOCALHOST,
        occupied.local_addr().expect("occupied remoting address").port(),
    ));
    let (_unused_remoting_addr, raft_addr) = reserve_controller_addresses();
    let config = ControllerConfig::default()
        .with_node_info(1, remoting_addr)
        .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
        .with_storage_backend(crate::config::StorageBackendType::Memory);
    let manager = Arc::new(
        ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("create manager"),
    );
    manager.initialize().await.expect("initialize manager");

    let error = manager
        .start()
        .await
        .expect_err("occupied remoting listener must fail startup");

    assert!(error.to_string().contains("Controller remoting server failed to start"));
    assert!(!manager.is_running());
    assert_eq!(manager.heartbeat_manager.scan_task_count(), 0);
    assert!(manager.manager_task_group.lock().is_none());
    assert!(manager.remoting_server_shutdown_tx.lock().is_none());
    drop(occupied);
    let restart_error = manager
        .start()
        .await
        .expect_err("a failed startup must not consume the released listener on retry");
    assert_eq!(
        restart_error.to_string(),
        "Runtime error: Controller manager cannot be restarted after shutdown or a failed startup"
    );
    assert!(!manager.is_running());
    manager
        .shutdown()
        .await
        .expect("shutdown remains idempotent after listener startup cleanup");
}

#[tokio::test]
async fn shutdown_before_start_does_not_consume_lifecycle() {
    let (remoting_addr, raft_addr) = reserve_controller_addresses();
    let config = ControllerConfig::default()
        .with_node_info(1, remoting_addr)
        .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
        .with_storage_backend(crate::config::StorageBackendType::Memory);

    let manager = ControllerManager::new(config, test_service_context(), test_telemetry_handle())
        .await
        .expect("Failed to create manager");
    let manager_arc = Arc::new(manager);

    manager_arc.initialize().await.expect("Failed to initialize");

    manager_arc.shutdown().await.expect("Failed to shutdown");
    manager_arc
        .start()
        .await
        .expect("shutdown before start must not prevent the first start");
    assert!(manager_arc.is_running());
    manager_arc.shutdown().await.expect("shutdown after start");

    std::mem::forget(manager_arc);
}

#[tokio::test]
async fn start_without_initialize_fails() {
    let config = ControllerConfig::default().with_node_info(1, "127.0.0.1:9880".parse::<SocketAddr>().unwrap());

    let manager = ControllerManager::new(config, test_service_context(), test_telemetry_handle())
        .await
        .expect("Failed to create manager");
    let manager_arc = Arc::new(manager);

    let result = manager_arc.start().await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ControllerError::NotInitialized(_)));

    std::mem::forget(manager_arc);
}

#[tokio::test]
async fn leadership_watch_enables_scheduling_for_openraft_leader() {
    let (remoting_addr, raft_addr) = reserve_controller_addresses();
    let config = ControllerConfig::default()
        .with_node_info(1, remoting_addr)
        .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
        .with_heartbeat_interval_ms(100)
        .with_election_timeout_ms(300)
        .with_storage_backend(crate::config::StorageBackendType::Memory);

    let manager = Arc::new(
        ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("Failed to create manager"),
    );
    manager.initialize().await.expect("initialize manager");
    manager.start().await.expect("start manager");

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: raft_addr.to_string(),
        },
    );
    manager
        .controller()
        .initialize_cluster(nodes)
        .await
        .expect("initialize single-node cluster");

    for _ in 0..30 {
        if manager.is_leader() && manager.scheduling_enabled() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(manager.is_leader(), "controller manager should become leader");
    assert!(
        manager.scheduling_enabled(),
        "leadership watcher should enable leader-only scheduling"
    );

    manager.shutdown().await.expect("shutdown manager");
    std::mem::forget(manager);
}

#[tokio::test]
async fn leadership_gate_recovers_from_pre_start_notifier_poisoning() {
    let (remoting_addr, raft_addr) = reserve_controller_addresses();
    let config = ControllerConfig::default()
        .with_node_info(1, remoting_addr)
        .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
        .with_storage_backend(crate::config::StorageBackendType::Memory);
    let manager = Arc::new(
        ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("create manager"),
    );
    manager.initialize().await.expect("initialize manager");

    manager.set_test_leadership_override(Some(true));
    manager
        .submit_broker_role_notifications([test_notify_task(1)])
        .await
        .expect("pre-start notification submission");
    let poisoned = manager.broker_role_notifier_snapshot();
    assert_eq!(
        poisoned.accepted, 0,
        "an unstarted notifier must reject the poisoned submission"
    );
    assert!(
        manager.scheduling_enabled(),
        "the pre-start leader path must have applied the poisoned gate state"
    );

    manager.start().await.expect("start manager");
    manager
        .submit_broker_role_notifications([test_notify_task(2)])
        .await
        .expect("post-start notification submission");
    let recovered = manager.broker_role_notifier_snapshot();
    assert_eq!(recovered.accepted, 1, "post-start leader notification must be accepted");
    assert!(
        manager.scheduling_enabled(),
        "the forced post-start synchronization must retain leader scheduling"
    );

    manager.shutdown().await.expect("shutdown manager");
    std::mem::forget(manager);
}

#[tokio::test]
async fn leadership_gate_orders_authoritative_promotion_and_demotion_before_notify_submission() {
    let (remoting_addr, raft_addr) = reserve_controller_addresses();
    let config = ControllerConfig::default()
        .with_node_info(1, remoting_addr)
        .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
        .with_storage_backend(crate::config::StorageBackendType::Memory);
    let manager = Arc::new(
        ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("create manager"),
    );
    manager.initialize().await.expect("initialize manager");
    manager.start().await.expect("start manager");

    manager.set_test_leadership_override(Some(true));
    let (promotion_submission, promotion_watch) = tokio::join!(biased;
        manager.submit_broker_role_notifications([test_notify_task(1)]),
        manager.synchronize_leadership_gate(),
    );
    promotion_submission.expect("leader notification submission");
    assert!(promotion_watch.expect("leader watch synchronization"));
    let promoted = manager.broker_role_notifier_snapshot();
    assert_eq!(promoted.accepted, 1, "leader submission must be retained immediately");
    assert!(manager.scheduling_enabled());

    manager.set_test_leadership_override(Some(false));
    let (demotion_watch, demotion_submission) = tokio::join!(biased;
        manager.synchronize_leadership_gate(),
        manager.submit_broker_role_notifications([test_notify_task(2)]),
    );
    assert!(!demotion_watch.expect("leader watch demotion"));
    demotion_submission.expect("follower notification submission");
    let demoted = manager.broker_role_notifier_snapshot();
    assert_eq!(
        demoted.accepted, promoted.accepted,
        "a demoted controller must not retain a stale role-change notification"
    );
    assert!(
        demoted.generation > promoted.generation,
        "demotion must reset the notifier generation"
    );
    assert_eq!(demoted.queued_keys, 0, "demotion must clear queued notifications");
    assert_eq!(
        demoted.retry_waiting_keys, 0,
        "demotion must clear retry-waiting notifications"
    );
    assert!(!manager.scheduling_enabled());

    manager.shutdown().await.expect("shutdown manager");
    manager.set_test_leadership_override(Some(true));
    manager
        .submit_broker_role_notifications([test_notify_task(3)])
        .await
        .expect("stopped notification submission");
    let stopped = manager.broker_role_notifier_snapshot();
    assert!(stopped.closed, "shutdown must close the notifier");
    assert_eq!(
        stopped.accepted, demoted.accepted,
        "a stopped controller must not accept a later leader notification"
    );
    std::mem::forget(manager);
}

#[tokio::test]
async fn inactive_slave_does_not_elect_but_inactive_master_does() {
    let (remoting_addr, raft_addr) = reserve_controller_addresses();
    let config = ControllerConfig::default()
        .with_node_info(1, remoting_addr)
        .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
        .with_heartbeat_interval_ms(100)
        .with_election_timeout_ms(300)
        .with_storage_backend(crate::config::StorageBackendType::Memory)
        .with_notify_broker_role_changed(false);

    let manager = Arc::new(
        ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("create manager"),
    );
    manager.initialize().await.expect("initialize manager");
    manager.start().await.expect("start manager");

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: raft_addr.to_string(),
        },
    );
    manager
        .controller()
        .initialize_cluster(nodes)
        .await
        .expect("initialize cluster");
    wait_until(Duration::from_secs(5), || manager.is_leader(), "controller leader").await;

    for (broker_id, addr, check_code) in [
        (1_i64, "127.0.0.1:10911", "master-check"),
        (2_i64, "127.0.0.1:10912", "slave-check"),
    ] {
        let apply_header = ApplyBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str("test-cluster"),
            broker_name: CheetahString::from_static_str("broker-a"),
            applied_broker_id: broker_id,
            register_check_code: CheetahString::from_string(format!("{addr};{check_code}")),
        };
        let apply_response = manager
            .controller()
            .apply_broker_id(&apply_header)
            .await
            .expect("apply broker id")
            .expect("apply response");
        assert_eq!(apply_response.code(), ResponseCode::Success as i32);

        let register_header = RegisterBrokerToControllerRequestHeader {
            cluster_name: Some(CheetahString::from_static_str("test-cluster")),
            broker_name: Some(CheetahString::from_static_str("broker-a")),
            broker_id: Some(broker_id),
            broker_address: Some(CheetahString::from_static_str(addr)),
            ..Default::default()
        };
        let register_response = manager
            .controller()
            .register_broker(&register_header)
            .await
            .expect("register broker")
            .expect("register response");
        assert_eq!(register_response.code(), ResponseCode::Success as i32);

        let heartbeat_header = BrokerHeartbeatRequestHeader {
            cluster_name: CheetahString::from_static_str("test-cluster"),
            broker_addr: CheetahString::from_static_str(addr),
            broker_name: CheetahString::from_static_str("broker-a"),
            broker_id: Some(broker_id),
            epoch: Some(1),
            max_offset: Some(100),
            confirm_offset: Some(80),
            store_ready: Some(true),
            heartbeat_timeout_mills: Some(60_000),
            election_priority: Some(1),
        };
        let heartbeat_response = manager
            .controller()
            .record_broker_heartbeat(&heartbeat_header)
            .await
            .expect("record heartbeat")
            .expect("heartbeat response");
        assert_eq!(heartbeat_response.code(), ResponseCode::Success as i32);
    }

    let elect_header = ElectMasterRequestHeader::new("test-cluster", "broker-a", 1, false, current_millis());
    let mut elect_response = manager
        .controller()
        .elect_master(&elect_header)
        .await
        .expect("elect master")
        .expect("elect response");
    assert_eq!(elect_response.code(), ResponseCode::Success as i32);
    elect_response.make_custom_header_to_net();
    let elect_response_header = elect_response
        .decode_command_custom_header::<ElectMasterResponseHeader>()
        .expect("decode elect response");
    let alter_header = AlterSyncStateSetRequestHeader {
        broker_name: CheetahString::from_static_str("broker-a"),
        master_broker_id: 1,
        master_epoch: elect_response_header.master_epoch.expect("master epoch"),
        invoke_time: rocketmq_model::time::current_millis(),
    };
    let alter_body = SyncStateSet::with_values(
        HashSet::from([1_i64, 2_i64]),
        elect_response_header
            .sync_state_set_epoch
            .expect("sync state set epoch"),
    );
    let alter_response = manager
        .controller()
        .alter_sync_state_set(&alter_header, alter_body)
        .await
        .expect("alter sync state")
        .expect("alter response");
    assert_eq!(alter_response.code(), ResponseCode::Success as i32);

    let listener = BrokerInactiveListener::new(Arc::downgrade(&manager));
    listener.on_broker_inactive(Some("test-cluster"), "broker-a", Some(2));
    sleep(Duration::from_millis(300)).await;

    let replica_header = GetReplicaInfoRequestHeader {
        broker_name: CheetahString::from_static_str("broker-a"),
    };
    let mut replica_response = manager
        .controller()
        .get_replica_info(&replica_header)
        .await
        .expect("get replica info after inactive slave")
        .expect("replica response");
    replica_response.make_custom_header_to_net();
    let replica_info = replica_response
        .decode_command_custom_header::<GetReplicaInfoResponseHeader>()
        .expect("decode replica info");
    assert_eq!(replica_info.master_broker_id, Some(1));

    let slave_heartbeat_header = BrokerHeartbeatRequestHeader {
        cluster_name: CheetahString::from_static_str("test-cluster"),
        broker_addr: CheetahString::from_static_str("127.0.0.1:10912"),
        broker_name: CheetahString::from_static_str("broker-a"),
        broker_id: Some(2),
        epoch: Some(1),
        max_offset: Some(100),
        confirm_offset: Some(80),
        store_ready: Some(true),
        heartbeat_timeout_mills: Some(60_000),
        election_priority: Some(1),
    };
    let slave_heartbeat_response = manager
        .controller()
        .record_broker_heartbeat(&slave_heartbeat_header)
        .await
        .expect("record slave heartbeat before master inactive")
        .expect("heartbeat response");
    assert_eq!(slave_heartbeat_response.code(), ResponseCode::Success as i32);

    listener.on_broker_inactive(Some("test-cluster"), "broker-a", Some(1));
    let start = current_millis();
    loop {
        let mut replica_response = manager
            .controller()
            .get_replica_info(&replica_header)
            .await
            .expect("get replica info after inactive master")
            .expect("replica response");
        replica_response.make_custom_header_to_net();
        let replica_info = replica_response
            .decode_command_custom_header::<GetReplicaInfoResponseHeader>()
            .expect("decode replica info");
        if replica_info.master_broker_id == Some(2) {
            break;
        }
        assert!(
            current_millis().saturating_sub(start) < 5_000,
            "timed out waiting for master reelection after inactive master"
        );
        sleep(Duration::from_millis(50)).await;
    }

    manager.shutdown().await.expect("shutdown manager");
    std::mem::forget(manager);
}

#[tokio::test]
async fn processor_successful_manual_election_records_role_change_notification() {
    let (remoting_addr, raft_addr) = reserve_controller_addresses();
    let config = ControllerConfig::default()
        .with_node_info(1, remoting_addr)
        .with_raft_peers(vec![crate::config::RaftPeer { id: 1, addr: raft_addr }])
        .with_heartbeat_interval_ms(100)
        .with_election_timeout_ms(300)
        .with_storage_backend(crate::config::StorageBackendType::Memory)
        .with_notify_broker_role_changed(true);

    let manager = Arc::new(
        ControllerManager::new(config, test_service_context(), test_telemetry_handle())
            .await
            .expect("create manager"),
    );
    manager.initialize().await.expect("initialize manager");
    manager.start().await.expect("start manager");

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: raft_addr.to_string(),
        },
    );
    manager
        .controller()
        .initialize_cluster(nodes)
        .await
        .expect("initialize cluster");
    wait_until(Duration::from_secs(5), || manager.is_leader(), "controller leader").await;

    for (broker_id, addr, check_code) in [
        (1_i64, "127.0.0.1:10911", "master-check"),
        (2_i64, "127.0.0.1:10912", "slave-check"),
    ] {
        let apply_header = ApplyBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str("test-cluster"),
            broker_name: CheetahString::from_static_str("broker-a"),
            applied_broker_id: broker_id,
            register_check_code: CheetahString::from_string(format!("{addr};{check_code}")),
        };
        let apply_response = manager
            .controller()
            .apply_broker_id(&apply_header)
            .await
            .expect("apply broker id")
            .expect("apply response");
        assert_eq!(apply_response.code(), ResponseCode::Success as i32);

        let register_header = RegisterBrokerToControllerRequestHeader {
            cluster_name: Some(CheetahString::from_static_str("test-cluster")),
            broker_name: Some(CheetahString::from_static_str("broker-a")),
            broker_id: Some(broker_id),
            broker_address: Some(CheetahString::from_static_str(addr)),
            ..Default::default()
        };
        let register_response = manager
            .controller()
            .register_broker(&register_header)
            .await
            .expect("register broker")
            .expect("register response");
        assert_eq!(register_response.code(), ResponseCode::Success as i32);

        let heartbeat_header = BrokerHeartbeatRequestHeader {
            cluster_name: CheetahString::from_static_str("test-cluster"),
            broker_addr: CheetahString::from_static_str(addr),
            broker_name: CheetahString::from_static_str("broker-a"),
            broker_id: Some(broker_id),
            epoch: Some(1),
            max_offset: Some(100),
            confirm_offset: Some(80),
            store_ready: Some(true),
            heartbeat_timeout_mills: Some(60_000),
            election_priority: Some(1),
        };
        let heartbeat_response = manager
            .controller()
            .record_broker_heartbeat(&heartbeat_header)
            .await
            .expect("record replicated heartbeat")
            .expect("heartbeat response");
        assert_eq!(heartbeat_response.code(), ResponseCode::Success as i32);
        manager.heartbeat_manager().on_broker_session_heartbeat(
            "test-cluster",
            "broker-a",
            addr,
            broker_id,
            Some(60_000),
            test_broker_session(broker_id as u64),
            Some(1),
            Some(100),
            Some(80),
            Some(1),
        );
    }

    let initial_elect_header = ElectMasterRequestHeader::new("test-cluster", "broker-a", 1, false, current_millis());
    let mut initial_elect_response = manager
        .controller()
        .elect_master(&initial_elect_header)
        .await
        .expect("elect initial master")
        .expect("initial elect response");
    assert_eq!(initial_elect_response.code(), ResponseCode::Success as i32);
    initial_elect_response.make_custom_header_to_net();
    let initial_header = initial_elect_response
        .decode_command_custom_header::<ElectMasterResponseHeader>()
        .expect("decode initial elect response");

    let alter_header = AlterSyncStateSetRequestHeader {
        broker_name: CheetahString::from_static_str("broker-a"),
        master_broker_id: 1,
        master_epoch: initial_header.master_epoch.expect("master epoch"),
        invoke_time: rocketmq_model::time::current_millis(),
    };
    let alter_body = SyncStateSet::with_values(
        HashSet::from([1_i64, 2_i64]),
        initial_header.sync_state_set_epoch.expect("sync state set epoch"),
    );
    let alter_response = manager
        .controller()
        .alter_sync_state_set(&alter_header, alter_body)
        .await
        .expect("alter sync state")
        .expect("alter response");
    assert_eq!(alter_response.code(), ResponseCode::Success as i32);
    assert!(
        manager
            .heartbeat_manager()
            .is_broker_active("test-cluster", "broker-a", 2),
        "local heartbeat manager must consider target broker active for role-change notification"
    );

    let processor = ControllerRequestProcessor::new(manager.clone());
    let mut request = RemotingCommand::create_request_command(
        RequestCode::ControllerElectMaster,
        ElectMasterRequestHeader::new("test-cluster", "broker-a", 2, true, current_millis()),
    );
    request.make_custom_header_to_net();
    let dispatch = processor.handle_request(test_broker_session(99), "transport-session-99", &mut request);
    let mut response = processor
        .complete_request(
            RequestCode::ControllerElectMaster.get_controller_request_name(),
            dispatch,
        )
        .await
        .expect("processor elect request");
    response.make_custom_header_to_net();
    assert_eq!(response.code(), ResponseCode::Success as i32);
    let response_header = response
        .decode_command_custom_header::<ElectMasterResponseHeader>()
        .expect("decode processor elect response");
    assert_eq!(response_header.master_broker_id, Some(2));
    let response_body = ElectMasterResponseBody::decode(response.body().expect("elect response body").as_ref())
        .expect("decode processor elect response body");
    assert!(
        response_body.broker_member_group.is_some(),
        "successful manual election must carry broker member group for role-change notification"
    );

    wait_until(
        Duration::from_secs(2),
        || {
            let snapshot = manager.broker_role_notifier_snapshot();
            snapshot.accepted > 0
        },
        "processor elect-master to record broker role notification",
    )
    .await;

    manager.shutdown().await.expect("shutdown manager");
    std::mem::forget(processor);
    std::mem::forget(manager);
}
