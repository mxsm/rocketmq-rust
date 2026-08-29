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

use std::collections::HashMap;
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use futures::FutureExt;
use rocketmq_model::common::attribute::subscription_group_attributes::LITE_BIND_TOPIC_ATTRIBUTE_NAME;
use rocketmq_model::common::attribute::topic_attributes;
use rocketmq_model::common::attribute::Attribute;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroup;
use rocketmq_store::MessageStoreConfig;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::LegacySessionExecutionHarness;
use rocketmq_transport::test_support::TestChannelBuilder;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::broker_runtime::BrokerRuntime;
use crate::config::broker_config::BrokerConfig;
use crate::deferred_generation_handoff::DeferredGeneration;
use crate::deferred_generation_handoff::DeferredGenerationTarget;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingService;
use crate::long_polling::polling_result::PollingResult;

const CLIENT_ID: &str = "broker-cutover-client";
const GROUP: &str = "broker-cutover-group";
const TOPIC: &str = "broker-cutover-topic";
const LEGACY_OPAQUE: i32 = 98_510_001;
const NORMAL_OPAQUE: i32 = 98_510_002;
const FAST_OPAQUE: i32 = 98_510_003;
const TIMEOUT_OPAQUE: i32 = 98_510_004;
const SHORT_POLL_TIME: i64 = 500;
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

struct LegacyWaiter {
    channel: Channel,
    session: LegacySessionExecutionHarness,
    task_group: TaskGroup,
    peer: Connection,
}

fn reserve_listener_ports() -> (u32, usize) {
    for _ in 0..128 {
        let normal =
            std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve Broker normal listener port");
        let normal_port = normal.local_addr().expect("normal listener address").port();
        let Some(fast_port) = normal_port.checked_sub(2) else {
            continue;
        };
        let Ok(fast) = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, fast_port)) else {
            continue;
        };
        let ha = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve Broker HA listener port");
        let ha_port = ha.local_addr().expect("HA listener address").port();
        if ha_port != normal_port && ha_port != fast_port {
            drop((normal, fast, ha));
            return (u32::from(normal_port), usize::from(ha_port));
        }
    }
    panic!("could not reserve a normal/fast/HA listener port set");
}

fn runtime_config(
    root: &TempDir,
    listen_port: u32,
    ha_listen_port: usize,
) -> (Arc<BrokerConfig>, Arc<MessageStoreConfig>) {
    let root_path = root.path().to_string_lossy().into_owned();
    let mut broker = BrokerConfig {
        broker_ip1: CheetahString::from_static_str("127.0.0.1"),
        broker_ip2: Some(CheetahString::from_static_str("127.0.0.1")),
        listen_port,
        store_path_root_dir: root_path.clone().into(),
        auth_config_path: root.path().join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    };
    broker.broker_server_config.bind_address = "127.0.0.1".to_owned();
    broker.broker_server_config.listen_port = listen_port;
    let store = MessageStoreConfig {
        store_path_root_dir: root_path.into(),
        ha_listen_port,
        ..MessageStoreConfig::default()
    };
    (Arc::new(broker), Arc::new(store))
}

fn pop_lite_request(opaque: i32, poll_time: i64) -> RemotingCommand {
    let header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str(CLIENT_ID),
        consumer_group: CheetahString::from_static_str(GROUP),
        topic: CheetahString::from_static_str(TOPIC),
        max_msg_num: 1,
        invisible_time: 30_000,
        poll_time,
        born_time: i64::try_from(current_millis()).expect("test clock fits the protocol field"),
        attempt_id: None,
        rpc: None,
    };
    let mut command = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, header).set_opaque(opaque);
    command.make_custom_header_to_net();
    command
}

fn seed_lite_topic_and_group(runtime: &mut BrokerRuntime) {
    let state = runtime.runtime_state_mut();
    let mut topic = TopicConfig::with_queues(TOPIC, 1, 1);
    topic.attributes.insert(
        CheetahString::from_string(format!(
            "+{}",
            topic_attributes::TopicAttributes::topic_message_type_attribute().name()
        )),
        CheetahString::from_static_str("LITE"),
    );
    state.topic_config_manager().update_topic_config(topic, 0);

    let mut group = SubscriptionGroupConfig::new(CheetahString::from_static_str(GROUP));
    group.set_attributes(HashMap::from([(
        CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
        CheetahString::from_static_str(TOPIC),
    )]));
    state
        .subscription_group_manager_mut()
        .update_subscription_group_config(&mut group);
}

async fn legacy_session(context: ChildServiceContext, owner_id: u64) -> (ConnectionHandlerContext, LegacyWaiter) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind legacy session listener");
    let address = listener.local_addr().expect("legacy session listener address");
    let (peer, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
    let peer = peer.expect("connect legacy session peer");
    let (transport, remote_address) = accepted.expect("accept legacy session transport");
    let local_address = transport.local_addr().expect("legacy transport local address");
    let task_group = context.task_group().clone();
    let channel = TestChannelBuilder::new(Connection::new(transport), task_group.clone())
        .addresses(local_address, remote_address)
        .build()
        .expect("build real transport-backed legacy channel");
    let session = LegacySessionExecutionHarness::new(owner_id, &task_group);
    let handler_context = session.context(channel.clone(), 4 * 1024, RequestCode::PopLiteMessage.to_i32());
    (
        handler_context,
        LegacyWaiter {
            channel,
            session,
            task_group,
            peer: Connection::new(peer),
        },
    )
}

async fn wait_until(mut condition: impl FnMut() -> bool, label: &'static str) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
}

async fn receive_one(connection: &mut Connection, label: &'static str) -> RemotingCommand {
    tokio::time::timeout(TEST_TIMEOUT, connection.receive_command())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
        .unwrap_or_else(|| panic!("connection closed before {label}"))
        .unwrap_or_else(|error| panic!("failed to receive {label}: {error}"))
}

async fn wait_until_deferred(connection: &mut Connection, condition: impl FnMut() -> bool, label: &'static str) {
    let registered = wait_until(condition, label);
    tokio::pin!(registered);
    tokio::select! {
        () = &mut registered => {}
        response = connection.receive_command() => {
            let response = response
                .unwrap_or_else(|| panic!("connection closed before {label}"))
                .unwrap_or_else(|error| panic!("failed while waiting for {label}: {error}"));
            panic!(
                "{label} returned an immediate response: code={}, opaque={}, remark={:?}",
                response.code(),
                response.opaque(),
                response.remark(),
            );
        }
    }
}

async fn collect_until_close(mut connection: Connection, label: &'static str) -> Vec<RemotingCommand> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut frames = Vec::new();
        while let Some(frame) = connection.receive_command().await {
            frames.push(frame.expect("receive terminal Broker frame"));
        }
        frames
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {label} connection close"))
}

async fn assert_no_buffered_frame_and_close(connection: &mut Connection, label: &'static str) {
    match connection.receive_command().now_or_never() {
        Some(Some(Ok(frame))) => panic!(
            "{label} emitted a duplicate frame: code={}, opaque={}, remark={:?}",
            frame.code(),
            frame.opaque(),
            frame.remark()
        ),
        Some(Some(Err(error))) => panic!("{label} failed while checking for a duplicate frame: {error}"),
        Some(None) | None => {}
    }
    connection
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("failed to close {label}: {error}"));
}

#[test]
fn broker_cutover_routes_legacy_then_normal_v2_once_and_keeps_fast_waiter_quiescent() {
    let mut owner_config = RuntimeConfig::server_default("broker-cutover-network-arrival");
    owner_config.thread_stack_size = Some(16 * 1024 * 1024);
    let owner = RuntimeOwner::new(owner_config).expect("Broker cutover runtime owner");
    let broker_context = owner.root_context().component("broker-cutover-runtime");
    let legacy_context = owner.root_context().component("broker-cutover-legacy-session");
    let temp = tempfile::tempdir().expect("create Broker cutover test root");
    let (listen_port, ha_listen_port) = reserve_listener_ports();
    let (broker_config, store_config) = runtime_config(&temp, listen_port, ha_listen_port);

    owner.block_on(async move {
        let mut runtime = BrokerRuntime::new_with_service_context(broker_config, store_config, broker_context);
        runtime.initialize().await.expect("initialize Broker cutover runtime");
        seed_lite_topic_and_group(&mut runtime);
        let (pre_publish, release_publish) = runtime.install_v2_pre_publish_checkpoint_for_test();

        let start = runtime.start_basic_service();
        let install_legacy = async move {
            let snapshot = pre_publish.await.expect("reach Broker V2 pre-publication checkpoint");
            assert_eq!(snapshot.handoff.default_generation(), DeferredGeneration::Legacy);
            let service = Arc::clone(snapshot.pop_lite_processor.pop_lite_long_polling_service());
            PopLiteLongPollingService::start(&service).await;
            let (handler_context, waiter) = legacy_session(legacy_context, 98_510).await;
            let mut request = pop_lite_request(LEGACY_OPAQUE, 60_000);
            assert_eq!(
                service.polling(
                    handler_context,
                    &mut request,
                    &CheetahString::from_static_str(CLIENT_ID),
                    i64::try_from(current_millis()).expect("test clock fits the protocol field"),
                    60_000,
                ),
                PollingResult::PollingSuc
            );
            let resources = service.legacy_resource_snapshot();
            assert_eq!(resources.table_entries, 1);
            assert_eq!(resources.tracked_waiters, 1);
            assert_eq!(snapshot.handoff.snapshot().occupancy, 1);
            release_publish.send(()).expect("release Broker V2 publication");
            (snapshot.handoff, service, waiter)
        };
        let (listeners, legacy) = tokio::time::timeout(TEST_TIMEOUT, async { tokio::join!(start, install_legacy) })
            .await
            .expect("Broker cutover startup should finish");
        let (normal_address, fast_address) = listeners.expect("start Broker cutover listeners");
        let (handoff, legacy_service, mut legacy_waiter) = legacy;

        assert_eq!(
            normal_address.port(),
            u16::try_from(listen_port).expect("normal port fits u16")
        );
        assert_eq!(fast_address.port(), normal_address.port() - 2);
        let identity = runtime
            .v2_dispatcher_identity_snapshot_for_test()
            .expect("record canonical dispatcher identities");
        assert!(identity.normal_is_canonical);
        assert!(identity.fast_is_canonical);
        assert!(identity.embedded_proxy_is_canonical);

        let mut normal = Connection::new(
            TcpStream::connect(normal_address)
                .await
                .expect("connect normal V2 listener"),
        );
        normal
            .send_command(pop_lite_request(NORMAL_OPAQUE, 60_000))
            .await
            .expect("send normal V2 PopLite waiter");
        wait_until_deferred(
            &mut normal,
            || {
                runtime
                    .composition
                    .data_plane
                    .deferred
                    .as_ref()
                    .expect("Broker deferred lifecycle")
                    .resource_snapshot()
                    .pop_lite_live
                    == 1
            },
            "normal V2 PopLite waiter",
        )
        .await;

        let mut fast = Connection::new(
            TcpStream::connect(fast_address)
                .await
                .expect("connect fast V2 listener"),
        );
        fast.send_command(pop_lite_request(FAST_OPAQUE, 60_000))
            .await
            .expect("send fast V2 PopLite waiter");
        wait_until_deferred(
            &mut fast,
            || {
                runtime
                    .composition
                    .data_plane
                    .deferred
                    .as_ref()
                    .expect("Broker deferred lifecycle")
                    .resource_snapshot()
                    .pop_lite_live
                    == 2
            },
            "normal and fast V2 PopLite waiters",
        )
        .await;

        let client_id = CheetahString::from_static_str(CLIENT_ID);
        let group = CheetahString::from_static_str(GROUP);
        let first_event = CheetahString::from_static_str("%LMQ%$broker-cutover-topic$legacy-arrival");
        assert_eq!(
            runtime.composition.state.lite_event_dispatcher().do_full_dispatch(
                &client_id,
                &group,
                &HashSet::from([first_event])
            ),
            1
        );
        let legacy_response = receive_one(&mut legacy_waiter.peer, "legacy PopLite response").await;
        assert_eq!(legacy_response.opaque(), LEGACY_OPAQUE);
        assert_eq!(ResponseCode::from(legacy_response.code()), ResponseCode::PollingTimeout);

        wait_until(
            || {
                let legacy = legacy_service.legacy_resource_snapshot();
                let deferred = runtime
                    .composition
                    .data_plane
                    .deferred
                    .as_ref()
                    .expect("Broker deferred lifecycle")
                    .resource_snapshot();
                legacy.table_entries == 0
                    && legacy.tracked_waiters == 0
                    && legacy.waking_clients == 0
                    && legacy.active_executions == 0
                    && deferred.pop_lite_live == 2
                    && handoff.generation_for(&DeferredGenerationTarget::pop_lite(client_id.clone()))
                        == DeferredGeneration::New
            },
            "legacy terminal, untouched V2 waiters, and target transition",
        )
        .await;

        let second_event = CheetahString::from_static_str("%LMQ%$broker-cutover-topic$new-arrival");
        assert_eq!(
            runtime.composition.state.lite_event_dispatcher().do_full_dispatch(
                &client_id,
                &group,
                &HashSet::from([second_event])
            ),
            1
        );
        let normal_response = receive_one(&mut normal, "normal V2 PopLite response").await;
        assert_eq!(normal_response.opaque(), NORMAL_OPAQUE);
        assert_eq!(ResponseCode::from(normal_response.code()), ResponseCode::PollingTimeout);
        wait_until(
            || {
                runtime
                    .composition
                    .data_plane
                    .deferred
                    .as_ref()
                    .expect("Broker deferred lifecycle")
                    .resource_snapshot()
                    .pop_lite_live
                    == 1
            },
            "one remaining fast V2 PopLite waiter",
        )
        .await;

        assert_no_buffered_frame_and_close(&mut fast, "fast V2 peer-close route").await;
        wait_until(
            || {
                runtime
                    .composition
                    .data_plane
                    .deferred
                    .as_ref()
                    .expect("Broker deferred lifecycle")
                    .resource_snapshot()
                    .pop_lite_live
                    == 0
                    && runtime.composition.request_pipeline.v2_session_registry().len() == 1
            },
            "fast V2 peer-close cleanup",
        )
        .await;
        let fast_frames = collect_until_close(fast, "fast V2 peer-close").await;
        assert!(fast_frames.is_empty(), "the peer-closed fast route emitted a frame");

        normal
            .send_command(pop_lite_request(TIMEOUT_OPAQUE, SHORT_POLL_TIME))
            .await
            .expect("send normal V2 protocol-timeout waiter");
        wait_until_deferred(
            &mut normal,
            || {
                runtime
                    .composition
                    .data_plane
                    .deferred
                    .as_ref()
                    .expect("Broker deferred lifecycle")
                    .resource_snapshot()
                    .pop_lite_live
                    == 1
            },
            "normal V2 protocol-timeout waiter",
        )
        .await;
        let timeout_response = receive_one(&mut normal, "normal V2 protocol-timeout response").await;
        assert_eq!(timeout_response.opaque(), TIMEOUT_OPAQUE);
        assert_eq!(
            ResponseCode::from(timeout_response.code()),
            ResponseCode::PollingTimeout
        );
        wait_until(
            || {
                let resources = runtime
                    .composition
                    .data_plane
                    .deferred
                    .as_ref()
                    .expect("Broker deferred lifecycle")
                    .resource_snapshot();
                resources.pop_lite_live == 0
                    && resources.pop_lite_reserved == 0
                    && resources.pop_lite_candidates == 0
                    && resources.pop_lite_clients == 0
                    && resources.pop_lite_event_batches == 0
                    && resources.pop_lite_event_count == 0
                    && resources.pop_lite_event_permits == 0
                    && resources.pop_lite_event_bytes == 0
                    && resources.pop_lite_active_client_gates == 0
                    && resources.pop_lite_active_event_producers == 0
                    && resources.pop_lite_prepared == 0
                    && resources.pop_lite_pending_claims == 0
                    && resources.pop_lite_accepted_resumes == 0
                    && resources.pop_lite_resume_executions == 0
                    && resources.pop_lite_resume_execution_bytes == 0
                    && resources.pop_lite_pending_replays == 0
            },
            "normal V2 protocol-timeout resource cleanup",
        )
        .await;

        legacy_waiter.session.close();
        let legacy_channel = legacy_waiter.channel.close_with_report(TEST_TIMEOUT).await;
        assert!(legacy_channel.is_healthy(), "{}", legacy_channel.to_json());
        drop(legacy_waiter.channel);
        let legacy_tasks = legacy_waiter
            .task_group
            .shutdown_until(ShutdownDeadline::after(TEST_TIMEOUT))
            .await;
        assert!(legacy_tasks.is_healthy(), "{}", legacy_tasks.to_json());
        assert_no_buffered_frame_and_close(&mut legacy_waiter.peer, "legacy route").await;

        let shutdown = runtime.shutdown_basic_service_until(ShutdownDeadline::after(TEST_TIMEOUT));
        let (report, normal_trailing) = tokio::join!(shutdown, collect_until_close(normal, "normal V2"));
        assert!(normal_trailing.is_empty(), "normal V2 route emitted duplicate frames");
        assert!(report.is_healthy(), "{report:?}");
        assert!(report
            .deferred_resources
            .expect("terminal Broker deferred snapshot")
            .is_zero());
        assert!(report
            .legacy_service_shutdown
            .iter()
            .all(crate::long_polling::long_polling_service::LegacyServiceShutdownReport::is_healthy));
        assert!(runtime.composition.request_pipeline.v2_session_registry().is_empty());
        drop(runtime);
    });

    let tasks = owner.block_on(owner.shutdown_tasks());
    assert!(tasks.is_healthy(), "{}", tasks.to_json());
    let background = owner.shutdown_background();
    assert!(background.is_healthy(), "{}", background.to_json());
}
