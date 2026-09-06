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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_model::common::constant::consume_init_mode::ConsumeInitMode;
use rocketmq_model::common::filter::expression_type::ExpressionType;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::CqExtUnit;
use rocketmq_store::FlushDiskType;
use rocketmq_store::MessageFilter;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageStatus;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestProcessor;

use crate::broker_runtime::BrokerMessageStore;
use crate::broker_runtime::BrokerRuntime;
use crate::config::broker_config::BrokerConfig;
use crate::deferred_generation_handoff::DeferredGenerationTarget;
use crate::long_polling::pop_deferred::service::PopDeferredPrepareOutcome;
use crate::long_polling::pop_deferred::service::PopDeferredRegisterOutcome;
use crate::long_polling::pop_deferred::service::PopDeferredService;
use crate::long_polling::pop_deferred::service::PopRetainedEstimate;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::processor_test_support::start_processor_server;
use crate::processor::pull_message_processor::PullMessageProcessor;

const PULL_TOPIC: &str = "deferred-e2e-pull";
const POP_TOPIC: &str = "deferred-e2e-pop";
const NOTIFICATION_TOPIC: &str = "deferred-e2e-notification";
const GROUP: &str = "deferred-e2e-group";
const MATCH_TAG: &str = "match";

#[derive(Clone)]
struct PullLeaf(Arc<PullMessageProcessor<BrokerMessageStore>>);

impl RequestProcessor for PullLeaf {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.0.process_shared(request).await
    }
}

#[derive(Clone)]
struct PopLeaf {
    service: Arc<PopDeferredService>,
    filter: rocketmq_store::ArcMessageFilter,
}

impl RequestProcessor for PopLeaf {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prepared = match self.service.prepare(
            request,
            None,
            Some(Arc::clone(&self.filter)),
            PopRetainedEstimate::default(),
        ) {
            Ok(PopDeferredPrepareOutcome::Prepared(prepared)) => *prepared,
            Ok(PopDeferredPrepareOutcome::Rejected(rejection)) => {
                panic!("POP frozen-filter preparation was rejected: {:?}", rejection.kind())
            }
            Err(error) => panic!("POP frozen-filter preparation failed: {:?}", error.kind()),
        };
        let registration = match self.service.register(prepared, request) {
            Ok(PopDeferredRegisterOutcome::Registered(registration)) => *registration,
            Ok(PopDeferredRegisterOutcome::Rejected(rejection)) => {
                panic!("POP frozen-filter registration was rejected: {:?}", rejection.kind())
            }
            Err(error) => panic!("POP frozen-filter registration failed: {:?}", error.kind()),
        };
        Ok(HandlerOutcome::Deferred(registration))
    }
}

struct FrozenPopTagFilter(i64);

impl MessageFilter for FrozenPopTagFilter {
    fn is_matched_by_consume_queue(&self, tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        tags_code == Some(self.0)
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        true
    }
}

#[derive(Clone)]
struct NotificationLeaf(Arc<NotificationProcessor<BrokerMessageStore>>);

impl RequestProcessor for NotificationLeaf {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.0.process_shared(request).await
    }
}

fn temp_root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "rocketmq-deferred-producer-e2e-{}-{}",
        std::process::id(),
        current_millis()
    ))
}

fn available_ha_port() -> usize {
    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("reserve deferred producer HA port")
        .local_addr()
        .expect("deferred producer HA address")
        .port() as usize
}

async fn wait_until(mut condition: impl FnMut() -> bool, label: &'static str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
}

fn tagged_message(topic: &str, tag: &str) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_slice(topic));
    message.message_ext_inner.set_queue_id(0);
    message.set_body(Bytes::from(format!("{topic}-{tag}")));
    message.set_wait_store_msg_ok(false);
    message.set_tags(CheetahString::from_slice(tag));
    message.tags_code = MessageExtBrokerInner::tags_string_to_tags_code(tag);
    message.properties_string = MessageDecoder::message_properties_to_string(message.get_properties());
    message
}

fn pull_request() -> RemotingCommand {
    let header = PullMessageRequestHeader {
        consumer_group: CheetahString::from_static_str(GROUP),
        topic: CheetahString::from_static_str(PULL_TOPIC),
        queue_id: 0,
        queue_offset: 0,
        max_msg_nums: 2,
        sys_flag: PullSysFlag::build_sys_flag(false, true, true, false) as i32,
        commit_offset: 0,
        suspend_timeout_millis: 60_000,
        sub_version: 1,
        subscription: Some(CheetahString::from_static_str(MATCH_TAG)),
        expression_type: Some(CheetahString::from_static_str(ExpressionType::TAG)),
        ..PullMessageRequestHeader::default()
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::PullMessage, header).set_opaque(91_001);
    request.make_custom_header_to_net();
    request
}

fn pop_request() -> RemotingCommand {
    let header = PopMessageRequestHeader {
        consumer_group: CheetahString::from_static_str(GROUP),
        topic: CheetahString::from_static_str(POP_TOPIC),
        queue_id: 0,
        max_msg_nums: 2,
        invisible_time: 30_000,
        poll_time: 60_000,
        born_time: current_millis(),
        init_mode: ConsumeInitMode::MIN,
        exp_type: None,
        exp: None,
        order: Some(false),
        attempt_id: None,
        topic_request_header: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::PopMessage, header).set_opaque(91_002);
    request.make_custom_header_to_net();
    request
}

fn notification_request() -> RemotingCommand {
    let header = NotificationRequestHeader {
        consumer_group: CheetahString::from_static_str(GROUP),
        topic: CheetahString::from_static_str(NOTIFICATION_TOPIC),
        queue_id: 0,
        poll_time: 60_000,
        born_time: i64::try_from(current_millis()).expect("wall time fits protocol"),
        order: false,
        attempt_id: None,
        exp_type: Some(CheetahString::from_static_str(ExpressionType::TAG)),
        exp: Some(CheetahString::from_static_str(MATCH_TAG)),
        is_lite_consumer: false,
        client_id: Some(CheetahString::from_static_str("deferred-e2e-client")),
        topic_request_header: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::Notification, header).set_opaque(91_003);
    request.make_custom_header_to_net();
    request
}

fn verify_canonical_target_accounting(runtime: &BrokerRuntime) {
    let deferred = runtime
        .composition
        .data_plane
        .deferred
        .as_ref()
        .expect("deferred lifecycle");
    let handoff = &deferred.handoff;
    let pull_target = DeferredGenerationTarget::pull(CheetahString::from_static_str(PULL_TOPIC), 0);
    let pop_target = DeferredGenerationTarget::pop(
        CheetahString::from_static_str(POP_TOPIC),
        CheetahString::from_static_str(GROUP),
        0,
    );
    let notification_target = DeferredGenerationTarget::notification(
        CheetahString::from_static_str(NOTIFICATION_TOPIC),
        CheetahString::from_static_str(GROUP),
        0,
    );
    let permits = [
        handoff.acquire_route(pull_target.clone()).expect("Pull route"),
        handoff.acquire_route(pop_target.clone()).expect("POP route"),
        handoff
            .acquire_route(notification_target.clone())
            .expect("Notification route"),
    ];
    drop(permits);
    assert!(handoff.zero_report().is_zero());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn store_replay_workers_resume_three_canonical_sessions_exactly_once_and_rearm_retry() {
    let root = temp_root();
    std::fs::create_dir_all(&root).expect("create deferred producer test root");
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        auth_config_path: root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        flush_disk_type: FlushDiskType::AsyncFlush,
        ha_listen_port: available_ha_port(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new_with_service_context(
        broker_config,
        message_store_config,
        crate::test_service_context("broker.deferred-producer-e2e"),
    );
    runtime
        .initialize()
        .await
        .expect("initialize deferred producer runtime");
    for topic in [PULL_TOPIC, POP_TOPIC, NOTIFICATION_TOPIC] {
        runtime.seed_pop_topic_and_group_for_test(topic, GROUP);
    }
    runtime
        .start_message_store_for_test()
        .await
        .expect("start deferred producer Store");
    runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    let (producer, controller) = {
        let deferred = runtime
            .composition
            .data_plane
            .deferred
            .as_ref()
            .expect("deferred lifecycle");
        (
            deferred.producer.as_ref().cloned().expect("deferred producer"),
            Arc::clone(&deferred.admission_controller),
        )
    };
    let pull_processor = runtime
        .composition
        .request_pipeline
        .pull_message_processor_for_test
        .as_ref()
        .cloned()
        .expect("Pull processor");
    let notification_processor = runtime
        .composition
        .state
        .notification_processor
        .as_ref()
        .cloned()
        .expect("Notification processor");

    let (mut pull_client, pull_server) =
        start_processor_server("deferred-e2e-pull", PullLeaf(pull_processor), Arc::clone(&controller)).await;
    let (mut pop_client, pop_server) = start_processor_server(
        "deferred-e2e-pop",
        PopLeaf {
            service: Arc::clone(
                &runtime
                    .composition
                    .data_plane
                    .deferred
                    .as_ref()
                    .expect("deferred lifecycle")
                    .pop,
            ),
            filter: Arc::new(FrozenPopTagFilter(MessageExtBrokerInner::tags_string_to_tags_code(
                MATCH_TAG,
            ))),
        },
        Arc::clone(&controller),
    )
    .await;
    let (mut notification_client, notification_server) = start_processor_server(
        "deferred-e2e-notification",
        NotificationLeaf(notification_processor),
        controller,
    )
    .await;
    verify_canonical_target_accounting(&runtime);

    pull_client.send_command(pull_request()).await.expect("send Pull wait");
    pop_client.send_command(pop_request()).await.expect("send POP wait");
    notification_client
        .send_command(notification_request())
        .await
        .expect("send Notification wait");
    wait_until(
        || {
            let snapshot = runtime
                .composition
                .data_plane
                .deferred
                .as_ref()
                .expect("deferred lifecycle")
                .resource_snapshot();
            snapshot.pull_live == 1 && snapshot.pop_live == 1 && snapshot.notification_live == 1
        },
        "three deferred registrations",
    )
    .await;

    assert!(
        producer.unbind_message_store(),
        "test should make Store replay unavailable"
    );
    let context = runtime.pull_message_context_for_test();
    for topic in [PULL_TOPIC, NOTIFICATION_TOPIC] {
        for tag in ["wrong", MATCH_TAG] {
            let result = context
                .store()
                .put_message_for_test(tagged_message(topic, tag))
                .await
                .expect("append replay test message");
            assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
        }
    }
    let result = context
        .store()
        .put_message_for_test(tagged_message(POP_TOPIC, "wrong"))
        .await
        .expect("append POP frozen-filter miss");
    assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
    drop(context);
    let mut queue_offsets = [0; 3];
    for _ in 0..100 {
        runtime.reput_message_store_once_for_test().await;
        let store = runtime.composition.state.message_store().expect("running Store");
        for (slot, topic) in queue_offsets
            .iter_mut()
            .zip([PULL_TOPIC, POP_TOPIC, NOTIFICATION_TOPIC])
        {
            *slot = store.get_max_offset_in_queue(&CheetahString::from_slice(topic), 0);
        }
        if queue_offsets == [2, 1, 2] {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        queue_offsets,
        [2, 1, 2],
        "the first replay phase must expose only the POP miss"
    );
    // Arrival callbacks carry the next logical upper bound; each service converts it to the
    // corresponding zero-based Store offset before replay.
    for logic_offset in [1, 2] {
        producer.route_pull_arrival(
            &CheetahString::from_static_str(PULL_TOPIC),
            0,
            logic_offset,
            None,
            0,
            None,
            None,
        );
        producer.route_notification_arrival_at(
            &CheetahString::from_static_str(NOTIFICATION_TOPIC),
            0,
            logic_offset,
            None,
            0,
            None,
            None,
        );
    }
    producer.route_pop_arrival_at(&CheetahString::from_static_str(POP_TOPIC), 0, 1, None, 0, None, None);
    wait_until(
        || {
            let snapshot = runtime
                .composition
                .data_plane
                .deferred
                .as_ref()
                .expect("deferred lifecycle")
                .resource_snapshot();
            snapshot.pull_pending_replays > 0
                && snapshot.pop_pending_replays > 0
                && snapshot.notification_pending_replays > 0
        },
        "unavailable Store pending replay retention",
    )
    .await;
    let unavailable = runtime
        .composition
        .data_plane
        .deferred
        .as_ref()
        .expect("deferred lifecycle")
        .resource_snapshot();
    assert_eq!(unavailable.pull_live, 1, "{unavailable:?}");
    assert_eq!(unavailable.pop_live, 1, "{unavailable:?}");
    assert_eq!(unavailable.notification_live, 1, "{unavailable:?}");
    assert!(unavailable.pull_pending_replays > 0, "{unavailable:?}");
    assert!(unavailable.pop_pending_replays > 0, "{unavailable:?}");
    assert!(unavailable.notification_pending_replays > 0, "{unavailable:?}");

    let store = runtime.composition.state.message_store_weak().expect("running Store");
    producer.bind_message_store(store).expect("rebind Store replay");
    producer.retry_pending_for_test();
    let range_matches = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = runtime
                .composition
                .data_plane
                .deferred
                .as_ref()
                .expect("deferred lifecycle")
                .resource_snapshot();
            if snapshot.pull_live == 0
                && snapshot.notification_live == 0
                && snapshot.pop_live == 1
                && snapshot.pop_pending_replays == 0
                && snapshot.pull_resume_executions == 0
                && snapshot.pull_active_continuations == 0
                && snapshot.notification_resume_executions == 0
                && snapshot.notification_active_continuations == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        range_matches.is_ok(),
        "range matches and POP exact frozen-filter miss: {:?}",
        runtime
            .composition
            .data_plane
            .deferred
            .as_ref()
            .expect("deferred lifecycle")
            .resource_snapshot()
    );

    assert!(
        producer.unbind_message_store(),
        "reput requires exclusive Store ownership before the POP match phase"
    );
    let context = runtime.pull_message_context_for_test();
    let result = context
        .store()
        .put_message_for_test(tagged_message(POP_TOPIC, MATCH_TAG))
        .await
        .expect("append POP frozen-filter match");
    assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
    drop(context);
    let mut pop_max_offset = 1;
    for _ in 0..100 {
        runtime.reput_message_store_once_for_test().await;
        pop_max_offset = runtime
            .composition
            .state
            .message_store()
            .expect("running Store")
            .get_max_offset_in_queue(&CheetahString::from_static_str(POP_TOPIC), 0);
        if pop_max_offset == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(pop_max_offset, 2, "POP match must become visible before replay");
    wait_until(
        || {
            runtime
                .composition
                .data_plane
                .deferred
                .as_ref()
                .expect("deferred lifecycle")
                .resource_snapshot()
                .pop_pending_replays
                > 0
        },
        "POP match reservation while Store replay is unavailable",
    )
    .await;
    let pop_retry = runtime
        .composition
        .data_plane
        .deferred
        .as_ref()
        .expect("deferred lifecycle")
        .resource_snapshot();
    assert_eq!(pop_retry.pop_live, 1, "{pop_retry:?}");
    assert!(pop_retry.pop_pending_replays > 0, "{pop_retry:?}");
    let store = runtime.composition.state.message_store_weak().expect("running Store");
    producer.bind_message_store(store).expect("rebind POP Store replay");
    producer.retry_pending_for_test();
    let all_terminals = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = runtime
                .composition
                .data_plane
                .deferred
                .as_ref()
                .expect("deferred lifecycle")
                .resource_snapshot();
            if snapshot.pull_live == 0
                && snapshot.pop_live == 0
                && snapshot.notification_live == 0
                && snapshot.pull_pending_replays == 0
                && snapshot.pop_pending_replays == 0
                && snapshot.notification_pending_replays == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        all_terminals.is_ok(),
        "all canonical deferred terminals: {:?}",
        runtime
            .composition
            .data_plane
            .deferred
            .as_ref()
            .expect("deferred lifecycle")
            .resource_snapshot()
    );

    let (pull_frames, pop_frames, notification_frames) = tokio::join!(
        pull_server.receive_one_then_finish_and_collect(pull_client),
        pop_server.receive_one_then_finish_and_collect(pop_client),
        notification_server.receive_one_then_finish_and_collect(notification_client),
    );
    for (frames, opaque) in [
        (pull_frames, 91_001),
        (pop_frames, 91_002),
        (notification_frames, 91_003),
    ] {
        assert_eq!(frames.len(), 1, "one canonical response for opaque {opaque}");
        assert_eq!(frames[0].opaque(), opaque);
        assert_eq!(
            frames[0].code(),
            ResponseCode::Success as i32,
            "canonical response for opaque {opaque}"
        );
    }

    let report = runtime
        .shutdown_basic_service_until(ShutdownDeadline::after(Duration::from_secs(10)))
        .await;
    assert!(
        report
            .deferred_producer_tasks
            .as_ref()
            .is_some_and(rocketmq_runtime::ShutdownReport::is_healthy),
        "{report:?}"
    );
    let terminal = runtime
        .composition
        .data_plane
        .deferred
        .as_ref()
        .expect("deferred lifecycle retained for terminal audit")
        .resource_snapshot();
    assert!(terminal.is_zero(), "{terminal:?}");
    drop(runtime);
    let _ = std::fs::remove_dir_all(root);
}
