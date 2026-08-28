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

use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::filter::expression_type::ExpressionType;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::request_source::RequestSource;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageStatus;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
use rocketmq_transport::api::v2::DeferredWakeReason;
use rocketmq_transport::api::v2::ResponseBodyKind;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::TestChannelBuilder;

use super::PullMessageProcessor;
use crate::broker_runtime::BrokerMessageStore;
use crate::broker_runtime::BrokerRuntime;
use crate::client::client_channel_info::ClientChannelInfo;
use crate::config::broker_config::BrokerConfig;
use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::long_polling::pull_deferred::PullHookMetadata;
use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;
use crate::processor::pull_message_processor::capability::PullMessageProcessorContext;
use crate::processor::pull_message_processor::capability::PullStoreReadBarrier;

fn temp_test_root(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("rocketmq-rust-pull-resume-{}-{label}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create Pull resume test root");
    path
}

fn available_ha_port() -> usize {
    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("reserve an ephemeral Pull resume HA port")
        .local_addr()
        .expect("read the ephemeral Pull resume HA port")
        .port() as usize
}

async fn runtime(label: &str) -> (BrokerRuntime, PathBuf) {
    let root = temp_test_root(label);
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        auth_config_path: root.join("auth.json").to_string_lossy().into_owned().into(),
        enable_broadcast_offset_store: true,
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        ha_listen_port: available_ha_port(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    runtime.initialize().await.expect("initialize Pull resume runtime");
    runtime.seed_pop_topic_and_group_for_test("topic-a", "group-a");
    runtime
        .start_message_store_for_test()
        .await
        .expect("start Pull resume message store");
    (runtime, root)
}

fn processor(
    context: Arc<PullMessageProcessorContext<BrokerMessageStore>>,
) -> Arc<PullMessageProcessor<BrokerMessageStore>> {
    let handler = Arc::new(DefaultPullMessageResultHandler::new(
        Arc::new(Vec::new()),
        Arc::clone(&context),
        None,
    ));
    Arc::new(PullMessageProcessor::new(handler, context))
}

fn request_header(queue_offset: i64, suspend: bool) -> PullMessageRequestHeader {
    PullMessageRequestHeader {
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("topic-a"),
        queue_id: 0,
        queue_offset,
        max_msg_nums: 1,
        sys_flag: PullSysFlag::build_sys_flag(false, suspend, true, false) as i32,
        commit_offset: 0,
        suspend_timeout_millis: 60_000,
        sub_version: 1,
        subscription: Some(CheetahString::from_static_str("*")),
        expression_type: Some(CheetahString::from_static_str(ExpressionType::TAG)),
        ..PullMessageRequestHeader::default()
    }
}

fn stored_message() -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str("topic-a"));
    message.message_ext_inner.set_queue_id(0);
    message.set_body(Bytes::from_static(b"deferred-pull-message"));
    message.set_wait_store_msg_ok(false);
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

async fn test_channel() -> Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind Pull V1 listener");
    let server_addr = listener.local_addr().expect("Pull V1 listener address");
    let accept = tokio::spawn(async move { listener.accept().await.expect("accept Pull V1 stream").0 });
    let stream = tokio::net::TcpStream::connect(server_addr)
        .await
        .expect("connect Pull V1 stream");
    let local_addr = stream.local_addr().expect("Pull V1 local address");
    let remote_addr = stream.peer_addr().expect("Pull V1 remote address");
    let peer = accept.await.expect("join Pull V1 accept task");
    drop(peer);
    TestChannelBuilder::new(
        Connection::new(stream),
        crate::test_task_group("pull-v1-suspension-channel"),
    )
    .addresses(local_addr, remote_addr)
    .build()
    .expect("build Pull V1 channel")
}

#[tokio::test]
async fn real_store_core_rereads_for_each_wake_reason_and_v1_suspension_stays_none() {
    let (mut runtime, root) = runtime("real-store-v1-parity").await;
    let context = runtime.pull_message_context_for_test();
    let processor = processor(Arc::clone(&context));
    let hold = Arc::new(PullRequestHoldService::new(Arc::downgrade(&processor)));
    assert!(context.install_pull_request_hold_service(Arc::clone(&hold)));

    let channel = test_channel().await;
    let ctx = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
    let mut fallback_request =
        RemotingCommand::create_request_command(RequestCode::PullMessage, request_header(0, true)).set_opaque(731);
    fallback_request.make_custom_header_to_net();
    let fallback = processor
        .process_request_shared(channel.clone(), Arc::clone(&ctx), &mut fallback_request)
        .await
        .expect("empty V1 Pull before hold-service start")
        .expect("inactive V1 hold service must return the PullNotFound fallback");
    assert_eq!(fallback.code(), ResponseCode::PullNotFound as i32);
    assert_eq!(fallback.opaque(), 731);
    assert!(fallback.body().is_none());

    PullRequestHoldService::start(&hold, crate::test_task_group("pull-v1-suspension-hold")).await;
    let mut suspended_request =
        RemotingCommand::create_request_command(RequestCode::PullMessage, request_header(0, true)).set_opaque(732);
    suspended_request.make_custom_header_to_net();
    let suspended = processor
        .process_request_shared(channel, ctx, &mut suspended_request)
        .await
        .expect("empty V1 Pull with active hold service");
    assert!(suspended.is_none(), "legacy admitted suspension must remain Ok(None)");
    hold.shutdown().await;

    let hook_metadata = PullHookMetadata::default();
    let effective_peer = "127.0.0.1:19001".parse().expect("Pull effective peer");
    let no_broadcast_client = |_header: &PullMessageRequestHeader| Ok(None);
    let empty = processor
        .resume_pull_parts(
            RequestCode::PullMessage,
            request_header(0, false),
            effective_peer,
            &hook_metadata,
            &no_broadcast_client,
            DeferredWakeReason::Timeout,
        )
        .await
        .expect("empty Pull timeout reread");
    assert_eq!(empty.response_code(), ResponseCode::PullNotFound as i32);
    assert_eq!(empty.body_kind(), ResponseBodyKind::Empty);
    assert_eq!(empty.body_len(), 0);
    drop(empty);

    let put = context
        .store()
        .put_message_for_test(stored_message())
        .await
        .expect("append Pull resume message");
    assert_eq!(put.put_message_status(), PutMessageStatus::PutOk);
    runtime.reput_message_store_once_for_test().await;

    let found = processor
        .resume_pull_parts(
            RequestCode::PullMessage,
            request_header(0, false),
            effective_peer,
            &hook_metadata,
            &no_broadcast_client,
            DeferredWakeReason::MessageArrived,
        )
        .await
        .expect("message-arrived Pull reread");
    assert_eq!(found.response_code(), ResponseCode::Success as i32);
    assert_ne!(found.body_kind(), ResponseBodyKind::Empty);
    assert!(found.body_len() > 0);
    drop(found);

    let forced = processor
        .resume_pull_parts(
            RequestCode::PullMessage,
            request_header(1, false),
            effective_peer,
            &hook_metadata,
            &no_broadcast_client,
            DeferredWakeReason::ForcedRefresh,
        )
        .await
        .expect("forced-refresh Pull reread");
    assert_eq!(forced.response_code(), ResponseCode::PullNotFound as i32);
    assert_eq!(forced.body_kind(), ResponseBodyKind::Empty);
    drop(forced);

    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn broadcast_client_resolution_requires_normal_session_lookup_and_proxy_bypasses_it() {
    let (mut runtime, root) = runtime("broadcast-client-resolution").await;
    let context = runtime.pull_message_context_for_test();
    let processor = processor(Arc::clone(&context));
    let channel = test_channel().await;
    context.consumers().register_consumer_without_sub(
        &CheetahString::from_static_str("group-a"),
        ClientChannelInfo::new(
            channel,
            CheetahString::from_static_str("registered-client"),
            LanguageCode::JAVA,
            1,
        ),
        ConsumeType::ConsumePassively,
        MessageModel::Broadcasting,
        ConsumeFromWhere::ConsumeFromLastOffset,
        false,
    );

    let normal = request_header(0, false);
    let lookup = RwLock::new(Some(CheetahString::from_static_str("session-client")));
    let calls = AtomicUsize::new(0);
    let resolve_current = || {
        calls.fetch_add(1, Ordering::SeqCst);
        lookup
            .read()
            .expect("session client test lock")
            .clone()
            .map(Some)
            .ok_or_else(|| {
                rocketmq_error::RocketMQError::invariant_violated("current Pull session registration is missing")
            })
    };
    let present = processor
        .resolve_broadcast_client_id_with(&normal, resolve_current)
        .expect("normal broadcast client lookup");
    assert_eq!(present.as_deref(), Some("session-client"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    *lookup.write().expect("session client test lock") = None;
    let missing = processor.resolve_broadcast_client_id_with(&normal, resolve_current);
    assert!(
        missing.is_err(),
        "normal broadcast must fail closed when session lookup misses"
    );

    *lookup.write().expect("session client test lock") = Some(CheetahString::from_static_str("replacement-client"));
    let replacement = processor
        .resolve_broadcast_client_id_with(&normal, resolve_current)
        .expect("replacement session registration");
    assert_eq!(replacement.as_deref(), Some("replacement-client"));

    let mut proxy = request_header(0, false);
    proxy.request_source = Some(RequestSource::ProxyForBroadcast.get_value());
    proxy.proxy_forward_client_id = Some(CheetahString::from_static_str("forwarded-client"));
    let forwarded = processor
        .resolve_broadcast_client_id_with(&proxy, resolve_current)
        .expect("proxy broadcast client id");
    assert_eq!(forwarded.as_deref(), Some("forwarded-client"));
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn v1_broadcast_reresolves_client_after_store_await_before_offset_update() {
    let (mut runtime, root) = runtime("broadcast-reregister-during-store").await;
    let context = runtime.pull_message_context_for_test();
    let processor = processor(Arc::clone(&context));

    for _ in 0..2 {
        let put = context
            .store()
            .put_message_for_test(stored_message())
            .await
            .expect("append broadcast Pull message");
        assert_eq!(put.put_message_status(), PutMessageStatus::PutOk);
    }
    runtime.reput_message_store_once_for_test().await;

    let channel = test_channel().await;
    let old_registration = ClientChannelInfo::new(
        channel.clone(),
        CheetahString::from_static_str("old-client"),
        LanguageCode::JAVA,
        1,
    );
    context.consumers().register_consumer_without_sub(
        &CheetahString::from_static_str("group-a"),
        old_registration.clone(),
        ConsumeType::ConsumePassively,
        MessageModel::Broadcasting,
        ConsumeFromWhere::ConsumeFromLastOffset,
        false,
    );
    context.update_broadcast_offset("topic-a", "group-a", 0, 0, "old-client", false);

    let barrier = Arc::new(PullStoreReadBarrier::new());
    assert!(context.store().install_read_barrier_for_test(Arc::clone(&barrier)));
    let ctx = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
    let mut request =
        RemotingCommand::create_request_command(RequestCode::PullMessage, request_header(1, false)).set_opaque(913);
    request.make_custom_header_to_net();
    let request_task = {
        let processor = Arc::clone(&processor);
        let channel = channel.clone();
        tokio::spawn(async move { processor.process_request_shared(channel, ctx, &mut request).await })
    };

    barrier.wait_until_entered().await;
    context
        .consumers()
        .unregister_consumer("group-a", &old_registration, false);
    context.consumers().register_consumer_without_sub(
        &CheetahString::from_static_str("group-a"),
        ClientChannelInfo::new(
            channel,
            CheetahString::from_static_str("new-client"),
            LanguageCode::JAVA,
            1,
        ),
        ConsumeType::ConsumePassively,
        MessageModel::Broadcasting,
        ConsumeFromWhere::ConsumeFromLastOffset,
        false,
    );
    barrier.release();

    request_task
        .await
        .expect("join broadcast Pull request")
        .expect("broadcast Pull request after re-registration");
    let new_client_offset = context.query_broadcast_offset("topic-a", "group-a", 0, "new-client", -1, true);
    let old_client_offset = context.query_broadcast_offset("topic-a", "group-a", 0, "old-client", -1, true);
    assert_eq!(
        new_client_offset, 1,
        "the current registration owns the post-store update"
    );
    assert_eq!(
        old_client_offset, 0,
        "the stale registration must not receive the post-store update"
    );

    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}
