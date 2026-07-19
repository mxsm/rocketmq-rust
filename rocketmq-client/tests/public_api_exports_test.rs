#![recursion_limit = "256"]

use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::AckCallback;
use rocketmq_client_rust::AckCallbackFn;
use rocketmq_client_rust::AckResult;
use rocketmq_client_rust::AckStatus;
use rocketmq_client_rust::AclClientRPCHook;
use rocketmq_client_rust::AdminToolResult;
use rocketmq_client_rust::AdminToolsResultCodeEnum;
use rocketmq_client_rust::AllocateMessageQueueAveragely;
use rocketmq_client_rust::AllocateMessageQueueAveragelyByCircle;
use rocketmq_client_rust::AllocateMessageQueueByConfig;
use rocketmq_client_rust::AllocateMessageQueueByMachineRoom;
use rocketmq_client_rust::AllocateMessageQueueConsistentHash;
use rocketmq_client_rust::AllocateMessageQueueStrategy;
use rocketmq_client_rust::ArcMessageQueueListener;
use rocketmq_client_rust::ArcTraceDispatcher;
use rocketmq_client_rust::AsyncTraceDispatcher;
use rocketmq_client_rust::ConsumeMessageContext;
use rocketmq_client_rust::ConsumeMessageHook;
use rocketmq_client_rust::ConsumeMessageHookArc;
use rocketmq_client_rust::ControllableOffset;
use rocketmq_client_rust::DefaultLitePullConsumer;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::DefaultMQAdminExtImpl;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::HashFunction;
use rocketmq_client_rust::JavaHashCode;
use rocketmq_client_rust::LitePullConsumer;
use rocketmq_client_rust::MQAdminExt;
use rocketmq_client_rust::MQAdminExtInner;
use rocketmq_client_rust::MQAdminExtInnerImpl;
use rocketmq_client_rust::MQConsumer;
use rocketmq_client_rust::MQProducer;
use rocketmq_client_rust::MQPushConsumer;
use rocketmq_client_rust::MessageQueueListener;
use rocketmq_client_rust::MessageQueueSelector;
use rocketmq_client_rust::MessageSelector;
use rocketmq_client_rust::NameserverAccessConfig;
use rocketmq_client_rust::NotifyResult;
use rocketmq_client_rust::OffsetSerialize;
use rocketmq_client_rust::OffsetSerializeWrapper;
use rocketmq_client_rust::OffsetStore;
use rocketmq_client_rust::PopCallbackFn;
use rocketmq_client_rust::PopResult;
use rocketmq_client_rust::PopStatus;
use rocketmq_client_rust::PullCallbackFn;
use rocketmq_client_rust::PullResult;
use rocketmq_client_rust::PullStatus;
use rocketmq_client_rust::ReadOffsetType;
use rocketmq_client_rust::SelectMessageQueueByHash;
use rocketmq_client_rust::SelectMessageQueueByMachineRoom;
use rocketmq_client_rust::SelectMessageQueueByRandom;
use rocketmq_client_rust::SessionCredentials;
use rocketmq_client_rust::TopicMessageQueueChangeListener;
use rocketmq_client_rust::TraceDispatcher;
use rocketmq_client_rust::TraceDispatcherOperation;
use rocketmq_client_rust::TraceDispatcherType;
use rocketmq_client_rust::TraceType;
use rocketmq_client_rust::TransactionMQProducer;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQError;

struct CustomOrderKey(i32);

impl JavaHashCode for CustomOrderKey {
    fn java_hash_code(&self) -> i32 {
        self.0
    }
}

struct RootHashFunction;

impl HashFunction for RootHashFunction {
    fn hash(&self, key: &str) -> i64 {
        if key.starts_with("client-a-") {
            return 100;
        }
        if key.starts_with("client-b-") {
            return 200;
        }
        if key.contains("queueId=0") {
            return 50;
        }
        150
    }
}

struct RootAckCallback;

impl AckCallback for RootAckCallback {
    fn on_success(&self, ack_result: AckResult) {
        assert_eq!(ack_result.status(), AckStatus::Ok);
    }

    fn on_exception(&self, _e: rocketmq_error::RocketMQError) {}
}

struct RootMessageQueueListener;

impl MessageQueueListener for RootMessageQueueListener {
    fn message_queue_changed(&self, topic: &str, mq_all: &HashSet<MessageQueue>, mq_assigned: &HashSet<MessageQueue>) {
        assert_eq!(topic, "TopicA");
        assert_eq!(mq_all.len(), mq_assigned.len());
    }
}

struct RootTopicMessageQueueChangeListener;

impl TopicMessageQueueChangeListener for RootTopicMessageQueueChangeListener {
    fn on_changed(&self, topic: &str, message_queues: HashSet<MessageQueue>) {
        assert_eq!(topic, "TopicA");
        assert_eq!(message_queues.len(), 1);
    }
}

struct RootConsumeHook;

impl ConsumeMessageHook for RootConsumeHook {
    fn hook_name(&self) -> &'static str {
        "RootConsumeHook"
    }

    fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
        context.add_prop("rootHook", "before");
    }

    fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
        context.add_prop("rootHook", "after");
    }
}

fn assert_unsupported_error(error: RocketMQError, api: &str, replacement: &str) {
    match error {
        RocketMQError::IllegalArgument(message) => {
            assert!(message.contains(api), "error should name {api}: {message}");
            assert!(
                message.contains("not supported"),
                "error should be explicit unsupported text: {message}"
            );
            assert!(
                message.contains(replacement),
                "error should name replacement {replacement}: {message}"
            );
        }
        other => panic!("expected IllegalArgument unsupported error for {api}, got {other:?}"),
    }
}

#[test]
fn crate_root_exports_modern_java_strategy_and_selector_api() {
    let group = CheetahString::from_static_str("group-a");
    let consumers = vec![
        CheetahString::from_static_str("client-a"),
        CheetahString::from_static_str("client-b"),
    ];
    let queues = vec![
        MessageQueue::from_parts("TopicA", "room-a@broker-a", 0),
        MessageQueue::from_parts("TopicA", "room-b@broker-b", 1),
    ];
    let msg = Message::builder()
        .topic("TopicA")
        .body_slice(b"body")
        .build()
        .expect("message should build");

    let avg = AllocateMessageQueueAveragely;
    let avg_circle = AllocateMessageQueueAveragelyByCircle;
    let by_config = AllocateMessageQueueByConfig::new(queues.clone());
    let by_machine_room =
        AllocateMessageQueueByMachineRoom::new(HashSet::from([CheetahString::from_static_str("room-a")]));
    let consistent_hash = AllocateMessageQueueConsistentHash::default();

    assert_eq!(avg.get_name(), "AVG");
    assert_eq!(avg_circle.get_name(), "AVG_BY_CIRCLE");
    assert_eq!(by_config.get_name(), "CONFIG");
    assert_eq!(by_machine_room.get_name(), "MACHINE_ROOM");
    assert_eq!(consistent_hash.get_name(), "CONSISTENT_HASH");
    let custom_hash = AllocateMessageQueueConsistentHash::with_hash_function(1, Arc::new(RootHashFunction))
        .expect("root-exported HashFunction should work with consistent hash strategy");
    assert_eq!(
        custom_hash
            .allocate(&group, &consumers[0], &queues, &consumers)
            .expect("custom consistent hash allocation should succeed"),
        vec![queues[0].clone()]
    );
    let allocated_total = consumers
        .iter()
        .map(|consumer| {
            consistent_hash
                .allocate(&group, consumer, &queues, &consumers)
                .expect("consistent hash allocation should succeed")
                .len()
        })
        .sum::<usize>();
    assert_eq!(allocated_total, queues.len());

    let hash_selector = SelectMessageQueueByHash;
    let random_selector = SelectMessageQueueByRandom;
    let machine_room_selector =
        SelectMessageQueueByMachineRoom::new(HashSet::from([CheetahString::from_static_str("room-a")]));

    assert!(hash_selector.select(&queues, &msg, &"order-a").is_some());
    assert_eq!(
        hash_selector
            .select(&queues, &msg, &CustomOrderKey(1))
            .expect("custom JavaHashCode key should select a queue")
            .queue_id(),
        1
    );
    assert!(random_selector.select(&queues, &msg, &()).is_some());
    assert_eq!(
        machine_room_selector
            .select(&queues, &msg, &())
            .expect("machine-room selector should choose room-a")
            .broker_name()
            .as_str(),
        "room-a@broker-a"
    );
}

#[test]
#[allow(deprecated)]
fn crate_root_legacy_java_apis_return_typed_unsupported_errors() {
    let consumer = rocketmq_client_rust::DefaultMQPullConsumer::with_consumer_group("legacy-group");
    assert_eq!(
        consumer.consumer_group().map(|group| group.as_str()),
        Some("legacy-group")
    );
    assert_unsupported_error(
        consumer
            .start()
            .expect_err("deprecated pull consumer should reject start"),
        "DefaultMQPullConsumer",
        "DefaultLitePullConsumer",
    );
    assert_unsupported_error(
        consumer
            .default_mq_pull_consumer_impl()
            .expect_err("deprecated pull consumer impl should be unavailable"),
        "DefaultMQPullConsumerImpl",
        "DefaultLitePullConsumer",
    );
    assert_unsupported_error(
        rocketmq_client_rust::DefaultMQPullConsumerImpl::new()
            .expect_err("deprecated pull consumer impl constructor should be unavailable"),
        "DefaultMQPullConsumerImpl",
        "DefaultLitePullConsumer",
    );

    let schedule_service = rocketmq_client_rust::MQPullConsumerScheduleService::new("legacy-group");
    assert_eq!(schedule_service.consumer_group().as_str(), "legacy-group");
    assert_unsupported_error(
        schedule_service
            .start()
            .expect_err("deprecated pull schedule service should reject start"),
        "MQPullConsumerScheduleService",
        "DefaultLitePullConsumer",
    );

    assert_unsupported_error(
        rocketmq_client_rust::MQHelper::reset_offset_by_timestamp("CLUSTERING", "legacy-group", "TopicA", 0)
            .expect_err("deprecated MQHelper should be unavailable"),
        "MQHelper",
        "DefaultLitePullConsumer",
    );

    let send_hook = rocketmq_client_rust::SendMessageOpenTracingHookImpl::new(());
    assert_eq!(send_hook.hook_name(), "SendMessageOpenTracingHook");
    assert_unsupported_error(
        send_hook
            .unsupported()
            .expect_err("OpenTracing send hook should be unavailable"),
        "SendMessageOpenTracingHookImpl",
        "RocketMQ trace hooks",
    );

    let consume_hook = rocketmq_client_rust::ConsumeMessageOpenTracingHookImpl::new(());
    assert_eq!(consume_hook.hook_name(), "ConsumeMessageOpenTracingHook");
    assert_unsupported_error(
        consume_hook
            .unsupported()
            .expect_err("OpenTracing consume hook should be unavailable"),
        "ConsumeMessageOpenTracingHookImpl",
        "RocketMQ trace hooks",
    );

    let transaction_hook = rocketmq_client_rust::EndTransactionOpenTracingHookImpl::new(());
    assert_eq!(transaction_hook.hook_name(), "EndTransactionOpenTracingHook");
    assert_unsupported_error(
        transaction_hook
            .unsupported()
            .expect_err("OpenTracing transaction hook should be unavailable"),
        "EndTransactionOpenTracingHookImpl",
        "RocketMQ trace hooks",
    );
}

#[test]
fn crate_root_exports_modern_client_facades_and_traits() {
    fn assert_mq_producer<T: MQProducer>() {}
    fn assert_mq_consumer<T: MQConsumer>() {}
    fn assert_mq_push_consumer<T: MQPushConsumer>() {}
    fn assert_lite_pull_consumer<T: LitePullConsumer>() {}

    assert_mq_producer::<DefaultMQProducer>();
    assert_mq_producer::<TransactionMQProducer>();
    assert_mq_consumer::<DefaultMQPushConsumer>();
    assert_mq_push_consumer::<DefaultMQPushConsumer>();
    assert_lite_pull_consumer::<DefaultLitePullConsumer>();

    let producer = DefaultMQProducer::builder()
        .producer_group("public-api-producer")
        .build();
    let producer_impl = producer
        .get_default_mq_producer_impl()
        .expect("the public producer facade should expose its implementation root");
    assert_eq!(Arc::strong_count(producer_impl), 1);
    let _transaction_producer = TransactionMQProducer::builder()
        .producer_group("public-api-transaction-producer")
        .build();
    let _push_consumer = DefaultMQPushConsumer::builder()
        .consumer_group("public-api-push-consumer")
        .build();
    let lite_pull_consumer = DefaultLitePullConsumer::builder()
        .consumer_group("public-api-lite-pull-consumer")
        .build()
        .expect("lite pull consumer should build");
    let client_config: Arc<_> = lite_pull_consumer.client_config();
    let consumer_config: Arc<_> = lite_pull_consumer.consumer_config();
    let consumer_group: CheetahString = lite_pull_consumer.consumer_group();
    assert!(!client_config.is_use_tls());
    assert_eq!(consumer_config.consumer_group, consumer_group);

    let selector = MessageSelector::by_tag("TagA || TagB");
    assert_eq!(selector.get_expression().as_str(), "TagA || TagB");
}

#[tokio::test]
async fn crate_root_exports_trace_dispatcher_api_for_custom_trace_wiring() {
    fn assert_trace_dispatcher<T: TraceDispatcher>() {}

    assert_trace_dispatcher::<AsyncTraceDispatcher>();
    assert_eq!(TraceDispatcherOperation::Produce, TraceDispatcherOperation::Produce);
    assert_eq!(TraceDispatcherType::Producer.as_str(), "PRODUCER");
    assert_eq!(TraceType::Recall.to_string(), "Recall");

    let dispatcher: ArcTraceDispatcher = Arc::new(AsyncTraceDispatcher::new(
        "public-api-trace-group",
        TraceDispatcherOperation::Produce,
        20,
        "PUBLIC_TRACE_TOPIC",
        None,
    ));

    assert_eq!(dispatcher.trace_topic_name(), Some("PUBLIC_TRACE_TOPIC"));

    let mut producer = DefaultMQProducer::builder()
        .producer_group("public-api-trace-producer")
        .trace_dispatcher(dispatcher.clone())
        .build();
    assert!(producer.get_trace_dispatcher().is_some());
    producer.set_trace_dispatcher(dispatcher.clone());
    assert!(producer.trace_dispatcher().is_some());

    let push_consumer = DefaultMQPushConsumer::builder()
        .consumer_group("public-api-trace-push-consumer")
        .trace_dispatcher(Some(dispatcher.clone()))
        .build();
    assert!(push_consumer.get_trace_dispatcher().is_some());

    let lite_pull_consumer = DefaultLitePullConsumer::builder()
        .consumer_group("public-api-trace-lite-pull-consumer")
        .trace_dispatcher(dispatcher)
        .build()
        .expect("lite pull consumer should accept root-exported trace dispatcher");
    assert!(lite_pull_consumer.get_trace_dispatcher().await.is_some());
}

#[test]
fn crate_root_exports_modern_admin_facades_and_results() {
    fn assert_mq_admin_ext<T: MQAdminExt>() {}
    fn assert_mq_admin_ext_inner<T: MQAdminExtInner>() {}
    fn assert_as_ref_admin_impl<T: AsRef<DefaultMQAdminExtImpl>>() {}

    assert_mq_admin_ext::<DefaultMQAdminExt>();
    assert_mq_admin_ext::<DefaultMQAdminExtImpl>();
    assert_mq_admin_ext_inner::<MQAdminExtInnerImpl>();
    assert_as_ref_admin_impl::<DefaultMQAdminExt>();

    let admin = DefaultMQAdminExt::new();
    assert!(admin.has_inner());
    assert!(admin.as_ref().has_inner());

    let success = AdminToolResult::success("ok");
    assert!(success.is_success());
    assert_eq!(success.get_code(), AdminToolsResultCodeEnum::Success.get_code());
    assert_eq!(success.get_data(), Some(&"ok"));

    let failure = AdminToolResult::<()>::failure(AdminToolsResultCodeEnum::MQClientError, "client error".to_string());
    assert!(!failure.is_success());
    assert_eq!(failure.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());
    assert_eq!(failure.get_error_msg(), "client error");
}

#[tokio::test]
async fn crate_root_exports_java_style_admin_list_user_alias() {
    let admin = DefaultMQAdminExt::new();
    let error = admin
        .list_user(CheetahString::from("127.0.0.1:10911"), CheetahString::new())
        .await
        .expect_err("list_user should delegate to the same started-client requirement as list_users");

    assert!(matches!(error, RocketMQError::ClientNotStarted));
}

#[test]
fn crate_root_exports_acl_and_nameserver_access_api() {
    let credentials = SessionCredentials::with_token("ak", "sk", "token");
    let hook = AclClientRPCHook::new(credentials);

    assert_eq!(
        hook.session_credentials().access_key().map(|value| value.as_str()),
        Some("ak")
    );
    assert_eq!(
        hook.session_credentials().secret_key().map(|value| value.as_str()),
        Some("sk")
    );
    assert_eq!(
        hook.session_credentials().security_token().map(|value| value.as_str()),
        Some("token")
    );

    let nameserver_access = NameserverAccessConfig::new("127.0.0.1:9876", "domain", "subgroup");
    assert_eq!(nameserver_access.namesrv_addr().as_str(), "127.0.0.1:9876");
    assert_eq!(nameserver_access.namesrv_domain().as_str(), "domain");
    assert_eq!(nameserver_access.namesrv_domain_subgroup().as_str(), "subgroup");
}

#[test]
fn crate_root_exports_consumer_result_status_callback_and_offset_types() {
    let mut notify_result = NotifyResult::default();
    assert!(!notify_result.is_has_msg());
    assert!(!notify_result.is_polling_full());
    notify_result.set_has_msg(true);
    notify_result.set_polling_full(true);
    assert!(notify_result.has_msg());
    assert!(notify_result.polling_full());
    assert_eq!(notify_result.to_string(), "NotifyResult{hasMsg=true, pollingFull=true}");

    let ack_result = AckResult::default();
    assert_eq!(ack_result.status(), AckStatus::Ok);
    assert_eq!(AckStatus::from_i32(1), Some(AckStatus::NotExist));

    let ack_callback = RootAckCallback;
    ack_callback.on_success(ack_result.clone());
    let ack_callback_fn: AckCallbackFn = Box::new(|result| {
        assert_eq!(result.status(), AckStatus::Ok);
        Ok(())
    });
    ack_callback_fn(ack_result).expect("ack callback should succeed");

    let pop_result = PopResult {
        pop_status: PopStatus::PollingNotFound,
        rest_num: 2,
        ..Default::default()
    };
    assert_eq!(
        pop_result.to_string(),
        "PopResult [popStatus=POLLING_NOT_FOUND,msgFoundList=0,restNum=2]"
    );

    let pull_result = PullResult::new(PullStatus::NoMatchedMsg, 12, 3, 45, None);
    assert_eq!(pull_result.pull_status(), &PullStatus::NoMatchedMsg);
    assert_eq!(pull_result.next_begin_offset(), 12);
    assert_eq!(i32::from(PopStatus::NoNewMsg), 1);
    assert_eq!(i32::from(PullStatus::OffsetIllegal), 3);

    let _pop_callback_fn: Option<PopCallbackFn> = None;
    let _pull_callback_fn: Option<PullCallbackFn> = None;

    let queue = MessageQueue::from_parts("TopicA", "broker-a", 0);
    let all_queues = HashSet::from([queue.clone()]);
    let assigned_queues = HashSet::from([queue.clone()]);
    let queue_listener: ArcMessageQueueListener = Arc::new(RootMessageQueueListener);
    queue_listener.message_queue_changed("TopicA", &all_queues, &assigned_queues);

    let topic_listener = RootTopicMessageQueueChangeListener;
    topic_listener.on_changed("TopicA", HashSet::from([queue.clone()]));

    let mut push_consumer = DefaultMQPushConsumer::builder()
        .consumer_group("public-api-push-hook-consumer")
        .build();
    push_consumer
        .register_consume_message_hook(RootConsumeHook)
        .expect("root-exported ConsumeMessageHook should register on push consumer");
    let _consume_hook_arc: Option<ConsumeMessageHookArc> = None;

    let controllable_offset = ControllableOffset::new(10);
    controllable_offset.update(15, true);
    assert_eq!(controllable_offset.get_offset(), 15);
    controllable_offset.update(12, true);
    assert_eq!(controllable_offset.get_offset(), 15);
    assert_eq!(
        ReadOffsetType::MemoryFirstThenStore.to_string(),
        "MEMORY_FIRST_THEN_STORE"
    );
    assert_eq!(i32::from(ReadOffsetType::ReadFromStore), 1);

    let mut wrapper = OffsetSerializeWrapper::default();
    wrapper.offset_table.insert(queue, AtomicI64::new(42));
    let offset_serialize = OffsetSerialize::from(wrapper);
    assert_eq!(offset_serialize.offset_table.values().next().copied(), Some(42));

    let offset_store: Option<OffsetStore> = None;
    assert!(offset_store.is_none());
}
