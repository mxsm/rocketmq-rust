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

#[allow(unused_imports)]
use super::admin::*;
#[allow(unused_imports)]
use super::consumer::*;
#[allow(unused_imports)]
use super::producer::*;
#[allow(unused_imports)]
use super::request_builder::*;
#[allow(unused_imports)]
use super::response_decoder::*;
#[allow(unused_imports)]
use super::route::*;
#[allow(unused_imports)]
use super::transaction::*;
#[allow(unused_imports)]
use super::transport::*;

use std::future::pending;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;

#[cfg(feature = "admin-mutation")]
use rocketmq_model::common::lite::LiteSubscriptionAction;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::command_custom_header::FromMap;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use rocketmq_protocol::protocol::SerializeType;

use super::callback_executor::ClientCallbackExecutor;
use super::*;

fn retry_strategy() -> MQFaultStrategy {
    MQFaultStrategy::new(
        crate::runtime::test_service_context("mq-client-api-retry-test"),
        &ClientConfig::default(),
    )
}

fn topic_publish_info() -> TopicPublishInfo {
    let mut info = TopicPublishInfo::new();
    info.message_queue_list = vec![
        MessageQueue::from_parts("topicA", "broker-a", 0),
        MessageQueue::from_parts("topicA", "broker-b", 1),
    ];
    info
}

#[derive(Debug)]
struct AsyncRetryTestHeader {
    retry_marker: CheetahString,
}

impl CommandCustomHeader for AsyncRetryTestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([(
            CheetahString::from_static_str("retryMarker"),
            self.retry_marker.clone(),
        )]))
    }
}

impl FromMap for AsyncRetryTestHeader {
    type Error = rocketmq_error::RocketMQError;
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self, Self::Error> {
        let retry_marker = map
            .get(&CheetahString::from_static_str("retryMarker"))
            .cloned()
            .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("missing retryMarker test header"))?;
        Ok(Self { retry_marker })
    }
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, AtomicOrdering::Release);
    }
}

struct RecordingPopCallback {
    calls: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
    completed: Option<tokio::sync::oneshot::Sender<()>>,
}

impl PopCallback for RecordingPopCallback {
    async fn on_success(&mut self, _pop_result: PopResult) {
        self.calls.fetch_add(1, AtomicOrdering::AcqRel);
        if let Some(completed) = self.completed.take() {
            let _ = completed.send(());
        }
    }

    fn on_error(&mut self, _error: RocketMQError) {
        self.calls.fetch_add(1, AtomicOrdering::AcqRel);
        self.errors.fetch_add(1, AtomicOrdering::AcqRel);
        if let Some(completed) = self.completed.take() {
            let _ = completed.send(());
        }
    }
}

struct CountingAfterSendHook {
    calls: Arc<AtomicUsize>,
}

impl SendMessageHook for CountingAfterSendHook {
    fn hook_name(&self) -> &'static str {
        "CountingAfterSendHook"
    }

    fn send_message_before(&self, _context: &Option<SendMessageContext<'_>>) {}

    fn send_message_after(&self, context: &Option<SendMessageContext<'_>>) {
        assert_eq!(
            context.as_ref().and_then(|context| context.producer_group.as_deref()),
            Some("async-hook-group")
        );
        self.calls.fetch_add(1, AtomicOrdering::AcqRel);
    }
}

struct CapturingFailureHook {
    observed: Arc<std::sync::Mutex<Option<Arc<rocketmq_error::Error>>>>,
}

impl SendMessageHook for CapturingFailureHook {
    fn hook_name(&self) -> &'static str {
        "CapturingFailureHook"
    }

    fn send_message_before(&self, _context: &Option<SendMessageContext<'_>>) {}

    fn send_message_after(&self, context: &Option<SendMessageContext<'_>>) {
        let Some(RocketMQError::Shared(canonical)) = context.as_ref().and_then(|context| context.exception.as_deref())
        else {
            panic!("failure hook should receive the canonical shared carrier")
        };
        *self.observed.lock().expect("hook observation lock") = Some(Arc::clone(canonical));
    }
}

#[test]
fn async_send_after_hook_uses_immutable_hook_snapshot_without_producer_owner() {
    let calls = Arc::new(AtomicUsize::new(0));
    let hook: Arc<dyn SendMessageHook> = Arc::new(CountingAfterSendHook { calls: calls.clone() });
    let context_data = Some(AsyncSendHookContext {
        producer_group: Some(CheetahString::from_static_str("async-hook-group")),
        hooks: vec![hook].into(),
        ..Default::default()
    });

    MQClientAPIImpl::execute_async_send_hook_after(&context_data, None, None);

    assert_eq!(calls.load(AtomicOrdering::Acquire), 1);
}

#[tokio::test]
async fn async_failure_hook_and_callback_share_the_original_canonical_source() {
    let canonical = Arc::new(rocketmq_error::Error::caused_by(
        &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "physical reset"),
    ));
    let hook_observed = Arc::new(std::sync::Mutex::new(None));
    let hook: Arc<dyn SendMessageHook> = Arc::new(CapturingFailureHook {
        observed: Arc::clone(&hook_observed),
    });
    let context_data = Some(AsyncSendHookContext {
        hooks: vec![hook].into(),
        ..Default::default()
    });
    let (callback_tx, callback_rx) = std::sync::mpsc::channel();
    let callback: ArcSendCallback = Arc::new(move |_result: Option<&SendResult>, error: Option<&RocketMQError>| {
        let Some(RocketMQError::Shared(canonical)) = error else {
            panic!("failure callback should receive the canonical shared carrier")
        };
        callback_tx
            .send(Arc::clone(canonical))
            .expect("callback receiver remains open");
    });

    MQClientAPIImpl::finish_async_retry_failure(
        RocketMQError::Shared(Arc::clone(&canonical)),
        &ClientCallbackExecutor::new(1),
        &Some(callback),
        &context_data,
    )
    .await;

    let hook_error = hook_observed
        .lock()
        .expect("hook observation lock")
        .take()
        .expect("hook should observe the failure");
    let callback_error = callback_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("callback should observe the failure");
    assert!(Arc::ptr_eq(&canonical, &hook_error));
    assert!(Arc::ptr_eq(&canonical, &callback_error));
    assert!(std::error::Error::source(canonical.as_ref())
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .is_some());
}

#[test]
fn java_long_to_u64_field_rejects_negative_protocol_values() {
    assert_eq!(
        java_long_to_u64_field("sendMessage", "queueOffset", 123).expect("positive Java long should convert"),
        123
    );

    let error =
        java_long_to_u64_field("pullMessage", "nextBeginOffset", -1).expect_err("negative broker offset must not wrap");

    assert!(error
        .to_string()
        .contains("pullMessage nextBeginOffset is negative and cannot be represented as Rust u64"));
}

#[test]
fn trace_on_from_ext_fields_matches_java_missing_default() {
    assert!(trace_on_from_ext_fields(None));
    assert!(trace_on_from_ext_fields(Some(&HashMap::new())));
}

#[test]
fn trace_on_from_ext_fields_only_false_disables_trace_like_java() {
    let trace_switch = CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_SWITCH);

    let mut fields = HashMap::new();
    fields.insert(trace_switch.clone(), CheetahString::from_static_str("false"));
    assert!(!trace_on_from_ext_fields(Some(&fields)));

    fields.insert(trace_switch.clone(), CheetahString::from_static_str("true"));
    assert!(trace_on_from_ext_fields(Some(&fields)));

    fields.insert(trace_switch.clone(), CheetahString::from_static_str("False"));
    assert!(trace_on_from_ext_fields(Some(&fields)));

    fields.insert(trace_switch, CheetahString::from_static_str("invalid"));
    assert!(trace_on_from_ext_fields(Some(&fields)));
}

#[test]
fn duration_millis_to_u64_rejects_values_outside_rust_range() {
    assert_eq!(
        duration_millis_to_u64("probeNameServer", Duration::from_millis(u64::MAX))
            .expect("max u64 millis should convert"),
        u64::MAX
    );

    let error = duration_millis_to_u64("probeNameServer", Duration::from_secs(u64::MAX))
        .expect_err("duration larger than u64 millis should fail");

    assert!(error
        .to_string()
        .contains("probeNameServer timeout exceeds Rust u64 millisecond range"));
}

#[test]
fn controller_leader_address_requires_controller_metadata_leader_like_java() {
    let leader_address = controller_leader_address(GetMetaDataResponseHeader {
        controller_leader_address: Some(CheetahString::from_static_str("127.0.0.1:9878")),
        ..Default::default()
    })
    .expect("leader address should be returned");

    assert_eq!(leader_address, CheetahString::from_static_str("127.0.0.1:9878"));

    let error = controller_leader_address(GetMetaDataResponseHeader::default())
        .expect_err("controller metadata without leader should be rejected");

    assert!(error.to_string().contains("Controller leader address"));
}

#[test]
fn controller_config_from_response_body_uses_java_properties_rules() {
    let body = b"
        # comment
        controllerType:Raft
        notifyBrokerRoleChanged = true
        blankValue
    ";

    let config = controller_config_from_response_body(body).expect("controller config body should parse");

    assert_eq!(
        config.get(&CheetahString::from_static_str("controllerType")),
        Some(&CheetahString::from_static_str("Raft"))
    );
    assert_eq!(
        config.get(&CheetahString::from_static_str("notifyBrokerRoleChanged")),
        Some(&CheetahString::from_static_str("true"))
    );
    assert_eq!(
        config.get(&CheetahString::from_static_str("blankValue")),
        Some(&CheetahString::new())
    );
}

#[test]
fn unit_topic_list_filters_retry_topics_like_java() {
    let mut topic_list = TopicList {
        topic_list: vec![
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("%RETRY%GroupA"),
            CheetahString::from_static_str("TopicB"),
        ],
        broker_addr: None,
    };

    filter_retry_topics_like_java(&mut topic_list, false);

    assert_eq!(
        topic_list.topic_list,
        vec![
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("TopicB")
        ]
    );
}

#[test]
fn unit_topic_list_keeps_retry_topics_when_requested_like_java() {
    let mut topic_list = TopicList {
        topic_list: vec![
            CheetahString::from_static_str("%RETRY%GroupA"),
            CheetahString::from_static_str("TopicA"),
        ],
        broker_addr: None,
    };

    filter_retry_topics_like_java(&mut topic_list, true);

    assert_eq!(
        topic_list.topic_list,
        vec![
            CheetahString::from_static_str("%RETRY%GroupA"),
            CheetahString::from_static_str("TopicA")
        ]
    );
}

#[test]
fn cluster_names_for_topic_route_maps_brokers_to_clusters() {
    let mut cluster_a_brokers = HashSet::new();
    cluster_a_brokers.insert(CheetahString::from_static_str("broker-a"));
    let mut cluster_b_brokers = HashSet::new();
    cluster_b_brokers.insert(CheetahString::from_static_str("broker-b"));

    let cluster_info = ClusterInfo::new(
        None,
        Some(HashMap::from([
            (CheetahString::from_static_str("cluster-a"), cluster_a_brokers),
            (CheetahString::from_static_str("cluster-b"), cluster_b_brokers),
        ])),
    );
    let topic_route_data = TopicRouteData {
        broker_datas: vec![
            BrokerData::new(
                CheetahString::from_static_str("cluster-a"),
                CheetahString::from_static_str("broker-a"),
                HashMap::new(),
                None,
            ),
            BrokerData::new(
                CheetahString::from_static_str("cluster-b"),
                CheetahString::from_static_str("broker-b"),
                HashMap::new(),
                None,
            ),
        ],
        ..Default::default()
    };

    let clusters = cluster_names_for_topic_route(&cluster_info, &topic_route_data);

    assert_eq!(clusters.len(), 2);
    assert!(clusters.contains(&CheetahString::from_static_str("cluster-a")));
    assert!(clusters.contains(&CheetahString::from_static_str("cluster-b")));
}

#[test]
fn system_topic_list_fetch_predicate_matches_java() {
    assert!(!should_fetch_system_topic_list_from_broker(&TopicList {
        topic_list: vec![],
        broker_addr: Some(CheetahString::from_static_str("127.0.0.1:10911")),
    }));
    assert!(!should_fetch_system_topic_list_from_broker(&TopicList {
        topic_list: vec![CheetahString::from_static_str("TopicA")],
        broker_addr: None,
    }));
    assert!(!should_fetch_system_topic_list_from_broker(&TopicList {
        topic_list: vec![CheetahString::from_static_str("TopicA")],
        broker_addr: Some(CheetahString::from_static_str("   ")),
    }));
    assert!(should_fetch_system_topic_list_from_broker(&TopicList {
        topic_list: vec![CheetahString::from_static_str("TopicA")],
        broker_addr: Some(CheetahString::from_static_str("127.0.0.1:10911")),
    }));
}

#[test]
fn system_topic_list_appends_broker_topics_without_dedup_like_java() {
    let mut topic_list = TopicList {
        topic_list: vec![CheetahString::from_static_str("TopicA")],
        broker_addr: Some(CheetahString::from_static_str("127.0.0.1:10911")),
    };
    let broker_topic_list = TopicList {
        topic_list: vec![
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("BrokerTopic"),
        ],
        broker_addr: None,
    };

    append_system_topic_list_from_broker_like_java(&mut topic_list, broker_topic_list);

    assert_eq!(
        topic_list.topic_list,
        vec![
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("BrokerTopic")
        ]
    );
}

#[test]
fn create_topic_request_header_matches_java_topic_config_mapping() {
    let mut topic_config = TopicConfig::with_sys_flag("TopicA", 2, 4, 6, 8);
    topic_config.order = true;
    topic_config.attributes.insert(
        CheetahString::from_static_str("+cleanup.policy"),
        CheetahString::from_static_str("DELETE"),
    );

    let header = create_topic_request_header_like_java(CheetahString::from_static_str("TBW102"), &topic_config)
        .expect("valid topic config should map to request header");

    assert_eq!(header.topic.as_str(), "TopicA");
    assert_eq!(header.default_topic.as_str(), "TBW102");
    assert_eq!(header.read_queue_nums, 2);
    assert_eq!(header.write_queue_nums, 4);
    assert_eq!(header.perm, 6);
    assert_eq!(header.topic_filter_type.as_str(), "SINGLE_TAG");
    assert_eq!(header.topic_sys_flag, Some(8));
    assert!(header.order);
    assert_eq!(header.attributes.as_deref(), Some("+cleanup.policy=DELETE"));
    assert_eq!(header.force, None);
}

#[test]
fn create_topic_request_header_rejects_values_outside_java_int_range() {
    let mut topic_config = TopicConfig::new("TopicA");
    topic_config.read_queue_nums = u32::MAX;

    let error = create_topic_request_header_like_java(CheetahString::from_static_str("TBW102"), &topic_config)
        .expect_err("Java int overflow should be rejected before encoding");

    assert!(error.to_string().contains("readQueueNums value"));
}

#[cfg(feature = "admin-mutation")]
#[test]
fn create_topic_list_request_matches_java_request_code_and_body() {
    let request = create_topic_list_request(
        &test_remoting_command_factory(),
        vec![TopicConfig::new("TopicA"), TopicConfig::new("TopicB")],
    )
    .expect("topic list request should encode");

    assert_eq!(request.code(), RequestCode::UpdateAndCreateTopicList as i32);
    let body = request.body().expect("topic list request body should be set");
    let decoded: CreateTopicListRequestBody =
        serde_json::from_slice(body.as_ref()).expect("topic list body should decode");
    assert_eq!(decoded.topic_config_list.len(), 2);
    assert_eq!(decoded.topic_config_list[0].topic_name.as_deref(), Some("TopicA"));
    assert_eq!(decoded.topic_config_list[1].topic_name.as_deref(), Some("TopicB"));
}

#[cfg(feature = "admin-mutation")]
#[test]
fn create_subscription_group_list_request_matches_java_request_code_and_body() {
    let request = create_subscription_group_list_request(
        &test_remoting_command_factory(),
        vec![
            SubscriptionGroupConfig::new(CheetahString::from_static_str("GroupA")),
            SubscriptionGroupConfig::new(CheetahString::from_static_str("GroupB")),
        ],
    )
    .expect("subscription group list request should encode");

    assert_eq!(request.code(), RequestCode::UpdateAndCreateSubscriptionGroupList as i32);
    let body = request
        .body()
        .expect("subscription group list request body should be set");
    let decoded: SubscriptionGroupList =
        serde_json::from_slice(body.as_ref()).expect("subscription group list body should decode");
    assert_eq!(decoded.group_config_list.len(), 2);
    assert_eq!(decoded.group_config_list[0].group_name().as_str(), "GroupA");
    assert_eq!(decoded.group_config_list[1].group_name().as_str(), "GroupB");
}

#[cfg(feature = "admin-mutation")]
#[test]
fn delete_topic_list_request_matches_java_request_code_and_body() {
    let request = delete_topic_list_request(
        &test_remoting_command_factory(),
        vec![
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("TopicB"),
        ],
    )
    .expect("topic delete list request should encode");

    assert_eq!(request.code(), RequestCode::DeleteTopicInBrokerList as i32);
    let body = request.body().expect("topic delete list request body should be set");
    let decoded: DeleteTopicListRequestBody =
        serde_json::from_slice(body.as_ref()).expect("topic delete list body should decode");
    assert_eq!(decoded.topic_list, vec!["TopicA", "TopicB"]);
}

#[cfg(feature = "admin-mutation")]
#[test]
fn delete_subscription_group_list_request_matches_java_request_code_and_body() {
    let request = delete_subscription_group_list_request(
        &test_remoting_command_factory(),
        vec![
            CheetahString::from_static_str("GroupA"),
            CheetahString::from_static_str("GroupB"),
        ],
        true,
    )
    .expect("subscription group delete list request should encode");

    assert_eq!(request.code(), RequestCode::DeleteSubscriptionGroupList as i32);
    let body = request
        .body()
        .expect("subscription group delete list request body should be set");
    let decoded: DeleteSubscriptionGroupListRequestBody =
        serde_json::from_slice(body.as_ref()).expect("subscription group delete list body should decode");
    assert_eq!(decoded.group_name_list, vec!["GroupA", "GroupB"]);
    assert!(decoded.clean_offset);
}

#[test]
fn query_correction_offset_request_joins_filter_groups_like_java() {
    let request = query_correction_offset_request(
        &test_remoting_command_factory(),
        CheetahString::from_static_str("TopicA"),
        CheetahString::from_static_str("CompareGroup"),
        Some(vec![
            CheetahString::from_static_str("GroupA"),
            CheetahString::from_static_str("GroupB"),
        ]),
    );

    assert_eq!(request.code(), RequestCode::QueryCorrectionOffset as i32);
    let header = request
        .try_read_custom_header_ref::<QueryCorrectionOffsetHeader>()
        .expect("query correction header should be attached");
    assert_eq!(header.topic.as_str(), "TopicA");
    assert_eq!(header.compare_group.as_str(), "CompareGroup");
    assert_eq!(header.filter_groups.as_deref(), Some("GroupA,GroupB"));
}

#[test]
fn parse_lite_order_count_info_matches_java_rules() {
    assert_eq!(
        parse_lite_order_count_info_like_java(Some(&CheetahString::from_static_str("1;0 7 3;bad")), 3),
        Some(vec![1, 3, 0])
    );
    assert_eq!(
        parse_lite_order_count_info_like_java(Some(&CheetahString::from_static_str("1;2")), 3),
        None
    );
    assert_eq!(
        parse_lite_order_count_info_like_java(Some(&CheetahString::from_static_str("")), 0),
        None
    );
}

#[test]
fn split_lite_dispatch_value_drops_empty_segments_like_java_string_utils_split() {
    assert_eq!(split_lite_dispatch_value("q0,,q1,"), vec!["q0", "q1"]);
    assert!(split_lite_dispatch_value("").is_empty());
}

#[test]
fn build_queue_offset_sorted_map_preserves_java_long_offsets() {
    let mut message = MessageExt::default();
    message.set_topic(CheetahString::from_static_str("TopicA"));
    message.set_queue_id(3);
    message.set_queue_offset(-7);

    let sort_map =
        MQClientAPIImpl::build_queue_offset_sorted_map("TopicA", &[message]).expect("queue offset map should build");
    let key = ExtraInfoUtil::get_start_offset_info_map_key_with_pop_ck("TopicA", None, 3)
        .expect("normal topic key should build");

    assert_eq!(sort_map.get(&key).map(Vec::as_slice), Some(&[-7][..]));
}

#[test]
fn build_queue_offset_sorted_map_preserves_lmq_java_long_offsets() {
    const TOPIC: &str = "%LMQ%TopicA";
    let mut message = MessageExt::default();
    message.set_topic(CheetahString::from_static_str(TOPIC));
    message.set_queue_id(0);
    message.set_reconsume_times(0);
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
        CheetahString::from_static_str("Other,%LMQ%TopicA"),
    );
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
        CheetahString::from_static_str("11,-9"),
    );

    let sort_map =
        MQClientAPIImpl::build_queue_offset_sorted_map(TOPIC, &[message]).expect("LMQ offset map should build");
    let key = ExtraInfoUtil::get_start_offset_info_map_key(TOPIC, mix_all::LMQ_QUEUE_ID as i64);

    assert_eq!(sort_map.get(&key).map(Vec::as_slice), Some(&[-9][..]));
}

#[test]
fn pop_msg_queue_offset_lookup_uses_msg_offset_info_like_java() {
    let queue_id_key = ExtraInfoUtil::get_start_offset_info_map_key("TopicA", 1);
    let sort_map = HashMap::from([(queue_id_key.clone(), vec![10, 11])]);
    let msg_offset_info = HashMap::from([(queue_id_key.clone(), vec![100, 101])]);

    assert_eq!(
        pop_msg_queue_offset_for_index(&queue_id_key, 11, &sort_map, &msg_offset_info),
        Some(101)
    );
    assert_eq!(
        pop_msg_queue_offset_for_index(&queue_id_key, 12, &sort_map, &msg_offset_info),
        None
    );
}

#[test]
fn pop_msg_queue_offset_lookup_preserves_negative_java_long_offsets() {
    let queue_id_key = ExtraInfoUtil::get_start_offset_info_map_key("TopicA", 1);
    let sort_map = HashMap::from([(queue_id_key.clone(), vec![-1])]);
    let msg_offset_info = HashMap::from([(queue_id_key.clone(), vec![-2])]);

    assert_eq!(
        pop_msg_queue_offset_for_index(&queue_id_key, -1, &sort_map, &msg_offset_info),
        Some(-2)
    );
}

#[test]
fn admin_query_filter_matches_java_unique_key_rules() {
    let mut message = MessageExt::default();
    message.set_topic(CheetahString::from_static_str("TopicA"));
    message.set_msg_id(CheetahString::from_static_str("OFFSET-MSG-1"));
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
        CheetahString::from_static_str("UNIQ-MSG-1"),
    );

    assert!(admin_message_matches_query(
        &CheetahString::from_static_str("TopicA"),
        &CheetahString::from_static_str("UNIQ-MSG-1"),
        &message,
        true
    ));
    assert!(!admin_message_matches_query(
        &CheetahString::from_static_str("OtherTopic"),
        &CheetahString::from_static_str("UNIQ-MSG-1"),
        &message,
        true
    ));
    assert!(!admin_message_matches_query(
        &CheetahString::from_static_str("TopicA"),
        &CheetahString::from_static_str("MSG-2"),
        &message,
        true
    ));

    let mut fallback_message = MessageExt::default();
    fallback_message.set_topic(CheetahString::from_static_str("TopicA"));
    fallback_message.set_msg_id(CheetahString::from_static_str("OFFSET-MSG-2"));
    assert!(admin_message_matches_query(
        &CheetahString::from_static_str("TopicA"),
        &CheetahString::from_static_str("OFFSET-MSG-2"),
        &fallback_message,
        true
    ));
}

#[test]
fn admin_query_filter_matches_java_key_separator_rules() {
    let mut message = MessageExt::default();
    message.set_topic(CheetahString::from_static_str("TopicA"));
    message.set_keys(CheetahString::from_static_str("KeyA KeyB"));

    assert!(admin_message_matches_query(
        &CheetahString::from_static_str("TopicA"),
        &CheetahString::from_static_str("KeyB"),
        &message,
        false
    ));
    assert!(!admin_message_matches_query(
        &CheetahString::from_static_str("TopicA"),
        &CheetahString::from_static_str("Key"),
        &message,
        false
    ));
}

#[test]
fn async_retry_queue_prefers_broker_different_from_failed_broker() {
    let strategy = retry_strategy();
    let info = topic_publish_info();

    let selected =
        MQClientAPIImpl::select_async_retry_queue(&strategy, Some(&info), &CheetahString::from_static_str("broker-a"))
            .expect("retry queue should be selected");

    assert_eq!(selected.broker_name(), "broker-b");
}

#[test]
fn async_retry_queue_without_topic_publish_info_returns_none() {
    let strategy = retry_strategy();

    let selected =
        MQClientAPIImpl::select_async_retry_queue(&strategy, None, &CheetahString::from_static_str("broker-a"));

    assert!(selected.is_none());
}

#[tokio::test]
async fn async_send_callback_success_runs_in_owned_send_task() {
    let (tx, rx) = std::sync::mpsc::channel();
    let callback: ArcSendCallback = Arc::new(move |result: Option<&SendResult>, error: Option<&RocketMQError>| {
        tx.send((result.is_some(), error.is_some()))
            .expect("test receiver should be alive");
    });

    MQClientAPIImpl::notify_send_callback_success(
        &ClientCallbackExecutor::new(1),
        &Some(callback),
        &SendResult::default(),
    )
    .await;

    let (has_result, has_error) = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("callback should execute in the owned send task");
    assert!(has_result);
    assert!(!has_error);
}

#[tokio::test]
async fn async_send_callback_exception_runs_in_owned_send_task() {
    let (tx, rx) = std::sync::mpsc::channel();
    let callback: ArcSendCallback = Arc::new(move |result: Option<&SendResult>, error: Option<&RocketMQError>| {
        tx.send((result.is_some(), error.map(ToString::to_string)))
            .expect("test receiver should be alive");
    });
    let error = RocketMQError::Shared(std::sync::Arc::new(rocketmq_error::Error::caused_by(
        &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
        std::io::Error::other("callback failure"),
    )));

    MQClientAPIImpl::notify_send_callback_exception(&ClientCallbackExecutor::new(1), &Some(callback), &error).await;

    let (has_result, error) = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("callback should execute in the owned send task");
    assert!(!has_result);
    assert!(error
        .as_deref()
        .is_some_and(|message| message.contains("Transport connection operation failed")));
    assert!(!error
        .as_deref()
        .is_some_and(|message| message.contains("callback failure")));
}

#[tokio::test]
async fn name_server_cache_serializes_shared_updates() {
    let cache = RwLock::new(None);
    let updates = AtomicUsize::new(0);
    let address = "127.0.0.1:9876";

    let (first, duplicate) = tokio::join!(
        update_cached_name_server_addr(&cache, address, |_| {
            updates.fetch_add(1, AtomicOrdering::Relaxed);
        }),
        update_cached_name_server_addr(&cache, address, |_| {
            updates.fetch_add(1, AtomicOrdering::Relaxed);
        })
    );

    assert_ne!(first, duplicate);
    assert_eq!(updates.load(AtomicOrdering::Relaxed), 1);
    assert_eq!(cache.read().await.as_deref(), Some(address));

    assert!(
        update_cached_name_server_addr(&cache, "127.0.0.2:9876", |_| {
            updates.fetch_add(1, AtomicOrdering::Relaxed);
        })
        .await
    );
    assert_eq!(updates.load(AtomicOrdering::Relaxed), 2);
    assert_eq!(cache.read().await.as_deref(), Some("127.0.0.2:9876"));
}

#[tokio::test]
async fn api_background_task_tracker_waits_for_completion() {
    let tracker = TaskTracker::new();
    let token = CancellationToken::new();
    let completed = Arc::new(AtomicBool::new(false));
    let completed_in_task = completed.clone();

    MQClientAPIImpl::spawn_api_background_task(
        &crate::runtime::test_service_context("client-api-background-test").component("api-background"),
        "rocketmq-client-api-background-test",
        &tracker,
        &token,
        async move {
            completed_in_task.store(true, AtomicOrdering::Release);
        },
    );
    tracker.close();

    tokio::time::timeout(Duration::from_secs(1), tracker.wait())
        .await
        .expect("tracked API background task should finish");

    assert!(completed.load(AtomicOrdering::Acquire));
}

#[tokio::test]
async fn api_background_task_shutdown_token_cancels_pending_task() {
    let tracker = TaskTracker::new();
    let token = CancellationToken::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let dropped_in_task = dropped.clone();

    MQClientAPIImpl::spawn_api_background_task(
        &crate::runtime::test_service_context("client-api-background-cancel-test").component("api-background"),
        "rocketmq-client-api-background-test",
        &tracker,
        &token,
        async move {
            let _drop_flag = DropFlag(dropped_in_task);
            pending::<()>().await;
        },
    );
    tracker.close();

    assert!(tokio::time::timeout(Duration::from_millis(20), tracker.wait())
        .await
        .is_err());

    token.cancel();

    tokio::time::timeout(Duration::from_secs(1), tracker.wait())
        .await
        .expect("shutdown token should release pending API background task");

    assert!(dropped.load(AtomicOrdering::Acquire));
}

#[tokio::test]
async fn pop_callback_task_does_not_wait_for_the_remoting_future() {
    let tracker = TaskTracker::new();
    let token = CancellationToken::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let (release, released) = tokio::sync::oneshot::channel();
    let (completed, completion) = tokio::sync::oneshot::channel();

    MQClientAPIImpl::spawn_pop_callback_task(
        &crate::runtime::test_service_context("client-pop-background-test").component("api-background"),
        &tracker,
        &token,
        ClientCallbackExecutor::new(1),
        async move {
            released.await.expect("test should release blocked POP request");
            Ok(PopResult::default())
        },
        RecordingPopCallback {
            calls: calls.clone(),
            errors: errors.clone(),
            completed: Some(completed),
        },
    );

    assert_eq!(calls.load(AtomicOrdering::Acquire), 0);
    release.send(()).expect("blocked POP request should still be alive");
    tokio::time::timeout(Duration::from_secs(1), completion)
        .await
        .expect("POP callback should complete")
        .expect("POP callback completion sender should remain alive");
    tracker.close();
    tracker.wait().await;

    assert_eq!(calls.load(AtomicOrdering::Acquire), 1);
    assert_eq!(errors.load(AtomicOrdering::Acquire), 0);
}

#[tokio::test]
async fn pop_callback_task_completes_once_when_shutdown_cancels_the_request() {
    let tracker = TaskTracker::new();
    let token = CancellationToken::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let (completed, completion) = tokio::sync::oneshot::channel();

    MQClientAPIImpl::spawn_pop_callback_task(
        &crate::runtime::test_service_context("client-pop-cancel-test").component("api-background"),
        &tracker,
        &token,
        ClientCallbackExecutor::new(1),
        pending::<rocketmq_error::RocketMQResult<PopResult>>(),
        RecordingPopCallback {
            calls: calls.clone(),
            errors: errors.clone(),
            completed: Some(completed),
        },
    );
    token.cancel();

    tokio::time::timeout(Duration::from_secs(1), completion)
        .await
        .expect("cancelled POP callback should complete")
        .expect("cancelled POP callback sender should remain alive");
    tracker.close();
    tracker.wait().await;

    assert_eq!(calls.load(AtomicOrdering::Acquire), 1);
    assert_eq!(errors.load(AtomicOrdering::Acquire), 1);
}

#[cfg(feature = "admin-mutation")]
#[test]
fn lite_subscription_ctl_request_matches_java_single_dto_body() {
    let lite_subscription_dto = LiteSubscriptionDTO::new()
        .with_action(LiteSubscriptionAction::CompleteAdd)
        .with_client_id(CheetahString::from_static_str("client-a"))
        .with_group(CheetahString::from_static_str("group-a"))
        .with_topic(CheetahString::from_static_str("topic-a"))
        .with_version(42);

    let request = lite_subscription_ctl_request(&test_remoting_command_factory(), lite_subscription_dto.clone())
        .expect("lite subscription request should encode");

    assert_eq!(request.code(), RequestCode::LiteSubscriptionCtl as i32);
    let body = request.body().expect("request body should be set");
    let decoded = LiteSubscriptionCtlRequestBody::decode(body.as_ref()).expect("body should decode");
    assert_eq!(decoded.subscription_set(), &[lite_subscription_dto]);
}

#[test]
fn notification_request_matches_java_header_fields() {
    let mut request = notification_request(
        &test_remoting_command_factory(),
        NotificationRequestHeader {
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("topic-a"),
            queue_id: -1,
            poll_time: 3_000,
            born_time: 10,
            order: true,
            attempt_id: Some(CheetahString::from_static_str("attempt-a")),
            exp_type: Some(CheetahString::from_static_str("TAG")),
            exp: Some(CheetahString::from_static_str("tag-a")),
            is_lite_consumer: false,
            client_id: None,
            topic_request_header: None,
        },
    );

    assert_eq!(request.code(), RequestCode::Notification as i32);
    request.make_custom_header_to_net();
    let ext_fields = request.ext_fields().expect("notification request should encode header");
    assert_eq!(
        ext_fields.get("consumerGroup").map(|value| value.as_str()),
        Some("group-a")
    );
    assert_eq!(ext_fields.get("topic").map(|value| value.as_str()), Some("topic-a"));
    assert_eq!(ext_fields.get("queueId").map(|value| value.as_str()), Some("-1"));
    assert_eq!(ext_fields.get("pollTime").map(|value| value.as_str()), Some("3000"));
    assert_eq!(ext_fields.get("bornTime").map(|value| value.as_str()), Some("10"));
    assert_eq!(ext_fields.get("order").map(|value| value.as_str()), Some("true"));
    assert_eq!(
        ext_fields.get("attemptId").map(|value| value.as_str()),
        Some("attempt-a")
    );
    assert_eq!(ext_fields.get("expType").map(|value| value.as_str()), Some("TAG"));
    assert_eq!(ext_fields.get("exp").map(|value| value.as_str()), Some("tag-a"));
}

fn test_remoting_command_factory() -> RemotingCommandFactory {
    RemotingCommandFactory::new(RemotingCommandDefaults::default())
}

#[test]
fn notification_request_uses_owning_factory_defaults() {
    let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(731, SerializeType::ROCKETMQ));
    let request = notification_request(
        &factory,
        NotificationRequestHeader {
            consumer_group: CheetahString::from_static_str("group-owner"),
            topic: CheetahString::from_static_str("topic-owner"),
            queue_id: 1,
            poll_time: 3_000,
            born_time: 10,
            order: false,
            attempt_id: None,
            exp_type: None,
            exp: None,
            is_lite_consumer: false,
            client_id: None,
            topic_request_header: None,
        },
    );

    assert_eq!(request.version(), 731);
    assert_eq!(request.serialize_type(), SerializeType::ROCKETMQ);
}

#[cfg(feature = "admin-mutation")]
#[test]
fn create_and_update_plain_access_config_request_matches_java_legacy_acl_body() {
    let config = PlainAccessConfig {
        access_key: Some(CheetahString::from_static_str("AK")),
        secret_key: Some(CheetahString::from_static_str("SK")),
        white_remote_address: Some(CheetahString::from_static_str("10.0.*.*")),
        admin: true,
        default_topic_perm: Some(CheetahString::from_static_str("DENY")),
        default_group_perm: Some(CheetahString::from_static_str("SUB")),
        topic_perms: vec![CheetahString::from_static_str("TopicA=PUB|SUB")],
        group_perms: vec![CheetahString::from_static_str("GroupA=SUB")],
    };

    let request = create_and_update_plain_access_config_request(&test_remoting_command_factory(), &config)
        .expect("plain access config request should encode");

    assert_eq!(request.code(), RequestCode::UpdateAndCreateAclConfig as i32);
    let body = std::str::from_utf8(request.body().expect("request body should be set").as_ref())
        .expect("body should be UTF-8 JSON");
    assert_eq!(
        body,
        r#"{"accessKey":"AK","secretKey":"SK","whiteRemoteAddress":"10.0.*.*","admin":true,"defaultTopicPerm":"DENY","defaultGroupPerm":"SUB","topicPerms":["TopicA=PUB|SUB"],"groupPerms":["GroupA=SUB"]}"#
    );
}

#[cfg(feature = "admin-mutation")]
#[test]
fn delete_plain_access_config_request_matches_java_legacy_acl_body() {
    let request =
        delete_plain_access_config_request(&test_remoting_command_factory(), &CheetahString::from_static_str("AK"));

    assert_eq!(request.code(), RequestCode::DeleteAclConfig as i32);
    assert_eq!(request.body().expect("request body should be set").as_ref(), b"AK");
}

#[test]
fn get_acl_request_matches_java_auth_get_acl_header() {
    let request = get_acl_request(
        &test_remoting_command_factory(),
        CheetahString::from_static_str("User:alice"),
    );

    assert_eq!(request.code(), RequestCode::AuthGetAcl as i32);
    let header = request
        .try_read_custom_header_ref::<GetAclRequestHeader>()
        .expect("get ACL header should be attached");
    assert_eq!(header.subject.as_str(), "User:alice");
}

#[test]
fn heartbeat_request_matches_java_register_client_request() {
    let heartbeat_data = HeartbeatData::default();

    let request = heartbeat_request(&test_remoting_command_factory(), &heartbeat_data, LanguageCode::RUST)
        .expect("heartbeat request should encode body");

    assert_eq!(request.code(), RequestCode::HeartBeat as i32);
    assert!(request.body().is_some());
}

#[test]
fn get_all_consumer_offset_request_uses_java_request_code() {
    let request = get_all_consumer_offset_request(&test_remoting_command_factory());

    assert_eq!(request.code(), RequestCode::GetAllConsumerOffset as i32);
}

#[test]
fn consumer_offset_json_from_response_returns_raw_broker_json() {
    let body = r#"{"dataVersion":{"counter":1},"offsetTable":{"TopicA@GroupA":{"0":42}}}"#;
    let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_body(body);

    let json = consumer_offset_json_from_response(&response).expect("success response should decode body");

    assert_eq!(json.as_str(), body);
}

#[test]
fn consumer_offset_json_from_response_rejects_success_without_body() {
    let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success);

    let error =
        consumer_offset_json_from_response(&response).expect_err("success response without body should be rejected");

    assert!(error
        .to_string()
        .contains("get_all_consumer_offset response body is empty"));
}

#[test]
fn consumer_send_message_back_header_omits_broker_name_when_java_passes_null() {
    let mut message = MessageExt::default();
    message.set_commit_log_offset(42);
    message.msg_id = CheetahString::from_static_str("MSG_ID_A");
    message.set_topic(CheetahString::from_static_str("TopicA"));

    let header = MQClientAPIImpl::consumer_send_message_back_request_header(&message, None, "GroupA", 3, 16);
    let map = header.to_map().expect("header should encode");

    assert_eq!(header.offset, 42);
    assert_eq!(header.group.as_str(), "GroupA");
    assert_eq!(header.origin_msg_id.as_deref(), Some("MSG_ID_A"));
    assert_eq!(header.origin_topic.as_deref(), Some("TopicA"));
    assert!(header
        .rpc_request_header
        .as_ref()
        .expect("RPC header should be present")
        .broker_name
        .is_none());
    assert!(!map.contains_key(&CheetahString::from_static_str("brokerName")));
}

#[test]
fn consumer_send_message_back_header_preserves_broker_name_when_java_passes_value() {
    let mut message = MessageExt::default();
    message.set_commit_log_offset(42);
    message.msg_id = CheetahString::from_static_str("MSG_ID_A");
    message.set_topic(CheetahString::from_static_str("TopicA"));

    let header =
        MQClientAPIImpl::consumer_send_message_back_request_header(&message, Some("broker-a"), "GroupA", 3, 16);
    let map = header.to_map().expect("header should encode");

    assert_eq!(
        header
            .rpc_request_header
            .as_ref()
            .and_then(|rpc| rpc.broker_name.as_deref()),
        Some("broker-a")
    );
    assert_eq!(
        map.get(&CheetahString::from_static_str("bname"))
            .map(CheetahString::as_str),
        Some("broker-a")
    );
    assert!(!map.contains_key(&CheetahString::from_static_str("brokerName")));
}

#[test]
fn notify_result_from_response_maps_polling_full_like_java() {
    let mut response = RemotingCommand::create_success_response_command_with_header(NotificationResponseHeader {
        has_msg: true,
        polling_full: true,
    });
    response.make_custom_header_to_net();

    let notify_result = notify_result_from_response(&response).expect("notification response should decode");

    assert!(notify_result.is_has_msg());
    assert!(notify_result.is_polling_full());
}

#[test]
fn decode_cluster_acl_version_info_response_body_decodes_java_json() {
    let body = bytes::Bytes::from_static(
        br#"{
            "brokerName":"broker-a",
            "brokerAddr":"127.0.0.1:10911",
            "aclConfigDataVersion":null,
            "allAclConfigDataVersion":{},
            "clusterName":"DefaultCluster"
        }"#,
    );

    let version_info =
        decode_cluster_acl_version_info_response_body(Some(&body)).expect("cluster ACL version response should decode");

    assert_eq!(version_info.broker_name.as_str(), "broker-a");
    assert_eq!(version_info.broker_addr.as_str(), "127.0.0.1:10911");
    assert_eq!(version_info.cluster_name.as_str(), "DefaultCluster");
    assert!(version_info.acl_config_data_version.is_none());
    assert!(version_info.all_acl_config_data_version.is_empty());
}

#[test]
fn decode_cluster_acl_version_info_response_body_rejects_success_without_body() {
    let error = decode_cluster_acl_version_info_response_body(None)
        .expect_err("SUCCESS cluster ACL version response must include a body");

    assert!(error
        .to_string()
        .contains("get_broker_cluster_acl_version_info response body is empty"));
}

#[test]
fn reset_offset_table_from_response_rejects_success_without_body_like_java() {
    let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success);

    let error = reset_offset_table_from_response(&response)
        .expect_err("Java invokeBrokerToResetOffset throws when SUCCESS has no body");

    assert!(error.to_string().contains("reset offset response body is empty"));
}

#[test]
fn reset_offset_table_from_response_decodes_java_body() {
    let mq = MessageQueue::from_parts("topic-a", "broker-a", 0);
    let mut body = ResetOffsetBody::new();
    body.offset_table.insert(mq.clone(), 42);
    let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_body(body.encode());

    let offset_table = reset_offset_table_from_response(&response).expect("valid reset offset body should decode");

    assert_eq!(offset_table.get(&mq), Some(&42));
}

#[test]
fn async_retry_request_reuses_final_attempt_after_first_failure() {
    let retry_key = CheetahString::from_static_str("retry-key");
    let retry_value = CheetahString::from_static_str("retry-value");
    let mut request = RemotingCommand::create_request_command(RequestCode::SendMessageV2, EmptyHeader {})
        .set_body(bytes::Bytes::from_static(b"retry-body"))
        .set_ext_fields(HashMap::from([(retry_key.clone(), retry_value.clone())]));
    let initial_opaque = RemotingCommand::create_new_request_id();
    let retry_opaque = RemotingCommand::create_new_request_id();
    request.set_opaque_mut(initial_opaque);

    let mut retry_request = AsyncRetryRequest::new(request);
    let first_attempt = retry_request.next_attempt(true);

    assert_eq!(first_attempt.opaque(), initial_opaque);
    assert_eq!(
        first_attempt.body().map(bytes::Bytes::as_ref),
        Some(b"retry-body".as_slice())
    );
    assert_eq!(
        first_attempt
            .ext_fields()
            .and_then(|fields| fields.get(&retry_key))
            .map(CheetahString::as_str),
        Some(retry_value.as_str())
    );
    assert!(!retry_request.is_consumed());

    retry_request.set_retry_opaque(retry_opaque);
    let second_attempt = retry_request.next_attempt(false);

    assert_eq!(second_attempt.opaque(), retry_opaque);
    assert_eq!(
        second_attempt.body().map(bytes::Bytes::as_ref),
        Some(b"retry-body".as_slice())
    );
    assert_eq!(
        second_attempt
            .ext_fields()
            .and_then(|fields| fields.get(&retry_key))
            .map(CheetahString::as_str),
        Some(retry_value.as_str())
    );
    assert!(retry_request.is_consumed());
}

#[test]
fn async_retry_request_consumes_immediate_final_attempt_without_clone_template() {
    let mut request = RemotingCommand::create_request_command(RequestCode::SendMessageV2, EmptyHeader {})
        .set_body(bytes::Bytes::from_static(b"single-attempt-body"));
    let initial_opaque = RemotingCommand::create_new_request_id();
    request.set_opaque_mut(initial_opaque);

    let mut retry_request = AsyncRetryRequest::new(request);
    let attempt = retry_request.next_attempt(false);

    assert_eq!(attempt.opaque(), initial_opaque);
    assert_eq!(
        attempt.body().map(bytes::Bytes::as_ref),
        Some(b"single-attempt-body".as_slice())
    );
    assert!(retry_request.is_consumed());
}

#[test]
fn async_retry_request_materializes_custom_header_for_clone_and_final_attempt() {
    let header_value = CheetahString::from_static_str("retry-header-value");
    let request = RemotingCommand::create_request_command(
        RequestCode::SendMessageV2,
        AsyncRetryTestHeader {
            retry_marker: header_value.clone(),
        },
    );

    let mut retry_request = AsyncRetryRequest::new(request);
    let first_attempt = retry_request.next_attempt(true);

    assert_eq!(
        first_attempt
            .ext_fields()
            .and_then(|fields| fields.get(&CheetahString::from_static_str("retryMarker")))
            .map(CheetahString::as_str),
        Some(header_value.as_str())
    );
    let decoded_first = first_attempt
        .decode_command_custom_header::<AsyncRetryTestHeader>()
        .expect("materialized custom header should decode from cloned retry attempt");
    assert_eq!(decoded_first.retry_marker.as_str(), header_value.as_str());
    assert!(!retry_request.is_consumed());

    retry_request.set_retry_opaque(RemotingCommand::create_new_request_id());
    let final_attempt = retry_request.next_attempt(false);

    assert_eq!(
        final_attempt
            .ext_fields()
            .and_then(|fields| fields.get(&CheetahString::from_static_str("retryMarker")))
            .map(CheetahString::as_str),
        Some(header_value.as_str())
    );
    let decoded_final = final_attempt
        .decode_command_custom_header::<AsyncRetryTestHeader>()
        .expect("materialized custom header should decode from final retry attempt");
    assert_eq!(decoded_final.retry_marker.as_str(), header_value.as_str());
    assert!(retry_request.is_consumed());
}
