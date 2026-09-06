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
use super::admin_api::*;
#[allow(unused_imports)]
use super::broker::*;
#[allow(unused_imports)]
use super::group::*;
#[allow(unused_imports)]
use super::security::*;
#[allow(unused_imports)]
use super::topic::*;

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use crate::admin::capability::{AuthAdmin, BrokerAdmin, ConsumerAdmin, OffsetAdmin, RouteAdmin, TopicAdmin};
use crate::base::client_config::ClientConfig;
use crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::base::plain_access_config::PlainAccessConfig;
use rocketmq_model::common::base::service_state::ServiceState;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::message::message_builder::MessageBuilder;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageConst;

#[test]
fn query_message_header_preserves_index_type_and_last_key() {
    let cursor = CheetahString::from_static_str("1700000000000@TopicA@K@KeyA@UniqA@1024");
    let header = query_message_request_header(
        &CheetahString::from_static_str("TopicA"),
        &CheetahString::from_static_str("KeyA"),
        32,
        1,
        2,
        &CheetahString::from_static_str(MessageConst::INDEX_KEY_TYPE),
        Some(&cursor),
    );

    assert_eq!(header.index_type.as_deref(), Some(MessageConst::INDEX_KEY_TYPE));
    assert_eq!(header.last_key.as_deref(), Some(cursor.as_str()));
}
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
#[allow(deprecated)]
use rocketmq_model::common::tools::track_type::TrackType;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::connection::Connection;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_protocol::protocol::body::producer_connection::ProducerConnection;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;

use super::DefaultMQAdminExtImpl;

#[test]
fn admin_route_not_found_uses_route_descriptor() {
    let error = admin_route_not_found(&CheetahString::from_static_str("RouteTopic"));

    assert_eq!(error.descriptor().code(), rocketmq_error::ROUTE_TOPIC_NOT_FOUND.code());
    assert!(error.to_string().contains("RouteTopic"));
}

#[test]
fn sync_pull_result_missing_uses_client_invalid_state() {
    let error = sync_pull_result_missing("DefaultMQAdminExtImpl::pull_message_from_queue");

    assert_eq!(
        error.descriptor().code(),
        rocketmq_error::CLIENT_LIFECYCLE_INVALID_STATE.code()
    );
    assert!(error
        .to_string()
        .contains("DefaultMQAdminExtImpl::pull_message_from_queue returned None"));
}

fn new_unstarted_admin() -> DefaultMQAdminExtImpl {
    DefaultMQAdminExtImpl::new(
        crate::runtime::test_client_runtime("default-admin-ext-impl-test"),
        None,
        Duration::from_secs(3),
        ClientConfig::default(),
        CheetahString::from("admin-group"),
    )
}

#[test]
fn retain_java_user_topic_config_filters_java_internal_topics() {
    let mut topic_table = HashMap::from([
        (
            CheetahString::from_static_str("UserTopic"),
            TopicConfig::new("UserTopic"),
        ),
        (
            CheetahString::from_static_str("BrokerInternalTopic"),
            TopicConfig::new("BrokerInternalTopic"),
        ),
        (
            CheetahString::from_static_str(TopicValidator::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC),
            TopicConfig::new(TopicValidator::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC),
        ),
        (
            CheetahString::from_string(format!("{RETRY_GROUP_TOPIC_PREFIX}group-a")),
            TopicConfig::new(format!("{RETRY_GROUP_TOPIC_PREFIX}group-a")),
        ),
        (
            CheetahString::from_string(format!("{DLQ_GROUP_TOPIC_PREFIX}group-a")),
            TopicConfig::new(format!("{DLQ_GROUP_TOPIC_PREFIX}group-a")),
        ),
        (
            CheetahString::from_static_str("InvalidPermTopic"),
            TopicConfig {
                perm: PermName::PERM_PRIORITY,
                ..TopicConfig::new("InvalidPermTopic")
            },
        ),
    ]);
    let broker_system_topics = vec![CheetahString::from_static_str("BrokerInternalTopic")];

    retain_java_user_topic_config(&mut topic_table, &broker_system_topics, false);

    assert_eq!(topic_table.len(), 1);
    assert!(topic_table.contains_key("UserTopic"));
}

#[test]
fn retain_java_user_topic_config_keeps_retry_and_dlq_when_special_topic_enabled() {
    let retry_topic = CheetahString::from_string(format!("{RETRY_GROUP_TOPIC_PREFIX}group-a"));
    let dlq_topic = CheetahString::from_string(format!("{DLQ_GROUP_TOPIC_PREFIX}group-a"));
    let mut topic_table = HashMap::from([
        (retry_topic.clone(), TopicConfig::new(retry_topic.clone())),
        (dlq_topic.clone(), TopicConfig::new(dlq_topic.clone())),
        (
            CheetahString::from_static_str("InvalidPermTopic"),
            TopicConfig {
                perm: PermName::PERM_PRIORITY,
                ..TopicConfig::new("InvalidPermTopic")
            },
        ),
    ]);

    retain_java_user_topic_config(&mut topic_table, &[], true);

    assert_eq!(topic_table.len(), 2);
    assert!(topic_table.contains_key(&retry_topic));
    assert!(topic_table.contains_key(&dlq_topic));
}

#[test]
fn use_tls_updates_admin_impl_client_config_before_start() {
    let mut admin = new_unstarted_admin();

    assert!(!admin.is_use_tls());

    admin.set_use_tls(true);

    assert!(admin.is_use_tls());
    assert!(admin.client_config.is_use_tls());
}

#[test]
fn controller_servers_or_namesrv_matches_java_controller_config_target_selection() {
    let namesrv_addrs = vec![
        CheetahString::from("127.0.0.1:9876"),
        CheetahString::from("127.0.0.2:9876"),
    ];

    assert_eq!(
        controller_servers_or_namesrv(Vec::new(), &namesrv_addrs),
        namesrv_addrs.clone()
    );

    let explicit_controllers = vec![CheetahString::from("127.0.0.3:9878")];
    assert_eq!(
        controller_servers_or_namesrv(explicit_controllers.clone(), &namesrv_addrs),
        explicit_controllers
    );

    assert!(controller_servers_or_namesrv(Vec::new(), &[]).is_empty());
}

#[test]
fn merge_order_conf_entries_replaces_existing_broker_value() {
    let merged = merge_order_conf_entries("broker-a:4;broker-b:4", "broker-a:8");
    assert_eq!(merged, "broker-a:8;broker-b:4");
}

#[test]
fn merge_order_conf_entries_adds_new_broker_value() {
    let merged = merge_order_conf_entries("broker-a:4", "broker-b:8");
    assert_eq!(merged, "broker-a:4;broker-b:8");
}

#[test]
fn encode_topic_attributes_matches_java_attribute_parser_format() {
    let mut attributes = HashMap::<CheetahString, CheetahString>::new();
    attributes.insert("+message.type".into(), "NORMAL".into());

    let encoded = encode_topic_attributes(&attributes);

    assert_eq!(encoded, Some(CheetahString::from("+message.type=NORMAL")));
}

#[test]
fn merge_consume_status_result_combines_offsets_by_client() {
    let mut target = HashMap::new();
    let client_id = CheetahString::from_static_str("client-a");
    let mut first_offsets = HashMap::new();
    first_offsets.insert(MessageQueue::from_parts("TopicA", "broker-a", 0), 12);
    merge_consume_status_result(&mut target, HashMap::from([(client_id.clone(), first_offsets)]))
        .expect("first broker status should merge");

    let mut second_offsets = HashMap::new();
    second_offsets.insert(MessageQueue::from_parts("TopicA", "broker-b", 1), 34);
    merge_consume_status_result(&mut target, HashMap::from([(client_id.clone(), second_offsets)]))
        .expect("second broker status should merge");

    let offsets = target.get(&client_id).expect("client offsets should be present");
    assert_eq!(offsets.len(), 2);
    assert_eq!(
        offsets.get(&MessageQueue::from_parts("TopicA", "broker-a", 0)),
        Some(&12)
    );
    assert_eq!(
        offsets.get(&MessageQueue::from_parts("TopicA", "broker-b", 1)),
        Some(&34)
    );
}

#[test]
fn lite_pull_topic_config_marks_lite_and_uses_queue_num_fallback() {
    let config = lite_pull_topic_config(CheetahString::from("LiteTopic"), 8, 3, 0, 0, false)
        .expect("create lite topic should use queueNum fallback");

    assert_eq!(config.topic_name.as_ref().map(CheetahString::as_str), Some("LiteTopic"));
    assert_eq!(config.read_queue_nums, 8);
    assert_eq!(config.write_queue_nums, 8);
    assert_eq!(config.topic_sys_flag, 3);
    assert_eq!(
        config.attributes.get(&CheetahString::from_static_str("message.type")),
        Some(&CheetahString::from_static_str("LITE"))
    );
}

#[test]
fn lite_pull_topic_config_update_requires_explicit_positive_queue_nums() {
    let error = lite_pull_topic_config(CheetahString::from("LiteTopic"), 0, 0, 0, 8, true)
        .expect_err("update lite topic should not use queueNum fallback");

    assert!(error.to_string().contains("readQueueNums must be positive"));
}

#[test]
fn lite_pull_topic_config_rejects_negative_topic_sys_flag() {
    let error = lite_pull_topic_config(CheetahString::from("LiteTopic"), 8, -1, 0, 0, false)
        .expect_err("negative topicSysFlag should be rejected");

    assert!(error.to_string().contains("topicSysFlag must be non-negative"));
}

#[test]
fn timestamp_to_java_long_rejects_values_outside_java_range() {
    assert_eq!(
        timestamp_to_java_long("resetOffsetNewConcurrent", i64::MAX as u64).expect("max Java long is valid"),
        i64::MAX
    );

    let error = timestamp_to_java_long("resetOffsetNewConcurrent", i64::MAX as u64 + 1)
        .expect_err("value larger than Java long should be rejected");

    assert!(error
        .to_string()
        .contains("resetOffsetNewConcurrent timestamp exceeds Java long range"));
}

#[test]
fn timeout_millis_to_u64_rejects_values_outside_rust_range() {
    assert_eq!(
        timeout_millis_to_u64(Duration::from_millis(u64::MAX)).expect("max u64 millis is valid"),
        u64::MAX
    );

    let error = timeout_millis_to_u64(Duration::from_secs(u64::MAX))
        .expect_err("duration larger than u64 milliseconds should be rejected");

    assert!(error
        .to_string()
        .contains("DefaultMQAdminExt timeoutMillis exceeds Rust u64 millisecond range"));
}

#[test]
fn master_flush_offset_to_java_long_rejects_values_outside_java_range() {
    assert_eq!(
        master_flush_offset_to_java_long(i64::MAX as u64).expect("max Java long is valid"),
        i64::MAX
    );

    let error = master_flush_offset_to_java_long(i64::MAX as u64 + 1)
        .expect_err("value larger than Java long should be rejected");

    assert!(error
        .to_string()
        .contains("resetMasterFlushOffset offset exceeds Java long range"));
}

#[test]
fn query_consume_queue_index_to_java_long_rejects_values_outside_java_range() {
    assert_eq!(
        query_consume_queue_index_to_java_long(i64::MAX as u64).expect("max Java long is valid"),
        i64::MAX
    );

    let error = query_consume_queue_index_to_java_long(i64::MAX as u64 + 1)
        .expect_err("value larger than Java long should be rejected");

    assert!(error
        .to_string()
        .contains("queryConsumeQueue offset exceeds Java long range"));
}

#[test]
fn search_offset_timestamp_to_java_long_rejects_values_outside_java_range() {
    assert_eq!(
        search_offset_timestamp_to_java_long(i64::MAX as u64).expect("max Java long is valid"),
        i64::MAX
    );

    let error = search_offset_timestamp_to_java_long(i64::MAX as u64 + 1)
        .expect_err("value larger than Java long should be rejected");

    assert!(error
        .to_string()
        .contains("searchOffset timestamp exceeds Java long range"));
}

#[test]
fn java_long_to_u64_rejects_negative_values_from_broker() {
    assert_eq!(
        java_long_to_u64("searchOffset", "offset", 42).expect("positive Java long should convert"),
        42
    );

    let error = java_long_to_u64("searchOffset", "offset", -1).expect_err("negative broker offset must not wrap");

    assert!(error
        .to_string()
        .contains("searchOffset offset is negative and cannot be represented as Rust u64"));
}

#[test]
fn global_white_addr_config_rejects_legacy_acl_file_path() {
    validate_acl_file_path_for_global_white_addr_config(Some(&CheetahString::from("/opt/rocketmq/conf/plain_acl.yml")))
        .expect_err("modern ACL 2.0 global white address RPC has no aclFileFullPath field");

    validate_acl_file_path_for_global_white_addr_config(None).expect("missing path is valid");
    validate_acl_file_path_for_global_white_addr_config(Some(&CheetahString::new()))
        .expect("empty path follows Java optional parameter behavior");
}

#[test]
#[allow(deprecated)]
fn broker_operator_result_sets_success_and_failure_lists() {
    let result = broker_operator_result(
        vec![CheetahString::from("broker-a")],
        vec![CheetahString::from("broker-b")],
    );

    assert_eq!(result.get_success_list(), &vec![CheetahString::from("broker-a")]);
    assert_eq!(result.get_failure_list(), &vec![CheetahString::from("broker-b")]);
}

#[test]
fn lite_topic_list_from_broker_info_sorts_topics_and_preserves_addr() {
    let mut lite_info = GetBrokerLiteInfoResponseBody::new();
    lite_info.get_topic_meta_mut().insert(CheetahString::from("topic-b"), 8);
    lite_info.get_topic_meta_mut().insert(CheetahString::from("topic-a"), 4);

    let topic_list = lite_topic_list_from_broker_lite_info(Some(CheetahString::from("127.0.0.1:10911")), &lite_info);

    assert_eq!(
        topic_list.topic_list,
        vec![CheetahString::from("topic-a"), CheetahString::from("topic-b")]
    );
    assert_eq!(topic_list.broker_addr, Some(CheetahString::from("127.0.0.1:10911")));
}

#[test]
fn lite_topic_list_from_names_deduplicates_cluster_results() {
    let topic_list = topic_list_from_lite_topic_names(
        None,
        [
            CheetahString::from("topic-b"),
            CheetahString::from("topic-a"),
            CheetahString::from("topic-b"),
        ],
    );

    assert_eq!(
        topic_list.topic_list,
        vec![CheetahString::from("topic-a"), CheetahString::from("topic-b")]
    );
    assert_eq!(topic_list.broker_addr, None);
}

#[test]
fn lite_subscription_group_list_filters_by_topic() {
    let mut lite_info = GetBrokerLiteInfoResponseBody::new();
    let topic = CheetahString::from("topic-a");
    let groups = HashSet::from([CheetahString::from("group-a"), CheetahString::from("group-b")]);
    let mut group_meta = HashMap::new();
    group_meta.insert(topic.clone(), groups.clone());
    lite_info.set_group_meta(group_meta);

    let group_list = lite_subscription_group_list_from_broker_lite_info(&topic, &lite_info);
    let missing_group_list =
        lite_subscription_group_list_from_broker_lite_info(&CheetahString::from("topic-missing"), &lite_info);

    assert_eq!(group_list.group_list, groups);
    assert!(missing_group_list.group_list.is_empty());
}

#[tokio::test]
async fn acl_info_facades_without_started_client_return_typed_errors() {
    let admin = new_unstarted_admin();
    let acl_info = AclInfo {
        subject: Some(CheetahString::from("User:alice")),
        policies: None,
    };

    let error = admin
        .create_acl_with_acl_info(CheetahString::from("127.0.0.1:10911"), acl_info.clone())
        .await
        .expect_err("create_acl_with_acl_info should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .update_acl_with_acl_info(CheetahString::from("127.0.0.1:10911"), acl_info)
        .await
        .expect_err("update_acl_with_acl_info should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[tokio::test]
async fn acl_subject_facades_without_started_client_return_typed_errors() {
    let admin = new_unstarted_admin();

    let error = AuthAdmin::create_acl_with_info(
        &admin,
        CheetahString::from("127.0.0.1:10911"),
        CheetahString::from("User:alice"),
    )
    .await
    .expect_err("create_acl_with_info should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = AuthAdmin::update_acl_with_info(
        &admin,
        CheetahString::from("127.0.0.1:10911"),
        CheetahString::from("User:alice"),
    )
    .await
    .expect_err("update_acl_with_info should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[tokio::test]
async fn examine_broker_cluster_acl_version_info_without_started_client_returns_typed_error() {
    let admin = new_unstarted_admin();

    let error = AuthAdmin::examine_broker_cluster_acl_version_info(&admin, CheetahString::from("127.0.0.1:10911"))
        .await
        .expect_err("examine_broker_cluster_acl_version_info should require a started client");

    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[tokio::test]
async fn acl_info_facades_reject_blank_subject_before_remoting() {
    let admin = new_unstarted_admin();
    let acl_info = AclInfo {
        subject: Some(CheetahString::default()),
        policies: None,
    };

    let error = admin
        .create_acl_with_acl_info(CheetahString::from("127.0.0.1:10911"), acl_info.clone())
        .await
        .expect_err("create_acl_with_acl_info should reject blank subject locally");
    assert!(matches!(error, rocketmq_error::RocketMQError::IllegalArgument(_)));

    let error = admin
        .update_acl_with_acl_info(CheetahString::from("127.0.0.1:10911"), acl_info)
        .await
        .expect_err("update_acl_with_acl_info should reject blank subject locally");
    assert!(matches!(error, rocketmq_error::RocketMQError::IllegalArgument(_)));
}

#[tokio::test]
async fn acl_subject_facades_reject_blank_subject_before_remoting() {
    let admin = new_unstarted_admin();

    let error =
        AuthAdmin::create_acl_with_info(&admin, CheetahString::from("127.0.0.1:10911"), CheetahString::default())
            .await
            .expect_err("create_acl_with_info should reject blank subject locally");
    assert!(matches!(error, rocketmq_error::RocketMQError::IllegalArgument(_)));

    let error =
        AuthAdmin::update_acl_with_info(&admin, CheetahString::from("127.0.0.1:10911"), CheetahString::default())
            .await
            .expect_err("update_acl_with_info should reject blank subject locally");
    assert!(matches!(error, rocketmq_error::RocketMQError::IllegalArgument(_)));
}

#[test]
fn producer_connection_empty_set_represents_offline_group() {
    let connection = ProducerConnection::new();
    assert!(connection.connection_set().is_empty());
}

#[test]
fn producer_connection_with_entries_represents_online_group() {
    let mut connection = ProducerConnection::new();
    let mut entry = Connection::new();
    entry.set_client_id("client-a".into());
    connection.connection_set_mut().insert(entry);

    assert_eq!(connection.connection_set().len(), 1);
}

#[test]
fn select_consumer_direct_connection_uses_requested_client_when_present() {
    let consumer_group = CheetahString::from("group-a");
    let requested_client_id = CheetahString::from("client-b");
    let mut consumer_connection = ConsumerConnection::new();
    let mut first = Connection::new();
    first.set_client_id("client-a".into());
    first.set_client_addr("127.0.0.1:1001".into());
    let mut second = Connection::new();
    second.set_client_id(requested_client_id.clone());
    second.set_client_addr("127.0.0.1:1002".into());
    consumer_connection.insert_connection(first);
    consumer_connection.insert_connection(second);

    let (client_id, client_addr) =
        select_consumer_direct_connection(&consumer_group, &consumer_connection, Some(&requested_client_id))
            .expect("requested client should be selected");

    assert_eq!(client_id, requested_client_id);
    assert_eq!(client_addr, CheetahString::from("127.0.0.1:1002"));
}

#[test]
fn select_consumer_direct_connection_returns_first_available_client_when_unspecified() {
    let consumer_group = CheetahString::from("group-a");
    let mut consumer_connection = ConsumerConnection::new();
    let mut only = Connection::new();
    only.set_client_id("client-a".into());
    only.set_client_addr("127.0.0.1:1001".into());
    consumer_connection.insert_connection(only);

    let (client_id, client_addr) =
        select_consumer_direct_connection(&consumer_group, &consumer_connection, Some(&CheetahString::default()))
            .expect("single consumer should be selected");

    assert_eq!(client_id, CheetahString::from("client-a"));
    assert_eq!(client_addr, CheetahString::from("127.0.0.1:1001"));
}

#[test]
fn select_consumer_direct_connection_errors_when_group_is_offline() {
    let consumer_group = CheetahString::from("group-a");
    let consumer_connection = ConsumerConnection::new();

    let error = select_consumer_direct_connection(&consumer_group, &consumer_connection, None)
        .expect_err("offline group should not resolve a client");

    assert!(error.to_string().contains("NO CONSUMER"));
}

#[test]
#[allow(deprecated)]
fn resolve_consumed_track_type_marks_filtered_subscription() {
    let message = MessageBuilder::new()
        .topic("TopicTest")
        .body_slice(b"payload")
        .tags("TagA")
        .build_unchecked();
    let mut message_ext = MessageExt::default();
    message_ext.set_message_inner(message);

    let mut subscription = SubscriptionData {
        topic: CheetahString::from("TopicTest"),
        ..Default::default()
    };
    subscription.tags_set = BTreeSet::from([CheetahString::from("TagB")]);

    let mut connection = ConsumerConnection::new();
    connection.set_consume_type(ConsumeType::ConsumePassively);
    connection
        .get_subscription_table_mut()
        .insert(CheetahString::from("TopicTest"), subscription);

    let track_type = resolve_consumed_track_type(&message_ext, &connection);

    assert_eq!(track_type, TrackType::ConsumedButFiltered);
}

#[test]
fn is_message_consumed_returns_true_when_offset_has_advanced_on_master() {
    let message = MessageBuilder::new()
        .topic("TopicTest")
        .body_slice(b"payload")
        .build_unchecked();
    let mut message_ext = MessageExt::default();
    message_ext.set_message_inner(message);
    message_ext.set_queue_id(1);
    message_ext.set_queue_offset(10);
    message_ext.set_store_host("127.0.0.1:10911".parse().expect("store host"));

    let mut consume_stats = ConsumeStats::new();
    let mut offset_wrapper = OffsetWrapper::default();
    offset_wrapper.set_consumer_offset(11);
    consume_stats
        .get_offset_table_mut()
        .insert(MessageQueue::from_parts("TopicTest", "broker-a", 1), offset_wrapper);

    let mut broker_addrs = HashMap::new();
    broker_addrs.insert(mix_all::MASTER_ID, CheetahString::from("127.0.0.1:10911"));
    let broker_data = BrokerData::new(
        CheetahString::from("cluster-a"),
        CheetahString::from("broker-a"),
        broker_addrs,
        None,
    );
    let cluster_info = ClusterInfo::new(
        Some(HashMap::from([(CheetahString::from("broker-a"), broker_data)])),
        None,
    );

    assert!(is_message_consumed(&message_ext, &consume_stats, &cluster_info));
}

#[test]
fn filter_consume_stats_keeps_only_matching_topic_and_queue() {
    let mut consume_stats = ConsumeStats::new();
    consume_stats.set_consume_tps(12.5);

    for (topic, queue_id, consumer_offset) in [("TopicTest", 0, 10), ("TopicTest", 1, 11), ("OtherTopic", 0, 12)] {
        let mut offset_wrapper = OffsetWrapper::default();
        offset_wrapper.set_consumer_offset(consumer_offset);
        consume_stats
            .get_offset_table_mut()
            .insert(MessageQueue::from_parts(topic, "broker-a", queue_id), offset_wrapper);
    }

    filter_consume_stats(&mut consume_stats, Some(&CheetahString::from("TopicTest")), Some(0));

    assert_eq!(consume_stats.get_consume_tps(), 12.5);
    assert_eq!(consume_stats.get_offset_table().len(), 1);
    let (queue, offset) = consume_stats
        .get_offset_table()
        .iter()
        .next()
        .expect("one queue should remain");
    assert_eq!(queue.topic(), &CheetahString::from("TopicTest"));
    assert_eq!(queue.queue_id(), 0);
    assert_eq!(offset.get_consumer_offset(), 10);
}

#[test]
fn broker_cleanup_cluster_helpers_match_java_cluster_expansion() {
    let broker_a = BrokerData::new(
        CheetahString::from("cluster-a"),
        CheetahString::from("broker-a"),
        HashMap::from([
            (mix_all::MASTER_ID, CheetahString::from("127.0.0.1:10911")),
            (1, CheetahString::from("127.0.0.1:10912")),
        ]),
        None,
    );
    let broker_b = BrokerData::new(
        CheetahString::from("cluster-b"),
        CheetahString::from("broker-b"),
        HashMap::from([(mix_all::MASTER_ID, CheetahString::from("127.0.0.2:10911"))]),
        None,
    );
    let cluster_info = ClusterInfo::new(
        Some(HashMap::from([
            (CheetahString::from("broker-a"), broker_a),
            (CheetahString::from("broker-b"), broker_b),
        ])),
        Some(HashMap::from([
            (
                CheetahString::from("cluster-a"),
                HashSet::from([CheetahString::from("broker-a")]),
            ),
            (
                CheetahString::from("cluster-b"),
                HashSet::from([CheetahString::from("broker-b")]),
            ),
        ])),
    );

    let mut cluster_names = cluster_names_for_admin_operation(&cluster_info, None);
    cluster_names.sort();
    assert_eq!(
        cluster_names,
        vec![CheetahString::from("cluster-a"), CheetahString::from("cluster-b")]
    );

    let mut addrs = broker_addrs_for_cluster(&cluster_info, &CheetahString::from("cluster-a"));
    addrs.sort();
    assert_eq!(
        addrs,
        vec![
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("127.0.0.1:10912")
        ]
    );
}

#[test]
fn parse_response_code_from_message_reads_consumer_not_online_code() {
    let code = parse_response_code_from_message("CODE: 206 DESC: Not found the consumer group connection");

    assert_eq!(code, Some(ResponseCode::ConsumerNotOnline));
}

#[test]
fn topic_config_lookup_maps_topic_not_exist_to_false_only() {
    let missing = mq_client_err!(ResponseCode::TopicNotExist as i32, "topic not exist");
    assert!(!map_topic_config_lookup_result::<()>(Err(missing)).expect("TopicNotExist should map to false"));

    let client_not_started = RocketMQError::ClientNotStarted;
    let error = map_topic_config_lookup_result::<()>(Err(client_not_started))
        .expect_err("non broker topic-not-exist errors should stay typed errors");
    assert!(matches!(error, RocketMQError::ClientNotStarted));
}

#[test]
fn reset_offset_new_fallback_classifier_matches_java_consumer_not_online_branch() {
    let broker_error =
        RocketMQError::broker_operation_failed("BROKER_OPERATION", ResponseCode::ConsumerNotOnline as i32, "offline")
            .with_broker_addr("127.0.0.1:10911");
    assert!(is_consumer_not_online_error(&broker_error));

    let legacy_client_error = mq_client_err!(
        ResponseCode::ConsumerNotOnline as i32,
        "Not found the consumer group connection"
    );
    assert!(is_consumer_not_online_error(&legacy_client_error));

    let other_error =
        RocketMQError::broker_operation_failed("BROKER_OPERATION", ResponseCode::SystemError as i32, "system error");
    assert!(!is_consumer_not_online_error(&other_error));
}

#[test]
fn update_consume_offset_request_header_matches_java_fields() {
    let mq = MessageQueue::from_parts("TopicTest", "broker-a", 3);

    let header = update_consume_offset_request_header(CheetahString::from("group-a"), &mq, 42)
        .expect("valid offset should build update offset header");

    assert_eq!(header.consumer_group, "group-a");
    assert_eq!(header.topic, "TopicTest");
    assert_eq!(header.queue_id, 3);
    assert_eq!(header.commit_offset, 42);
    assert_eq!(
        header
            .topic_request_header
            .as_ref()
            .and_then(|topic_header| topic_header.rpc.as_ref())
            .and_then(|rpc_header| rpc_header.broker_name.as_ref()),
        Some(&CheetahString::from("broker-a"))
    );
}

#[test]
fn update_consume_offset_request_header_rejects_offsets_outside_java_long_range() {
    let mq = MessageQueue::from_parts("TopicTest", "broker-a", 0);

    let error = update_consume_offset_request_header(CheetahString::from("group-a"), &mq, i64::MAX as u64 + 1)
        .expect_err("offset larger than Java long should be rejected");

    assert!(error.to_string().contains("offset exceeds Java long range"));
}

#[test]
fn lite_pull_update_consumer_offset_request_header_matches_java_fields() {
    let header = lite_pull_update_consumer_offset_request_header(
        CheetahString::from("TopicTest"),
        CheetahString::from("group-a"),
        2,
        42,
    )
    .expect("valid lite pull offset should build update offset header");

    assert_eq!(header.consumer_group, "group-a");
    assert_eq!(header.topic, "TopicTest");
    assert_eq!(header.queue_id, 2);
    assert_eq!(header.commit_offset, 42);
    assert!(header.topic_request_header.is_none());
}

#[test]
fn lite_pull_update_consumer_offset_rejects_offsets_outside_java_long_range() {
    let error = lite_pull_update_consumer_offset_request_header(
        CheetahString::from("TopicTest"),
        CheetahString::from("group-a"),
        0,
        i64::MAX as u64 + 1,
    )
    .expect_err("offset larger than Java long should be rejected");

    assert!(error.to_string().contains("offset exceeds Java long range"));
}

#[test]
fn notify_min_broker_id_change_request_header_matches_java_fields() {
    let header = notify_min_broker_id_change_request_header(
        1,
        CheetahString::from("127.0.0.1:10912"),
        Some(CheetahString::from("127.0.0.1:10911")),
        Some(CheetahString::from("127.0.0.1:10913")),
    )
    .expect("valid notify-min-broker header should build");

    assert_eq!(header.min_broker_id, Some(1));
    assert!(header.broker_name.is_none());
    assert_eq!(header.min_broker_addr.as_deref(), Some("127.0.0.1:10912"));
    assert_eq!(header.offline_broker_addr.as_deref(), Some("127.0.0.1:10911"));
    assert_eq!(header.ha_broker_addr.as_deref(), Some("127.0.0.1:10913"));
}

#[test]
fn notify_min_broker_id_change_request_header_rejects_blank_min_broker_addr() {
    let error = notify_min_broker_id_change_request_header(1, CheetahString::new(), None, None)
        .expect_err("blank min broker address should be rejected before remoting");

    assert!(error.to_string().contains("requires minBrokerAddr"));
}

#[test]
fn choose_min_broker_notify_addrs_matches_java_new_broker_rule() {
    let broker_addrs = HashMap::from([
        (0, CheetahString::from("127.0.0.1:10911")),
        (1, CheetahString::from("127.0.0.1:10912")),
    ]);

    let notify_addrs = choose_min_broker_notify_addrs(&broker_addrs, 0, None);

    assert_eq!(notify_addrs, vec![CheetahString::from("127.0.0.1:10912")]);
}

#[test]
fn choose_min_broker_notify_addrs_matches_java_offline_and_single_broker_rules() {
    let broker_addrs = HashMap::from([
        (0, CheetahString::from("127.0.0.1:10911")),
        (1, CheetahString::from("127.0.0.1:10912")),
    ]);

    let notify_addrs = choose_min_broker_notify_addrs(&broker_addrs, 1, Some(&CheetahString::from("127.0.0.1:10911")));
    assert_eq!(
        notify_addrs,
        vec![
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("127.0.0.1:10912")
        ]
    );

    let single_broker = HashMap::from([(2, CheetahString::from("127.0.0.1:10913"))]);
    let notify_addrs = choose_min_broker_notify_addrs(&single_broker, 2, None);
    assert_eq!(notify_addrs, vec![CheetahString::from("127.0.0.1:10913")]);
}

#[tokio::test]
async fn update_consume_offset_without_started_client_returns_typed_error() {
    let admin = new_unstarted_admin();
    let mq = MessageQueue::from_parts("TopicTest", "broker-a", 0);

    let error = admin
        .update_consume_offset(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("group-a"),
            mq,
            1,
        )
        .await
        .expect_err("unstarted admin should not try to send update offset");

    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[tokio::test]
async fn update_lite_pull_consumer_offset_without_started_client_returns_typed_error() {
    let admin = new_unstarted_admin();

    let error = admin
        .update_lite_pull_consumer_offset(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("TopicTest"),
            CheetahString::from("group-a"),
            0,
            1,
        )
        .await
        .expect_err("unstarted admin should not try to send lite pull update offset");

    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[tokio::test]
async fn sync_broker_member_group_without_started_client_returns_typed_error() {
    let admin = new_unstarted_admin();

    let error = admin
        .sync_broker_member_group(
            CheetahString::new(),
            CheetahString::from("cluster-a"),
            CheetahString::from("broker-a"),
        )
        .await
        .expect_err("sync_broker_member_group should require a started client");

    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[tokio::test]
async fn sync_broker_member_group_rejects_controller_addr() {
    let admin = new_unstarted_admin();

    let error = admin
        .sync_broker_member_group(
            CheetahString::from("127.0.0.1:9878"),
            CheetahString::from("cluster-a"),
            CheetahString::from("broker-a"),
        )
        .await
        .expect_err("sync_broker_member_group should not silently ignore controller_addr");

    assert!(matches!(
        error,
        rocketmq_error::RocketMQError::IllegalArgument(message)
            if message.contains("controllerAddr is not supported")
    ));
}

#[tokio::test]
async fn notify_min_broker_id_changed_without_started_client_returns_typed_error() {
    let admin = new_unstarted_admin();

    let error = admin
        .notify_min_broker_id_changed(
            CheetahString::from("cluster-a"),
            CheetahString::from("broker-a"),
            1,
            CheetahString::from("127.0.0.1:10912"),
            None,
            None,
        )
        .await
        .expect_err("notify_min_broker_id_changed should require a started client");

    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[tokio::test]
async fn export_rocksdb_consumer_offset_to_json_rejects_local_file_path() {
    let admin = new_unstarted_admin();

    let error = admin
        .export_rocksdb_consumer_offset_to_json(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("D:/tmp/consumerOffsets.json"),
        )
        .await
        .expect_err("RPC export cannot accept local export file path");

    assert!(matches!(
        error,
        rocketmq_error::RocketMQError::IllegalArgument(message)
            if message.contains("filePath is local-mode only")
    ));
}

#[tokio::test]
async fn export_rocksdb_consumer_offset_to_json_without_started_client_returns_typed_error() {
    let admin = new_unstarted_admin();

    let error = admin
        .export_rocksdb_consumer_offset_to_json(CheetahString::from("127.0.0.1:10911"), CheetahString::new())
        .await
        .expect_err("RPC export should require a started client");

    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[tokio::test]
async fn export_rocksdb_consumer_offset_from_memory_without_started_client_returns_typed_error() {
    let admin = new_unstarted_admin();

    let error = admin
        .export_rocksdb_consumer_offset_from_memory(CheetahString::from("127.0.0.1:10911"))
        .await
        .expect_err("memory export should require a started client");

    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[test]
fn reset_offset_by_queue_id_request_headers_match_java_fields() {
    let (update_header, reset_header) = reset_offset_by_queue_id_request_headers(
        CheetahString::from("group-a"),
        CheetahString::from("TopicTest"),
        2,
        100,
    )
    .expect("valid reset offset should build request headers");

    assert_eq!(update_header.consumer_group, "group-a");
    assert_eq!(update_header.topic, "TopicTest");
    assert_eq!(update_header.queue_id, 2);
    assert_eq!(update_header.commit_offset, 100);
    assert!(update_header.topic_request_header.is_none());

    assert_eq!(reset_header.group, "group-a");
    assert_eq!(reset_header.topic, "TopicTest");
    assert_eq!(reset_header.queue_id, 2);
    assert_eq!(reset_header.offset, Some(100));
    assert_eq!(reset_header.timestamp, 0);
    assert!(!reset_header.is_force);
    assert!(reset_header.topic_request_header.is_none());
}

#[test]
fn reset_offset_by_queue_id_rejects_offsets_outside_java_long_range() {
    let error = reset_offset_by_queue_id_request_headers(
        CheetahString::from("group-a"),
        CheetahString::from("TopicTest"),
        0,
        i64::MAX as u64 + 1,
    )
    .expect_err("offset larger than Java long should be rejected");

    assert!(error
        .to_string()
        .contains("resetOffsetByQueueId offset exceeds Java long range"));
}

#[tokio::test]
async fn reset_offset_by_queue_id_without_started_client_returns_typed_error() {
    let admin = new_unstarted_admin();

    let error = admin
        .reset_offset_by_queue_id(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("group-a"),
            CheetahString::from("TopicTest"),
            0,
            100,
        )
        .await
        .expect_err("unstarted admin should not try to reset offset");

    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[test]
fn update_group_forbidden_request_header_matches_java_fields() {
    let header = update_group_forbidden_request_header(
        CheetahString::from("group-a"),
        CheetahString::from("TopicTest"),
        Some(false),
    );

    assert_eq!(header.group, "group-a");
    assert_eq!(header.topic, "TopicTest");
    assert_eq!(header.readable, Some(false));
    assert!(header.topic_request_header.is_none());
}

#[test]
fn update_group_forbidden_request_header_preserves_unspecified_readable() {
    let header =
        update_group_forbidden_request_header(CheetahString::from("group-a"), CheetahString::from("TopicTest"), None);

    assert_eq!(header.readable, None);
}

#[tokio::test]
async fn update_and_get_group_read_forbidden_without_started_client_returns_typed_error() {
    let admin = new_unstarted_admin();

    let error = admin
        .update_and_get_group_read_forbidden(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("group-a"),
            CheetahString::from("TopicTest"),
            Some(false),
        )
        .await
        .expect_err("unstarted admin should not try to update group forbidden state");

    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));
}

#[tokio::test]
async fn directly_owned_impl_starts_and_stops_without_self_reference() {
    let mut admin = new_unstarted_admin();

    admin
        .start()
        .await
        .expect("directly owned admin implementation should start without self wiring");
    assert_eq!(admin.service_state, ServiceState::Running);

    admin.shutdown().await;

    assert_eq!(admin.service_state, ServiceState::ShutdownAlready);
}

#[tokio::test]
async fn admin_api_facades_without_started_client_return_typed_errors() {
    let admin = new_unstarted_admin();
    let mq = MessageQueue::from_parts("TopicTest", "broker-a", 0);

    let result = admin
        .pull_message_from_queue("127.0.0.1:10911", &mq, "*", 0, 32, 3000)
        .await;
    assert!(matches!(result, Err(rocketmq_error::RocketMQError::ClientNotStarted)));

    let error = admin
        .delete_topic_in_broker(
            HashSet::from([CheetahString::from("127.0.0.1:10911")]),
            CheetahString::from("TopicTest"),
        )
        .await
        .expect_err("delete_topic_in_broker should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .set_message_request_mode(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("TopicTest"),
            CheetahString::from("group-a"),
            MessageRequestMode::Pull,
            0,
            3000,
        )
        .await
        .expect_err("set_message_request_mode should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .get_parent_topic_info(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
        .await
        .expect_err("get_parent_topic_info should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .create_lite_pull_topic(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("LiteTopic"),
            4,
            0,
            0,
            0,
        )
        .await
        .expect_err("create_lite_pull_topic should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .update_lite_pull_topic(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("LiteTopic"),
            4,
            4,
        )
        .await
        .expect_err("update_lite_pull_topic should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .get_lite_pull_topic(CheetahString::from("127.0.0.1:10911"), CheetahString::from("LiteTopic"))
        .await
        .expect_err("get_lite_pull_topic should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .delete_lite_pull_topic(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("cluster-a"),
            CheetahString::from("LiteTopic"),
        )
        .await
        .expect_err("delete_lite_pull_topic should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .query_lite_pull_topic_list(CheetahString::from("127.0.0.1:10911"))
        .await
        .expect_err("query_lite_pull_topic_list should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .query_lite_pull_topic_by_cluster(CheetahString::from("cluster-a"))
        .await
        .expect_err("query_lite_pull_topic_by_cluster should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .query_lite_pull_subscription_list(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
        .await
        .expect_err("query_lite_pull_subscription_list should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .examine_topic_config(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
        .await
        .expect_err("examine_topic_config should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .get_topic_config_by_topic_name(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
        .await
        .expect_err("get_topic_config_by_topic_name should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .get_topic_stats_info(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
        .await
        .expect_err("get_topic_stats_info should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .query_broker_has_topic(CheetahString::from("127.0.0.1:10911"), CheetahString::from("TopicTest"))
        .await
        .expect_err("query_broker_has_topic should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .fetch_topics_by_cluster(CheetahString::from("cluster-a"))
        .await
        .expect_err("fetch_topics_by_cluster should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .create_and_update_plain_access_config(
            CheetahString::from("127.0.0.1:10911"),
            PlainAccessConfig {
                access_key: Some(CheetahString::from("AK")),
                secret_key: Some(CheetahString::from("SK")),
                admin: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("create_and_update_plain_access_config should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .delete_plain_access_config(CheetahString::from("127.0.0.1:10911"), CheetahString::from("AK"))
        .await
        .expect_err("delete_plain_access_config should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .get_system_topic_list_from_broker(CheetahString::from("127.0.0.1:10911"))
        .await
        .expect_err("get_system_topic_list_from_broker should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .get_kv_list_by_namespace(CheetahString::from("namespace-a"))
        .await
        .expect_err("get_kv_list_by_namespace should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .examine_topic_route_info_with_timeout(CheetahString::from("TopicTest"), 3000)
        .await
        .expect_err("examine_topic_route_info_with_timeout should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .query_consume_time_span(CheetahString::from("TopicTest"), CheetahString::from("group-a"))
        .await
        .expect_err("query_consume_time_span should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .reset_offset_new(
            CheetahString::from("group-a"),
            CheetahString::from("TopicTest"),
            1_700_000_000_000,
        )
        .await
        .expect_err("reset_offset_new should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let result = admin
        .reset_offset_new_concurrent(
            CheetahString::from("group-a"),
            CheetahString::from("TopicTest"),
            1_700_000_000_000,
        )
        .await;
    assert!(!result.is_success());
    assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

    let error = admin
        .query_topics_by_consumer(CheetahString::from("group-a"))
        .await
        .expect_err("query_topics_by_consumer should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let result = admin
        .query_topics_by_consumer_concurrent(CheetahString::from("group-a"))
        .await;
    assert!(!result.is_success());
    assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

    let error = admin
        .examine_consume_stats_with_queue(
            CheetahString::from("group-a"),
            Some(CheetahString::from("TopicTest")),
            Some(0),
        )
        .await
        .expect_err("examine_consume_stats_with_queue should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let result = admin
        .examine_consume_stats_concurrent(CheetahString::from("group-a"), Some(CheetahString::from("TopicTest")))
        .await;
    assert!(!result.is_success());
    assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

    let result = admin
        .examine_consume_stats_concurrent_with_cluster(
            CheetahString::from("group-a"),
            Some(CheetahString::from("TopicTest")),
            Some(CheetahString::from("cluster-a")),
        )
        .await;
    assert!(!result.is_success());
    assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

    let error = admin
        .query_subscription(CheetahString::from("group-a"), CheetahString::from("TopicTest"))
        .await
        .expect_err("query_subscription should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .clone_group_offset(
            CheetahString::from("source-group"),
            CheetahString::from("target-group"),
            CheetahString::from("TopicTest"),
            false,
        )
        .await
        .expect_err("clone_group_offset should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .get_cluster_list(String::from("TopicTest"))
        .await
        .expect_err("get_cluster_list should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let result = admin
        .fetch_consume_stats_in_broker(CheetahString::from("127.0.0.1:10911"), false, 3000)
        .await;
    assert!(matches!(result, Err(rocketmq_error::RocketMQError::ClientNotStarted)));

    let error = admin
        .update_global_white_addr_config(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("10.10.*.*"),
            None,
        )
        .await
        .expect_err("update_global_white_addr_config should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .update_global_white_addr_config(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("10.10.*.*"),
            Some(CheetahString::from("/opt/rocketmq/conf/plain_acl.yml")),
        )
        .await
        .expect_err("update_global_white_addr_config should require a started client before validating RPC fields");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .create_static_topic(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("TBW102"),
            TopicConfig::new("TopicTest"),
            TopicQueueMappingDetail::default(),
            true,
        )
        .await
        .expect_err("create_static_topic should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .resume_check_half_message(
            CheetahString::from("TopicTest"),
            CheetahString::from("AC11000100002A9F0000000000000001"),
        )
        .await
        .expect_err("resume_check_half_message should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .switch_timer_engine(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from(MessageConst::TIMER_ENGINE_ROCKSDB_TIMELINE),
        )
        .await
        .expect_err("switch_timer_engine should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .remove_cold_data_flow_ctr_group_config(CheetahString::from("127.0.0.1:10911"), CheetahString::from("group-a"))
        .await
        .expect_err("remove_cold_data_flow_ctr_group_config should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .get_cold_data_flow_ctr_info(CheetahString::from("127.0.0.1:10911"))
        .await
        .expect_err("get_cold_data_flow_ctr_info should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .set_commit_log_read_ahead_mode(CheetahString::from("127.0.0.1:10911"), CheetahString::from("1"))
        .await
        .expect_err("set_commit_log_read_ahead_mode should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .create_user_with_info(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("alice"),
            CheetahString::from("secret"),
        )
        .await
        .expect_err("create_user_with_info should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .update_user_with_info(
            CheetahString::from("127.0.0.1:10911"),
            CheetahString::from("alice"),
            CheetahString::from("new-secret"),
        )
        .await
        .expect_err("update_user_with_info should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .export_pop_records(CheetahString::from("127.0.0.1:10911"), 3000)
        .await
        .expect_err("export_pop_records should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .clean_expired_consumer_queue(None, Some(CheetahString::from("127.0.0.1:10911")))
        .await
        .expect_err("clean_expired_consumer_queue should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .clean_expired_consumer_queue_by_addr(CheetahString::from("127.0.0.1:10911"))
        .await
        .expect_err("clean_expired_consumer_queue_by_addr should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .delete_expired_commit_log(None, Some(CheetahString::from("127.0.0.1:10911")))
        .await
        .expect_err("delete_expired_commit_log should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .delete_expired_commit_log_by_addr(CheetahString::from("127.0.0.1:10911"))
        .await
        .expect_err("delete_expired_commit_log_by_addr should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .clean_unused_topic(None, Some(CheetahString::from("127.0.0.1:10911")))
        .await
        .expect_err("clean_unused_topic should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let error = admin
        .clean_unused_topic_by_addr(CheetahString::from("127.0.0.1:10911"))
        .await
        .expect_err("clean_unused_topic_by_addr should require a started client");
    assert!(matches!(error, rocketmq_error::RocketMQError::ClientNotStarted));

    let result = admin
        .delete_topic_in_broker_concurrent(
            HashSet::from([CheetahString::from("127.0.0.1:10911")]),
            CheetahString::from("TopicTest"),
        )
        .await;
    assert!(!result.is_success());
    assert_eq!(result.get_code(), AdminToolsResultCodeEnum::MQClientError.get_code());

    let namesrv_list = admin.get_name_server_address_list().await;
    assert!(namesrv_list.is_empty());
}
