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
use std::sync::Arc;
use std::sync::Weak;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::FAQUrl;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::header::notification_response_header::NotificationResponseHeader;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageStatus;
use rocketmq_transport::api::DeferredWakeReason;

use super::super::core::NotificationCoreOutcome;
use super::super::NotificationPolicy;
use super::super::NotificationPopOffsetCapability;
use super::super::NotificationProcessor;
use super::super::NotificationProcessorContext;
use super::super::NotificationStoreCapability;
use crate::broker_runtime::BrokerMessageStore;
use crate::broker_runtime::BrokerRuntime;
use crate::failover::escape_bridge::EscapeBridge;
use crate::long_polling::notification_deferred::deadline::NotificationWaitDeadline;
use crate::long_polling::notification_deferred::index::NotificationMatchCriteria;
use crate::long_polling::notification_deferred::service::NotificationRequestData;
use crate::long_polling::notification_deferred::service::ResumeNotification;

fn temp_test_root(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "rocketmq-rust-notification-resume-{}-{label}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create Notification resume test root");
    path
}

fn available_ha_port() -> usize {
    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("reserve an ephemeral Notification resume HA port")
        .local_addr()
        .expect("read the ephemeral Notification resume HA port")
        .port() as usize
}

async fn runtime(label: &str) -> (BrokerRuntime, PathBuf) {
    let root = temp_test_root(label);
    let broker_config = Arc::new(crate::config::broker_config::BrokerConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        auth_config_path: root.join("auth.json").to_string_lossy().into_owned().into(),
        auto_create_subscription_group: false,
        ..Default::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        ha_listen_port: available_ha_port(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    runtime
        .initialize()
        .await
        .expect("initialize Notification resume runtime");
    runtime.seed_pop_topic_and_group_for_test("topic-a", "group-a");
    runtime
        .start_message_store_for_test()
        .await
        .expect("start Notification resume message store");
    (runtime, root)
}

fn processor(
    runtime: &mut BrokerRuntime,
) -> (
    Arc<NotificationProcessor<BrokerMessageStore>>,
    Arc<EscapeBridge<BrokerMessageStore>>,
) {
    let inner = runtime.runtime_state_mut();
    let policy = NotificationPolicy::from_config(&inner.broker_config());
    processor_with_policy(inner, policy)
}

fn processor_with_policy(
    inner: &mut crate::broker_runtime::BrokerRuntimeState<BrokerMessageStore>,
    policy: NotificationPolicy,
) -> (
    Arc<NotificationProcessor<BrokerMessageStore>>,
    Arc<EscapeBridge<BrokerMessageStore>>,
) {
    let topic_config_manager = inner.topic_config_manager_handle();
    let subscription_group_lookup = inner.subscription_group_manager().config_lookup();
    let consumer_filter_manager = Arc::new(inner.consumer_filter_manager().clone());
    let escape_bridge = inner.escape_bridge();
    let processor = NotificationProcessor::new(NotificationProcessorContext::new(
        policy,
        inner.pop_policy_state(),
        topic_config_manager,
        subscription_group_lookup,
        consumer_filter_manager,
        inner.consumer_order_info_manager_handle(),
        inner.consumer_offset_manager_handle().query_capability(),
        NotificationStoreCapability::new(&escape_bridge),
        NotificationPopOffsetCapability {
            merge_service: Weak::new(),
        },
    ));
    (processor, escape_bridge)
}

fn core_header(topic: &str, group: &str, queue_id: i32) -> NotificationRequestHeader {
    let now = i64::try_from(current_millis()).expect("current wall time fits Notification protocol");
    NotificationRequestHeader {
        consumer_group: CheetahString::from_string(group.to_owned()),
        topic: CheetahString::from_string(topic.to_owned()),
        queue_id,
        poll_time: 0,
        born_time: now,
        order: false,
        attempt_id: None,
        exp_type: None,
        exp: None,
        is_lite_consumer: false,
        client_id: None,
        topic_request_header: None,
    }
}

fn resume_request() -> ResumeNotification {
    let now = i64::try_from(current_millis()).expect("current wall time fits Notification protocol");
    let header = NotificationRequestHeader {
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("topic-a"),
        queue_id: 0,
        poll_time: 60_000,
        born_time: now,
        order: false,
        attempt_id: None,
        exp_type: None,
        exp: None,
        is_lite_consumer: false,
        client_id: None,
        topic_request_header: None,
    };
    let deadline = NotificationWaitDeadline::checked(now, 60_000, now, tokio::time::Instant::now())
        .expect("live Notification test deadline");
    ResumeNotification::for_test(
        NotificationRequestData::new(header, "127.0.0.1:19001".parse().expect("test peer")),
        Arc::new(NotificationMatchCriteria::new(None, None)),
        deadline,
    )
}

fn stored_message() -> MessageExtBrokerInner {
    stored_message_for("topic-a")
}

fn stored_message_for(topic: &str) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_string(topic.to_owned()));
    message.message_ext_inner.set_queue_id(0);
    message.set_body(Bytes::from_static(b"deferred-notification-message"));
    message.set_wait_store_msg_ok(false);
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

#[tokio::test]
async fn notification_core_finds_message_from_configured_retry_topic() {
    let (mut runtime, root) = runtime("retry-found").await;
    let (processor, escape_bridge) = processor(&mut runtime);
    let retry_topic = processor
        .context
        .retry_policies
        .retry_policy(&CheetahString::from_static_str("group-a"))
        .read_topics("topic-a", "group-a")
        .into_iter()
        .next()
        .expect("Notification retry policy has a readable topic");
    let _ = runtime
        .runtime_state_mut()
        .topic_config_manager_handle()
        .update_topic_config(TopicConfig::with_queues(retry_topic.clone(), 1, 1), 0);
    let put = escape_bridge
        .put_message_to_local_store(stored_message_for(&retry_topic))
        .await
        .expect("append Notification retry message");
    assert_eq!(put.put_message_status(), PutMessageStatus::PutOk);
    runtime.reput_message_store_once_for_test().await;

    let peer = "127.0.0.1:19003".parse().expect("Notification retry peer");
    let ready = match processor
        .execute_notification_core(&core_header("topic-a", "group-a", 0), peer, 20_005, None)
        .await
    {
        NotificationCoreOutcome::Ready(ready) => ready,
        NotificationCoreOutcome::Reply(response) => {
            panic!("retry-store Notification core failed with code {}", response.code())
        }
    };
    assert!(ready.has_msg);

    drop((processor, escape_bridge));
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}

fn assert_notification_header(
    mut command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    has_msg: bool,
) {
    assert_eq!(command.code(), ResponseCode::Success as i32);
    assert!(command.body().is_none());
    command.make_custom_header_to_net();
    let header = command
        .decode_command_custom_header::<NotificationResponseHeader>()
        .expect("Notification response header");
    assert_eq!(header.has_msg, has_msg);
    assert!(!header.polling_full);
}

#[tokio::test]
async fn notification_deferred_actual_store_reread_builds_empty_then_found_response_headers() {
    let (mut runtime, root) = runtime("empty-then-found").await;
    let (processor, escape_bridge) = processor(&mut runtime);

    let empty = processor
        .resume_notification_command(resume_request(), DeferredWakeReason::Timeout)
        .await
        .expect("empty Notification resume reread");
    assert_notification_header(empty, false);

    let put = escape_bridge
        .put_message_to_local_store(stored_message())
        .await
        .expect("append Notification resume message");
    assert_eq!(put.put_message_status(), PutMessageStatus::PutOk);
    runtime.reput_message_store_once_for_test().await;

    let found = processor
        .resume_notification_command(resume_request(), DeferredWakeReason::MessageArrived)
        .await
        .expect("found Notification resume reread");
    assert_notification_header(found, true);

    drop((processor, escape_bridge));
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn notification_core_characterizes_permission_topic_group_queue_filter_and_order() {
    let (mut runtime, root) = runtime("core-characterization").await;
    let peer = "127.0.0.1:19002".parse().expect("Notification core peer");

    let mut denied_policy = NotificationPolicy::from_config(&runtime.runtime_state_mut().broker_config());
    denied_policy.broker_permission = 0;
    denied_policy.broker_ip1 = CheetahString::from_static_str("127.0.0.1");
    let (denied, denied_bridge) = processor_with_policy(runtime.runtime_state_mut(), denied_policy);
    let denied_response = match denied
        .execute_notification_core(&core_header("topic-a", "group-a", 0), peer, 20_001, None)
        .await
    {
        NotificationCoreOutcome::Reply(response) => response,
        NotificationCoreOutcome::Ready(_) => panic!("broker permission denial must be terminal"),
    };
    assert_eq!(denied_response.code(), ResponseCode::NoPermission as i32);
    assert_eq!(denied_response.opaque(), 20_001);
    assert_eq!(
        denied_response.remark().map(CheetahString::as_str),
        Some("the broker[127.0.0.1] peeking message is forbidden")
    );
    assert!(denied_response.body().is_none());
    drop((denied, denied_bridge));

    let (processor, escape_bridge) = processor(&mut runtime);
    let cases = [
        (
            core_header("missing-topic", "group-a", 0),
            ResponseCode::TopicNotExist,
            format!(
                "topic[missing-topic] not exist, apply first please! {}",
                FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
            ),
        ),
        (
            core_header("topic-a", "missing-group", 0),
            ResponseCode::SubscriptionGroupNotExist,
            format!(
                "subscription group [missing-group] does not exist, {}",
                FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
            ),
        ),
        (
            core_header("topic-a", "group-a", 99),
            ResponseCode::InvalidParameter,
            format!("queueId[99] is illegal, topic:[topic-a] topicConfig.readQueueNums:[1] consumer:[{peer}]"),
        ),
    ];
    for (header, expected, expected_remark) in cases {
        let response = match processor.execute_notification_core(&header, peer, 20_002, None).await {
            NotificationCoreOutcome::Reply(response) => response,
            NotificationCoreOutcome::Ready(_) => panic!("Notification validation failure must be terminal"),
        };
        assert_eq!(response.code(), expected as i32);
        assert_eq!(response.opaque(), 20_002);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some(expected_remark.as_str())
        );
        assert!(response.body().is_none());
    }

    let _ = runtime
        .runtime_state_mut()
        .topic_config_manager_handle()
        .update_topic_config(TopicConfig::with_perm("topic-a", 1, 1, PermName::PERM_WRITE), 0);
    let topic_denied = match processor
        .execute_notification_core(&core_header("topic-a", "group-a", 0), peer, 20_003, None)
        .await
    {
        NotificationCoreOutcome::Reply(response) => response,
        NotificationCoreOutcome::Ready(_) => panic!("unreadable topic must be terminal"),
    };
    assert_eq!(topic_denied.code(), ResponseCode::NoPermission as i32);
    assert_eq!(topic_denied.opaque(), 20_003);
    assert_eq!(
        topic_denied.remark().map(CheetahString::as_str),
        Some("the topic[topic-a] peeking message is forbidden")
    );
    assert!(topic_denied.body().is_none());
    let _ = runtime
        .runtime_state_mut()
        .topic_config_manager_handle()
        .update_topic_config(TopicConfig::with_queues("topic-a", 1, 1), 0);

    let mut disabled_group = SubscriptionGroupConfig::new(CheetahString::from_static_str("group-a"));
    disabled_group.set_consume_enable(false);
    assert!(runtime
        .runtime_state_mut()
        .subscription_group_manager()
        .update_subscription_group_config(&mut disabled_group));
    let group_denied = match processor
        .execute_notification_core(&core_header("topic-a", "group-a", 0), peer, 20_004, None)
        .await
    {
        NotificationCoreOutcome::Reply(response) => response,
        NotificationCoreOutcome::Ready(_) => panic!("disabled group must be terminal"),
    };
    assert_eq!(group_denied.code(), ResponseCode::NoPermission as i32);
    assert_eq!(group_denied.opaque(), 20_004);
    assert_eq!(
        group_denied.remark().map(CheetahString::as_str),
        Some("subscription group no permission, group-a")
    );
    assert!(group_denied.body().is_none());
    disabled_group.set_consume_enable(true);
    assert!(runtime
        .runtime_state_mut()
        .subscription_group_manager()
        .update_subscription_group_config(&mut disabled_group));

    let put = escape_bridge
        .put_message_to_local_store(stored_message())
        .await
        .expect("append ordered Notification core message");
    assert_eq!(put.put_message_status(), PutMessageStatus::PutOk);
    runtime.reput_message_store_once_for_test().await;
    let topic = CheetahString::from_static_str("topic-a");
    let group = CheetahString::from_static_str("group-a");
    let attempt = CheetahString::from_static_str("attempt-1");
    let mut order_info = String::new();
    assert!(runtime.runtime_state_mut().consumer_order_info_manager().update(
        attempt.clone(),
        false,
        &topic,
        &group,
        0,
        current_millis(),
        60_000,
        vec![0],
        &mut order_info,
    ));
    assert!(!order_info.is_empty());

    let mut ordered = core_header("topic-a", "group-a", 0);
    ordered.order = true;
    let ready = match processor.execute_notification_core(&ordered, peer, 20_005, None).await {
        NotificationCoreOutcome::Ready(ready) => ready,
        NotificationCoreOutcome::Reply(response) => {
            panic!("unblocked order request failed with code {}", response.code())
        }
    };
    assert!(ready.has_msg, "a missing attempt id intentionally skips order blocking");

    ordered.attempt_id = Some(attempt);
    let same_attempt = match processor.execute_notification_core(&ordered, peer, 20_006, None).await {
        NotificationCoreOutcome::Ready(ready) => ready,
        NotificationCoreOutcome::Reply(response) => {
            panic!("same-attempt order request failed with code {}", response.code())
        }
    };
    assert!(same_attempt.has_msg, "the active order attempt remains readable");

    ordered.attempt_id = Some(CheetahString::from_static_str("attempt-2"));
    let blocked_attempt = match processor.execute_notification_core(&ordered, peer, 20_007, None).await {
        NotificationCoreOutcome::Ready(ready) => ready,
        NotificationCoreOutcome::Reply(response) => {
            panic!("blocked order request failed with code {}", response.code())
        }
    };
    assert!(!blocked_attempt.has_msg, "a competing order attempt is blocked");

    drop((processor, escape_bridge));
    let mut filter_policy = NotificationPolicy::from_config(&runtime.runtime_state_mut().broker_config());
    filter_policy.use_message_filter_for_notification = true;
    let (filtered, filtered_bridge) = processor_with_policy(runtime.runtime_state_mut(), filter_policy);
    let mut invalid_filter = core_header("topic-a", "group-a", 0);
    invalid_filter.exp_type = Some(CheetahString::from_static_str("TAG"));
    invalid_filter.exp = Some(CheetahString::from_static_str("||"));
    let response = match filtered
        .execute_notification_core(&invalid_filter, peer, 20_008, None)
        .await
    {
        NotificationCoreOutcome::Reply(response) => response,
        NotificationCoreOutcome::Ready(_) => panic!("enabled invalid Notification filter must be terminal"),
    };
    assert_eq!(response.code(), ResponseCode::SubscriptionParseFailed as i32);
    assert_eq!(response.opaque(), 20_008);
    assert_eq!(
        response.remark().map(CheetahString::as_str),
        Some("parse the consumer's subscription failed")
    );
    assert!(response.body().is_none());
    drop((filtered, filtered_bridge));
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}
