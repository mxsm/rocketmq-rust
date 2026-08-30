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

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_model::common::constant::consume_init_mode::ConsumeInitMode;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageStatus;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::ResponseBodyKind;

use super::PopResumeRequest;
use crate::broker_runtime::BrokerRuntime;
use crate::config::broker_config::BrokerConfig;

fn temp_test_root(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("rocketmq-rust-pop-resume-{}-{label}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create POP resume test root");
    path
}

fn available_ha_port() -> usize {
    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("reserve an ephemeral POP resume HA port")
        .local_addr()
        .expect("read the ephemeral POP resume HA port")
        .port() as usize
}

async fn runtime(label: &str) -> (BrokerRuntime, PathBuf) {
    let root = temp_test_root(label);
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        auth_config_path: root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        ha_listen_port: available_ha_port(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    runtime.initialize().await.expect("initialize POP resume runtime");
    runtime.seed_pop_topic_and_group_for_test("topic-a", "group-a");
    runtime
        .start_message_store_for_test()
        .await
        .expect("start POP resume message store");
    (runtime, root)
}

fn request() -> PopMessageRequestHeader {
    PopMessageRequestHeader {
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("topic-a"),
        queue_id: 0,
        max_msg_nums: 1,
        invisible_time: 30_000,
        poll_time: 60_000,
        born_time: current_millis(),
        init_mode: ConsumeInitMode::MIN,
        exp_type: None,
        exp: None,
        order: Some(false),
        attempt_id: None,
        topic_request_header: None,
    }
}

fn stored_message() -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str("topic-a"));
    message.message_ext_inner.set_queue_id(0);
    message.set_body(Bytes::from_static(b"deferred-pop-message"));
    message.set_wait_store_msg_ok(false);
    message.properties_string = message_properties_to_string(message.get_properties());
    message
}

#[tokio::test]
async fn actual_store_reread_builds_empty_then_message_found_plans() {
    let (mut runtime, root) = runtime("empty-then-found").await;
    let processor = runtime.pop_message_processor_for_test();

    let empty = processor
        .resume_pop_request(
            PopResumeRequest::for_test(request(), CheetahString::from_static_str("127.0.0.1:19002"), None),
            DeferredWakeReason::Timeout,
        )
        .await
        .expect("empty POP resume reread");
    assert_eq!(empty.response_code(), ResponseCode::PollingTimeout as i32);
    assert_eq!(empty.body_kind(), ResponseBodyKind::Empty);
    assert_eq!(empty.body_len(), 0);
    drop(empty);

    let put = processor
        .context
        .store
        .put_local(stored_message())
        .await
        .expect("append POP resume message");
    assert_eq!(put.put_message_status(), PutMessageStatus::PutOk);
    runtime.reput_message_store_once_for_test().await;

    let found = processor
        .resume_pop_request(
            PopResumeRequest::for_test(request(), CheetahString::from_static_str("127.0.0.1:19001"), None),
            DeferredWakeReason::MessageArrived,
        )
        .await
        .expect("real POP resume reread");

    assert_eq!(found.response_code(), ResponseCode::Success as i32);
    assert_ne!(found.body_kind(), ResponseBodyKind::Empty);
    assert!(found.body_len() > 0);
    drop(found);
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(root);
}
