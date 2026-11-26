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

use super::*;

#[tokio::test]
async fn pop_empty_outcomes_do_not_require_success_delivery_headers() {
    let runtime = crate::runtime::test_client_runtime("pop-empty-response-contract");
    let instance = MQClientInstance::new_arc(
        ClientConfig::default(),
        0,
        "pop-empty-response-client",
        None,
        runtime.component("instance"),
        runtime.telemetry_handle().clone(),
        runtime.pool().request_future_holder(),
    );
    let api = instance.get_mq_client_api_impl().expect("client API");
    let outcomes = [
        (ResponseCode::PollingTimeout, PopStatus::PollingNotFound),
        (ResponseCode::PullNotFound, PopStatus::PollingNotFound),
        (ResponseCode::PollingFull, PopStatus::PollingFull),
    ]
    .map(|(code, expected)| {
        let result = api.process_pop_response(
            &CheetahString::from_static_str("broker-a"),
            RemotingCommand::create_response_command_with_code(code),
            &CheetahString::from_static_str("TopicA"),
            false,
        );
        (expected, result)
    });
    assert!(api.shutdown_graceful(Duration::from_secs(1)).await);
    drop(api);
    drop(instance);
    assert!(runtime.shutdown().await.is_healthy());

    for (expected, result) in outcomes {
        let result = result.expect("empty/polling outcomes do not carry Found-only delivery metadata");
        assert_eq!(result.pop_status, expected);
        assert!(result.msg_found_list.is_some_and(|messages| messages.is_empty()));
    }
}

#[tokio::test]
async fn pop_found_without_start_offsets_builds_receipts_per_topic_queue() {
    let runtime = crate::runtime::test_client_runtime("pop-found-response-contract");
    let mut config = ClientConfig::default();
    config.set_namespace(CheetahString::from_static_str("ns"));
    let instance = MQClientInstance::new_arc(
        config,
        0,
        "pop-found-response-client",
        None,
        runtime.component("instance"),
        runtime.telemetry_handle().clone(),
        runtime.pool().request_future_holder(),
    );
    let api = instance.get_mq_client_api_impl().expect("client API");
    // ("ns%TopicA1", 2) and ("ns%TopicA", 12) once shared one formatted cache key.
    let queues = [
        ("ns%TopicA", 1, 5),
        ("ns%TopicA", 1, 6),
        ("ns%TopicA1", 2, 7),
        ("ns%TopicA", 12, 8),
    ];
    let mut body = bytes::BytesMut::new();
    for (topic, queue_id, queue_offset) in queues {
        let mut message = MessageExt::default();
        message.set_topic(CheetahString::from_static_str(topic));
        message.set_queue_id(queue_id);
        message.set_queue_offset(queue_offset);
        message.message.set_body(Some(bytes::Bytes::from_static(b"body")));
        body.extend_from_slice(&MessageDecoder::encode(&message, false).expect("message should encode"));
    }
    let mut response = RemotingCommand::create_success_response_command_with_header(PopMessageResponseHeader {
        pop_time: 1_700_000_000_123,
        invisible_time: 30_000,
        revive_qid: 2,
        ..Default::default()
    })
    .set_body(body.freeze());
    response.make_custom_header_to_net();

    let result = api.process_pop_response(&"broker-a".into(), response, &"ns%TopicA".into(), false);
    assert!(api.shutdown_graceful(Duration::from_secs(1)).await);
    drop(api);
    drop(instance);
    assert!(runtime.shutdown().await.is_healthy());

    let messages = result
        .expect("a Found response should decode")
        .msg_found_list
        .expect("Found carries messages");
    let extra_info = |topic, queue_id, first_offset| {
        ExtraInfoUtil::build_extra_info(first_offset, 1_700_000_000_123, 30_000, 2, topic, "broker-a", queue_id)
    };
    let expected = [
        format!("{} 5", extra_info("ns%TopicA", 1, 5)),
        format!("{} 6", extra_info("ns%TopicA", 1, 5)),
        format!("{} 7", extra_info("ns%TopicA1", 2, 7)),
        format!("{} 8", extra_info("ns%TopicA", 12, 8)),
    ];
    assert_eq!(messages.len(), expected.len());
    for (message, expected) in messages.iter().zip(expected) {
        assert_eq!(
            message
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
                .as_deref(),
            Some(expected.as_str())
        );
        assert_eq!(
            message
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME))
                .as_deref(),
            Some("1700000000123")
        );
        assert_eq!(message.topic().as_str(), "TopicA");
        assert_eq!(message.broker_name.as_str(), "broker-a");
    }
}

#[tokio::test]
async fn pop_unknown_status_and_incomplete_found_response_remain_errors() {
    let runtime = crate::runtime::test_client_runtime("pop-invalid-response-contract");
    let instance = MQClientInstance::new_arc(
        ClientConfig::default(),
        0,
        "pop-invalid-response-client",
        None,
        runtime.component("instance"),
        runtime.telemetry_handle().clone(),
        runtime.pool().request_future_holder(),
    );
    let api = instance.get_mq_client_api_impl().expect("client API");
    let missing_header =
        RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_body(bytes::Bytes::new());
    let responses = [
        RemotingCommand::create_response_command_with_code(ResponseCode::SystemError),
        RemotingCommand::create_response_command_with_code(ResponseCode::Success),
        missing_header,
    ];
    let rejected = responses.map(|response| {
        api.process_pop_response(&"broker-a".into(), response, &"TopicA".into(), false)
            .is_err()
    });
    assert!(api.shutdown_graceful(Duration::from_secs(1)).await);
    drop(api);
    drop(instance);
    assert!(runtime.shutdown().await.is_healthy());
    assert_eq!(
        rejected, [true; 3],
        "Found still requires delivery body/header and unknown status is not empty"
    );
}
