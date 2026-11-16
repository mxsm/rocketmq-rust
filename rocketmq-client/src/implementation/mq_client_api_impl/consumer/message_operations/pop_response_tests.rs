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
