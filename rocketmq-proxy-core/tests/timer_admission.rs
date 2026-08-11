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

use std::collections::HashMap;

use rocketmq_proxy_core::ingress::grpc::adapter::build_send_message_request_with_config;
use rocketmq_proxy_core::proto::v2;
use rocketmq_proxy_core::GrpcConfig;
use rocketmq_proxy_core::ProxyContext;

fn context() -> ProxyContext {
    ProxyContext::from_grpc_request("SendMessage", &tonic::Request::new(())).expect("gRPC context")
}

fn delay_request(deliver_ms: u64) -> v2::SendMessageRequest {
    v2::SendMessageRequest {
        messages: vec![v2::Message {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TimerAdmission".to_owned(),
            }),
            user_properties: HashMap::new(),
            system_properties: Some(v2::SystemProperties {
                message_id: "timer-admission-1".to_owned(),
                message_type: v2::MessageType::Delay as i32,
                body_encoding: v2::Encoding::Identity as i32,
                delivery_timestamp: Some(prost_types::Timestamp {
                    seconds: (deliver_ms / 1_000) as i64,
                    nanos: ((deliver_ms % 1_000) * 1_000_000) as i32,
                }),
                ..Default::default()
            }),
            body: bytes::Bytes::from_static(b"timer"),
        }],
    }
}

#[test]
fn timer_admission_enforces_configured_proxy_horizon() {
    let now_ms = rocketmq_runtime::common::time_utils::current_millis();
    let config = GrpcConfig {
        timer_max_delay_ms: 1_000,
        ..GrpcConfig::default()
    };

    let error = build_send_message_request_with_config(&config, &context(), &delay_request(now_ms + 2_000))
        .expect_err("delivery beyond the Proxy horizon must fail");

    assert!(error.to_string().contains("exceeds the configured maximum"));
}

#[test]
fn timer_admission_rejects_unsupported_precision_without_panicking() {
    let now_ms = rocketmq_runtime::common::time_utils::current_millis();
    let config = GrpcConfig {
        timer_precision_ms: 0,
        ..GrpcConfig::default()
    };

    let error = build_send_message_request_with_config(&config, &context(), &delay_request(now_ms + 1_000))
        .expect_err("zero precision must fail");

    assert!(error.to_string().contains("precision"));
}
