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

use bytes::Bytes;
use rocketmq_protocol::code::response_code::RemotingSysResponseCode;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::SerializeType;

#[test]
fn explicit_factories_isolate_request_and_response_defaults() {
    let json_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(101, SerializeType::JSON));
    let rocketmq_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(202, SerializeType::ROCKETMQ));

    let json_request = json_factory.create_request_command(11, GetRouteInfoRequestHeader::new("json-topic", None));
    let json_response = json_factory.create_success_response_command();
    let rocketmq_request =
        rocketmq_factory.create_request_command(22, GetRouteInfoRequestHeader::new("rocketmq-topic", None));
    let rocketmq_response = rocketmq_factory.create_success_response_command();

    for command in [&json_request, &json_response] {
        assert_eq!(command.version(), 101);
        assert_eq!(command.get_serialize_type(), SerializeType::JSON);
    }
    for command in [&rocketmq_request, &rocketmq_response] {
        assert_eq!(command.version(), 202);
        assert_eq!(command.get_serialize_type(), SerializeType::ROCKETMQ);
    }

    assert_eq!(json_request.code(), 11);
    assert!(json_response.is_response_type());
    assert_eq!(rocketmq_request.code(), 22);
    assert!(rocketmq_response.is_response_type());
    assert_eq!(
        json_factory.defaults(),
        RemotingCommandDefaults::new(101, SerializeType::JSON)
    );
    assert_eq!(
        rocketmq_factory.defaults(),
        RemotingCommandDefaults::new(202, SerializeType::ROCKETMQ)
    );
}

#[test]
fn factory_exposes_explicit_request_and_response_semantics() {
    let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(303, SerializeType::ROCKETMQ));
    let cloned = factory;

    let body_request = cloned.create_request(31, Bytes::from_static(b"body"));
    assert_eq!(body_request.code(), 31);
    assert_eq!(body_request.body().map(Bytes::as_ref), Some(b"body".as_ref()));

    let coded = factory.create_response_command_with_code(44);
    assert_eq!(coded.code(), 44);
    assert!(coded.is_response_type());

    let remarked = factory.create_response_command_with_code_remark(45, "reason");
    assert_eq!(remarked.code(), 45);
    assert_eq!(remarked.remark().map(|remark| remark.as_str()), Some("reason"));

    let success = factory.create_success_response_command();
    assert_eq!(success.code(), RemotingSysResponseCode::Success as i32);

    let success_with_header = factory
        .create_success_response_command_with_header(GetRouteInfoRequestHeader::new("success-topic", Some(true)));
    let success_header = success_with_header
        .read_custom_header_ref::<GetRouteInfoRequestHeader>()
        .expect("successful response should retain its typed header");
    assert_eq!(success_header.topic.as_str(), "success-topic");

    let coded_with_header =
        factory.create_response_command_with_code_and_header(46, GetRouteInfoRequestHeader::new("error-topic", None));
    assert_eq!(coded_with_header.code(), 46);
    assert!(coded_with_header.is_response_type());
    assert!(coded_with_header
        .read_custom_header_ref::<GetRouteInfoRequestHeader>()
        .is_some());

    let java_default = factory.create_java_default_error_response_command();
    assert_eq!(java_default.code(), RemotingSysResponseCode::SystemError as i32);
    assert_eq!(
        java_default.remark().map(|remark| remark.as_str()),
        Some("not set any response code")
    );

    let java_default_with_header = factory.create_java_default_error_response_command_with_header(
        GetRouteInfoRequestHeader::new("java-default-topic", None),
    );
    assert_eq!(
        java_default_with_header.code(),
        RemotingSysResponseCode::SystemError as i32
    );
    assert!(java_default_with_header
        .read_custom_header_ref::<GetRouteInfoRequestHeader>()
        .is_some());

    for command in [
        &body_request,
        &coded,
        &remarked,
        &success,
        &success_with_header,
        &coded_with_header,
        &java_default,
        &java_default_with_header,
    ] {
        assert_eq!(command.version(), 303);
        assert_eq!(command.get_serialize_type(), SerializeType::ROCKETMQ);
    }
}
