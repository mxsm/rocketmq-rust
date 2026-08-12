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

use rocketmq_protocol::code::response_code::RemotingSysResponseCode;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::RemotingCommand;

#[test]
fn response_command_explicit_semantics() {
    let success = RemotingCommand::create_success_response_command();
    assert_eq!(success.code(), RemotingSysResponseCode::Success as i32);
    assert!(success.is_response_type());
    assert!(success.remark().is_none());

    let explicit_error = RemotingCommand::create_response_command_with_code_remark(17, "explicit error");
    assert_eq!(explicit_error.code(), 17);
    assert!(explicit_error.is_response_type());
    assert_eq!(
        explicit_error.remark().map(|remark| remark.as_str()),
        Some("explicit error")
    );

    let success_with_header = RemotingCommand::create_success_response_command_with_header(
        GetRouteInfoRequestHeader::new("topic-a", Some(true)),
    );
    assert_eq!(success_with_header.code(), RemotingSysResponseCode::Success as i32);
    assert!(success_with_header.is_response_type());
    let success_header = success_with_header
        .read_custom_header_ref::<GetRouteInfoRequestHeader>()
        .expect("success response should retain its typed header");
    assert_eq!(success_header.topic.as_str(), "topic-a");
    assert_eq!(success_header.accept_standard_json_only, Some(true));

    let explicit_code_with_header = RemotingCommand::create_response_command_with_code_and_header(
        23,
        GetRouteInfoRequestHeader::new("topic-explicit", None),
    );
    assert_eq!(explicit_code_with_header.code(), 23);
    assert!(explicit_code_with_header.is_response_type());
    let explicit_header = explicit_code_with_header
        .read_custom_header_ref::<GetRouteInfoRequestHeader>()
        .expect("explicit-code response should retain its typed header");
    assert_eq!(explicit_header.topic.as_str(), "topic-explicit");
    assert_eq!(explicit_header.accept_standard_json_only, None);

    let java_default = RemotingCommand::create_java_default_error_response_command();
    assert_eq!(java_default.code(), RemotingSysResponseCode::SystemError as i32);
    assert!(java_default.is_response_type());
    assert_eq!(
        java_default.remark().map(|remark| remark.as_str()),
        Some("not set any response code")
    );

    let java_default_with_header = RemotingCommand::create_java_default_error_response_command_with_header(
        GetRouteInfoRequestHeader::new("topic-b", None),
    );
    assert_eq!(
        java_default_with_header.code(),
        RemotingSysResponseCode::SystemError as i32
    );
    assert!(java_default_with_header.is_response_type());
    assert_eq!(
        java_default_with_header.remark().map(|remark| remark.as_str()),
        Some("not set any response code")
    );
    let java_default_header = java_default_with_header
        .read_custom_header_ref::<GetRouteInfoRequestHeader>()
        .expect("Java-compatible response should retain its typed header");
    assert_eq!(java_default_header.topic.as_str(), "topic-b");
    assert_eq!(java_default_header.accept_standard_json_only, None);
}
