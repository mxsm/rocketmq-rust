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

//! Business compatibility tests for Java `RemotingCommand` behavior.
//!
//! The reference behavior is `RemotingCommand.addExtField` in Apache RocketMQ:
//! the extension-field map is initialized on the first write.

use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::RemotingCommand;

fn round_trip(mut command: RemotingCommand) -> RemotingCommand {
    let mut encoded = BytesMut::new();
    command
        .try_fast_header_encode(&mut encoded)
        .expect("command should encode");
    RemotingCommand::decode(&mut encoded)
        .expect("command should decode")
        .expect("command frame should be complete")
}

#[test]
fn add_ext_field_on_absent_matches_java() {
    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        let mut command = RemotingCommand::create_response_command().set_serialize_type(serialize_type);
        assert!(command.ext_fields().is_none());

        command.add_ext_field("regionId", "DefaultRegion");
        let decoded = round_trip(command);

        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get("regionId"))
                .map(CheetahString::as_str),
            Some("DefaultRegion")
        );
    }
}

#[test]
fn add_ext_field_if_not_exist_on_absent_matches_java() {
    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        let mut command = RemotingCommand::create_response_command().set_serialize_type(serialize_type);
        assert!(command.ext_fields().is_none());

        command.add_ext_field_if_not_exist("traceOn", "true");
        command.add_ext_field_if_not_exist("traceOn", "false");
        let decoded = round_trip(command);

        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get("traceOn"))
                .map(CheetahString::as_str),
            Some("true")
        );
    }
}
