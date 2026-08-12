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
use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
use rocketmq_protocol::protocol::remoting_command_defaults::initialize_remoting_command_defaults;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::RemotingCommand;

#[test]
fn business_factories_share_immutable_application_defaults() {
    let defaults = RemotingCommandDefaults::new(4242, SerializeType::ROCKETMQ);
    initialize_remoting_command_defaults(defaults).expect("application defaults should initialize");

    let commands = [
        RemotingCommand::new_request(10, Bytes::from_static(b"body")),
        RemotingCommand::create_request_command(11, EmptyHeader {}),
        RemotingCommand::create_remoting_command(12),
        RemotingCommand::create_response_command(),
        RemotingCommand::create_response_command_with_code(1),
        RemotingCommand::create_response_command_with_code_remark(1, "error"),
        RemotingCommand::create_response_command_with_header(EmptyHeader {}),
    ];

    for command in commands {
        assert_eq!(command.version(), 4242);
        assert_eq!(command.serialize_type(), SerializeType::ROCKETMQ);
    }

    let neutral = RemotingCommand::default();
    assert_eq!(neutral.version(), 0);
    assert_eq!(neutral.serialize_type(), SerializeType::JSON);
}
