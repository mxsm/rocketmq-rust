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

use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_protocol::protocol::remoting_command_facade::initialize_remoting_defaults;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::RemotingCommand;

#[test]
fn environment_defaults_are_resolved_before_command_construction() {
    // SAFETY: this integration-test binary contains one test, so no concurrent thread can read or
    // mutate these process environment keys while the assertions run.
    unsafe {
        std::env::set_var("rocketmq.remoting.version", "4242");
        std::env::set_var("rocketmq.serialize.type", "ROCKETMQ");
    }

    initialize_remoting_defaults(4242).expect("valid process defaults should initialize");
    let command = RemotingCommand::create_request_command(31, GetMinOffsetRequestHeader::default());

    assert_eq!(command.version(), 4242);
    assert_eq!(command.serialize_type(), SerializeType::ROCKETMQ);
}
