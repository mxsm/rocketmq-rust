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

use rocketmq_remoting::CommandCustomHeader;
use rocketmq_remoting::RemotingCommand;
use rocketmq_rust::ArcMut;

type LegacyHeader = ArcMut<Box<dyn CommandCustomHeader + Send + Sync + 'static>>;

#[test]
#[allow(deprecated)]
fn legacy_origin_method_keeps_concrete_function_item_and_none_inference() {
    let method: fn(RemotingCommand, Option<LegacyHeader>) -> RemotingCommand =
        rocketmq_remoting::protocol::remoting_command_facade::set_command_custom_header_origin;

    let command = method(RemotingCommand::create_response_command(), None);

    assert!(command.ext_fields().is_none());
}
