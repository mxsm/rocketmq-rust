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

pub use rocketmq_protocol::protocol::remoting_command::*;

use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_rust::ArcMut;

/// Preserves the concrete historical synchronization parameter at the legacy path.
#[deprecated(note = "use set_command_custom_header or set_command_custom_header_boxed on RemotingCommand")]
pub fn set_command_custom_header_origin(
    command: RemotingCommand,
    header: Option<ArcMut<Box<dyn CommandCustomHeader + Send + Sync + 'static>>>,
) -> RemotingCommand {
    command.set_command_custom_header_origin(header)
}
