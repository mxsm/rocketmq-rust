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

#![deny(deprecated)]

use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::RemotingCommand;

fn main() {
    let mut command = RemotingCommand::create_request_command(1, GetRouteInfoRequestHeader::new("topic-a", None));
    let _ = command.read_custom_header_ref_unchecked::<GetRouteInfoRequestHeader>();
    let _ = command.read_custom_header_mut_unchecked::<GetRouteInfoRequestHeader>();
}
