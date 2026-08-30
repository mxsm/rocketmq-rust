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

pub(crate) mod inner {
    use std::net::SocketAddr;

    use rocketmq_error::RocketMQResult;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

    use crate::hook_registry::HookSnapshot;

    pub(crate) fn run_before_rpc_hooks(
        snapshot: Option<&HookSnapshot>,
        remote_address: SocketAddr,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<()> {
        if let Some(snapshot) = snapshot {
            for hook in snapshot.hooks() {
                hook.do_before_request(remote_address, request)?;
            }
        }
        Ok(())
    }

    pub(crate) fn run_after_rpc_hooks(
        snapshot: Option<&HookSnapshot>,
        remote_address: SocketAddr,
        request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> RocketMQResult<()> {
        if let Some(snapshot) = snapshot {
            for hook in snapshot.hooks() {
                hook.do_after_response(remote_address, request, response)?;
            }
        }
        Ok(())
    }
}
