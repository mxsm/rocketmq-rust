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

use std::sync::Arc;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::TaskGroup;
use rocketmq_security_api::Principal;

use crate::deadline::RequestDeadline;
use crate::dispatch::AuthorizedCommandDispatcher;
use crate::dispatch::EmbeddedDispatchError;
use crate::dispatch::EmbeddedDispatchOutcome;
use crate::runtime::processor::RequestProcessor;

/// Socket-free fixture for exercising the public embedded dispatch boundary.
pub struct EmbeddedRequestHarness<P> {
    dispatcher: Arc<AuthorizedCommandDispatcher<P>>,
    task_group: TaskGroup,
    principal: Principal,
}

impl<P> EmbeddedRequestHarness<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    /// Creates a harness from the same dispatcher, lifecycle owner, and
    /// authenticated identity used by an embedded composition root.
    #[must_use]
    pub fn new(dispatcher: Arc<AuthorizedCommandDispatcher<P>>, task_group: TaskGroup, principal: Principal) -> Self {
        Self {
            dispatcher,
            task_group,
            principal,
        }
    }

    /// Dispatches one command without constructing a channel or socket pair.
    pub async fn dispatch(
        &self,
        deadline: Option<RequestDeadline>,
        command: RemotingCommand,
    ) -> Result<EmbeddedDispatchOutcome, EmbeddedDispatchError> {
        self.dispatcher
            .dispatch_embedded(&self.task_group, self.principal.clone(), deadline, command)
            .await
    }
}
