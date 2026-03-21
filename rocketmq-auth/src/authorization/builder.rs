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

pub mod default_authorization_context_builder;

use std::any::Any;

use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::provider::AuthorizationResult;

pub trait AuthorizationContextBuilder: Send + Sync {
    fn build_from_remoting(
        &self,
        channel_context: &(dyn Any + Send + Sync),
        command: &RemotingCommand,
    ) -> AuthorizationResult<Vec<DefaultAuthorizationContext>>;
}
