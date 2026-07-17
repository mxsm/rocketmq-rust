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

use crate::proto::v2;
use crate::ProxyContextWithPrincipal;
use crate::ProxyError;
use crate::ProxyResult;

pub fn validate_client_context<P>(context: &ProxyContextWithPrincipal<P>) -> ProxyResult<&str> {
    context.require_client_id()
}

pub fn validate_heartbeat_request<P>(context: &ProxyContextWithPrincipal<P>, client_type: i32) -> ProxyResult<()> {
    validate_client_context(context)?;

    match v2::ClientType::try_from(client_type) {
        Ok(v2::ClientType::Producer)
        | Ok(v2::ClientType::PushConsumer)
        | Ok(v2::ClientType::SimpleConsumer)
        | Ok(v2::ClientType::PullConsumer)
        | Ok(v2::ClientType::LitePushConsumer)
        | Ok(v2::ClientType::LiteSimpleConsumer) => Ok(()),
        _ => Err(ProxyError::UnrecognizedClientType(client_type)),
    }
}
