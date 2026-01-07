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

use std::hash::Hash;
use std::hash::Hasher;

use crate::protocol::remoting_command::RemotingCommand;

pub struct ResponseFuture {
    pub(crate) opaque: i32,
    pub(crate) timeout_millis: u64,
    pub(crate) send_request_ok: bool,
    //pub(crate) response_command: Option<RemotingCommand>,
    pub(crate) tx: tokio::sync::oneshot::Sender<rocketmq_error::RocketMQResult<RemotingCommand>>,
}

impl PartialEq for ResponseFuture {
    fn eq(&self, other: &Self) -> bool {
        self.opaque == other.opaque
            && self.timeout_millis == other.timeout_millis
            && self.send_request_ok == other.send_request_ok
    }
}

impl Eq for ResponseFuture {}

impl Hash for ResponseFuture {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.opaque.hash(state);
        self.timeout_millis.hash(state);
        self.send_request_ok.hash(state);
    }
}

impl ResponseFuture {
    pub fn new(
        opaque: i32,
        timeout_millis: u64,
        send_request_ok: bool,
        //response_command: Option<RemotingCommand>,
        tx: tokio::sync::oneshot::Sender<rocketmq_error::RocketMQResult<RemotingCommand>>,
    ) -> Self {
        Self {
            opaque,
            timeout_millis,
            send_request_ok,
            // response_command,
            tx,
        }
    }
}
