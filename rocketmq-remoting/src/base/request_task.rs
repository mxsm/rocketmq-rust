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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use rocketmq_common::TimeUtils::get_current_millis;

use crate::net::channel::Channel;
use crate::protocol::remoting_command::RemotingCommand;

pub struct RequestTask {
    runnable: Arc<dyn Fn() + Send + Sync>,
    create_timestamp: u64,
    channel: Channel,
    request: RemotingCommand,
    stop_run: Arc<parking_lot::Mutex<bool>>,
}

impl RequestTask {
    pub fn new(runnable: Arc<dyn Fn() + Send + Sync>, channel: Channel, request: RemotingCommand) -> Self {
        Self {
            runnable,
            create_timestamp: get_current_millis(),
            channel,
            request,
            stop_run: Arc::new(parking_lot::Mutex::new(false)),
        }
    }

    pub fn set_stop_run(&self, stop_run: bool) {
        let mut stop_run_lock = self.stop_run.lock();
        *stop_run_lock = stop_run;
    }

    pub fn get_create_timestamp(&self) -> u64 {
        self.create_timestamp
    }

    pub fn is_stop_run(&self) -> bool {
        *self.stop_run.lock()
    }

    pub async fn return_response(&self, _code: i32, _remark: String) {
        unimplemented!("return_response")
    }
}

impl RequestTask {
    pub async fn run(&self) {
        if !self.is_stop_run() {
            (self.runnable)();
        }
    }
}

impl Future for RequestTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.is_stop_run() {
            (self.runnable)();
            return Poll::Ready(());
        }
        Poll::Pending
    }
}
