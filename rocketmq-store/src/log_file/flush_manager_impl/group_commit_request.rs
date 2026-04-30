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

use rocketmq_common::TimeUtils::current_nano;
use tokio::sync::oneshot;

use crate::base::message_status_enum::PutMessageStatus;

#[derive(Debug)]
pub(crate) struct GroupCommitRequest {
    pub(crate) next_offset: i64,
    flush_ok_sender: Option<oneshot::Sender<PutMessageStatus>>,
    pub(crate) dead_line: u64,
}

impl GroupCommitRequest {
    pub(crate) fn new(next_offset: i64, timeout_millis: u64) -> (Self, oneshot::Receiver<PutMessageStatus>) {
        let dead_line = current_nano() + timeout_millis * 1_000_000;
        let (flush_ok_sender, flush_ok_receiver) = oneshot::channel();
        (
            Self {
                next_offset,
                flush_ok_sender: Some(flush_ok_sender),
                dead_line,
            },
            flush_ok_receiver,
        )
    }

    pub(crate) fn complete(mut self, status: PutMessageStatus) {
        if let Some(sender) = self.flush_ok_sender.take() {
            let _ = sender.send(status);
        }
    }

    pub(crate) fn is_expired(&self) -> bool {
        current_nano() >= self.dead_line
    }
}
