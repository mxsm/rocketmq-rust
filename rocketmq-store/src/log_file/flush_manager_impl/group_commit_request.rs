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

use std::sync::atomic::AtomicI32;

use rocketmq_common::TimeUtils::get_current_nano;

use crate::base::message_status_enum::PutMessageStatus;

#[derive(Debug)]
pub(crate) struct GroupCommitRequest {
    pub(crate) next_offset: i64,
    pub(crate) flush_ok: Option<PutMessageStatus>,
    pub(crate) ack_nums: AtomicI32,
    pub(crate) dead_line: u64,
}

impl Default for GroupCommitRequest {
    fn default() -> Self {
        Self {
            next_offset: 0,
            flush_ok: None,
            ack_nums: AtomicI32::new(1),
            dead_line: 0,
        }
    }
}

impl GroupCommitRequest {
    pub(crate) fn new(next_offset: i64, timeout_millis: u64) -> Self {
        let dead_line = get_current_nano() + timeout_millis * 1_000_000;
        Self {
            next_offset,
            dead_line,
            ..Self::default()
        }
    }
}
