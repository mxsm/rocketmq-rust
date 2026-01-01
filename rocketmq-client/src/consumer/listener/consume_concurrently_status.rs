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

use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsumeConcurrentlyStatus {
    /// Success consumption
    #[default]
    ConsumeSuccess,
    /// Failure consumption, later try to consume
    ReconsumeLater,
}

impl From<i32> for ConsumeConcurrentlyStatus {
    fn from(value: i32) -> Self {
        match value {
            0 => ConsumeConcurrentlyStatus::ConsumeSuccess,
            1 => ConsumeConcurrentlyStatus::ReconsumeLater,
            _ => ConsumeConcurrentlyStatus::ConsumeSuccess,
        }
    }
}

impl From<ConsumeConcurrentlyStatus> for i32 {
    fn from(status: ConsumeConcurrentlyStatus) -> i32 {
        match status {
            ConsumeConcurrentlyStatus::ConsumeSuccess => 0,
            ConsumeConcurrentlyStatus::ReconsumeLater => 1,
        }
    }
}

impl Display for ConsumeConcurrentlyStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumeConcurrentlyStatus::ConsumeSuccess => write!(f, "CONSUME_SUCCESS"),
            ConsumeConcurrentlyStatus::ReconsumeLater => write!(f, "RECONSUME_LATER"),
        }
    }
}
