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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(deprecated)]
pub enum ConsumeOrderlyStatus {
    /// Success consumption
    #[default]
    Success,
    /// Rollback consumption (only for binlog consumption)
    #[deprecated]
    Rollback,
    /// Commit offset (only for binlog consumption)
    #[deprecated]
    Commit,
    /// Suspend current queue a moment
    SuspendCurrentQueueAMoment,
}

#[allow(deprecated)]
impl From<i32> for ConsumeOrderlyStatus {
    fn from(value: i32) -> Self {
        match value {
            0 => ConsumeOrderlyStatus::Success,
            1 => ConsumeOrderlyStatus::Rollback,
            2 => ConsumeOrderlyStatus::Commit,
            3 => ConsumeOrderlyStatus::SuspendCurrentQueueAMoment,
            _ => ConsumeOrderlyStatus::Success,
        }
    }
}

#[allow(deprecated)]
impl From<ConsumeOrderlyStatus> for i32 {
    fn from(status: ConsumeOrderlyStatus) -> i32 {
        match status {
            ConsumeOrderlyStatus::Success => 0,
            ConsumeOrderlyStatus::Rollback => 1,
            ConsumeOrderlyStatus::Commit => 2,
            ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => 3,
        }
    }
}

#[allow(deprecated)]
impl std::fmt::Display for ConsumeOrderlyStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumeOrderlyStatus::Success => write!(f, "SUCCESS"),
            ConsumeOrderlyStatus::Rollback => write!(f, "ROLLBACK"),
            ConsumeOrderlyStatus::Commit => write!(f, "COMMIT"),
            ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                write!(f, "SUSPEND_CURRENT_QUEUE_A_MOMENT")
            }
        }
    }
}
