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

/// Stats tracking various runtime metrics of RocketMQ
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunningStats {
    /// Maximum offset in the commit log
    CommitLogMaxOffset,

    /// Minimum offset in the commit log
    CommitLogMinOffset,

    /// Disk usage ratio for commit log
    CommitLogDiskRatio,

    /// Disk usage ratio for consume queue
    ConsumeQueueDiskRatio,

    /// Offset for scheduled messages
    ScheduleMessageOffset,
}

impl RunningStats {
    /// Get the string representation of the enum value
    pub fn as_str(&self) -> &'static str {
        match self {
            RunningStats::CommitLogMaxOffset => "commitLogMaxOffset",
            RunningStats::CommitLogMinOffset => "commitLogMinOffset",
            RunningStats::CommitLogDiskRatio => "commitLogDiskRatio",
            RunningStats::ConsumeQueueDiskRatio => "consumeQueueDiskRatio",
            RunningStats::ScheduleMessageOffset => "scheduleMessageOffset",
        }
    }
}

impl std::fmt::Display for RunningStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for RunningStats {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "commitLogMaxOffset" => Ok(RunningStats::CommitLogMaxOffset),
            "commitLogMinOffset" => Ok(RunningStats::CommitLogMinOffset),
            "commitLogDiskRatio" => Ok(RunningStats::CommitLogDiskRatio),
            "consumeQueueDiskRatio" => Ok(RunningStats::ConsumeQueueDiskRatio),
            "scheduleMessageOffset" => Ok(RunningStats::ScheduleMessageOffset),
            _ => Err(format!("Unknown RunningStats value: {s}")),
        }
    }
}
