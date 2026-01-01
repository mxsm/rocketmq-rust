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
pub enum PullStatus {
    /// Founded
    #[default]
    Found,
    /// No new message can be pulled
    NoNewMsg,
    /// Filtering results do not match
    NoMatchedMsg,
    /// Illegal offset, may be too big or too small
    OffsetIllegal,
}

impl From<i32> for PullStatus {
    fn from(i: i32) -> Self {
        match i {
            0 => PullStatus::Found,
            1 => PullStatus::NoNewMsg,
            2 => PullStatus::NoMatchedMsg,
            3 => PullStatus::OffsetIllegal,
            _ => PullStatus::Found,
        }
    }
}

impl From<PullStatus> for i32 {
    fn from(p: PullStatus) -> Self {
        match p {
            PullStatus::Found => 0,
            PullStatus::NoNewMsg => 1,
            PullStatus::NoMatchedMsg => 2,
            PullStatus::OffsetIllegal => 3,
        }
    }
}

impl Display for PullStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PullStatus::Found => write!(f, "FOUND"),
            PullStatus::NoNewMsg => write!(f, "NO_NEW_MSG"),
            PullStatus::NoMatchedMsg => write!(f, "NO_MATCHED_MSG"),
            PullStatus::OffsetIllegal => write!(f, "OFFSET_ILLEGAL"),
        }
    }
}
