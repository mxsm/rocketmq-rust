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
pub enum ConsumeReturnType {
    /// consume return success
    #[default]
    Success,
    /// consume timeout, even if success
    TimeOut,
    /// consume throw exception
    Exception,
    /// consume return null
    ReturnNull,
    /// consume return failed
    Failed,
}

impl From<i32> for ConsumeReturnType {
    fn from(value: i32) -> Self {
        match value {
            0 => ConsumeReturnType::Success,
            1 => ConsumeReturnType::TimeOut,
            2 => ConsumeReturnType::Exception,
            3 => ConsumeReturnType::ReturnNull,
            4 => ConsumeReturnType::Failed,
            _ => ConsumeReturnType::Success,
        }
    }
}

impl From<ConsumeReturnType> for i32 {
    fn from(status: ConsumeReturnType) -> i32 {
        match status {
            ConsumeReturnType::Success => 0,
            ConsumeReturnType::TimeOut => 1,
            ConsumeReturnType::Exception => 2,
            ConsumeReturnType::ReturnNull => 3,
            ConsumeReturnType::Failed => 4,
        }
    }
}

impl std::fmt::Display for ConsumeReturnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumeReturnType::Success => write!(f, "SUCCESS"),
            ConsumeReturnType::TimeOut => write!(f, "TIME_OUT"),
            ConsumeReturnType::Exception => write!(f, "EXCEPTION"),
            ConsumeReturnType::ReturnNull => write!(f, "RETURN_NULL"),
            ConsumeReturnType::Failed => write!(f, "FAILED"),
        }
    }
}
