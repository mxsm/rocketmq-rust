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
pub enum ReadOffsetType {
    /// From memory
    #[default]
    ReadFromMemory,
    /// From storage
    ReadFromStore,
    /// From memory, then from storage
    MemoryFirstThenStore,
}

impl Display for ReadOffsetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadOffsetType::ReadFromMemory => write!(f, "READ_FROM_MEMORY"),
            ReadOffsetType::ReadFromStore => write!(f, "READ_FROM_STORE"),
            ReadOffsetType::MemoryFirstThenStore => write!(f, "MEMORY_FIRST_THEN_STORE"),
        }
    }
}

impl From<i32> for ReadOffsetType {
    fn from(value: i32) -> Self {
        match value {
            0 => ReadOffsetType::ReadFromMemory,
            1 => ReadOffsetType::ReadFromStore,
            2 => ReadOffsetType::MemoryFirstThenStore,
            _ => ReadOffsetType::ReadFromMemory,
        }
    }
}

impl From<ReadOffsetType> for i32 {
    fn from(value: ReadOffsetType) -> Self {
        match value {
            ReadOffsetType::ReadFromMemory => 0,
            ReadOffsetType::ReadFromStore => 1,
            ReadOffsetType::MemoryFirstThenStore => 2,
        }
    }
}
