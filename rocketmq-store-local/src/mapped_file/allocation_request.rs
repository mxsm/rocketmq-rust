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

//! Runtime-neutral identity and priority ordering for mapped-file allocation requests.

/// Path and size identity used by a mapped-file allocation request.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedFileAllocationRequestKey {
    file_path: String,
    file_size: i32,
}

impl MappedFileAllocationRequestKey {
    /// Creates an allocation request identity.
    #[doc(hidden)]
    pub fn new(file_path: String, file_size: i32) -> Self {
        Self { file_path, file_size }
    }

    /// Returns the full allocation path.
    #[doc(hidden)]
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Returns the requested file size.
    #[doc(hidden)]
    pub fn file_size(&self) -> i32 {
        self.file_size
    }

    /// Extracts the numeric file offset used for request priority.
    #[doc(hidden)]
    pub fn file_offset(&self) -> i64 {
        if let Some(separator_index) = self.file_path.rfind(std::path::MAIN_SEPARATOR) {
            if let Ok(offset) = self.file_path[(separator_index + 1)..].parse::<i64>() {
                return offset;
            }
        }
        0
    }
}

impl std::fmt::Display for MappedFileAllocationRequestKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "AllocateRequest[file_path={},file_size={}]",
            self.file_path, self.file_size
        )
    }
}

impl PartialOrd for MappedFileAllocationRequestKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MappedFileAllocationRequestKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.file_offset().cmp(&self.file_offset())
    }
}
