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

//! Runtime-neutral storage identity for a Local mapped-file queue.

/// Canonical path, segment-size, and collection owner for a mapped-file queue.
#[doc(hidden)]
#[derive(Debug)]
pub struct MappedFileQueueStorage<T> {
    store_path: String,
    mapped_file_size: u64,
    mapped_files: T,
}

impl<T> MappedFileQueueStorage<T> {
    /// Creates a queue storage owner around the backend collection.
    #[doc(hidden)]
    pub fn new(store_path: String, mapped_file_size: u64, mapped_files: T) -> Self {
        Self {
            store_path,
            mapped_file_size,
            mapped_files,
        }
    }

    /// Returns the configured queue directory.
    #[doc(hidden)]
    pub fn store_path(&self) -> &str {
        &self.store_path
    }

    /// Returns the configured segment size.
    #[doc(hidden)]
    pub fn mapped_file_size(&self) -> u64 {
        self.mapped_file_size
    }

    /// Returns the backend-owned concurrent mapped-file collection.
    #[doc(hidden)]
    pub fn mapped_files(&self) -> &T {
        &self.mapped_files
    }
}
