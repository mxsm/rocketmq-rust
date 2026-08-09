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

/// Aggregate statistics for lazy mapped-file initialization.
///
/// Eager mappings report the default value because they are not eligible for lazy initialization.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LazyMmapStats {
    /// Number of files configured for lazy mapping.
    pub eligible_files: u64,
    /// Number of eligible files whose mapping has been initialized.
    pub mapped_files: u64,
    /// Number of successful lazy initialization operations.
    pub map_operations: u64,
    /// Number of failed lazy initialization attempts.
    pub map_failures: u64,
    /// Total elapsed time of successful lazy initialization operations, in milliseconds.
    pub total_millis: u64,
    /// Elapsed time of the most recent successful lazy initialization, in milliseconds.
    pub last_millis: u64,
}

impl LazyMmapStats {
    /// Saturating-adds counters from `other` and retains its latest non-zero latency.
    pub fn saturating_add_assign(&mut self, other: Self) {
        self.eligible_files = self.eligible_files.saturating_add(other.eligible_files);
        self.mapped_files = self.mapped_files.saturating_add(other.mapped_files);
        self.map_operations = self.map_operations.saturating_add(other.map_operations);
        self.map_failures = self.map_failures.saturating_add(other.map_failures);
        self.total_millis = self.total_millis.saturating_add(other.total_millis);
        if other.last_millis != 0 {
            self.last_millis = other.last_millis;
        }
    }
}
