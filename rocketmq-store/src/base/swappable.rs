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

/// Clean up page-table on super large disk.
pub trait Swappable: Send + Sync {
    /// Swap map with specified parameters.
    ///
    /// `reserve_num`: Number of reserved items.
    /// `force_swap_interval_ms`: Force swap interval in milliseconds.
    /// `normal_swap_interval_ms`: Normal swap interval in milliseconds.
    fn swap_map(&self, reserve_num: i32, force_swap_interval_ms: i64, normal_swap_interval_ms: i64);

    /// Clean swapped map with specified force clean swap interval.
    ///
    /// `force_clean_swap_interval_ms`: Force clean swap interval in milliseconds.
    fn clean_swapped_map(&self, force_clean_swap_interval_ms: i64);
}
