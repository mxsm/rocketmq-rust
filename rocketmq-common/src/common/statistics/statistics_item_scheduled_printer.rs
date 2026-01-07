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

use crate::common::statistics::statistics_item::StatisticsItem;

/// `StatisticsItemScheduledPrinter` is a struct that provides functionality for scheduling and
/// removing `StatisticsItem`.
pub struct StatisticsItemScheduledPrinter;

impl StatisticsItemScheduledPrinter {
    /// Schedules a `StatisticsItem`.
    ///
    /// # Arguments
    ///
    /// * `statistics_item` - A reference to the `StatisticsItem` that needs to be scheduled.
    ///
    /// # Panics
    ///
    /// This function currently panics with `unimplemented!()`.
    pub fn schedule(&self, _statistics_item: &StatisticsItem) {
        unimplemented!()
    }

    /// Removes a `StatisticsItem`.
    ///
    /// # Arguments
    ///
    /// * `statistics_item` - A reference to the `StatisticsItem` that needs to be removed.
    ///
    /// # Panics
    ///
    /// This function currently panics with `unimplemented!()`.
    pub fn remove(&self, _statistics_item: &StatisticsItem) {
        unimplemented!()
    }
}
