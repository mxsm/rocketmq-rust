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

/// `StatisticsItemStateGetter` is a trait that provides a common interface for objects that can
/// determine the online status of a `StatisticsItem`.
pub trait StatisticsItemStateGetter {
    /// The `online` method accepts a reference to a `StatisticsItem` and returns a boolean
    /// indicating whether the item is online or not.
    ///
    /// # Arguments
    ///
    /// * `item` - A reference to a `StatisticsItem` whose online status is to be determined.
    ///
    /// # Returns
    ///
    /// * `bool` - A boolean indicating whether the `StatisticsItem` is online (`true`) or not
    ///   (`false`).
    fn online(&self, item: &StatisticsItem) -> bool;
}
