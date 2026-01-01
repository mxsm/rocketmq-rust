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

use cheetah_string::CheetahString;
use rocketmq_rust::ArcMut;

/// `StateGetter` is a trait that provides a method for checking the online status of a specific
/// instance.
///
/// This trait is used to determine whether a specific instance, identified by its `instance_id`,
/// `group`, and `topic`, is online.
pub trait StateGetter: Send + Sync + 'static {
    /// Checks if the specified instance is online.
    ///
    /// # Arguments
    ///
    /// * `instance_id` - A string slice that holds the ID of the instance.
    /// * `group` - A string slice that holds the group of the instance.
    /// * `topic` - A string slice that holds the topic of the instance.
    ///
    /// # Returns
    ///
    /// * `bool` - The return value is `true` if the specified instance is online, and `false`
    ///   otherwise.
    fn online(&self, instance_id: &CheetahString, group: &CheetahString, topic: &CheetahString) -> bool;
}
