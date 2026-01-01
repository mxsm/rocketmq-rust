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

use std::collections::HashMap;

use cheetah_string::CheetahString;

pub trait BrokerAttachedPlugin: Send + Sync {
    /// Get the name of this plugin
    ///
    /// # Returns
    /// The plugin's name
    fn plugin_name(&self) -> &str;

    /// Load the plugin
    ///
    /// This is called during broker initialization to load and prepare the plugin.
    ///
    /// # Returns
    /// `true` if the plugin loaded successfully, `false` otherwise
    fn load(&self) -> bool;

    /// Start the plugin
    ///
    /// This is called when the broker starts to activate the plugin.
    fn start(&self);

    /// Shutdown the plugin
    ///
    /// This is called when the broker shuts down to cleanly stop the plugin.
    fn shutdown(&self);

    /// Synchronize metadata from the master broker
    ///
    /// This is called to pull metadata updates from the master when running as a slave.
    fn sync_metadata(&self);

    /// Synchronize metadata from a slave broker (reverse direction)
    ///
    /// This is called to pull metadata from a slave when running as master.
    ///
    /// # Parameters
    /// * `broker_addr` - The address of the slave broker
    ///
    /// # Returns
    /// Result indicating success or an error if synchronization failed
    fn sync_metadata_reverse(&self, broker_addr: &CheetahString) -> rocketmq_error::RocketMQResult<()>;

    /// Build runtime information for monitoring
    ///
    /// This allows the plugin to add its own metrics and status information
    /// to the broker's runtime information.
    ///
    /// # Parameters
    /// * `runtime_info` - Map of runtime information to add to
    fn build_runtime_info(&self, runtime_info: &mut HashMap<CheetahString, CheetahString>);

    /// React to broker status changes
    ///
    /// This is called when the broker's role changes (e.g., from slave to master).
    ///
    /// # Parameters
    /// * `should_start` - Whether the plugin should be in active mode after the status change
    fn status_changed(&self, should_start: bool);
}
