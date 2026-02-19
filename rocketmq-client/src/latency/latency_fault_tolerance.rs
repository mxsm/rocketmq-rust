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

use rocketmq_rust::ArcMut;

/// Broker-level fault tolerance abstraction for latency-based routing decisions.
///
/// Tracks broker availability and reachability status, enabling intelligent
/// broker selection that avoids unhealthy brokers during message sending.
#[allow(async_fn_in_trait)]
pub trait LatencyFaultTolerance<T>: Send + Sync + 'static {
    /// Updates a broker's fault status.
    ///
    /// # Arguments
    ///
    /// * `name` - Broker's name.
    /// * `current_latency` - Current message sending process's latency.
    /// * `not_available_duration` - Corresponding not available time, ms. The broker will be not
    ///   available until it spends such time.
    /// * `reachable` - To decide if this broker is reachable or not.
    async fn update_fault_item(&self, name: T, current_latency: u64, not_available_duration: u64, reachable: bool);

    /// Checks if this broker is available.
    ///
    /// # Arguments
    ///
    /// * `name` - Broker's name.
    ///
    /// # Returns
    ///
    /// `true` if the broker is available, `false` otherwise.
    fn is_available(&self, name: &T) -> bool;

    /// Checks if this broker is reachable.
    ///
    /// # Arguments
    ///
    /// * `name` - Broker's name.
    ///
    /// # Returns
    ///
    /// `true` if the broker is reachable, `false` otherwise.
    fn is_reachable(&self, name: &T) -> bool;

    /// Removes the broker from this fault item table.
    ///
    /// # Arguments
    ///
    /// * `name` - Broker's name.
    async fn remove(&self, name: &T);

    /// When no broker is available, picks a random reachable one as fallback.
    ///
    /// # Returns
    ///
    /// A random broker name, or `None` if no reachable broker exists.
    async fn pick_one_at_least(&self) -> Option<T>;

    /// Starts a background task to periodically detect broker reachability.
    fn start_detector(this: ArcMut<Self>);

    /// Shuts down the background detector task.
    fn shutdown(&self);

    /// Runs a single round of broker reachability detection.
    async fn detect_by_one_round(&self);

    /// Sets the detect timeout bound in milliseconds.
    fn set_detect_timeout(&mut self, detect_timeout: u32);

    /// Sets the detector's interval for each broker in milliseconds.
    fn set_detect_interval(&mut self, detect_interval: u32);

    /// Enables or disables the background detector.
    fn set_start_detector_enable(&mut self, start_detector_enable: bool);

    /// Returns whether the background detector is enabled.
    fn is_start_detector_enable(&self) -> bool;
}
