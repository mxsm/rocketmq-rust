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

use std::any::Any;

use rocketmq_rust::ArcMut;

#[allow(async_fn_in_trait)]
pub trait LatencyFaultTolerance<T, R, S>: Send + Sync + 'static {
    /// Update brokers' states, to decide if they are good or not.
    ///
    /// # Arguments
    ///
    /// * `name` - Broker's name.
    /// * `current_latency` - Current message sending process's latency.
    /// * `not_available_duration` - Corresponding not available time, ms. The broker will be not
    ///   available until it
    /// * `reachable` - To decide if this broker is reachable or not.
    async fn update_fault_item(&mut self, name: T, current_latency: u64, not_available_duration: u64, reachable: bool);

    /// To check if this broker is available.
    ///
    /// # Arguments
    ///
    /// * `name` - Broker's name.
    ///
    /// # Returns
    ///
    /// * `true` if the broker is available, `false` otherwise.
    fn is_available(&self, name: &T) -> bool;

    /// To check if this broker is reachable.
    ///
    /// # Arguments
    ///
    /// * `name` - Broker's name.
    ///
    /// # Returns
    ///
    /// * `true` if the broker is reachable, `false` otherwise.
    fn is_reachable(&self, name: &T) -> bool;

    /// Remove the broker in this fault item table.
    ///
    /// # Arguments
    ///
    /// * `name` - Broker's name.
    async fn remove(&mut self, name: &T);

    /// The worst situation, no broker can be available. Then choose a random one.
    ///
    /// # Returns
    ///
    /// * A random broker will be returned.
    async fn pick_one_at_least(&self) -> Option<T>;

    /// Start a new thread, to detect the broker's reachable tag.
    fn start_detector(this: ArcMut<Self>);

    /// Shutdown threads that started by `LatencyFaultTolerance`.
    fn shutdown(&self);

    /// A function reserved, just detect by once, won't create a new thread.
    async fn detect_by_one_round(&self);

    /// Use it to set the detect timeout bound.
    ///
    /// # Arguments
    ///
    /// * `detect_timeout` - Timeout bound.
    fn set_detect_timeout(&mut self, detect_timeout: u32);

    /// Use it to set the detector's interval for each broker (each broker will be detected once
    /// during this time).
    ///
    /// # Arguments
    ///
    /// * `detect_interval` - Each broker's detecting interval.
    fn set_detect_interval(&mut self, detect_interval: u32);

    /// Use it to set the detector work or not.
    ///
    /// # Arguments
    ///
    /// * `start_detector_enable` - Set the detector's work status.
    fn set_start_detector_enable(&mut self, start_detector_enable: bool);

    /// Use it to judge if the detector is enabled.
    ///
    /// # Returns
    ///
    /// * `true` if the detector should be started, `false` otherwise.
    fn is_start_detector_enable(&self) -> bool;

    fn set_resolver(&mut self, resolver: R);

    fn set_service_detector(&mut self, service_detector: S);

    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}
