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

/// Thread service std types and operations.
pub mod thread_service_std;
/// Thread service tokio types and operations.
pub mod thread_service_tokio;

/// Defines service thread behavior.
pub trait ServiceThread {
    /// Starts the owned service.
    fn start(&mut self);
    /// Shuts down the owned service.
    fn shutdown(&mut self);
    /// Executes make stop.
    fn make_stop(&mut self);
    /// Executes wakeup.
    fn wakeup(&mut self);
    /// Executes wait for running.
    fn wait_for_running(&mut self, interval: i64);
    /// Returns whether stopped.
    fn is_stopped(&self) -> bool;
    /// Returns service name.
    fn get_service_name(&self) -> String;
}

/// Defines runnable behavior.
pub trait Runnable: Send + Sync + 'static {
    /// Executes run.
    fn run(&mut self) {}
}

#[trait_variant::make(TokioRunnable: Send)]
/// Defines rocket mqtokio runnable behavior.
pub trait RocketMQTokioRunnable: Sync + 'static {
    /// Executes run.
    async fn run(&mut self);
}
