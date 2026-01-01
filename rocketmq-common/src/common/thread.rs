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

use crate::common::thread::thread_service_std::ServiceThreadStd;

pub mod thread_service_std;
pub mod thread_service_tokio;

pub trait ServiceThread {
    fn start(&mut self);
    fn shutdown(&mut self);
    fn make_stop(&mut self);
    fn wakeup(&mut self);
    fn wait_for_running(&mut self, interval: i64);
    fn is_stopped(&self) -> bool;
    fn get_service_name(&self) -> String;
}

pub trait Runnable: Send + Sync + 'static {
    fn run(&mut self) {}
}

#[trait_variant::make(TokioRunnable: Send)]
pub trait RocketMQTokioRunnable: Sync + 'static {
    async fn run(&mut self);
}
