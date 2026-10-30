// Copyright 2026 The RocketMQ Rust Authors
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

//! The `native` timer engine: a tiered time-bucket log for long delays.
//!
//! A timer due beyond the mid horizon has its record copied once and is indexed in a day
//! bucket, which moves into hours when its day comes within the mid horizon. Other timers are
//! indexed in hour buckets that point into the CommitLog. An hour bucket is split into sorted
//! minute runs when its hour comes near, and a minute run is loaded into the in-memory near
//! wheel when its minute does; delivery takes due timers from the wheel per lane, in
//! `(due_ms, cq_offset)` order. Recalls go to the cancel log and an in-memory cancel set. A
//! group commit checkpoints how far the Timer CQ is indexed and how far each lane delivered.
//! Delivery reads each due timer's message, settles the recalled ones, and writes the rest to
//! their real topics in the lane's order. The store runs the engine under `timerStoreMode =
//! native` through [`NativeTimerService`], beside the Java-compatible engine, which still
//! delivers the timers it admitted before the switch.

mod cancel_set;
mod config;
mod delivery;
mod engine;
mod near_wheel;
mod service;

pub(crate) use engine::NativeRecallVerdict;
pub(crate) use service::NativeTimerService;

#[cfg(test)]
mod tests;
