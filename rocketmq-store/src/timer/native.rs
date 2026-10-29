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
//! Timers due within the mid horizon are indexed in hour buckets that point into the
//! CommitLog; an hour bucket is loaded into the in-memory near wheel when its hour comes near,
//! and delivery takes due timers from the wheel per lane, in `(due_ms, cq_offset)` order.
//! Recalls go to the cancel log and an in-memory cancel set, scoped to the hour of the timer
//! they cancel. Group commit, recovery replay, day buckets with payload copies, minute runs
//! and delivery come in later changes; until then nothing outside this module uses it.

mod cancel_set;
mod config;
mod engine;
mod near_wheel;

#[cfg(test)]
mod tests;
