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

mod build_service;
pub mod checkpoint;
pub mod codec;
pub mod state_index;
pub mod timeline_index;

pub use build_service::RocksDbTimerBuildConfig;
pub use build_service::RocksDbTimerBuildService;
pub use build_service::RocksDbTimerDispatch;
pub use build_service::TimerRocksDbBuildEntry;

/// Extended Timeline records ordered by original millisecond deadline.
pub const TIMELINE_CF: &str = "timeline";
/// Durable state machine records.
pub const STATE_CF: &str = "state";
/// Structured Recall lookup records.
pub const LOOKUP_CF: &str = "lookup";
/// Durable delivery outbox.
pub const READY_CF: &str = "ready";
/// Late materialization outbox.
pub const LATE_READY_CF: &str = "late_ready";
/// Java-compatible shadow-only timeline namespace.
pub const SHADOW_TIMELINE_CF: &str = "shadow_timeline";
/// Shadow observations that can never be claimed for delivery.
pub const SHADOW_OBSERVATION_CF: &str = "shadow_observation";
/// Source and per-lane scan checkpoints.
pub const CHECKPOINT_CF: &str = "checkpoint";
/// Sparse due-day/hour counts used to skip empty ranges.
pub const BUCKET_SUMMARY_CF: &str = "bucket_summary";
