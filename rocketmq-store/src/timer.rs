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

pub(crate) mod clock;
pub(crate) mod completion;
pub(crate) mod delivery;
pub(crate) mod engine;
pub(crate) mod error;
pub(crate) mod format_registry;
pub(crate) mod index;
pub(crate) mod java_compat;
#[allow(
    dead_code,
    reason = "the native engine is wired into the timer store by a later change; its tests use it until then"
)]
pub(crate) mod native;
pub(crate) mod payload_cursor;
pub(crate) mod pipeline;
pub(crate) mod request;
pub(crate) mod role;
pub mod slot;
pub(crate) mod slot_drain;
pub(crate) mod source;
pub(crate) mod source_wake;
pub(crate) mod storage;
#[cfg(feature = "extended_timeline")]
pub(crate) mod timeline;
pub mod timer_checkpoint;
pub mod timer_log;
pub mod timer_message_store;
pub mod timer_metrics;
pub mod timer_wheel;

#[cfg(test)]
mod tests;
