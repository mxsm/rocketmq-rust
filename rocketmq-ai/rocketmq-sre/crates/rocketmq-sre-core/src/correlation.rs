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

//! Deterministic, tenant-safe primitives used by the Phase 2 correlation engine.

mod fingerprint;
mod graph;
mod merge;
mod window;

pub use fingerprint::CorrelationFingerprintMaterial;
pub use graph::ResourceGraph;
pub use merge::CorrelationCandidate;
pub use merge::select_candidate;
pub use window::DEFAULT_CORRELATION_WINDOW_SECONDS;
pub use window::bounded_window_start_epoch;
