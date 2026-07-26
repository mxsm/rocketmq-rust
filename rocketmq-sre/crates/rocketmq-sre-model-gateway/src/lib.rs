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

//! Canonical model request/response types and offline provider descriptors.
//!
//! Phase 00 intentionally contains no model SDK and performs no network calls.

mod fixtures;
mod ir;

pub use fixtures::phase00_provider_descriptors;
pub use ir::CanonicalModelRequest;
pub use ir::CanonicalModelResponse;
pub use ir::FinishReason;
pub use ir::ModelMessage;
pub use ir::ModelRole;
pub use ir::ModelTool;
pub use ir::ModelToolCall;
pub use ir::ResponseFormat;
