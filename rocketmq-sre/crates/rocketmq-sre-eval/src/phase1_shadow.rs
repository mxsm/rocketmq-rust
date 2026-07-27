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

//! Offline Phase 01 Wave A replay and read-only shadow safety harness.

mod error;
mod fixture;
mod manifest;
mod provider;
mod runner;
mod security;

pub use error::ShadowEvalError;
pub use fixture::DiagnosticReplayFixture;
pub use fixture::load_diagnostic_fixture;
pub use manifest::ScenarioCase;
pub use manifest::ScenarioClass;
pub use manifest::ScenarioDefinition;
pub use manifest::ShadowManifest;
pub use manifest::ShadowPolicy;
pub use manifest::load_shadow_manifest;
pub use provider::ProviderMode;
pub use runner::ScenarioResult;
pub use runner::ShadowHarness;
pub use runner::ShadowSuiteSummary;
pub use security::ShadowModelSynthesis;
pub use security::build_model_request;
pub use security::validate_citations;
pub use security::validate_model_response;
