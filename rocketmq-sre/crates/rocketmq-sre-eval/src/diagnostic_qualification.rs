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

//! Rules-only live qualification for the complete diagnostic-pack catalog.

mod fixture;
mod live;
mod model;

pub use fixture::generated_manifest;
pub use fixture::load_committed_manifest;
pub use fixture::write_generated_manifest;
pub use live::run_live_qualification;
pub use model::DiagnosticQualificationError;
pub use model::DiagnosticQualificationManifest;
pub use model::DiagnosticQualificationReport;
pub use model::LiveQualificationConfig;
pub use model::QualificationScenario;
