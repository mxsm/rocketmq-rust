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

pub use model::DiagnosticQualificationManifest;
pub use model::DiagnosticQualificationReport;
pub(crate) use model::DiagnosticQualificationSource;
pub use model::LiveQualificationConfig;
pub use model::QualificationScenario;

use std::path::Path;

/// Builds the diagnostic qualification manifest from the compiled registry.
///
/// # Errors
///
/// Returns an opaque evaluation failure when the registry or fixtures are invalid.
pub fn generated_manifest() -> Result<crate::EvalOutcome<DiagnosticQualificationManifest>, crate::EvalError> {
    match fixture::generated_manifest() {
        Ok(manifest) => Ok(crate::EvalOutcome::Completed(manifest)),
        Err(failure) => failure.into_boundary(),
    }
}

/// Loads and validates the committed diagnostic qualification manifest.
///
/// # Errors
///
/// Returns an opaque evaluation failure when the manifest cannot be loaded or validated.
pub fn load_committed_manifest(
    path: &Path,
) -> Result<crate::EvalOutcome<DiagnosticQualificationManifest>, crate::EvalError> {
    match fixture::load_committed_manifest(path) {
        Ok(manifest) => Ok(crate::EvalOutcome::Completed(manifest)),
        Err(failure) => failure.into_boundary(),
    }
}

/// Writes the generated diagnostic qualification manifest.
///
/// # Errors
///
/// Returns an opaque evaluation failure when generation, encoding, or writing fails.
pub fn write_generated_manifest(path: &Path) -> Result<crate::EvalOutcome<()>, crate::EvalError> {
    match fixture::write_generated_manifest(path) {
        Ok(()) => Ok(crate::EvalOutcome::Completed(())),
        Err(failure) => failure.into_boundary(),
    }
}

/// Runs the live qualification suite against the configured endpoints.
///
/// # Errors
///
/// Returns an opaque evaluation failure when setup, requests, queries, or assertions fail.
pub async fn run_live_qualification(
    config: &LiveQualificationConfig,
) -> Result<crate::EvalOutcome<DiagnosticQualificationReport>, crate::EvalError> {
    match live::run_live_qualification(config).await {
        Ok(report) => Ok(crate::EvalOutcome::Completed(report)),
        Err(failure) => failure.into_boundary(),
    }
}
