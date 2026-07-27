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

use std::path::PathBuf;

use rocketmq_sre_core::diagnostics::DiagnosticError;
use rocketmq_sre_model_gateway::ProviderError;
use thiserror::Error;

/// Stable failure categories emitted by the offline shadow harness.
#[derive(Debug, Error)]
pub enum ShadowEvalError {
    #[error("failed to access `{path}`: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid shadow manifest: {0}")]
    InvalidManifest(String),
    #[error("invalid diagnostic fixture `{path}`: {detail}")]
    InvalidFixture { path: PathBuf, detail: String },
    #[error("shadow policy is not read-only: {0}")]
    UnsafePolicy(String),
    #[error("cluster scope mismatch: requested `{requested}`, authorized `{authorized}`")]
    ClusterScopeMismatch { requested: String, authorized: String },
    #[error("model cited evidence outside the authorized evidence pack: `{0}`")]
    InvalidCitation(String),
    #[error("model proposed a tool outside the read-only allowlist: `{0}`")]
    UnauthorizedTool(String),
    #[error("model synthesis is invalid: {0}")]
    InvalidSynthesis(String),
    #[error("diagnostic replay failed: {0}")]
    Diagnostic(#[from] DiagnosticError),
    #[error("model gateway failed: {0}")]
    Provider(#[from] ProviderError),
}

impl ShadowEvalError {
    /// Returns a stable machine-facing error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Io { .. } => "source_unavailable",
            Self::InvalidManifest(_) => "invalid_shadow_manifest",
            Self::InvalidFixture { .. } => "invalid_evidence_fixture",
            Self::UnsafePolicy(_) => "mutation_boundary_violation",
            Self::ClusterScopeMismatch { .. } => "cluster_not_allowed",
            Self::InvalidCitation(_) => "invalid_evidence_citation",
            Self::UnauthorizedTool(_) => "unauthorized_tool",
            Self::InvalidSynthesis(_) => "invalid_model_synthesis",
            Self::Diagnostic(_) => "diagnostic_replay_failed",
            Self::Provider(_) => "provider_failed",
        }
    }
}
