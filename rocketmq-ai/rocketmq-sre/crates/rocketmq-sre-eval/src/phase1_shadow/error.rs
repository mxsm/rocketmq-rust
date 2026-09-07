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

use rocketmq_sre_contracts::SreContractError;
use rocketmq_sre_model_gateway::ProviderError;
use thiserror::Error;

/// Private mixed completion channel. It does not implement
/// [`std::error::Error`], so expected safety and validation results cannot be
/// promoted into the operational [`crate::EvalError`] facade.
pub(crate) enum ShadowEvalFailure {
    Io { _path: PathBuf, source: std::io::Error },
    InvalidManifest(String),
    ManifestDecode(serde_yaml::Error),
    InvalidFixture { _path: PathBuf, _detail: String },
    FixtureDecode { _path: PathBuf, source: serde_json::Error },
    UnsafePolicy(String),
    ClusterScopeMismatch { _requested: String, _authorized: String },
    InvalidCitation(String),
    UnauthorizedTool(String),
    SynthesisDecode(serde_json::Error),
    SynthesisEncode(serde_json::Error),
    Diagnostic(SreContractError),
    Provider(ProviderError),
}

impl std::fmt::Debug for ShadowEvalFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ShadowEvalFailure")
    }
}

#[derive(Debug, Error)]
pub(crate) enum ShadowEvalSource {
    #[error("shadow evaluation source is unavailable")]
    Io {
        _path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("shadow evaluation manifest cannot be decoded")]
    ManifestDecode(#[source] serde_yaml::Error),
    #[error("shadow evaluation fixture cannot be decoded")]
    FixtureDecode {
        _path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("shadow evaluation synthesis cannot be decoded")]
    SynthesisDecode(#[source] serde_json::Error),
    #[error("shadow evaluation synthesis cannot be encoded")]
    SynthesisEncode(#[source] serde_json::Error),
    #[error("shadow evaluation provider failed")]
    Provider(#[source] ProviderError),
    #[error("shadow diagnostic contract operation failed")]
    Diagnostic(#[source] SreContractError),
}

impl ShadowEvalFailure {
    pub(crate) fn into_boundary<T>(self) -> Result<crate::EvalOutcome<T>, crate::EvalError> {
        match self {
            Self::InvalidManifest(detail) => {
                let _ = detail;
                Ok(crate::EvalOutcome::Rejected(
                    crate::EvalRejection::InvalidShadowManifest,
                ))
            }
            Self::InvalidFixture { _path, _detail } => {
                let _ = (_path, _detail);
                Ok(crate::EvalOutcome::Rejected(
                    crate::EvalRejection::InvalidEvidenceFixture,
                ))
            }
            Self::UnsafePolicy(detail) => {
                let _ = detail;
                Ok(crate::EvalOutcome::Rejected(
                    crate::EvalRejection::MutationBoundaryViolation,
                ))
            }
            Self::ClusterScopeMismatch {
                _requested,
                _authorized,
            } => {
                let _ = (_requested, _authorized);
                Ok(crate::EvalOutcome::Rejected(crate::EvalRejection::ClusterNotAllowed))
            }
            Self::InvalidCitation(detail) => {
                let _ = detail;
                Ok(crate::EvalOutcome::Rejected(
                    crate::EvalRejection::InvalidEvidenceCitation,
                ))
            }
            Self::UnauthorizedTool(detail) => {
                let _ = detail;
                Ok(crate::EvalOutcome::Rejected(crate::EvalRejection::UnauthorizedTool))
            }
            Self::Diagnostic(source) => {
                if std::error::Error::source(&source).is_some() {
                    Err(crate::EvalError::shadow_source(ShadowEvalSource::Diagnostic(source)))
                } else {
                    Ok(crate::EvalOutcome::Rejected(
                        crate::EvalRejection::DiagnosticReplayRejected,
                    ))
                }
            }
            Self::Io { _path, source } => Err(crate::EvalError::shadow_source(ShadowEvalSource::Io { _path, source })),
            Self::ManifestDecode(source) => Err(crate::EvalError::shadow_source(ShadowEvalSource::ManifestDecode(
                source,
            ))),
            Self::FixtureDecode { _path, source } => {
                Err(crate::EvalError::shadow_source(ShadowEvalSource::FixtureDecode {
                    _path,
                    source,
                }))
            }
            Self::SynthesisDecode(source) => Err(crate::EvalError::shadow_source(ShadowEvalSource::SynthesisDecode(
                source,
            ))),
            Self::SynthesisEncode(source) => Err(crate::EvalError::shadow_source(ShadowEvalSource::SynthesisEncode(
                source,
            ))),
            Self::Provider(source) => Err(crate::EvalError::shadow_source(ShadowEvalSource::Provider(source))),
        }
    }
}

impl From<SreContractError> for ShadowEvalFailure {
    fn from(source: SreContractError) -> Self {
        Self::Diagnostic(source)
    }
}

impl From<ProviderError> for ShadowEvalFailure {
    fn from(source: ProviderError) -> Self {
        Self::Provider(source)
    }
}

impl ShadowEvalSource {
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::Io { .. } => "source_unavailable",
            Self::ManifestDecode(_) => "invalid_shadow_manifest",
            Self::FixtureDecode { .. } => "invalid_evidence_fixture",
            Self::SynthesisDecode(_) | Self::SynthesisEncode(_) => "invalid_model_synthesis",
            Self::Provider(_) => "provider_failed",
            Self::Diagnostic(_) => "source_unavailable",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use rocketmq_sre_contracts::PublicErrorCode;

    use super::*;

    #[test]
    fn diagnostic_without_source_is_a_closed_rejection() {
        let outcome = ShadowEvalFailure::Diagnostic(SreContractError::new(PublicErrorCode::InvalidDescriptor))
            .into_boundary::<()>()
            .expect("source-free diagnostic failure must not be operational");

        assert_eq!(
            outcome,
            crate::EvalOutcome::Rejected(crate::EvalRejection::DiagnosticReplayRejected)
        );
    }

    #[test]
    fn diagnostic_with_source_preserves_the_typed_chain() {
        let error = ShadowEvalFailure::Diagnostic(SreContractError::with_source(
            PublicErrorCode::SourceUnavailable,
            std::io::Error::other("private diagnostic source"),
        ))
        .into_boundary::<()>()
        .expect_err("source-bearing diagnostic failure must remain operational");

        let shadow = error.source().expect("shadow source");
        assert!(shadow.is::<ShadowEvalSource>());
        let contract = shadow.source().expect("contract source");
        assert!(contract.is::<SreContractError>());
        assert!(contract.source().is_some_and(|source| source.is::<std::io::Error>()));
    }
}
