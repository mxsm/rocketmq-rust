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

use std::error::Error;
use std::fmt;

use rocketmq_sre_client::ClientError;
use rocketmq_sre_client::ClientFailure;

/// Closed CLI failure classification for exit-code and presentation policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CliFailureCode {
    Usage,
    MissingBaseUrl,
    MissingToken,
    InvalidEnvironment,
    DraftTooLarge,
    DraftRejected,
    ClientRejected,
    Operational,
}

/// Opaque operational CLI error.
pub struct CliError {
    kind: CliErrorKind,
}

enum CliErrorKind {
    DraftIo(std::io::Error),
    Client(ClientError),
    Json(serde_json::Error),
}

impl CliError {
    fn draft_io(source: std::io::Error) -> Self {
        Self {
            kind: CliErrorKind::DraftIo(source),
        }
    }

    fn client(source: ClientError) -> Self {
        Self {
            kind: CliErrorKind::Client(source),
        }
    }

    fn json(source: serde_json::Error) -> Self {
        Self {
            kind: CliErrorKind::Json(source),
        }
    }

    const fn label(&self) -> &'static str {
        match &self.kind {
            CliErrorKind::DraftIo(_) => "draft_io",
            CliErrorKind::Client(_) => "client",
            CliErrorKind::Json(_) => "json",
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match &self.kind {
            CliErrorKind::DraftIo(_) => "draft file operation failed",
            CliErrorKind::Client(_) => "Control Plane client operation failed",
            CliErrorKind::Json(_) => "JSON output failed",
        })
    }
}

impl fmt::Debug for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CliError").field("kind", &self.label()).finish()
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            CliErrorKind::DraftIo(source) => Some(source),
            CliErrorKind::Client(source) => Some(source),
            CliErrorKind::Json(source) => Some(source),
        }
    }
}

/// An expected CLI rejection or an opaque operational failure.
///
/// This type deliberately does not implement [`Error`].
pub struct CliFailure {
    kind: CliFailureKind,
}

enum CliFailureKind {
    Rejected(CliFailureCode),
    Client(ClientFailure),
    Operational(CliError),
}

impl CliFailure {
    pub(crate) const fn usage() -> Self {
        Self::rejected(CliFailureCode::Usage)
    }

    pub(crate) const fn missing_base_url() -> Self {
        Self::rejected(CliFailureCode::MissingBaseUrl)
    }

    pub(crate) const fn missing_token() -> Self {
        Self::rejected(CliFailureCode::MissingToken)
    }

    pub(crate) const fn invalid_environment() -> Self {
        Self::rejected(CliFailureCode::InvalidEnvironment)
    }

    pub(crate) const fn draft_too_large() -> Self {
        Self::rejected(CliFailureCode::DraftTooLarge)
    }

    pub(crate) const fn draft_rejected() -> Self {
        Self::rejected(CliFailureCode::DraftRejected)
    }

    const fn rejected(code: CliFailureCode) -> Self {
        Self {
            kind: CliFailureKind::Rejected(code),
        }
    }

    pub(crate) fn draft_io(source: std::io::Error) -> Self {
        Self {
            kind: CliFailureKind::Operational(CliError::draft_io(source)),
        }
    }

    pub(crate) fn json(source: serde_json::Error) -> Self {
        Self {
            kind: CliFailureKind::Operational(CliError::json(source)),
        }
    }

    /// Returns the stable classification used for exit-code decisions.
    #[must_use]
    pub const fn code(&self) -> CliFailureCode {
        match &self.kind {
            CliFailureKind::Rejected(code) => *code,
            CliFailureKind::Client(_) => CliFailureCode::ClientRejected,
            CliFailureKind::Operational(_) => CliFailureCode::Operational,
        }
    }

    /// Returns whether command syntax or operands were rejected.
    #[must_use]
    pub const fn is_usage(&self) -> bool {
        matches!(self.kind, CliFailureKind::Rejected(CliFailureCode::Usage))
    }

    /// Returns the typed source for an operational failure.
    #[must_use]
    pub const fn operational_error(&self) -> Option<&CliError> {
        match &self.kind {
            CliFailureKind::Operational(error) => Some(error),
            CliFailureKind::Rejected(_) | CliFailureKind::Client(_) => None,
        }
    }

    /// Returns closed HTTP/client metadata for a rejected remote operation.
    #[must_use]
    pub const fn client_failure(&self) -> Option<&ClientFailure> {
        match &self.kind {
            CliFailureKind::Client(failure) => Some(failure),
            CliFailureKind::Rejected(_) | CliFailureKind::Operational(_) => None,
        }
    }

    /// Returns a bounded, non-sensitive message for stderr.
    #[must_use]
    pub const fn public_message(&self) -> &'static str {
        match &self.kind {
            CliFailureKind::Rejected(CliFailureCode::Usage) => "invalid command usage",
            CliFailureKind::Rejected(CliFailureCode::MissingBaseUrl) => {
                "Control Plane URL is required via --url or ROCKETMQ_SRE_URL"
            }
            CliFailureKind::Rejected(CliFailureCode::MissingToken) => {
                "bearer token is required in the configured environment variable"
            }
            CliFailureKind::Rejected(CliFailureCode::InvalidEnvironment) => {
                "configured environment variable is invalid"
            }
            CliFailureKind::Rejected(CliFailureCode::DraftTooLarge) => "draft file exceeds the configured byte limit",
            CliFailureKind::Rejected(CliFailureCode::DraftRejected) => "draft does not match the typed local contract",
            CliFailureKind::Rejected(CliFailureCode::ClientRejected) => "Control Plane request was rejected",
            CliFailureKind::Rejected(CliFailureCode::Operational) => "CLI operation failed",
            CliFailureKind::Client(failure) => failure.public_message(),
            CliFailureKind::Operational(error) => match &error.kind {
                CliErrorKind::DraftIo(_) => "draft file operation failed",
                CliErrorKind::Client(_) => "Control Plane client operation failed",
                CliErrorKind::Json(_) => "JSON output failed",
            },
        }
    }
}

impl fmt::Display for CliFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.public_message())
    }
}

impl fmt::Debug for CliFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliFailure")
            .field("code", &self.code())
            .finish()
    }
}

impl From<ClientFailure> for CliFailure {
    fn from(failure: ClientFailure) -> Self {
        match failure.into_operational_error() {
            Ok(error) => Self {
                kind: CliFailureKind::Operational(CliError::client(error)),
            },
            Err(rejection) => Self {
                kind: CliFailureKind::Client(rejection),
            },
        }
    }
}
