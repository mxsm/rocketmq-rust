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

use rocketmq_admin_core::core::AdminError;
use rocketmq_dashboard_common::DashboardCommonError;
use rocketmq_error::CanonicalCondition;
use rocketmq_error::RecoveryHint;
use serde::Serialize;
use thiserror::Error;

pub(crate) type DashboardResult<T> = Result<T, DashboardError>;
pub(crate) type CommandResult<T> = Result<T, CommandError>;

pub(crate) async fn authorize_command(
    session_id: &str,
    session_state: &crate::auth::SessionState,
) -> CommandResult<()> {
    session_state
        .authorize_dashboard(session_id)
        .await
        .map(|_| ())
        .map_err(CommandError::from)
}

#[derive(Debug, Error)]
pub(crate) enum DashboardError {
    #[error("invalid request: {0}")]
    Validation(String),
    #[error("dashboard configuration is incomplete: {0}")]
    Configuration(String),
    #[error("authentication was rejected: {0}")]
    Authentication(String),
    #[error("the session is not active")]
    Unauthenticated,
    #[error("the account password must be changed before dashboard access")]
    PasswordChangeRequired,
    #[error("connection settings changed; review the current configuration")]
    ConfigurationConflict,
    #[error("monitor rule changed; review the current rule")]
    MonitorConflict,
    #[error("I/O operation failed")]
    Io(#[from] std::io::Error),
    #[error("database operation failed")]
    Database(#[from] rusqlite::Error),
    #[error("the database schema is not supported")]
    UnsupportedStorageVersion,
    #[error("dashboard storage is shutting down")]
    StorageClosed,
    #[error("dashboard persistence work could not complete")]
    Persistence(#[source] rocketmq_runtime::RuntimeError),
    #[error("password processing failed")]
    PasswordHash(password_hash::Error),
    #[error("application integration failed")]
    Tauri(#[from] tauri::Error),
    #[error("admin operation failed")]
    Admin(#[from] AdminError),
    #[error("dashboard state operation failed")]
    Common(#[from] DashboardCommonError),
    #[error("JSON processing failed")]
    Json(#[from] serde_json::Error),
    #[error("internal dashboard state is invalid: {0}")]
    Internal(&'static str),
}

impl DashboardError {
    fn public_view(&self) -> CommandError {
        let (code, message, category, retryable, field) = match self {
            Self::Validation(_) => (
                "dashboard.invalid_argument",
                "The request is invalid.",
                CommandErrorCategory::Validation,
                false,
                None,
            ),
            Self::MonitorConflict => (
                "dashboard.monitor_conflict",
                "This rule changed. Review its current version before saving again.",
                CommandErrorCategory::Validation,
                false,
                None,
            ),
            Self::ConfigurationConflict => (
                "dashboard.configuration_conflict",
                "Connection settings changed. Refresh and review your changes before saving again.",
                CommandErrorCategory::Validation,
                false,
                None,
            ),
            Self::Configuration(_) => (
                "dashboard.configuration_required",
                "Dashboard configuration is incomplete.",
                CommandErrorCategory::Configuration,
                false,
                None,
            ),
            Self::Authentication(_) => (
                "auth.credentials.invalid",
                "Authentication failed.",
                CommandErrorCategory::Authentication,
                false,
                None,
            ),
            Self::Unauthenticated => (
                "auth.session.invalid",
                "Your session is no longer valid. Sign in again.",
                CommandErrorCategory::Authentication,
                false,
                None,
            ),
            Self::PasswordChangeRequired => (
                "auth.password_change_required",
                "Change the account password before using the dashboard.",
                CommandErrorCategory::Authentication,
                false,
                None,
            ),
            Self::Admin(error) => (
                error.code().as_str(),
                error.descriptor().public_message(),
                canonical_category(error.condition()),
                error.is_retryable(),
                error.field().map(str::to_owned),
            ),
            Self::Common(error) => (
                error.code().as_str(),
                error.descriptor().public_message(),
                canonical_category(error.condition()),
                automatic_recovery(error.recovery_hint()),
                None,
            ),
            Self::UnsupportedStorageVersion => (
                "dashboard.storage_version_unsupported",
                "This database format is not supported. Select a new data directory.",
                CommandErrorCategory::Configuration,
                false,
                None,
            ),
            Self::StorageClosed => (
                "dashboard.storage_closed",
                "Dashboard storage is shutting down.",
                CommandErrorCategory::Unavailable,
                false,
                None,
            ),
            Self::Io(_) | Self::Database(_) | Self::Tauri(_) | Self::Persistence(_) => (
                "dashboard.storage_unavailable",
                "Dashboard storage is unavailable.",
                CommandErrorCategory::Unavailable,
                true,
                None,
            ),
            Self::PasswordHash(_) | Self::Json(_) | Self::Internal(_) => (
                "dashboard.internal",
                "The dashboard could not complete the operation.",
                CommandErrorCategory::Internal,
                false,
                None,
            ),
        };

        CommandError {
            code,
            message,
            category,
            retryable,
            field,
            audit_warning: None,
        }
    }
}

fn canonical_category(condition: CanonicalCondition) -> CommandErrorCategory {
    match condition {
        CanonicalCondition::InvalidArgument
        | CanonicalCondition::AlreadyExists
        | CanonicalCondition::ResourceExhausted
        | CanonicalCondition::FailedPrecondition
        | CanonicalCondition::Aborted => CommandErrorCategory::Validation,
        CanonicalCondition::NotFound => CommandErrorCategory::NotFound,
        CanonicalCondition::Unauthenticated | CanonicalCondition::PermissionDenied => {
            CommandErrorCategory::Authentication
        }
        CanonicalCondition::Unavailable | CanonicalCondition::DeadlineExceeded | CanonicalCondition::Cancelled => {
            CommandErrorCategory::Unavailable
        }
        CanonicalCondition::DataLoss | CanonicalCondition::Unimplemented | CanonicalCondition::Internal => {
            CommandErrorCategory::Internal
        }
    }
}

fn automatic_recovery(hint: RecoveryHint) -> bool {
    matches!(
        hint,
        RecoveryHint::Backoff
            | RecoveryHint::RefreshRoute
            | RecoveryHint::RefreshLeader
            | RecoveryHint::SwitchBroker
            | RecoveryHint::RefreshCredentials
    )
}

impl From<password_hash::Error> for DashboardError {
    fn from(error: password_hash::Error) -> Self {
        Self::PasswordHash(error)
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CommandErrorCategory {
    Authentication,
    Validation,
    NotFound,
    Configuration,
    Unavailable,
    Internal,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandError {
    pub(crate) code: &'static str,
    pub(crate) message: &'static str,
    pub(crate) category: CommandErrorCategory,
    pub(crate) retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) audit_warning: Option<&'static str>,
}

impl From<DashboardError> for CommandError {
    fn from(error: DashboardError) -> Self {
        let public = error.public_view();
        log::error!("Dashboard command failed with code {}", public.code);
        public
    }
}

#[cfg(test)]
mod tests {
    use super::CommandError;
    use super::CommandErrorCategory;
    use super::DashboardCommonError;
    use super::DashboardError;
    use rocketmq_admin_core::core::AdminError;
    use rocketmq_dashboard_common::DashboardEndpointKind;
    use std::error::Error;

    #[test]
    fn source_chain_is_retained_for_io_and_admin_errors() {
        let io_error = DashboardError::from(std::io::Error::other("sensitive path"));
        assert_eq!(
            io_error.source().map(ToString::to_string).as_deref(),
            Some("sensitive path")
        );

        let admin_error = DashboardError::from(AdminError::backend("query", "sensitive broker detail"));
        assert_eq!(
            admin_error.source().map(ToString::to_string).as_deref(),
            Some("tools.operation.failed: Administrative operation failed")
        );
    }

    #[test]
    fn public_error_is_stable_and_redacted() {
        let command_error = CommandError::from(DashboardError::from(AdminError::unavailable(
            "query",
            "password=do-not-disclose",
        )));

        assert_eq!(command_error.code, "client.component.unavailable");
        assert_eq!(command_error.message, "Client component is unavailable");
        assert_eq!(command_error.category, CommandErrorCategory::Unavailable);
        assert!(command_error.retryable);
        let serialized = serde_json::to_string(&command_error).expect("command error should serialize");
        assert!(!serialized.contains("password"));
        assert!(!serialized.contains("token"));
    }

    #[test]
    fn authentication_error_has_fixed_public_message() {
        let command_error = CommandError::from(DashboardError::Authentication("database detail".to_string()));

        assert_eq!(command_error.code, "auth.credentials.invalid");
        assert_eq!(command_error.message, "Authentication failed.");
        assert!(
            !serde_json::to_string(&command_error)
                .expect("command error should serialize")
                .contains("database detail")
        );
    }

    #[test]
    fn common_error_uses_canonical_public_projection() {
        const SENTINEL: &str = "private-port";
        let parse_source = SENTINEL.parse::<u16>().expect_err("invalid port");
        let error = DashboardCommonError::endpoint_port(DashboardEndpointKind::NameServer, parse_source);
        let command_error = CommandError::from(DashboardError::from(error));

        assert_eq!(command_error.code, "core.argument.invalid");
        assert_eq!(command_error.message, "Argument is invalid");
        assert_eq!(command_error.category, CommandErrorCategory::Validation);
        assert!(!command_error.retryable);
        assert!(
            !serde_json::to_string(&command_error)
                .expect("command error should serialize")
                .contains(SENTINEL)
        );
    }

    #[test]
    fn every_non_auth_command_requires_dashboard_authorization() {
        const COMMAND_SOURCES: &[&str] = &[
            include_str!("cluster/commands.rs"),
            include_str!("consumer/commands.rs"),
            include_str!("dashboard/commands.rs"),
            include_str!("message/commands.rs"),
            include_str!("nameserver/commands.rs"),
            include_str!("producer/commands.rs"),
            include_str!("proxy/commands.rs"),
            include_str!("topic/commands.rs"),
        ];

        let mut command_count = 0;
        for source in COMMAND_SOURCES {
            for command in source.split("#[tauri::command]").skip(1) {
                command_count += 1;
                assert!(command.contains("session_id: String"));
                assert!(command.contains("State<'_, SessionState>"));
                assert!(
                    command.contains("authorize_command(&session_id, &session_state).await?;")
                        || command.contains("AuditAccess::dashboard(&session_state, session_id)")
                );
            }
        }

        assert_eq!(command_count, 50);
    }
}
