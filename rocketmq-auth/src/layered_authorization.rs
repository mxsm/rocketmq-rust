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

//! Adapters from the legacy authorization model to layered decisions.

use rocketmq_error::CanonicalCondition;
use rocketmq_security_api::DetailedDecision;
use rocketmq_security_api::LayerEvaluation;
use rocketmq_security_api::LayerFailureKind;

use crate::authorization::enums::decision::Decision as PolicyDecision;
use crate::authorization::provider::AuthorizationError;

/// Projects a legacy policy decision into the detailed authorization contract.
///
/// The legacy policy decision remains binary. In particular, an ACL or policy
/// `Deny` can never become a layered `Abstain`.
#[must_use]
pub const fn project_policy_decision(decision: PolicyDecision) -> DetailedDecision {
    match decision {
        PolicyDecision::Allow => DetailedDecision::Allow,
        PolicyDecision::Deny => DetailedDecision::Deny,
    }
}

/// Classifies a legacy authorization failure for the fail-closed layered contract.
///
/// This classification carries no underlying error text. Callers must use the
/// fixed denial output from `rocketmq-security-api` rather than returning the
/// source error to a peer.
#[must_use]
pub fn project_authorization_error(error: &AuthorizationError) -> LayerFailureKind {
    match error {
        AuthorizationError::ConfigurationError(_)
        | AuthorizationError::NotInitialized(_)
        | AuthorizationError::StorageReadFailed { .. }
        | AuthorizationError::StorageLockFailed(_) => LayerFailureKind::Unavailable,
        AuthorizationError::MetadataIo(error) => match error.condition() {
            CanonicalCondition::DeadlineExceeded => LayerFailureKind::Timeout,
            CanonicalCondition::Unavailable => LayerFailureKind::Unavailable,
            _ => LayerFailureKind::Error,
        },
        AuthorizationError::PermissionDenied { .. }
        | AuthorizationError::PolicyEvaluationFailed(_)
        | AuthorizationError::SubjectNotFound(_)
        | AuthorizationError::ResourceNotFound(_)
        | AuthorizationError::ProviderRuntimeFailed(_)
        | AuthorizationError::StorageWriteFailed { .. }
        | AuthorizationError::SerializationFailed { .. }
        | AuthorizationError::InvalidContext(_) => LayerFailureKind::Error,
    }
}

/// Projects a legacy authorization operation into a detailed layer result.
///
/// A successful operation allows the request. A legacy permission denial is a
/// detailed denial. Every other error remains a layer failure and is never
/// converted to `Abstain`.
pub fn project_authorization_result(result: Result<(), AuthorizationError>) -> LayerEvaluation<DetailedDecision> {
    match result {
        Ok(()) => Ok(DetailedDecision::Allow),
        Err(error @ AuthorizationError::PermissionDenied { .. }) => {
            let _ = error;
            Ok(DetailedDecision::Deny)
        }
        Err(error) => Err(project_authorization_error(&error)),
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_runtime::RuntimeError;

    use super::*;

    #[test]
    fn policy_decisions_remain_binary() {
        assert_eq!(project_policy_decision(PolicyDecision::Allow), DetailedDecision::Allow);
        assert_eq!(project_policy_decision(PolicyDecision::Deny), DetailedDecision::Deny);
    }

    #[test]
    fn permission_denial_is_not_an_abstention() {
        let result = project_authorization_result(Err(AuthorizationError::PermissionDenied {
            subject: "alice".to_owned(),
            resource: "topic:orders".to_owned(),
            reason: "not granted".to_owned(),
        }));

        assert_eq!(result, Ok(DetailedDecision::Deny));
    }

    #[test]
    fn unavailable_and_operational_errors_never_abstain() {
        assert_eq!(
            project_authorization_result(Err(AuthorizationError::NotInitialized("not ready".to_owned()))),
            Err(LayerFailureKind::Unavailable)
        );
        assert_eq!(
            project_authorization_result(Err(AuthorizationError::PolicyEvaluationFailed("failed".to_owned()))),
            Err(LayerFailureKind::Error)
        );
    }

    #[test]
    fn metadata_io_errors_keep_timeout_unavailable_and_error_distinct() {
        assert_eq!(
            project_authorization_error(&AuthorizationError::MetadataIo(RuntimeError::timed_out(
                rocketmq_runtime::RuntimeOperation::PersistMetadata
            ))),
            LayerFailureKind::Timeout
        );
        assert_eq!(
            project_authorization_error(&AuthorizationError::MetadataIo(RuntimeError::context_unavailable(
                rocketmq_runtime::RuntimeOperation::MetadataIo
            ))),
            LayerFailureKind::Unavailable
        );
        assert_eq!(
            project_authorization_error(&AuthorizationError::MetadataIo(RuntimeError::context_unavailable(
                rocketmq_runtime::RuntimeOperation::MetadataIo
            ))),
            LayerFailureKind::Unavailable
        );
        assert_eq!(
            project_authorization_error(&AuthorizationError::MetadataIo(RuntimeError::capacity(
                rocketmq_runtime::RuntimeOperation::MetadataIo,
            ))),
            LayerFailureKind::Error
        );
    }
}
