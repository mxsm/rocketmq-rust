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

//! Adapters from policy evaluation to layered authorization decisions.

use rocketmq_security_api::DetailedDecision;
use rocketmq_security_api::LayerFailureKind;

use crate::authorization::enums::decision::Decision as PolicyDecision;
use crate::AuthFailureKind;
use crate::AuthServiceError;

/// Projects an internal policy decision into the detailed authorization contract.
///
/// The internal policy decision remains binary. In particular, an ACL or policy
/// `Deny` can never become a layered `Abstain`.
#[must_use]
pub const fn project_policy_decision(decision: PolicyDecision) -> DetailedDecision {
    match decision {
        PolicyDecision::Allow => DetailedDecision::Allow,
        PolicyDecision::Deny => DetailedDecision::Deny,
    }
}

/// Classifies an authorization failure for the fail-closed layered contract.
///
/// This classification carries no underlying error text. Callers must use the
/// fixed denial output from `rocketmq-security-api` rather than returning the
/// source error to a peer.
#[must_use]
pub fn project_authorization_error(error: &AuthServiceError) -> LayerFailureKind {
    match error.kind() {
        AuthFailureKind::Timeout => LayerFailureKind::Timeout,
        AuthFailureKind::InvalidConfiguration | AuthFailureKind::Unavailable => LayerFailureKind::Unavailable,
        _ => LayerFailureKind::Error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AuthOperation;

    #[test]
    fn policy_decisions_remain_binary() {
        assert_eq!(project_policy_decision(PolicyDecision::Allow), DetailedDecision::Allow);
        assert_eq!(project_policy_decision(PolicyDecision::Deny), DetailedDecision::Deny);
    }

    #[test]
    fn unavailable_and_operational_errors_never_abstain() {
        assert_eq!(
            project_authorization_error(&AuthServiceError::new(
                AuthOperation::Initialize,
                AuthFailureKind::Unavailable,
            )),
            LayerFailureKind::Unavailable
        );
        assert_eq!(
            project_authorization_error(&AuthServiceError::new(
                AuthOperation::Authorize,
                AuthFailureKind::Internal,
            )),
            LayerFailureKind::Error
        );
    }

    #[test]
    fn service_errors_keep_timeout_unavailable_and_error_distinct() {
        assert_eq!(
            project_authorization_error(&AuthServiceError::new(
                AuthOperation::WriteMetadata,
                AuthFailureKind::Timeout,
            )),
            LayerFailureKind::Timeout
        );
        assert_eq!(
            project_authorization_error(&AuthServiceError::new(
                AuthOperation::ReadMetadata,
                AuthFailureKind::Unavailable,
            )),
            LayerFailureKind::Unavailable
        );
        assert_eq!(
            project_authorization_error(&AuthServiceError::new(
                AuthOperation::Authorize,
                AuthFailureKind::Internal,
            )),
            LayerFailureKind::Error
        );
    }
}
