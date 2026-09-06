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

//! Runtime-neutral contracts for staged authorization.

use crate::SecurityRequestView;

/// Final authorization decision returned by a policy layer.
#[must_use = "authorization decisions must be checked; Ok(Deny(_)) rejects the operation"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationDecision {
    /// The operation is authorized.
    Allow,
    /// The operation is rejected for a closed, non-sensitive reason.
    Deny(AuthorizationDenial),
}

/// Closed, non-sensitive reasons for rejecting an authorization request.
///
/// Resource, subject, credential, policy, and request values deliberately do
/// not appear in this contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationDenial {
    /// The subject cannot be authorized without exposing whether it exists.
    SubjectUnknown,
    /// The resource cannot be authorized without exposing whether it exists.
    ResourceUnknown,
    /// The evaluated policy does not grant the requested operation.
    PermissionDenied,
    /// No applicable policy grants the requested operation.
    PolicyNotApplicable,
    /// A maintenance restriction rejects the requested operation.
    MaintenanceRestricted,
}

/// The coarse ingress decision made before authentication or detailed policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressDecision {
    /// The request may continue to authentication and detailed authorization.
    AllowToContinue,
    /// The request must be rejected before evaluating a detailed layer.
    Deny,
}

/// The result of a detailed authorization policy evaluation.
///
/// `Abstain` is intentionally local to the layered contract and is resolved
/// only by [`combine_layered_authorization`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailedDecision {
    /// The detailed policy allows the request.
    Allow,
    /// The detailed policy denies the request.
    Deny,
    /// The detailed policy is deliberately not installed or enabled.
    Abstain,
}

/// A fail-closed category for a layer that could not return a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerFailureKind {
    /// The layer or its required dependency is not available.
    Unavailable,
    /// The layer returned an operational or policy-evaluation error.
    Error,
    /// The layer exceeded its configured deadline.
    Timeout,
}

/// Whether a detailed authorization decision is required for a request path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerRequirement {
    /// A detailed decision must be present; `Abstain` is a denial.
    Required,
    /// An explicitly optional detailed layer permits `Abstain`.
    ///
    /// This variant is only for a deliberately disabled optional layer. It must
    /// not convert an unavailable layer or any other failure into an allow.
    Optional,
}

/// The result returned by one authorization layer.
///
/// [`combine_layered_authorization`] preserves every [`Err`] value for the
/// owning boundary to project while remaining fail closed.
pub type LayerEvaluation<T> = Result<T, LayerFailureKind>;

/// A coarse ingress policy that can inspect a request without authenticating it.
///
/// Implementations classify only the ingress surface. Authentication and
/// resource-level authorization remain the responsibility of later layers.
pub trait IngressPolicy: Send + Sync {
    /// Evaluates whether the request may proceed to the detailed layer.
    fn evaluate_ingress(&self, request: SecurityRequestView<'_>) -> LayerEvaluation<IngressDecision>;
}

/// Combines coarse ingress and detailed authorization under fail-closed semantics.
///
/// A coarse deny is sticky and does not invoke `detailed`. An ingress failure, a
/// detailed failure, or a detailed deny never produces an allow. Failures remain
/// typed [`Err`] values for the owning boundary to project. A detailed abstention
/// is allowed only for an explicit [`LayerRequirement::Optional`]; required
/// authorization resolves it to [`AuthorizationDenial::PolicyNotApplicable`].
pub fn combine_layered_authorization<F>(
    ingress: LayerEvaluation<IngressDecision>,
    requirement: LayerRequirement,
    detailed: F,
) -> LayerEvaluation<AuthorizationDecision>
where
    F: FnOnce() -> LayerEvaluation<DetailedDecision>,
{
    match ingress {
        Ok(IngressDecision::AllowToContinue) => match detailed() {
            Ok(DetailedDecision::Allow) => Ok(AuthorizationDecision::Allow),
            Ok(DetailedDecision::Deny) => Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied)),
            Ok(DetailedDecision::Abstain) if requirement == LayerRequirement::Optional => {
                Ok(AuthorizationDecision::Allow)
            }
            Ok(DetailedDecision::Abstain) => Ok(AuthorizationDecision::Deny(AuthorizationDenial::PolicyNotApplicable)),
            Err(failure) => Err(failure),
        },
        Ok(IngressDecision::Deny) => Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied)),
        Err(failure) => Err(failure),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    fn is_allow(decision: LayerEvaluation<AuthorizationDecision>) -> bool {
        matches!(decision, Ok(AuthorizationDecision::Allow))
    }

    #[test]
    fn complete_truth_table_is_fail_closed() {
        for ingress in [IngressDecision::AllowToContinue, IngressDecision::Deny] {
            for requirement in [LayerRequirement::Required, LayerRequirement::Optional] {
                for detailed in [
                    DetailedDecision::Allow,
                    DetailedDecision::Deny,
                    DetailedDecision::Abstain,
                ] {
                    let detailed_called = Cell::new(false);
                    let result = combine_layered_authorization(Ok(ingress), requirement, || {
                        detailed_called.set(true);
                        Ok(detailed)
                    });
                    let expected_allow = matches!(
                        (ingress, requirement, detailed),
                        (IngressDecision::AllowToContinue, _, DetailedDecision::Allow)
                            | (
                                IngressDecision::AllowToContinue,
                                LayerRequirement::Optional,
                                DetailedDecision::Abstain
                            )
                    );

                    assert_eq!(is_allow(result), expected_allow);
                    assert_eq!(detailed_called.get(), ingress == IngressDecision::AllowToContinue);
                }
            }
        }
    }

    #[test]
    fn coarse_deny_is_sticky_and_skips_detailed_evaluation() {
        let detailed_called = Cell::new(false);
        let result = combine_layered_authorization(Ok(IngressDecision::Deny), LayerRequirement::Optional, || {
            detailed_called.set(true);
            Ok(DetailedDecision::Allow)
        });

        assert_eq!(
            result,
            Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied))
        );
        assert!(!detailed_called.get());
    }

    #[test]
    fn every_layer_failure_is_preserved() {
        for failure in [
            LayerFailureKind::Unavailable,
            LayerFailureKind::Error,
            LayerFailureKind::Timeout,
        ] {
            assert_eq!(
                combine_layered_authorization(Err(failure), LayerRequirement::Optional, || {
                    Ok(DetailedDecision::Allow)
                }),
                Err(failure)
            );
            assert_eq!(
                combine_layered_authorization(
                    Ok(IngressDecision::AllowToContinue),
                    LayerRequirement::Optional,
                    || Err(failure),
                ),
                Err(failure)
            );
        }
    }

    #[test]
    fn detailed_deny_and_required_abstention_have_closed_reasons() {
        let denied =
            combine_layered_authorization(Ok(IngressDecision::AllowToContinue), LayerRequirement::Required, || {
                Ok(DetailedDecision::Deny)
            });
        let abstained =
            combine_layered_authorization(Ok(IngressDecision::AllowToContinue), LayerRequirement::Required, || {
                Ok(DetailedDecision::Abstain)
            });

        assert_eq!(
            denied,
            Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied))
        );
        assert_eq!(
            abstained,
            Ok(AuthorizationDecision::Deny(AuthorizationDenial::PolicyNotApplicable))
        );
    }

    #[test]
    fn every_denial_reason_is_closed_and_value_free() {
        let reasons = [
            AuthorizationDenial::SubjectUnknown,
            AuthorizationDenial::ResourceUnknown,
            AuthorizationDenial::PermissionDenied,
            AuthorizationDenial::PolicyNotApplicable,
            AuthorizationDenial::MaintenanceRestricted,
        ];

        for reason in reasons {
            let decision = AuthorizationDecision::Deny(reason);
            assert_eq!(decision, AuthorizationDecision::Deny(reason));
        }
    }
}
