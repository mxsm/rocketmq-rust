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

use crate::Decision;
use crate::SecurityRequestView;

/// Stable, non-sensitive reason used when the layered pipeline rejects a request.
///
/// Layer implementations must not substitute credential, signature, token, header,
/// or body data into this value.
pub const LAYERED_AUTHORIZATION_DENIED_REASON: &str = "authorization denied";

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
/// `Abstain` is intentionally local to the layered contract. It is not a
/// variant of the legacy public [`Decision`] type and is resolved only by
/// [`combine_layered_authorization`].
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
    /// An explicit compatibility boundary permits `Abstain`.
    ///
    /// This variant is only for a deliberately disabled compatibility layer. It
    /// must not be used to convert an unavailable layer or any other failure
    /// into an allow.
    Optional,
}

/// The result returned by one authorization layer.
///
/// Every [`Err`] value is denied by [`combine_layered_authorization`].
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
/// detailed failure, or a detailed deny always produces the fixed public denial.
/// A detailed abstention is allowed only at an explicit [`LayerRequirement::Optional`]
/// compatibility boundary; required authorization fails closed.
pub fn combine_layered_authorization<F>(
    ingress: LayerEvaluation<IngressDecision>,
    requirement: LayerRequirement,
    detailed: F,
) -> Decision
where
    F: FnOnce() -> LayerEvaluation<DetailedDecision>,
{
    match ingress {
        Ok(IngressDecision::AllowToContinue) => match detailed() {
            Ok(DetailedDecision::Allow) => Decision::Allow,
            Ok(DetailedDecision::Abstain) if requirement == LayerRequirement::Optional => Decision::Allow,
            Ok(DetailedDecision::Deny | DetailedDecision::Abstain) | Err(_) => layered_deny(),
        },
        Ok(IngressDecision::Deny) | Err(_) => layered_deny(),
    }
}

fn layered_deny() -> Decision {
    Decision::deny(LAYERED_AUTHORIZATION_DENIED_REASON)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    fn is_allow(decision: Decision) -> bool {
        matches!(decision, Decision::Allow)
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

        assert!(matches!(result, Decision::Deny { .. }));
        assert!(!detailed_called.get());
    }

    #[test]
    fn every_layer_failure_is_denied() {
        for failure in [
            LayerFailureKind::Unavailable,
            LayerFailureKind::Error,
            LayerFailureKind::Timeout,
        ] {
            assert!(matches!(
                combine_layered_authorization(Err(failure), LayerRequirement::Optional, || {
                    Ok(DetailedDecision::Allow)
                }),
                Decision::Deny { .. }
            ));
            assert!(matches!(
                combine_layered_authorization(
                    Ok(IngressDecision::AllowToContinue),
                    LayerRequirement::Optional,
                    || Err(failure),
                ),
                Decision::Deny { .. }
            ));
        }
    }

    #[test]
    fn denial_reason_is_fixed_and_redaction_safe() {
        let result =
            combine_layered_authorization(Ok(IngressDecision::AllowToContinue), LayerRequirement::Required, || {
                Ok(DetailedDecision::Deny)
            });

        assert_eq!(result, Decision::deny(LAYERED_AUTHORIZATION_DENIED_REASON));
    }
}
