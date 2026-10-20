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

//! Binds an execution to the plan that a dry run reported.
//!
//! A dry run answers with the digest of its plan. An execution that names such a digest runs its
//! own preflight, digests the plan it would report as a dry run, and writes only when the two
//! digests are equal. Both sides use [`plan_digest`], so they cannot drift apart.

use serde_json::Map;
use serde_json::Value;

use super::MutationToolRequest;
use super::MutationToolResponse;
use crate::error::ControlError;

/// The members of a dry-run response that state its plan: what was asked of which target, the
/// state that was read, and what each target would get.
///
/// The other members say how the call went, so two dry runs of an unchanged state agree on the
/// plan whatever else differs between them.
const PLAN_MEMBERS: [&str; 6] = ["operation", "cluster", "target", "requested", "before", "targets"];

/// Members of the state of a Proxy that count the work it holds at one moment.
///
/// They move while a Proxy serves, whatever is planned for it, so a plan leaves them out. The
/// phase of the Proxy, its holder and the gates that a drain closes stay in.
const PROXY_WORK_MEMBERS: [&str; 2] = ["zero_pending", "pending"];

/// Digests the plan that `planned`, the response of a dry run, states for a request.
///
/// `requested_digest` is the digest of what the request asks for, the one its audit records
/// carry. It adds what a response does not repeat, such as the identifier of an operation-bound
/// call, so a plan made for one request is not the plan of another.
///
/// # Errors
///
/// Returns `execution_failed` when `planned` does not state a whole plan.
pub(super) fn plan_digest(requested_digest: &str, planned: &MutationToolResponse) -> Result<String, ControlError> {
    let Ok(Value::Object(mut response)) = serde_json::to_value(planned) else {
        return Err(ControlError::execution_failed());
    };
    let mut plan = Map::new();
    for member in PLAN_MEMBERS {
        let stated = response.remove(member).ok_or_else(ControlError::execution_failed)?;
        plan.insert(member.to_owned(), stated);
    }
    if matches!(planned, MutationToolResponse::ProxyDrain(_)) {
        forget_proxy_work(&mut plan);
    }
    let bound = serde_json::json!({ "requested_digest": requested_digest, "plan": plan });
    Ok(crate::audit::digest_canonical_json(&bound))
}

/// Drops the work counters from every Proxy state that `plan` holds.
fn forget_proxy_work(plan: &mut Map<String, Value>) {
    let forget = |state: &mut Value| {
        if let Value::Object(state) = state {
            state.retain(|member, _| !PROXY_WORK_MEMBERS.contains(&member.as_str()));
        }
    };
    if let Some(Value::Object(before)) = plan.get_mut("before") {
        before.values_mut().for_each(forget);
    }
    if let Some(Value::Array(targets)) = plan.get_mut("targets") {
        targets
            .iter_mut()
            .filter_map(|target| target.get_mut("before"))
            .for_each(forget);
    }
}

/// What an execution expects of the plan it is about to write.
pub(crate) struct PlanBinding(Option<ExpectedPlan>);

struct ExpectedPlan {
    requested_digest: String,
    plan_digest: String,
}

impl PlanBinding {
    /// Reads the plan that `request` expects to find.
    ///
    /// A dry run expects none: it reports a plan and writes nothing. Neither does an execution
    /// that names no plan, which runs on the state it reads.
    ///
    /// # Errors
    ///
    /// Returns `invalid_argument` when what the request asks for cannot be digested.
    pub(crate) fn of(request: &MutationToolRequest) -> Result<Self, ControlError> {
        let Some(expected) = request.expected_plan_digest().filter(|_| !request.dry_run()) else {
            return Ok(Self(None));
        };
        Ok(Self(Some(ExpectedPlan {
            requested_digest: request.requested_digest()?,
            plan_digest: expected.to_owned(),
        })))
    }

    /// A binding that expects no plan, for a run that is not made from a whole request.
    #[cfg(test)]
    pub(crate) const fn unbound() -> Self {
        Self(None)
    }

    /// Lets an execution write when it expects no plan, or the plan that `planned` states.
    ///
    /// `planned` builds the response that a dry run would return for the state this execution
    /// has just read. Call this after the preflight and before the first write.
    ///
    /// # Errors
    ///
    /// Returns `precondition_conflict` when the execution expects another plan. Nothing has been
    /// written then.
    pub(crate) fn admit(&self, planned: impl FnOnce() -> MutationToolResponse) -> Result<(), ControlError> {
        let Some(expected) = &self.0 else {
            return Ok(());
        };
        if plan_digest(&expected.requested_digest, &planned())? == expected.plan_digest {
            Ok(())
        } else {
            Err(ControlError::precondition_conflict())
        }
    }
}

#[cfg(test)]
#[path = "plan_binding/tests.rs"]
mod tests;
