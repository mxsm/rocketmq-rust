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

//! Serves `rocketmq_get_mutation_result` from what the audit trail remembers.
//!
//! A lookup is not a mutation. It needs no mutation runtime, so it keeps working after mutations
//! are switched off; it opens no session and writes no durable audit record.

use super::admission::LookupBudget;
use crate::audit::AuditTrail;
use crate::audit::InvocationQuery;
use crate::error::ControlError;
use crate::guard::AuthorizedResultLookup;
use crate::tools::GetMutationResultArgs;
use crate::tools::MutationResultToolResponse;

pub(crate) struct ResultLookup {
    audit: AuditTrail,
    budget: LookupBudget,
}

impl ResultLookup {
    /// `lookups_per_minute` is the budget of one principal.
    pub(crate) fn new(audit: AuditTrail, lookups_per_minute: u32) -> Self {
        Self {
            audit,
            budget: LookupBudget::new(lookups_per_minute),
        }
    }

    /// Charges one lookup to the authorized caller.
    ///
    /// # Errors
    ///
    /// Returns `rate_limited` when the caller's budget is spent.
    pub(crate) fn admit(&self, authorized: &AuthorizedResultLookup) -> Result<(), ControlError> {
        self.budget.take(authorized.operator())
    }

    /// Answers how the selected invocation of the authorized caller ended.
    ///
    /// Only the caller's own invocations on the authorized cluster are visible. The request key is
    /// compared by its digest, the form the audit trail keeps.
    ///
    /// # Errors
    ///
    /// Returns `audit_unavailable` when the trail no longer knows how its invocations ended.
    pub(crate) async fn find(
        &self,
        authorized: &AuthorizedResultLookup,
        args: &GetMutationResultArgs,
    ) -> Result<MutationResultToolResponse, ControlError> {
        let query = InvocationQuery {
            invocation_id: args.invocation_id,
            request_key_digest: args
                .request_key
                .as_deref()
                .map(|key| crate::audit::sha256_hex(key.as_bytes())),
        };
        let recorded = self
            .audit
            .lookup(authorized.operator(), authorized.cluster(), &query)
            .await?;
        Ok(MutationResultToolResponse::new(authorized.cluster().as_str(), recorded))
    }
}
