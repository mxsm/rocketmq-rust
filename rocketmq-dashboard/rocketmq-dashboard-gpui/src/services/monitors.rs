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

//! Owned local rule operations. The canonical commit includes the terminal audit event.

use rocketmq_dashboard_common::{
    AuditAction, AuditFacts, AuditOutcome, MonitorMutationOutcome, MonitorRuleChange, MonitorRuleMutation,
    MonitorRulesSnapshot, OperationResource,
};

use super::{
    AppServices,
    audit::{MutationAudit, rejected_conflict},
    config_ui_error,
};
use crate::state::{UiError, UiErrorCode};

impl MutationAudit for MonitorMutationOutcome {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        match self {
            Self::Saved(_) | Self::Deleted => (AuditOutcome::Succeeded, AuditFacts::default()),
            Self::Conflict { .. } => rejected_conflict(),
        }
    }
}

#[cfg(test)]
#[path = "monitors_tests.rs"]
pub(crate) mod tests;

fn unavailable() -> UiError {
    UiError::new(
        "Local Monitor rule storage is unavailable.",
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}

impl AppServices {
    pub async fn monitor_rules(&self, revision: u64) -> Result<MonitorRulesSnapshot, UiError> {
        let store = self.monitors.clone().ok_or_else(unavailable)?;
        let scope = store.scope(revision).map_err(config_ui_error)?;
        let runtime = self.runtime_bridge.as_ref().ok_or_else(unavailable)?;
        runtime
            .run("gpui-monitor-rules", async move {
                store.snapshot(scope).await.map_err(config_ui_error)
            })
            .await
    }

    pub async fn mutate_monitor_rule(
        &self,
        revision: u64,
        request: MonitorRuleMutation,
    ) -> Result<MonitorMutationOutcome, UiError> {
        request.validate().map_err(|source| {
            UiError::caused_by("The Monitor rule is invalid.", UiErrorCode::Validation, false, source)
        })?;
        let store = self.monitors.clone().ok_or_else(unavailable)?;
        let action = match &request.change {
            MonitorRuleChange::Save { .. } => AuditAction::MonitorSaved,
            MonitorRuleChange::Delete => AuditAction::MonitorDeleted,
        };
        let operation = self.accept_operation(
            revision,
            action,
            OperationResource::Monitor(request.consumer_group.clone()),
        )?;
        let context = operation.as_ref().ok_or_else(unavailable)?.context.clone();
        if context.environment_id != request.environment_id {
            return Err(UiError::new(
                "The environment changed. Reload Monitor rules before saving.",
                UiErrorCode::Connection,
                true,
            ));
        }
        self.run_mutation("gpui-monitor-rule-commit", operation, async move {
            store.commit(request, context).await.map_err(config_ui_error)
        })
        .await
    }
}
