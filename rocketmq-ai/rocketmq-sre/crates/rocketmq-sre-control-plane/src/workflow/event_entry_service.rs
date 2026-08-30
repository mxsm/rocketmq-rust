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

use rocketmq_sre_contracts::CorrelationId;
use serde_json::json;

use super::UnifiedEventEntryRequest;
use super::UnifiedEventEntryResult;
use super::WorkflowService;
use super::WorkflowStreamEvent;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::alerting::AlertingService;
use crate::auth::AuthContext;

/// Authenticated, versioned facade that maps five external source classes to
/// the three workflow aggregates they are permitted to create.
#[derive(Clone)]
pub(crate) struct UnifiedEventEntryService {
    repository: PostgresRepository,
    workflow: WorkflowService,
    alerting: AlertingService,
}

impl UnifiedEventEntryService {
    pub(crate) fn new(repository: PostgresRepository, workflow: WorkflowService, alerting: AlertingService) -> Self {
        Self {
            repository,
            workflow,
            alerting,
        }
    }

    pub(crate) async fn ingest(
        &self,
        auth: &AuthContext,
        request: &UnifiedEventEntryRequest,
        correlation_id: CorrelationId,
    ) -> Result<UnifiedEventEntryResult, ControlPlaneError> {
        request.validate()?;
        authorize_cluster(auth, request.cluster_id)?;
        let request_hash = request.request_hash()?;
        if let Some(existing) = self.repository.event_entry(auth, request, &request_hash).await? {
            return Ok(existing);
        }

        let result = if let Some(alert_request) = request.alert_request() {
            let outcome = self
                .alerting
                .ingest_unified_alert_event(auth, &alert_request, correlation_id)
                .await?;
            self.repository
                .record_alert_event_entry(auth, request, &request_hash, outcome.incident_id, correlation_id)
                .await?
        } else {
            self.repository
                .create_non_alert_event_entry(auth, request, &request_hash, correlation_id)
                .await?
        };

        if result.created {
            self.workflow.publish_external(WorkflowStreamEvent {
                tenant_id: auth.tenant_id,
                cluster_id: request.cluster_id,
                aggregate_type: result.target_kind.as_str(),
                aggregate_id: result.target_id.to_string(),
                event_type: "event_entry_created",
                payload: json!({
                    "entry_id": result.entry_id,
                    "source_kind": result.source_kind,
                    "target_kind": result.target_kind,
                    "target_id": result.target_id,
                }),
                correlation_id: result.correlation_id,
                occurred_at: result.accepted_at,
            });
        }
        Ok(result)
    }
}

fn authorize_cluster(
    auth: &AuthContext,
    cluster_id: rocketmq_sre_contracts::ClusterId,
) -> Result<(), ControlPlaneError> {
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "event entry cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    #[test]
    fn event_entry_rejects_a_cluster_outside_the_identity_scope() {
        let allowed = ClusterId::new();
        let auth = AuthContext {
            tenant_id: TenantId::new(),
            subject: "event-entry-test".to_owned(),
            clusters: BTreeSet::from([allowed]),
            roles: BTreeSet::new(),
        };
        let error = authorize_cluster(&auth, ClusterId::new()).expect_err("cross-cluster entry must fail");
        assert!(matches!(
            error,
            ControlPlaneError::Forbidden {
                code: "cluster_not_allowed",
                ..
            }
        ));
    }
}
