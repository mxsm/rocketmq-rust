// Copyright 2023 The RocketMQ Rust Authors
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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ContractError;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::InvestigationId;
use serde::Deserialize;
use serde::Serialize;

use crate::ControlPlaneError;

fn validate_schema(evidence: &EvidenceSnapshot) -> Result<(), ControlPlaneError> {
    let supported = rocketmq_sre_contracts::current_evidence_schema();
    evidence
        .schema
        .ensure_compatible(&supported.family, supported.major, &BTreeSet::new())
        .map_err(|error| match error {
            ContractError::UnsupportedSchemaFamily { .. } => {
                ControlPlaneError::validation("unsupported_schema_family", "evidence schema family is unsupported")
            }
            ContractError::UnsupportedSchemaMajor { .. } => {
                ControlPlaneError::validation("unsupported_schema_major", "evidence schema major is unsupported")
            }
            ContractError::MissingRequiredFeature { .. } => {
                ControlPlaneError::validation("missing_required_feature", "evidence requires an unsupported feature")
            }
            ContractError::InvalidTimeRange
            | ContractError::InvalidContentHash
            | ContractError::InvalidStateTransition { .. }
            | ContractError::InvalidDescriptor { .. } => {
                ControlPlaneError::validation("invalid_request", "evidence schema is invalid")
            }
        })
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PersistEvidenceRequest {
    pub investigation_id: Option<InvestigationId>,
    pub incident_id: Option<IncidentId>,
    pub evidence: EvidenceSnapshot,
}

impl PersistEvidenceRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        if self.investigation_id.is_none() && self.incident_id.is_none() {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "evidence must be attached to an investigation or incident",
            ));
        }
        validate_schema(&self.evidence)?;
        self.evidence
            .verify_content_hash()
            .map_err(|_| ControlPlaneError::validation("invalid_content_hash", "evidence content hash is invalid"))
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::EvidenceContent;
    use rocketmq_sre_contracts::EvidenceQuery;
    use rocketmq_sre_contracts::QueryId;
    use rocketmq_sre_contracts::SchemaVersion;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::TimeRange;
    use serde_json::json;

    use super::*;

    fn request_with_schema(schema: SchemaVersion) -> PersistEvidenceRequest {
        let at = Utc::now();
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            source: "qualification".to_owned(),
            resource: "diagnostic/fixture".to_owned(),
            time_range: TimeRange::new(at, at).expect("valid time range"),
        };
        let evidence = EvidenceSnapshot::capture(query, schema, at, EvidenceContent::Inline(json!({"ok": true})))
            .expect("fixture should seal");
        PersistEvidenceRequest {
            investigation_id: Some(InvestigationId::new()),
            incident_id: None,
            evidence,
        }
    }

    #[test]
    fn persistence_rejects_unknown_schema_major() {
        let request = request_with_schema(SchemaVersion::new("rocketmq-sre.evidence", 99, 0));

        assert!(matches!(
            request.validate(),
            Err(ControlPlaneError::Validation {
                code: "unsupported_schema_major",
                ..
            })
        ));
    }

    #[test]
    fn persistence_rejects_unknown_required_feature() {
        let request = request_with_schema(
            rocketmq_sre_contracts::current_evidence_schema().requiring(["unknown-qualification-feature"]),
        );

        assert!(matches!(
            request.validate(),
            Err(ControlPlaneError::Validation {
                code: "missing_required_feature",
                ..
            })
        ));
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct EvidenceListQuery {
    pub cluster_id: ClusterId,
    pub incident_id: Option<IncidentId>,
    pub source: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl EvidenceListQuery {
    pub(crate) fn bounded_limit(&self) -> Result<u32, ControlPlaneError> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=200).contains(&limit) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "evidence page limit must be between 1 and 200",
            ));
        }
        Ok(limit)
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct EvidencePage {
    pub items: Vec<EvidenceSnapshot>,
    pub next_cursor: Option<String>,
    pub partial: bool,
}
