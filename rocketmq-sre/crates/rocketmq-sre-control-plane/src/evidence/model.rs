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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::InvestigationId;
use serde::Deserialize;
use serde::Serialize;

use crate::ControlPlaneError;

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
        self.evidence
            .verify_content_hash()
            .map_err(|_| ControlPlaneError::validation("invalid_content_hash", "evidence content hash is invalid"))
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
