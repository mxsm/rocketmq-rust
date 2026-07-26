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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ClusterId;
use crate::ContractError;
use crate::Hypothesis;
use crate::IncidentId;
use crate::TenantId;

/// Phase 00 incident lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentStatus {
    New,
    Collecting,
    Diagnosing,
    NeedsEvidence,
    Monitoring,
    Resolved,
    Escalated,
}

/// Explicit, deterministic request to advance an incident lifecycle.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct IncidentTransition {
    pub next: IncidentStatus,
    pub at: DateTime<Utc>,
}

impl IncidentStatus {
    /// Returns true when no further transitions are allowed.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Resolved | Self::Escalated)
    }

    const fn permits(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::New, Self::Collecting | Self::Escalated)
                | (
                    Self::Collecting,
                    Self::Diagnosing | Self::NeedsEvidence | Self::Escalated
                )
                | (
                    Self::Diagnosing,
                    Self::NeedsEvidence | Self::Monitoring | Self::Resolved | Self::Escalated
                )
                | (
                    Self::NeedsEvidence,
                    Self::Collecting | Self::Diagnosing | Self::Escalated
                )
                | (
                    Self::Monitoring,
                    Self::Collecting | Self::Diagnosing | Self::Resolved | Self::Escalated
                )
        )
    }
}

/// Versioned incident aggregate used by the diagnosis workflow.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Incident {
    pub id: IncidentId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub title: String,
    pub status: IncidentStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hypotheses: Vec<Hypothesis>,
}

impl Incident {
    /// Creates an incident in the new state.
    #[must_use]
    pub fn new(
        tenant_id: TenantId,
        cluster_id: ClusterId,
        title: impl Into<String>,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: IncidentId::new(),
            tenant_id,
            cluster_id,
            title: title.into(),
            status: IncidentStatus::New,
            created_at,
            updated_at: created_at,
            hypotheses: Vec::new(),
        }
    }

    /// Applies one valid lifecycle transition.
    ///
    /// # Errors
    ///
    /// Returns [`ContractError::InvalidStateTransition`] for skipped stages,
    /// self-transitions, or any attempt to reopen a terminal incident.
    pub fn transition(&mut self, next: IncidentStatus, at: DateTime<Utc>) -> Result<(), ContractError> {
        if !self.status.permits(next) {
            return Err(ContractError::InvalidStateTransition {
                from: format!("{:?}", self.status).to_lowercase(),
                to: format!("{next:?}").to_lowercase(),
            });
        }
        self.status = next;
        self.updated_at = at;
        Ok(())
    }

    /// Applies a serialized transition request.
    ///
    /// # Errors
    ///
    /// Returns the same state-machine error as [`Self::transition`].
    pub fn apply_transition(&mut self, transition: IncidentTransition) -> Result<(), ContractError> {
        self.transition(transition.next, transition.at)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn incident() -> Incident {
        let now = Utc
            .with_ymd_and_hms(2026, 7, 26, 1, 0, 0)
            .single()
            .expect("timestamp should be valid");
        Incident::new(TenantId::new(), ClusterId::new(), "lag spike", now)
    }

    #[test]
    fn follows_supported_diagnosis_lifecycle() {
        let mut incident = incident();
        let at = incident.created_at;

        incident
            .transition(IncidentStatus::Collecting, at)
            .expect("new incident can start collecting");
        incident
            .transition(IncidentStatus::Diagnosing, at)
            .expect("collection can enter diagnosis");
        incident
            .transition(IncidentStatus::NeedsEvidence, at)
            .expect("diagnosis can request more evidence");
        incident
            .transition(IncidentStatus::Collecting, at)
            .expect("missing evidence can restart collection");
        incident
            .transition(IncidentStatus::Diagnosing, at)
            .expect("collection can re-enter diagnosis");
        incident
            .transition(IncidentStatus::Monitoring, at)
            .expect("diagnosis can enter monitoring");
        incident
            .transition(IncidentStatus::Resolved, at)
            .expect("monitoring can resolve");

        assert_eq!(incident.status, IncidentStatus::Resolved);
    }

    #[test]
    fn terminal_incidents_cannot_reopen() {
        let mut incident = incident();
        let at = incident.created_at;
        incident
            .transition(IncidentStatus::Escalated, at)
            .expect("new incident can escalate");

        assert!(matches!(
            incident.transition(IncidentStatus::Collecting, at),
            Err(ContractError::InvalidStateTransition { .. })
        ));
    }

    #[test]
    fn skipped_stages_are_rejected() {
        let mut incident = incident();
        let at = incident.created_at;

        assert!(matches!(
            incident.transition(IncidentStatus::Diagnosing, at),
            Err(ContractError::InvalidStateTransition { .. })
        ));
    }
}
