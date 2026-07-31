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

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::ContractError;
use rocketmq_sre_contracts::Incident;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IncidentTransition;

/// In-memory aggregate coordinator used before the persistent repository lands.
#[derive(Debug, Default)]
pub struct IncidentManager {
    incidents: BTreeMap<IncidentId, Incident>,
}

/// Failures exposed by the incident coordinator.
#[derive(Debug)]
pub enum IncidentManagerError {
    NotFound(IncidentId),
    Contract(ContractError),
}

impl fmt::Display for IncidentManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(id) => write!(formatter, "incident `{id}` does not exist"),
            Self::Contract(error) => error.fmt(formatter),
        }
    }
}

impl Error for IncidentManagerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NotFound(_) => None,
            Self::Contract(error) => Some(error),
        }
    }
}

impl From<ContractError> for IncidentManagerError {
    fn from(error: ContractError) -> Self {
        Self::Contract(error)
    }
}

impl IncidentManager {
    /// Adds a new aggregate.
    ///
    /// The caller owns construction, which keeps time and identity explicit in
    /// tests and future persistence adapters.
    pub fn create(&mut self, incident: Incident) -> &Incident {
        let id = incident.id;
        self.incidents.entry(id).or_insert(incident)
    }

    /// Reads one aggregate without exposing mutable storage.
    #[must_use]
    pub fn get(&self, id: IncidentId) -> Option<&Incident> {
        self.incidents.get(&id)
    }

    /// Applies one validated lifecycle transition.
    ///
    /// # Errors
    ///
    /// Returns [`IncidentManagerError::NotFound`] when the aggregate is
    /// unknown, or a contract error when the transition is invalid.
    pub fn transition(
        &mut self,
        id: IncidentId,
        transition: IncidentTransition,
    ) -> Result<&Incident, IncidentManagerError> {
        let incident = self.incidents.get_mut(&id).ok_or(IncidentManagerError::NotFound(id))?;
        incident.apply_transition(transition)?;
        Ok(incident)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::IncidentStatus;
    use rocketmq_sre_contracts::IncidentTransition;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    #[test]
    fn coordinates_incident_transition_without_storage_coupling() {
        let now = Utc
            .with_ymd_and_hms(2026, 7, 26, 1, 0, 0)
            .single()
            .expect("timestamp should be valid");
        let incident = Incident::new(TenantId::new(), ClusterId::new(), "lag spike", now);
        let id = incident.id;
        let mut manager = IncidentManager::default();
        manager.create(incident);

        let updated = manager
            .transition(
                id,
                IncidentTransition {
                    next: IncidentStatus::Collecting,
                    at: now,
                },
            )
            .expect("transition should be valid");

        assert_eq!(updated.status, IncidentStatus::Collecting);
    }
}
