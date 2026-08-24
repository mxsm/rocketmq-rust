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

//! Narrow Broker inventory, Inspector, and generation-CAS mutation service.

use std::sync::Arc;

use rocketmq_dashboard_common::{
    BrokerConfigPatch, BrokerConfigSnapshot, BrokerIdentity, BrokerInventoryItem, RuntimeEntry, is_sensitive_key,
};

use crate::{
    infrastructure::admin_provider::{
        GpuiAdminProvider, SafeBrokerTarget, SafeConfigPatchOutcome, SafeConfigPatchRequest,
    },
    state::{UiError, UiErrorCode},
};

use super::dashboard::map_broker_inventory;

/// A cache entry invalidated after a successful Broker config mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrokerCacheInvalidation {
    /// Dashboard overview Broker evidence.
    DashboardOverview,
    /// Dashboard current Broker ranking.
    DashboardBrokerCurrent,
    /// Brokers inventory rows.
    BrokerInventory,
    /// Selected Broker runtime data.
    BrokerRuntime(BrokerIdentity),
    /// Selected Broker configuration data.
    BrokerConfig(BrokerIdentity),
}

/// Typed result of one generation-aware Broker config patch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrokerConfigMutationResult {
    /// Patch applied once and only related caches should refresh.
    Applied {
        /// Previous generation accepted by the backend.
        previous_generation: u64,
        /// Reloaded server truth after the patch.
        snapshot: BrokerConfigSnapshot,
        /// Explicit targeted invalidation intent.
        invalidations: Vec<BrokerCacheInvalidation>,
    },
    /// The patch was accepted, but reloading authoritative server state failed.
    AppliedReloadFailed {
        /// Previous generation accepted by the backend.
        previous_generation: u64,
        /// Generation reported by the mutation response, not treated as loaded truth.
        reported_generation: u64,
        /// Explicit targeted invalidation intent.
        invalidations: Vec<BrokerCacheInvalidation>,
        /// Safe reload failure retained for Retry.
        error: UiError,
    },
    /// The backend generation changed; the caller must keep its draft.
    GenerationConflict {
        /// Generation submitted by the caller.
        expected_generation: u64,
        /// Generation observed by the backend.
        actual_generation: u64,
    },
}

/// Real Broker service. Pages receive only protocol-independent common DTOs.
pub struct BrokerService {
    provider: Option<Arc<GpuiAdminProvider>>,
}

impl BrokerService {
    pub(crate) fn new(provider: Arc<GpuiAdminProvider>) -> Arc<Self> {
        Arc::new(Self {
            provider: Some(provider),
        })
    }

    pub(crate) fn unavailable() -> Arc<Self> {
        Arc::new(Self { provider: None })
    }

    /// Lists real inventory with complete identities.
    pub async fn inventory(&self, revision: u64) -> Result<Vec<BrokerInventoryItem>, UiError> {
        self.provider()?
            .list_brokers(revision)
            .await
            .map(|response| map_broker_inventory(&response))
            .map_err(query_error)
    }

    /// Loads sorted/redaction-aware runtime entries for one exact target.
    pub async fn runtime(&self, revision: u64, identity: BrokerIdentity) -> Result<Vec<RuntimeEntry>, UiError> {
        let response = self
            .provider()?
            .broker_runtime(revision, broker_target(&identity))
            .await
            .map_err(query_error)?;
        ensure_target(&identity, &response.broker_name, &response.address)?;
        Ok(response.entries)
    }

    /// Loads Broker config and its mutation generation as one inspector snapshot.
    pub async fn config(&self, revision: u64, identity: BrokerIdentity) -> Result<BrokerConfigSnapshot, UiError> {
        let provider = self.provider()?;
        let (config, generation) = tokio::try_join!(
            provider.broker_config(revision, broker_target(&identity)),
            provider.query_config_generation(revision, identity.address.clone()),
        )
        .map_err(query_error)?;
        ensure_target(&identity, &config.broker_name, &config.address)?;
        Ok(BrokerConfigSnapshot::new(
            identity,
            generation.generation,
            config.entries,
        ))
    }

    /// Submits one reviewed non-sensitive patch with generation CAS and no replay.
    pub async fn patch_config(
        &self,
        revision: u64,
        patch: BrokerConfigPatch,
    ) -> Result<BrokerConfigMutationResult, UiError> {
        if patch.entries().is_empty() {
            return Err(UiError::new(
                "No Broker configuration values changed.",
                UiErrorCode::Validation,
                false,
            ));
        }
        if patch.entries().keys().any(|key| is_sensitive_key(key)) {
            return Err(UiError::new(
                "Sensitive Broker configuration keys cannot be edited here.",
                UiErrorCode::Validation,
                false,
            ));
        }
        let identity = patch.identity.clone();
        let expected_generation = patch.expected_generation;
        let entries = patch.into_entries();
        let outcome = self
            .provider()?
            .patch_config_if_generation(
                revision,
                SafeConfigPatchRequest {
                    address: identity.address.clone(),
                    expected_generation,
                    entries,
                },
            )
            .await
            .map_err(mutation_error)?;
        Ok(match outcome {
            SafeConfigPatchOutcome::Applied {
                previous_generation,
                generation,
            } => {
                let invalidations = targeted_invalidations(identity.clone());
                match self.config(revision, identity).await {
                    Ok(snapshot) => BrokerConfigMutationResult::Applied {
                        previous_generation,
                        snapshot,
                        invalidations,
                    },
                    Err(error) => BrokerConfigMutationResult::AppliedReloadFailed {
                        previous_generation,
                        reported_generation: generation,
                        invalidations,
                        error,
                    },
                }
            }
            SafeConfigPatchOutcome::GenerationConflict {
                expected_generation,
                actual_generation,
            } => BrokerConfigMutationResult::GenerationConflict {
                expected_generation,
                actual_generation,
            },
        })
    }

    fn provider(&self) -> Result<&Arc<GpuiAdminProvider>, UiError> {
        self.provider.as_ref().ok_or_else(|| {
            UiError::new(
                "Broker data is unavailable in this application configuration.",
                UiErrorCode::CapabilityUnavailable,
                false,
            )
        })
    }
}

fn targeted_invalidations(identity: BrokerIdentity) -> Vec<BrokerCacheInvalidation> {
    vec![
        BrokerCacheInvalidation::DashboardOverview,
        BrokerCacheInvalidation::DashboardBrokerCurrent,
        BrokerCacheInvalidation::BrokerInventory,
        BrokerCacheInvalidation::BrokerRuntime(identity.clone()),
        BrokerCacheInvalidation::BrokerConfig(identity),
    ]
}

fn broker_target(identity: &BrokerIdentity) -> SafeBrokerTarget {
    SafeBrokerTarget {
        broker_name: identity.broker_name.clone(),
        address: identity.address.clone(),
    }
}

fn ensure_target(identity: &BrokerIdentity, broker_name: &str, address: &str) -> Result<(), UiError> {
    if identity.broker_name == broker_name && identity.address == address {
        Ok(())
    } else {
        Err(UiError::new(
            "The Broker response did not match the selected target.",
            UiErrorCode::Connection,
            true,
        ))
    }
}

fn query_error(_error: impl std::fmt::Display) -> UiError {
    UiError::new(
        "Unable to load Broker data from the selected connection.",
        UiErrorCode::Connection,
        true,
    )
}

fn mutation_error(_error: impl std::fmt::Display) -> UiError {
    UiError::new("Unable to update Broker configuration.", UiErrorCode::Connection, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> BrokerIdentity {
        BrokerIdentity {
            cluster: "cluster-a".into(),
            broker_name: "broker-a".into(),
            broker_id: 0,
            address: "127.0.0.1:10911".into(),
        }
    }

    #[test]
    fn successful_mutation_invalidates_only_related_dashboard_and_broker_resources() {
        let invalidations = targeted_invalidations(identity());
        assert_eq!(invalidations.len(), 5);
        assert!(invalidations.contains(&BrokerCacheInvalidation::DashboardOverview));
        assert!(invalidations.contains(&BrokerCacheInvalidation::DashboardBrokerCurrent));
        assert!(invalidations.contains(&BrokerCacheInvalidation::BrokerInventory));
        assert!(!format!("{invalidations:?}").contains("History"));
        assert!(!format!("{invalidations:?}").contains("Topic"));
    }

    #[test]
    fn target_validation_uses_broker_name_and_address() {
        assert!(ensure_target(&identity(), "broker-a", "127.0.0.1:10911").is_ok());
        assert!(ensure_target(&identity(), "broker-a", "127.0.0.1:20911").is_err());
        assert!(ensure_target(&identity(), "broker-b", "127.0.0.1:10911").is_err());
    }
}
