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

//! Built-in read-only diagnostic packs.

mod broker_ha;
mod broker_health_v1;
mod catalog;
mod client_message;
mod cluster_topology_v1;
mod common;
mod consumer_lag_v2;
mod consumer_runtime_v1;
mod deployment_drift_v1;
mod message_path_v1;
mod prevention;
mod producer_connectivity_v1;
mod routing_proxy;
mod security_runtime;
mod storage;
mod telemetry_pipeline_v1;

pub use broker_health_v1::BrokerHealthV1;
pub use cluster_topology_v1::ClusterTopologyV1;
pub use consumer_lag_v2::ConsumerLagV2;
pub use consumer_runtime_v1::ConsumerRuntimeV1;
pub use deployment_drift_v1::DeploymentDriftV1;
pub use message_path_v1::MessagePathV1;
pub use producer_connectivity_v1::ProducerConnectivityV1;
pub use telemetry_pipeline_v1::TelemetryPipelineV1;

use super::DiagnosticPackRegistry;
use super::DiagnosticRegistryError;

const WAVE_A_IDS: [&str; 8] = [
    "cluster-topology.v1",
    "consumer-lag.v2",
    "consumer-runtime.v1",
    "producer-connectivity.v1",
    "broker-health.v1",
    "message-path.v1",
    "telemetry-pipeline.v1",
    "deployment-drift.v1",
];

/// Builds the complete eight-pack Wave A registry.
///
/// # Errors
///
/// Returns a registry validation error if a built-in descriptor violates the
/// same constraints enforced for external packs.
pub fn wave_a_registry() -> Result<DiagnosticPackRegistry, DiagnosticRegistryError> {
    let mut registry = DiagnosticPackRegistry::default();
    register_wave_a(&mut registry)?;
    Ok(registry)
}

/// Builds the 18-pack Wave B registry.
///
/// # Errors
///
/// Returns a registry validation error if any compiled pack descriptor is
/// incomplete or ambiguous.
pub fn wave_b_registry() -> Result<DiagnosticPackRegistry, DiagnosticRegistryError> {
    let mut registry = DiagnosticPackRegistry::default();
    register_specs(&mut registry, wave_b_specs())?;
    Ok(registry)
}

/// Builds the six-pack Wave C registry.
///
/// # Errors
///
/// Returns a registry validation error if any compiled pack descriptor is
/// incomplete or ambiguous.
pub fn wave_c_registry() -> Result<DiagnosticPackRegistry, DiagnosticRegistryError> {
    let mut registry = DiagnosticPackRegistry::default();
    register_specs(&mut registry, prevention::specs())?;
    Ok(registry)
}

/// Builds the complete Wave A, B, and C registry.
///
/// # Errors
///
/// Returns a registry validation error if any built-in descriptor violates
/// the same constraints enforced for external packs.
pub fn full_registry() -> Result<DiagnosticPackRegistry, DiagnosticRegistryError> {
    let mut registry = DiagnosticPackRegistry::default();
    register_wave_a(&mut registry)?;
    register_specs(&mut registry, wave_b_specs())?;
    register_specs(&mut registry, prevention::specs())?;
    Ok(registry)
}

/// Stable major-qualified IDs of all built-in packs.
#[must_use]
pub fn full_pack_ids() -> Vec<String> {
    WAVE_A_IDS
        .iter()
        .map(|id| (*id).to_owned())
        .chain(
            wave_b_specs()
                .iter()
                .chain(prevention::specs())
                .map(|spec| format!("{}.v1", spec.id)),
        )
        .collect()
}

fn register_wave_a(registry: &mut DiagnosticPackRegistry) -> Result<(), DiagnosticRegistryError> {
    registry.register(ClusterTopologyV1)?;
    registry.register(ConsumerLagV2)?;
    registry.register(ConsumerRuntimeV1)?;
    registry.register(ProducerConnectivityV1)?;
    registry.register(BrokerHealthV1)?;
    registry.register(MessagePathV1)?;
    registry.register(TelemetryPipelineV1)?;
    registry.register(DeploymentDriftV1)?;
    Ok(())
}

fn wave_b_specs() -> &'static [&'static catalog::PackSpec] {
    const SPECS: &[&catalog::PackSpec] = &[
        &storage::STORE_PRESSURE,
        &storage::STORE_INTEGRITY,
        &storage::ROCKSDB_HEALTH,
        &storage::TIERED_STORE,
        &broker_ha::BROKER_HA,
        &broker_ha::CONTROLLER_HA,
        &broker_ha::NAMESRV_ROUTE,
        &routing_proxy::SEND_LATENCY,
        &routing_proxy::PROXY_CONNECTIVITY,
        &routing_proxy::STATIC_TOPIC_ROUTE,
        &routing_proxy::TOPIC_SUBSCRIPTION_CONFIG,
        &client_message::RETRY_DLQ,
        &client_message::TRANSACTION_MESSAGE,
        &client_message::POP_REVIVE,
        &client_message::TIMER_BACKLOG,
        &client_message::QUEUE_HOTSPOT,
        &security_runtime::AUTH_FAILURE,
        &security_runtime::RUNTIME_SATURATION,
    ];
    SPECS
}

fn register_specs(
    registry: &mut DiagnosticPackRegistry,
    specs: &'static [&'static catalog::PackSpec],
) -> Result<(), DiagnosticRegistryError> {
    for spec in specs {
        registry.register(catalog::CatalogPack::new(spec))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wave_a_registry_contains_all_major_qualified_packs() {
        let registry = wave_a_registry().expect("built-in descriptors should be valid");
        let expected = [
            "cluster-topology.v1",
            "consumer-lag.v2",
            "consumer-runtime.v1",
            "producer-connectivity.v1",
            "broker-health.v1",
            "message-path.v1",
            "telemetry-pipeline.v1",
            "deployment-drift.v1",
        ];

        assert_eq!(registry.len(), expected.len());
        for id in expected {
            assert!(registry.resolve(id).is_some(), "{id} should be registered");
        }
    }

    #[test]
    fn full_registry_contains_all_32_unique_packs() {
        let registry = full_registry().expect("built-in descriptors should be valid");
        let ids = full_pack_ids();

        assert_eq!(registry.len(), 32);
        assert_eq!(ids.len(), 32);
        for id in ids {
            assert!(registry.resolve(&id).is_some(), "{id} should be registered");
        }
    }
}
