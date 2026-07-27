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

//! Built-in Wave A read-only diagnostic packs.

mod broker_health_v1;
mod cluster_topology_v1;
mod common;
mod consumer_lag_v2;
mod consumer_runtime_v1;
mod deployment_drift_v1;
mod message_path_v1;
mod producer_connectivity_v1;
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

/// Builds the complete eight-pack Wave A registry.
///
/// # Errors
///
/// Returns a registry validation error if a built-in descriptor violates the
/// same constraints enforced for external packs.
pub fn wave_a_registry() -> Result<DiagnosticPackRegistry, DiagnosticRegistryError> {
    let mut registry = DiagnosticPackRegistry::default();
    registry.register(ClusterTopologyV1)?;
    registry.register(ConsumerLagV2)?;
    registry.register(ConsumerRuntimeV1)?;
    registry.register(ProducerConnectivityV1)?;
    registry.register(BrokerHealthV1)?;
    registry.register(MessagePathV1)?;
    registry.register(TelemetryPipelineV1)?;
    registry.register(DeploymentDriftV1)?;
    Ok(registry)
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
}
