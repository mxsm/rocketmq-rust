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

use rocketmq_sre_contracts::AlertSource;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ResourceKind;
use rocketmq_sre_contracts::TenantId;

/// Canonical, length-delimited input for a cryptographic alert fingerprint.
///
/// Hashing is intentionally left to the owning application so this core crate
/// keeps its contracts-only dependency boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelationFingerprintMaterial {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    source: AlertSource,
    source_identity: String,
    resource_kind: ResourceKind,
    resource_key: String,
    symptom_family: String,
}

impl CorrelationFingerprintMaterial {
    /// Creates canonical fingerprint material from already-sanitized fields.
    #[must_use]
    pub fn new(
        tenant_id: TenantId,
        cluster_id: ClusterId,
        source: AlertSource,
        source_identity: impl Into<String>,
        resource_kind: ResourceKind,
        resource_key: impl Into<String>,
        symptom_family: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id,
            cluster_id,
            source,
            source_identity: source_identity.into(),
            resource_kind,
            resource_key: resource_key.into(),
            symptom_family: symptom_family.into(),
        }
    }

    /// Returns a collision-resistant framing for the caller to hash.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let fields = [
            self.tenant_id.to_string(),
            self.cluster_id.to_string(),
            alert_source_name(self.source).to_owned(),
            self.source_identity.clone(),
            resource_kind_name(self.resource_kind).to_owned(),
            self.resource_key.clone(),
            self.symptom_family.clone(),
        ];
        let mut encoded = Vec::new();
        for field in fields {
            encoded.extend_from_slice(field.len().to_string().as_bytes());
            encoded.push(b':');
            encoded.extend_from_slice(field.as_bytes());
            encoded.push(b'|');
        }
        encoded
    }
}

pub(crate) const fn resource_kind_name(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Cluster => "cluster",
        ResourceKind::NameServer => "name_server",
        ResourceKind::Controller => "controller",
        ResourceKind::Broker => "broker",
        ResourceKind::Proxy => "proxy",
        ResourceKind::Store => "store",
        ResourceKind::Topic => "topic",
        ResourceKind::Queue => "queue",
        ResourceKind::ConsumerGroup => "consumer_group",
        ResourceKind::ProducerGroup => "producer_group",
        ResourceKind::Pod => "pod",
        ResourceKind::Node => "node",
        ResourceKind::PersistentVolumeClaim => "persistent_volume_claim",
        ResourceKind::Certificate => "certificate",
        ResourceKind::Runtime => "runtime",
        ResourceKind::Telemetry => "telemetry",
    }
}

const fn alert_source_name(source: AlertSource) -> &'static str {
    match source {
        AlertSource::Alertmanager => "alertmanager",
        AlertSource::KubernetesEvent => "kubernetes_event",
        AlertSource::HealthProbe => "health_probe",
        AlertSource::OperatorQuery => "operator_query",
        AlertSource::Inspection => "inspection",
        AlertSource::Deployment => "deployment",
        AlertSource::SyntheticProbe => "synthetic_probe",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_is_stable_and_prevents_field_boundary_collisions() {
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        let first = CorrelationFingerprintMaterial::new(
            tenant,
            cluster,
            AlertSource::Alertmanager,
            "ab",
            ResourceKind::Broker,
            "c",
            "unavailable",
        );
        let second = CorrelationFingerprintMaterial::new(
            tenant,
            cluster,
            AlertSource::Alertmanager,
            "a",
            ResourceKind::Broker,
            "bc",
            "unavailable",
        );

        assert_ne!(first.canonical_bytes(), second.canonical_bytes());
        assert_eq!(first.canonical_bytes(), first.clone().canonical_bytes());
    }
}
