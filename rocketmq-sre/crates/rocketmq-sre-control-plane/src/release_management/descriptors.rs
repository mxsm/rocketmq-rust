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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::DescriptorStatus;
use rocketmq_sre_contracts::IntegrationAdapterKind;
use rocketmq_sre_contracts::IntegrationDescriptor;
use rocketmq_sre_contracts::SchemaVersion;
use serde_json::json;

const DESCRIPTOR_VERSION: &str = "1.0.0";

pub(super) fn descriptor_for(kind: IntegrationAdapterKind) -> IntegrationDescriptor {
    let (id, integration_kind, inbound, outbound, required_capabilities) = match kind {
        IntegrationAdapterKind::MockItsm => (
            "rocketmq-sre.integration.mock-itsm.v1",
            "mock_itsm",
            true,
            true,
            vec!["integration:itsm", "integration:outbox"],
        ),
        IntegrationAdapterKind::SignedWebhookItsm => (
            "rocketmq-sre.integration.signed-webhook-itsm.v1",
            "signed_webhook_itsm",
            true,
            true,
            vec!["integration:itsm", "integration:outbox", "integration:signed-webhook"],
        ),
        IntegrationAdapterKind::ChatOpsWebhook => (
            "rocketmq-sre.integration.chatops-webhook.v1",
            "chatops_webhook",
            false,
            true,
            vec!["integration:notification-outbox", "integration:chatops"],
        ),
        IntegrationAdapterKind::Pager => (
            "rocketmq-sre.integration.pager.v1",
            "pager",
            false,
            true,
            vec!["integration:notification-outbox", "integration:pager"],
        ),
        IntegrationAdapterKind::Email => (
            "rocketmq-sre.integration.email.v1",
            "email",
            false,
            true,
            vec!["integration:notification-outbox", "integration:email"],
        ),
    };
    IntegrationDescriptor {
        id: id.to_owned(),
        version: DESCRIPTOR_VERSION.to_owned(),
        owner: "rocketmq-sre".to_owned(),
        supported_versions: vec![
            SchemaVersion::new("rocketmq-sre.integration-delivery", 1, 0),
            SchemaVersion::new("rocketmq-sre.external-approval", 1, 0),
        ],
        required_capabilities: required_capabilities
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect::<BTreeSet<_>>(),
        config_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["endpoint"],
            "properties": {
                "endpoint": {"type": "string", "maxLength": 2048},
                "secret_reference": {
                    "type": ["string", "null"],
                    "pattern": "^env:[A-Z0-9_]{1,128}$"
                },
                "notification_target_id": {
                    "type": ["string", "null"],
                    "format": "uuid"
                }
            }
        }),
        status: DescriptorStatus::Active,
        deprecation: None,
        integration_kind: integration_kind.to_owned(),
        inbound,
        outbound,
    }
}

pub(super) fn resolve_descriptor(
    id: &str,
    version: &str,
    kind: IntegrationAdapterKind,
) -> Option<IntegrationDescriptor> {
    let descriptor = descriptor_for(kind);
    (descriptor.id == id && descriptor.version == version).then_some(descriptor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_descriptors_are_versioned_and_keep_inbound_approval_on_itsm_only() {
        for kind in [
            IntegrationAdapterKind::MockItsm,
            IntegrationAdapterKind::SignedWebhookItsm,
            IntegrationAdapterKind::ChatOpsWebhook,
            IntegrationAdapterKind::Pager,
            IntegrationAdapterKind::Email,
        ] {
            let descriptor = descriptor_for(kind);
            assert_eq!(descriptor.version, DESCRIPTOR_VERSION);
            assert!(descriptor.required_capabilities.iter().any(|capability| {
                capability == "integration:outbox" || capability == "integration:notification-outbox"
            }));
            assert_eq!(
                descriptor.inbound,
                matches!(
                    kind,
                    IntegrationAdapterKind::MockItsm | IntegrationAdapterKind::SignedWebhookItsm
                )
            );
        }
    }
}
