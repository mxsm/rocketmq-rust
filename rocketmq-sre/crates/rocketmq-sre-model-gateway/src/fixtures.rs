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

use rocketmq_sre_contracts::DescriptorStatus;
use rocketmq_sre_contracts::ProviderDescriptor;
use rocketmq_sre_contracts::SchemaVersion;
use serde_json::json;

/// Offline protocol fixture used to validate provider onboarding.
struct ProviderFixture {
    id: &'static str,
    protocol: &'static str,
    streaming: bool,
    tools: bool,
    structured_output: bool,
    embeddings: bool,
}

const PROVIDERS: [ProviderFixture; 8] = [
    ProviderFixture {
        id: "openai",
        protocol: "openai-compatible",
        streaming: true,
        tools: true,
        structured_output: true,
        embeddings: true,
    },
    ProviderFixture {
        id: "anthropic",
        protocol: "anthropic-messages",
        streaming: true,
        tools: true,
        structured_output: true,
        embeddings: false,
    },
    ProviderFixture {
        id: "google-gemini",
        protocol: "gemini-generate-content",
        streaming: true,
        tools: true,
        structured_output: true,
        embeddings: true,
    },
    ProviderFixture {
        id: "aws-bedrock",
        protocol: "bedrock-converse",
        streaming: true,
        tools: true,
        structured_output: true,
        embeddings: true,
    },
    ProviderFixture {
        id: "deepseek",
        protocol: "openai-compatible",
        streaming: true,
        tools: true,
        structured_output: true,
        embeddings: false,
    },
    ProviderFixture {
        id: "zhipu-glm",
        protocol: "openai-compatible",
        streaming: true,
        tools: true,
        structured_output: true,
        embeddings: true,
    },
    ProviderFixture {
        id: "kimi-moonshot",
        protocol: "openai-compatible",
        streaming: true,
        tools: true,
        structured_output: true,
        embeddings: false,
    },
    ProviderFixture {
        id: "local-openai-compatible",
        protocol: "openai-compatible",
        streaming: true,
        tools: true,
        structured_output: false,
        embeddings: false,
    },
];

/// Returns the Phase 00 provider matrix without constructing any client.
#[must_use]
pub fn phase00_provider_descriptors() -> Vec<ProviderDescriptor> {
    PROVIDERS
        .iter()
        .map(|fixture| ProviderDescriptor {
            id: fixture.id.to_owned(),
            version: "1.0.0".to_owned(),
            owner: "rocketmq-sre".to_owned(),
            supported_versions: vec![SchemaVersion::new("rocketmq-sre.model", 1, 0)],
            required_capabilities: BTreeSet::new(),
            config_schema: json!({
                "type": "object",
                "properties": {
                    "endpoint": {"type": "string", "format": "uri"},
                    "credential_ref": {"type": "string", "minLength": 1}
                },
                "required": ["endpoint", "credential_ref"],
                "additionalProperties": false
            }),
            status: DescriptorStatus::Active,
            deprecation: None,
            protocols: BTreeSet::from([fixture.protocol.to_owned()]),
            supports_streaming: fixture.streaming,
            supports_tools: fixture.tools,
            supports_structured_output: fixture.structured_output,
            supports_embeddings: fixture.embeddings,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_matrix_contains_requested_chinese_providers() {
        let providers = phase00_provider_descriptors();
        let ids: BTreeSet<_> = providers.iter().map(|provider| provider.id.as_str()).collect();

        assert_eq!(providers.len(), 8);
        assert!(ids.contains("deepseek"));
        assert!(ids.contains("zhipu-glm"));
        assert!(ids.contains("kimi-moonshot"));
        assert!(
            providers
                .iter()
                .all(|provider| { provider.config_schema["properties"].get("api_key").is_none() })
        );
    }
}
