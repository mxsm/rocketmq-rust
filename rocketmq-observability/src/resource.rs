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

use crate::config::ObservabilityConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceAttribute {
    pub key: String,
    pub value: String,
}

pub fn build_resource_attributes(config: &ObservabilityConfig) -> Vec<ResourceAttribute> {
    let mut attributes = vec![
        ResourceAttribute::new("service.name", &config.service_name),
        ResourceAttribute::new("service.namespace", &config.service_namespace),
        ResourceAttribute::new("service.version", &config.service_version),
        ResourceAttribute::new("deployment.environment.name", &config.environment),
        ResourceAttribute::new("rocketmq.cluster", &config.cluster),
        ResourceAttribute::new("rocketmq.node.type", &config.node_type),
    ];

    if !config.service_instance_id.is_empty() {
        attributes.push(ResourceAttribute::new(
            "service.instance.id",
            &config.service_instance_id,
        ));
    }

    if !config.node_id.is_empty() {
        attributes.push(ResourceAttribute::new("rocketmq.node.id", &config.node_id));
    }

    attributes.extend(
        config
            .resource_attributes
            .iter()
            .map(|(key, value)| ResourceAttribute::new(key, value)),
    );
    attributes
}

impl ResourceAttribute {
    fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[cfg(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs"))]
pub fn build_key_values(config: &ObservabilityConfig) -> Vec<opentelemetry::KeyValue> {
    build_resource_attributes(config)
        .into_iter()
        .map(|attribute| opentelemetry::KeyValue::new(attribute.key, attribute.value))
        .collect()
}

#[cfg(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs"))]
pub fn build_resource(config: &ObservabilityConfig) -> opentelemetry_sdk::Resource {
    opentelemetry_sdk::Resource::builder()
        .with_service_name(config.service_name.clone())
        .with_attributes(build_key_values(config))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn includes_core_resource_attributes() {
        let config = ObservabilityConfig::default();
        let attributes = build_resource_attributes(&config);

        assert!(attributes
            .iter()
            .any(|attribute| attribute.key == "service.name" && attribute.value == "rocketmq-rust"));
        assert!(attributes
            .iter()
            .any(|attribute| attribute.key == "rocketmq.cluster" && attribute.value == "DefaultCluster"));
    }
}
