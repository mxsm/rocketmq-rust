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

use rmcp::model::ListResourceTemplatesResult;
use rmcp::model::ListResourcesResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::Resource;
use rmcp::model::ResourceTemplate;
use rmcp::ErrorData;

use crate::config::McpConfig;
use crate::model::contract::paginate;
use crate::model::contract::PageRequest;
use crate::resources::uri::ResourceKind;
use crate::resources::uri::RocketmqResourceUri;
use crate::resources::uri::JSON_MIME_TYPE;

const DISCOVERY_PAGE_LIMIT: u32 = 50;

pub fn list_resources(
    config: &McpConfig,
    request: Option<&PaginatedRequestParams>,
) -> Result<ListResourcesResult, ErrorData> {
    let resources = config
        .clusters
        .iter()
        .flat_map(|cluster| {
            ResourceKind::ROOTS
                .into_iter()
                .map(|kind| RocketmqResourceUri::new(cluster.name.clone(), kind))
        })
        .map(resource_descriptor)
        .collect::<Vec<_>>();
    let page = discovery_page(resources, request)?;
    Ok(ListResourcesResult {
        meta: None,
        next_cursor: page.next_cursor,
        resources: page.items,
    })
}

pub fn list_resource_templates(
    request: Option<&PaginatedRequestParams>,
) -> Result<ListResourceTemplatesResult, ErrorData> {
    let templates = vec![
        resource_template(
            "rocketmq://clusters/{cluster}/topics/{topic}",
            "rocketmq_topic",
            "RocketMQ topic",
            "Read-only details for one RocketMQ topic.",
        ),
        resource_template(
            "rocketmq://clusters/{cluster}/topics/{topic}/route",
            "rocketmq_topic_route",
            "RocketMQ topic route",
            "Read-only routing information for one RocketMQ topic.",
        ),
        resource_template(
            "rocketmq://clusters/{cluster}/consumer-groups/{group}",
            "rocketmq_consumer_group",
            "RocketMQ consumer group",
            "Read-only details for one RocketMQ consumer group.",
        ),
        resource_template(
            "rocketmq://clusters/{cluster}/consumer-groups/{group}/lag{?topic}",
            "rocketmq_consumer_lag",
            "RocketMQ consumer lag",
            "Read-only lag details for one consumer group and topic.",
        ),
        resource_template(
            "rocketmq://clusters/{cluster}/brokers/{broker}",
            "rocketmq_broker",
            "RocketMQ broker",
            "Read-only details for one RocketMQ broker.",
        ),
    ];
    let page = discovery_page(templates, request)?;
    Ok(ListResourceTemplatesResult {
        meta: None,
        next_cursor: page.next_cursor,
        resource_templates: page.items,
    })
}

fn resource_descriptor(uri: RocketmqResourceUri) -> Resource {
    Resource::new(uri.as_string(), uri.name())
        .with_title(uri.kind.title())
        .with_description(uri.kind.description())
        .with_mime_type(JSON_MIME_TYPE)
}

fn resource_template(uri: &str, name: &str, title: &str, description: &str) -> ResourceTemplate {
    ResourceTemplate::new(uri, name)
        .with_title(title)
        .with_description(description)
        .with_mime_type(JSON_MIME_TYPE)
}

fn discovery_page<T>(
    items: Vec<T>,
    request: Option<&PaginatedRequestParams>,
) -> Result<crate::model::contract::Page<T>, ErrorData> {
    paginate(
        items,
        &PageRequest {
            limit: Some(DISCOVERY_PAGE_LIMIT),
            cursor: request.and_then(|request| request.cursor.clone()),
        },
    )
    .map_err(|error| ErrorData::invalid_params(error.to_string(), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lists_cluster_scoped_resources() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let result = list_resources(&config, None).unwrap();
        let uris = result
            .resources
            .iter()
            .map(|resource| resource.uri.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            uris,
            [
                "rocketmq://clusters/local-dev/overview",
                "rocketmq://clusters/local-dev/topics",
                "rocketmq://clusters/local-dev/brokers",
                "rocketmq://clusters/local-dev/consumer-groups",
            ]
        );
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn registry_lists_parameterized_resource_templates() {
        let result = list_resource_templates(None).unwrap();
        let templates = result
            .resource_templates
            .iter()
            .map(|template| template.uri_template.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            templates,
            [
                "rocketmq://clusters/{cluster}/topics/{topic}",
                "rocketmq://clusters/{cluster}/topics/{topic}/route",
                "rocketmq://clusters/{cluster}/consumer-groups/{group}",
                "rocketmq://clusters/{cluster}/consumer-groups/{group}/lag{?topic}",
                "rocketmq://clusters/{cluster}/brokers/{broker}",
            ]
        );
    }

    #[test]
    fn registry_rejects_invalid_discovery_cursor() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let request = PaginatedRequestParams::default().with_cursor(Some("legacy-cursor".to_string()));

        assert!(list_resources(&config, Some(&request)).is_err());
    }

    #[test]
    fn registry_cursor_resumes_resource_discovery() {
        let mut config = McpConfig::load(example_config_path()).unwrap();
        let cluster = config.clusters[0].clone();
        config.clusters = (0..13)
            .map(|index| {
                let mut cluster = cluster.clone();
                cluster.name = format!("cluster-{index}");
                cluster
            })
            .collect();

        let first = list_resources(&config, None).unwrap();
        let request = PaginatedRequestParams::default().with_cursor(first.next_cursor.clone());
        let second = list_resources(&config, Some(&request)).unwrap();

        assert_eq!(first.resources.len(), 50);
        assert!(first.next_cursor.is_some());
        assert_eq!(second.resources.len(), 2);
        assert!(second.next_cursor.is_none());
        assert_ne!(first.resources[0].uri, second.resources[0].uri);
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
