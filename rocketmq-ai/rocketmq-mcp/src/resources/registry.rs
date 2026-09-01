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
use crate::resources::cursor::DiscoveryCursorCodec;
use crate::resources::cursor::DiscoveryCursorError;
use crate::resources::cursor::DiscoverySurface;
use crate::resources::uri::ResourceKind;
use crate::resources::uri::RocketmqResourceUri;
use crate::resources::uri::JSON_MIME_TYPE;

const DISCOVERY_PAGE_LIMIT: u32 = 50;

#[derive(Clone)]
pub struct ResourceRegistry {
    cursors: DiscoveryCursorCodec,
}

impl std::fmt::Debug for ResourceRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ResourceRegistry").finish_non_exhaustive()
    }
}

impl ResourceRegistry {
    pub(crate) fn new() -> Result<Self, DiscoveryCursorError> {
        Ok(Self {
            cursors: DiscoveryCursorCodec::new()?,
        })
    }

    pub fn list_resources(
        &self,
        config: &McpConfig,
        request: Option<&PaginatedRequestParams>,
        canonical_auth_claims: &[u8],
        mut allows_resource: impl FnMut(&str, &ResourceKind) -> bool,
        include_system: bool,
    ) -> Result<ListResourcesResult, ErrorData> {
        let mut resources = Vec::new();
        for cluster in &config.clusters {
            for kind in ResourceKind::ROOTS {
                let Some(uri) = RocketmqResourceUri::try_new(cluster.name.clone(), kind.clone()) else {
                    continue;
                };
                if allows_resource(&cluster.name, &kind) {
                    resources.push(resource_descriptor(uri));
                }
            }
        }
        if include_system {
            resources.extend(
                ResourceKind::SYSTEM_ROOTS
                    .into_iter()
                    .filter_map(RocketmqResourceUri::system)
                    .map(resource_descriptor),
            );
        }
        let page = discovery_page(
            &self.cursors,
            resources,
            request,
            canonical_auth_claims,
            DiscoverySurface::Resources,
        )?;
        let mut result = ListResourcesResult::with_all_items(page.items);
        result.next_cursor = page.next_cursor;
        Ok(result)
    }

    pub fn list_resource_templates(
        &self,
        config: &McpConfig,
        request: Option<&PaginatedRequestParams>,
        canonical_auth_claims: &[u8],
        mut allows_resource: impl FnMut(&str, &ResourceKind) -> bool,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        let templates = resource_templates()
            .into_iter()
            .filter(|(kind, _)| {
                config.clusters.iter().any(|cluster| {
                    crate::model::identifier::is_logical_alias(&cluster.name) && allows_resource(&cluster.name, kind)
                })
            })
            .map(|(_, template)| template)
            .collect();
        let page = discovery_page(
            &self.cursors,
            templates,
            request,
            canonical_auth_claims,
            DiscoverySurface::Templates,
        )?;
        let mut result = ListResourceTemplatesResult::with_all_items(page.items);
        result.next_cursor = page.next_cursor;
        Ok(result)
    }
}

fn resource_templates() -> Vec<(ResourceKind, ResourceTemplate)> {
    vec![
        resource_template(
            ResourceKind::Topic(String::new()),
            "rocketmq://clusters/{cluster}/topics/{topic}",
            "rocketmq_topic",
            "RocketMQ topic",
            "Read-only details for one RocketMQ topic.",
        ),
        resource_template(
            ResourceKind::TopicRoute(String::new()),
            "rocketmq://clusters/{cluster}/topics/{topic}/route",
            "rocketmq_topic_route",
            "RocketMQ topic route",
            "Read-only routing information for one RocketMQ topic.",
        ),
        resource_template(
            ResourceKind::ConsumerGroup(String::new()),
            "rocketmq://clusters/{cluster}/consumer-groups/{group}",
            "rocketmq_consumer_group",
            "RocketMQ consumer group",
            "Read-only details for one RocketMQ consumer group.",
        ),
        resource_template(
            ResourceKind::ConsumerLag {
                group: String::new(),
                topic: String::new(),
            },
            "rocketmq://clusters/{cluster}/consumer-groups/{group}/lag{?topic}",
            "rocketmq_consumer_lag",
            "RocketMQ consumer lag",
            "Read-only lag details for one consumer group and topic.",
        ),
        resource_template(
            ResourceKind::Broker(String::new()),
            "rocketmq://clusters/{cluster}/brokers/{broker}",
            "rocketmq_broker",
            "RocketMQ broker",
            "Read-only details for one RocketMQ broker.",
        ),
        resource_template(
            ResourceKind::Topics,
            "rocketmq://clusters/{cluster}/topics{?filter,limit,cursor}",
            "rocketmq_topics_page",
            "RocketMQ topic page",
            "Read a filtered page from one stable topic inventory snapshot.",
        ),
        resource_template(
            ResourceKind::Topic(String::new()),
            "rocketmq://clusters/{cluster}/topics/{topic}{?limit,cursor}",
            "rocketmq_topic_page",
            "RocketMQ topic detail page",
            "Read a bounded page from one stable topic detail snapshot.",
        ),
        resource_template(
            ResourceKind::TopicRoute(String::new()),
            "rocketmq://clusters/{cluster}/topics/{topic}/route{?limit,cursor}",
            "rocketmq_topic_route_page",
            "RocketMQ topic route page",
            "Read a bounded page from one stable topic route snapshot.",
        ),
        resource_template(
            ResourceKind::ConsumerGroups,
            "rocketmq://clusters/{cluster}/consumer-groups{?filter,limit,cursor}",
            "rocketmq_consumer_groups_page",
            "RocketMQ consumer group page",
            "Read and enrich one filtered page from a stable consumer-group inventory snapshot.",
        ),
        resource_template(
            ResourceKind::ConsumerLag {
                group: String::new(),
                topic: String::new(),
            },
            "rocketmq://clusters/{cluster}/consumer-groups/{group}/lag{?topic,limit,cursor}",
            "rocketmq_consumer_lag_page",
            "RocketMQ consumer lag page",
            "Read a bounded page from one stable consumer-lag snapshot.",
        ),
        resource_template(
            ResourceKind::BrokerDiagnostics(String::new()),
            "rocketmq://clusters/{cluster}/brokers/{broker}/diagnostics",
            "rocketmq_broker_diagnostics",
            "RocketMQ broker diagnostics",
            "Read diagnostics for one logical RocketMQ broker.",
        ),
        resource_template(
            ResourceKind::BrokerConfigSummary(String::new()),
            "rocketmq://clusters/{cluster}/brokers/{broker}/config-summary",
            "rocketmq_broker_config_summary",
            "RocketMQ broker configuration summary",
            "Read the allowlisted configuration summary for one logical RocketMQ broker.",
        ),
        resource_template(
            ResourceKind::TopicStats(String::new()),
            "rocketmq://clusters/{cluster}/topics/{topic}/stats{?limit,cursor}",
            "rocketmq_topic_stats",
            "RocketMQ topic statistics",
            "Read a bounded page from one stable topic-statistics snapshot.",
        ),
        resource_template(
            ResourceKind::TopicConfig(String::new()),
            "rocketmq://clusters/{cluster}/topics/{topic}/config",
            "rocketmq_topic_config",
            "RocketMQ topic configuration",
            "Read address-free configuration observations for one RocketMQ topic.",
        ),
        resource_template(
            ResourceKind::ConsumerProgress(String::new()),
            "rocketmq://clusters/{cluster}/consumer-groups/{group}/progress{?limit,cursor}",
            "rocketmq_consumer_progress",
            "RocketMQ consumer progress",
            "Read a bounded page from one stable consumer-progress snapshot.",
        ),
    ]
}

fn resource_descriptor(uri: RocketmqResourceUri) -> Resource {
    Resource::new(uri.as_string(), uri.name())
        .with_title(uri.kind.title())
        .with_description(uri.kind.description())
        .with_mime_type(JSON_MIME_TYPE)
}

fn resource_template(
    kind: ResourceKind,
    uri: &str,
    name: &str,
    title: &str,
    description: &str,
) -> (ResourceKind, ResourceTemplate) {
    (
        kind,
        ResourceTemplate::new(uri, name)
            .with_title(title)
            .with_description(description)
            .with_mime_type(JSON_MIME_TYPE),
    )
}

fn discovery_page<T>(
    cursors: &DiscoveryCursorCodec,
    items: Vec<T>,
    request: Option<&PaginatedRequestParams>,
    canonical_auth_claims: &[u8],
    surface: DiscoverySurface,
) -> Result<crate::model::contract::Page<T>, ErrorData> {
    let cursor = request
        .and_then(|request| request.cursor.as_deref())
        .map(|cursor| {
            cursors
                .open(surface, cursor, canonical_auth_claims)
                .map(|offset| format!("rmq-v1-{offset:x}"))
                .map_err(|_| invalid_discovery_cursor())
        })
        .transpose()?;
    let mut page = paginate(
        items,
        &PageRequest {
            limit: Some(DISCOVERY_PAGE_LIMIT),
            cursor,
        },
    )
    .map_err(|error| ErrorData::invalid_params(error.to_string(), None))?;
    page.next_cursor = page
        .next_cursor
        .map(|cursor| {
            let offset = decode_internal_cursor(&cursor)?;
            cursors
                .seal(surface, offset, canonical_auth_claims)
                .map_err(|_| ErrorData::internal_error("failed to create discovery cursor", None))
        })
        .transpose()?;
    Ok(page)
}

fn decode_internal_cursor(cursor: &str) -> Result<usize, ErrorData> {
    let offset = cursor
        .strip_prefix("rmq-v1-")
        .filter(|offset| !offset.is_empty())
        .ok_or_else(invalid_discovery_cursor)?;
    let parsed = usize::from_str_radix(offset, 16).map_err(|_| invalid_discovery_cursor())?;
    if format!("{parsed:x}") != offset {
        return Err(invalid_discovery_cursor());
    }
    Ok(parsed)
}

fn invalid_discovery_cursor() -> ErrorData {
    ErrorData::invalid_params("invalid discovery cursor", None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lists_cluster_scoped_resources() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let registry = ResourceRegistry::new().unwrap();
        let result = registry
            .list_resources(&config, None, b"unrestricted", |_, _| true, true)
            .unwrap();
        let uris = result
            .resources
            .iter()
            .map(|resource| resource.uri.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            uris,
            [
                "rocketmq://clusters/local-dev/capabilities",
                "rocketmq://clusters/local-dev/overview",
                "rocketmq://clusters/local-dev/topics",
                "rocketmq://clusters/local-dev/brokers",
                "rocketmq://clusters/local-dev/consumer-groups",
                "rocketmq://system/runtime/v1",
                "rocketmq://system/observability/v1",
            ]
        );
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn registry_lists_fifteen_stable_resource_templates() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let registry = ResourceRegistry::new().unwrap();
        let result = registry
            .list_resource_templates(&config, None, b"unrestricted", |_, _| true)
            .unwrap();
        let templates = result
            .resource_templates
            .iter()
            .map(|template| template.uri_template.as_str())
            .collect::<Vec<_>>();
        assert_eq!(templates.len(), 15);
        assert_eq!(
            &templates[10..],
            [
                "rocketmq://clusters/{cluster}/brokers/{broker}/diagnostics",
                "rocketmq://clusters/{cluster}/brokers/{broker}/config-summary",
                "rocketmq://clusters/{cluster}/topics/{topic}/stats{?limit,cursor}",
                "rocketmq://clusters/{cluster}/topics/{topic}/config",
                "rocketmq://clusters/{cluster}/consumer-groups/{group}/progress{?limit,cursor}",
            ]
        );
        assert!(result.resource_templates.iter().all(|template| {
            template.mime_type.as_deref() == Some(JSON_MIME_TYPE)
                && template.title.is_some()
                && template.description.is_some()
        }));
    }

    #[test]
    fn registry_cursor_is_principal_bound() {
        let mut config = McpConfig::load(example_config_path()).unwrap();
        let cluster = config.clusters[0].clone();
        config.clusters = (0..13)
            .map(|index| {
                let mut cluster = cluster.clone();
                cluster.name = format!("cluster-{index}");
                cluster
            })
            .collect();
        let registry = ResourceRegistry::new().unwrap();
        let first = registry
            .list_resources(&config, None, b"principal-a-standard", |_, _| true, true)
            .unwrap();
        let request = PaginatedRequestParams::default().with_cursor(first.next_cursor.clone());
        let second = registry
            .list_resources(&config, Some(&request), b"principal-a-standard", |_, _| true, true)
            .unwrap();
        assert_eq!(first.resources.len(), 50);
        assert_eq!(second.resources.len(), 17);
        assert!(registry
            .list_resources(&config, Some(&request), b"principal-b-standard", |_, _| true, true,)
            .is_err());
        assert!(registry
            .list_resources(&config, Some(&request), b"principal-a-sensitive", |_, _| true, true,)
            .is_err());
        assert!(ResourceRegistry::new()
            .unwrap()
            .list_resources(&config, Some(&request), b"principal-a-standard", |_, _| true, true,)
            .is_err());

        let mut tampered = first.next_cursor.unwrap().into_bytes();
        let index = tampered.len() - 1;
        tampered[index] = if tampered[index] == b'0' { b'1' } else { b'0' };
        let request = PaginatedRequestParams::default().with_cursor(Some(String::from_utf8(tampered).unwrap()));
        assert!(registry
            .list_resources(&config, Some(&request), b"principal-a-standard", |_, _| true, true,)
            .is_err());
    }

    #[test]
    fn templates_require_backing_access_on_a_configured_cluster() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let registry = ResourceRegistry::new().unwrap();
        let result = registry
            .list_resource_templates(&config, None, b"principal", |_, kind| {
                matches!(kind, ResourceKind::TopicStats(_))
            })
            .unwrap();
        assert_eq!(result.resource_templates.len(), 1);
        assert_eq!(
            result.resource_templates[0].uri_template,
            "rocketmq://clusters/{cluster}/topics/{topic}/stats{?limit,cursor}"
        );
    }

    #[test]
    fn discovery_ignores_unrepresentable_configured_clusters_without_changing_safe_pages() {
        let mut safe_config = McpConfig::load(example_config_path()).unwrap();
        let prototype = safe_config.clusters[0].clone();
        safe_config.clusters = (0..13)
            .map(|index| {
                let mut cluster = prototype.clone();
                cluster.name = format!("cluster-{index}");
                cluster
            })
            .collect();
        let mut mixed_config = safe_config.clone();
        for name in ["token=secret", "%74oken%3Dsecret", "127.0.0.1:9876"] {
            let mut cluster = prototype.clone();
            cluster.name = name.to_string();
            mixed_config.clusters.push(cluster);
        }

        let registry = ResourceRegistry::new().unwrap();
        let safe = registry
            .list_resources(&safe_config, None, b"principal", |_, _| true, false)
            .unwrap();
        let mixed = registry
            .list_resources(&mixed_config, None, b"principal", |_, _| true, false)
            .unwrap();
        assert_eq!(mixed.resources, safe.resources);
        assert_eq!(mixed.next_cursor, safe.next_cursor);
        let mixed_wire = serde_json::to_string(&mixed).unwrap();
        for value in ["token=secret", "%74oken%3Dsecret", "127.0.0.1:9876"] {
            assert!(!mixed_wire.contains(value));
        }
        assert_eq!(
            registry
                .list_resource_templates(&mixed_config, None, b"principal", |_, _| true)
                .unwrap()
                .resource_templates
                .len(),
            15
        );

        let mut unsafe_only = mixed_config;
        unsafe_only
            .clusters
            .retain(|cluster| !crate::model::identifier::is_logical_alias(&cluster.name));
        assert!(registry
            .list_resources(&unsafe_only, None, b"principal", |_, _| true, false)
            .unwrap()
            .resources
            .is_empty());
        assert!(registry
            .list_resource_templates(&unsafe_only, None, b"principal", |_, _| true)
            .unwrap()
            .resource_templates
            .is_empty());
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
