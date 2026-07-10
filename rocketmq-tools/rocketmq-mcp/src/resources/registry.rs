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
use rmcp::model::Resource;

use crate::config::McpConfig;
use crate::resources::uri::ResourceKind;
use crate::resources::uri::RocketmqResourceUri;
use crate::resources::uri::JSON_MIME_TYPE;

pub fn list_resources(config: &McpConfig) -> ListResourcesResult {
    let resources = config
        .clusters
        .iter()
        .flat_map(|cluster| {
            ResourceKind::ALL
                .into_iter()
                .map(|kind| RocketmqResourceUri::new(cluster.name.clone(), kind))
        })
        .map(resource_descriptor)
        .collect();
    ListResourcesResult::with_all_items(resources)
}

pub fn list_resource_templates() -> ListResourceTemplatesResult {
    ListResourceTemplatesResult::default()
}

fn resource_descriptor(uri: RocketmqResourceUri) -> Resource {
    Resource::new(uri.as_string(), uri.name())
        .with_title(uri.kind.title())
        .with_description(uri.kind.description())
        .with_mime_type(JSON_MIME_TYPE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lists_cluster_scoped_resources() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let result = list_resources(&config);
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

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
