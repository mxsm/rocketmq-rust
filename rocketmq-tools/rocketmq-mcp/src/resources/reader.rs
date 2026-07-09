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

use rmcp::model::ReadResourceResult;
use rmcp::model::ResourceContents;
use rmcp::ErrorData;
use serde_json::json;
use serde_json::Map;
use serde_json::Value;

use crate::config::McpConfig;
use crate::resources::uri::RocketmqResourceUri;
use crate::resources::uri::JSON_MIME_TYPE;

pub fn read_resource(config: &McpConfig, uri: &str) -> Result<ReadResourceResult, ErrorData> {
    let resource_uri = RocketmqResourceUri::parse(uri)
        .ok_or_else(|| ErrorData::resource_not_found(format!("resource not found: {uri}"), None))?;
    let payload = resource_payload(config, resource_uri);
    let text = serde_json::to_string_pretty(&payload)
        .map_err(|err| ErrorData::internal_error(format!("failed to serialize resource {uri}: {err}"), None))?;

    Ok(ReadResourceResult::new(vec![ResourceContents::text(
        text,
        resource_uri.as_str(),
    )
    .with_mime_type(JSON_MIME_TYPE)]))
}

fn resource_payload(config: &McpConfig, uri: RocketmqResourceUri) -> Value {
    match uri {
        RocketmqResourceUri::ClusterOverview => cluster_overview_payload(config),
        RocketmqResourceUri::Topics => inventory_payload(uri, "topics"),
        RocketmqResourceUri::Brokers => inventory_payload(uri, "brokers"),
        RocketmqResourceUri::ConsumerGroups => inventory_payload(uri, "consumer_groups"),
    }
}

fn cluster_overview_payload(config: &McpConfig) -> Value {
    let clusters = config
        .clusters
        .iter()
        .map(|cluster| {
            json!({
                "name": cluster.name,
                "namesrv_addr": cluster.namesrv_addr,
                "default": cluster.default.unwrap_or(false),
            })
        })
        .collect::<Vec<_>>();

    json!({
        "schema_version": "mvp.v1",
        "resource": RocketmqResourceUri::ClusterOverview.as_str(),
        "source": "configuration",
        "cluster_count": clusters.len(),
        "clusters": clusters,
        "cache_ttl_ms": config.cache.cluster_overview_ttl_ms,
    })
}

fn inventory_payload(uri: RocketmqResourceUri, field: &str) -> Value {
    let mut payload = Map::new();
    payload.insert("schema_version".to_string(), json!("mvp.v1"));
    payload.insert("resource".to_string(), json!(uri.as_str()));
    payload.insert("source".to_string(), json!("mvp-static"));
    payload.insert(field.to_string(), json!([]));
    Value::Object(payload)
}

#[cfg(test)]
mod tests {
    use rmcp::model::ResourceContents;

    use super::*;
    use crate::config::McpConfig;

    #[test]
    fn read_cluster_overview_uses_configured_clusters() {
        let result = read_resource(
            &McpConfig::load(example_config_path()).unwrap(),
            "rocketmq://cluster/overview",
        )
        .unwrap();
        let payload = read_json_payload(&result);

        assert_eq!(payload["resource"], "rocketmq://cluster/overview");
        assert_eq!(payload["source"], "configuration");
        assert_eq!(payload["cluster_count"], 1);
        assert_eq!(payload["clusters"][0]["name"], "local-dev");
        assert_eq!(payload["clusters"][0]["namesrv_addr"], "127.0.0.1:9876");
        assert_eq!(payload["clusters"][0]["default"], true);
        assert_eq!(payload["cache_ttl_ms"], 3000);
    }

    #[test]
    fn read_inventory_resource_returns_stable_json_shape() {
        let result = read_resource(
            &McpConfig::load(example_config_path()).unwrap(),
            "rocketmq://consumer-groups",
        )
        .unwrap();
        let payload = read_json_payload(&result);

        assert_eq!(payload["schema_version"], "mvp.v1");
        assert_eq!(payload["resource"], "rocketmq://consumer-groups");
        assert_eq!(payload["source"], "mvp-static");
        assert_eq!(payload["consumer_groups"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn read_unknown_resource_returns_protocol_not_found() {
        let err = read_resource(&McpConfig::load(example_config_path()).unwrap(), "rocketmq://unknown").unwrap_err();

        assert_eq!(err.code, rmcp::model::ErrorCode::RESOURCE_NOT_FOUND);
        assert!(err.message.contains("resource not found"));
    }

    fn read_json_payload(result: &ReadResourceResult) -> Value {
        assert_eq!(result.contents.len(), 1);
        match &result.contents[0] {
            ResourceContents::TextResourceContents { mime_type, text, .. } => {
                assert_eq!(mime_type.as_deref(), Some(JSON_MIME_TYPE));
                serde_json::from_str(text).unwrap()
            }
            ResourceContents::BlobResourceContents { .. } => {
                panic!("resource should be returned as text")
            }
            _ => panic!("unsupported resource content variant"),
        }
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
