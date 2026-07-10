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
use crate::model::contract::observed_at;
use crate::model::contract::SCHEMA_VERSION;
use crate::resources::uri::ResourceKind;
use crate::resources::uri::RocketmqResourceUri;
use crate::resources::uri::JSON_MIME_TYPE;

pub fn read_resource(config: &McpConfig, uri: &str) -> Result<ReadResourceResult, ErrorData> {
    let resource_uri = RocketmqResourceUri::parse(uri)
        .ok_or_else(|| ErrorData::resource_not_found(format!("resource not found: {uri}"), None))?;
    if !config
        .clusters
        .iter()
        .any(|cluster| cluster.name == resource_uri.cluster)
    {
        return Err(ErrorData::resource_not_found(
            format!("resource cluster is not configured: {}", resource_uri.cluster),
            None,
        ));
    }

    let payload = resource_payload(&resource_uri);
    let text = serde_json::to_string_pretty(&payload)
        .map_err(|err| ErrorData::internal_error(format!("failed to serialize resource {uri}: {err}"), None))?;

    Ok(ReadResourceResult::new(vec![
        ResourceContents::text(text, uri).with_mime_type(JSON_MIME_TYPE)
    ]))
}

fn resource_payload(uri: &RocketmqResourceUri) -> Value {
    match uri.kind {
        ResourceKind::Overview => json!({
            "schema_version": SCHEMA_VERSION,
            "resource": uri.as_string(),
            "cluster": uri.cluster,
            "observed_at": observed_at(),
            "source": "configuration",
            "status": "configured",
        }),
        ResourceKind::Topics => inventory_payload(uri, "topics"),
        ResourceKind::Brokers => inventory_payload(uri, "brokers"),
        ResourceKind::ConsumerGroups => inventory_payload(uri, "consumer_groups"),
    }
}

fn inventory_payload(uri: &RocketmqResourceUri, field: &str) -> Value {
    let mut payload = Map::new();
    payload.insert("schema_version".to_string(), json!(SCHEMA_VERSION));
    payload.insert("resource".to_string(), json!(uri.as_string()));
    payload.insert("cluster".to_string(), json!(uri.cluster));
    payload.insert("observed_at".to_string(), json!(observed_at()));
    payload.insert("source".to_string(), json!("placeholder"));
    payload.insert("partial".to_string(), json!(true));
    payload.insert(
        "warnings".to_string(),
        json!(["Live Resource queries are introduced by the QueryFacade stage."]),
    );
    payload.insert(field.to_string(), json!([]));
    Value::Object(payload)
}

#[cfg(test)]
mod tests {
    use rmcp::model::ResourceContents;

    use super::*;

    #[test]
    fn read_cluster_overview_is_cluster_scoped_and_hides_nameserver() {
        let result = read_resource(
            &McpConfig::load(example_config_path()).unwrap(),
            "rocketmq://clusters/local-dev/overview",
        )
        .unwrap();
        let payload = read_json_payload(&result);

        assert_eq!(payload["cluster"], "local-dev");
        assert_eq!(payload["source"], "configuration");
        assert!(payload.get("namesrv_addr").is_none());
        assert!(chrono::DateTime::parse_from_rfc3339(payload["observed_at"].as_str().unwrap()).is_ok());
    }

    #[test]
    fn read_inventory_resource_marks_placeholder_as_partial() {
        let result = read_resource(
            &McpConfig::load(example_config_path()).unwrap(),
            "rocketmq://clusters/local-dev/consumer-groups",
        )
        .unwrap();
        let payload = read_json_payload(&result);

        assert_eq!(payload["schema_version"], SCHEMA_VERSION);
        assert_eq!(payload["source"], "placeholder");
        assert_eq!(payload["partial"], true);
        assert!(payload["warnings"][0].as_str().unwrap().contains("QueryFacade"));
    }

    #[test]
    fn read_unknown_or_unconfigured_resource_returns_not_found() {
        let config = McpConfig::load(example_config_path()).unwrap();
        let unknown_resource = read_resource(&config, "rocketmq://clusters/local-dev/unknown").unwrap_err();
        let unknown_cluster = read_resource(&config, "rocketmq://clusters/missing/overview").unwrap_err();

        assert_eq!(unknown_resource.code, rmcp::model::ErrorCode::RESOURCE_NOT_FOUND);
        assert_eq!(unknown_cluster.code, rmcp::model::ErrorCode::RESOURCE_NOT_FOUND);
    }

    fn read_json_payload(result: &ReadResourceResult) -> Value {
        assert_eq!(result.contents.len(), 1);
        match &result.contents[0] {
            ResourceContents::TextResourceContents { mime_type, text, .. } => {
                assert_eq!(mime_type.as_deref(), Some(JSON_MIME_TYPE));
                serde_json::from_str(text).unwrap()
            }
            ResourceContents::BlobResourceContents { .. } => panic!("resource should be returned as text"),
            _ => panic!("unsupported resource content variant"),
        }
    }

    fn example_config_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml")
    }
}
