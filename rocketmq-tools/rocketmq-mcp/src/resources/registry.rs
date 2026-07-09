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

use crate::resources::uri::RocketmqResourceUri;
use crate::resources::uri::JSON_MIME_TYPE;

pub fn list_resources() -> ListResourcesResult {
    ListResourcesResult::with_all_items(RocketmqResourceUri::ALL.into_iter().map(resource_descriptor).collect())
}

pub fn list_resource_templates() -> ListResourceTemplatesResult {
    ListResourceTemplatesResult::default()
}

fn resource_descriptor(uri: RocketmqResourceUri) -> Resource {
    Resource::new(uri.as_str(), uri.name())
        .with_title(uri.title())
        .with_description(uri.description())
        .with_mime_type(JSON_MIME_TYPE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mvp_registry_lists_static_resources() {
        let result = list_resources();
        let uris = result
            .resources
            .iter()
            .map(|resource| resource.uri.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            uris,
            [
                "rocketmq://cluster/overview",
                "rocketmq://topics",
                "rocketmq://brokers",
                "rocketmq://consumer-groups",
            ]
        );
        assert!(result.next_cursor.is_none());
        assert!(result.resources.iter().all(|resource| {
            resource.mime_type.as_deref() == Some(JSON_MIME_TYPE)
                && resource.title.is_some()
                && resource.description.is_some()
        }));
    }

    #[test]
    fn mvp_registry_has_no_resource_templates() {
        let result = list_resource_templates();

        assert!(result.resource_templates.is_empty());
        assert!(result.next_cursor.is_none());
    }
}
