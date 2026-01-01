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

use rocketmq_common::common::mix_all;
use rocketmq_common::common::topic::TopicValidator;

pub struct NamespaceUtil;

impl NamespaceUtil {
    pub const DLQ_PREFIX_LENGTH: usize = mix_all::DLQ_GROUP_TOPIC_PREFIX.len();
    pub const NAMESPACE_SEPARATOR: char = '%';
    pub const RETRY_PREFIX_LENGTH: usize = mix_all::RETRY_GROUP_TOPIC_PREFIX.len();
    pub const STRING_BLANK: &'static str = "";

    pub fn without_namespace(resource_with_namespace: &str) -> String {
        if resource_with_namespace.is_empty() || NamespaceUtil::is_system_resource(resource_with_namespace) {
            return resource_with_namespace.to_string();
        }

        let mut string_builder = String::new();
        if NamespaceUtil::is_retry_topic(resource_with_namespace) {
            string_builder.push_str(mix_all::RETRY_GROUP_TOPIC_PREFIX);
        }
        if NamespaceUtil::is_dlq_topic(resource_with_namespace) {
            string_builder.push_str(mix_all::DLQ_GROUP_TOPIC_PREFIX);
        }

        if let Some(index) =
            NamespaceUtil::without_retry_and_dlq(resource_with_namespace).find(NamespaceUtil::NAMESPACE_SEPARATOR)
        {
            let resource_without_namespace =
                &NamespaceUtil::without_retry_and_dlq(resource_with_namespace)[index + 1..];
            return string_builder + resource_without_namespace;
        }

        resource_with_namespace.to_string()
    }

    pub fn without_namespace_with_namespace(resource_with_namespace: &str, namespace: &str) -> String {
        if resource_with_namespace.is_empty() || namespace.is_empty() {
            return resource_with_namespace.to_string();
        }

        let resource_without_retry_and_dlq = NamespaceUtil::without_retry_and_dlq(resource_with_namespace);
        if resource_without_retry_and_dlq.starts_with(&format!("{}{}", namespace, NamespaceUtil::NAMESPACE_SEPARATOR)) {
            return NamespaceUtil::without_namespace(resource_with_namespace);
        }

        resource_with_namespace.to_string()
    }

    pub fn wrap_namespace(namespace: &str, resource_without_namespace: &str) -> String {
        if namespace.is_empty() || resource_without_namespace.is_empty() {
            return resource_without_namespace.to_string();
        }

        if NamespaceUtil::is_system_resource(resource_without_namespace)
            || NamespaceUtil::is_already_with_namespace(resource_without_namespace, namespace)
        {
            return resource_without_namespace.to_string();
        }

        let mut string_builder = String::new();

        if NamespaceUtil::is_retry_topic(resource_without_namespace) {
            string_builder.push_str(mix_all::RETRY_GROUP_TOPIC_PREFIX);
        }

        if NamespaceUtil::is_dlq_topic(resource_without_namespace) {
            string_builder.push_str(mix_all::DLQ_GROUP_TOPIC_PREFIX);
        }
        let resource_without_retry_and_dlq = NamespaceUtil::without_retry_and_dlq(resource_without_namespace);
        string_builder + namespace + &NamespaceUtil::NAMESPACE_SEPARATOR.to_string() + resource_without_retry_and_dlq
    }

    pub fn is_already_with_namespace(resource: &str, namespace: &str) -> bool {
        if namespace.is_empty() || resource.is_empty() || NamespaceUtil::is_system_resource(resource) {
            return false;
        }

        let resource_without_retry_and_dlq = NamespaceUtil::without_retry_and_dlq(resource);

        resource_without_retry_and_dlq.starts_with(&format!("{}{}", namespace, NamespaceUtil::NAMESPACE_SEPARATOR))
    }

    pub fn wrap_namespace_and_retry(namespace: &str, consumer_group: &str) -> Option<String> {
        if consumer_group.is_empty() {
            return None;
        }

        Some(mix_all::RETRY_GROUP_TOPIC_PREFIX.to_string() + &NamespaceUtil::wrap_namespace(namespace, consumer_group))
    }

    pub fn get_namespace_from_resource(resource: &str) -> String {
        if resource.is_empty() || NamespaceUtil::is_system_resource(resource) {
            return NamespaceUtil::STRING_BLANK.to_string();
        }
        let resource_without_retry_and_dlq = NamespaceUtil::without_retry_and_dlq(resource);
        if let Some(index) = resource_without_retry_and_dlq.find(NamespaceUtil::NAMESPACE_SEPARATOR) {
            return resource_without_retry_and_dlq[..index].to_string();
        }

        NamespaceUtil::STRING_BLANK.to_string()
    }

    fn without_retry_and_dlq(original_resource: &str) -> &str {
        if original_resource.is_empty() {
            return NamespaceUtil::STRING_BLANK;
        }
        if NamespaceUtil::is_retry_topic(original_resource) {
            return &original_resource[NamespaceUtil::RETRY_PREFIX_LENGTH..];
        }

        if NamespaceUtil::is_dlq_topic(original_resource) {
            return &original_resource[NamespaceUtil::DLQ_PREFIX_LENGTH..];
        }

        original_resource
    }

    fn is_system_resource(resource: &str) -> bool {
        if resource.is_empty() {
            return false;
        }
        TopicValidator::is_system_topic(resource) || mix_all::is_sys_consumer_group(resource)
    }

    #[inline]
    pub fn is_retry_topic(resource: &str) -> bool {
        !resource.is_empty() && resource.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
    }

    fn is_dlq_topic(resource: &str) -> bool {
        !resource.is_empty() && resource.starts_with(mix_all::DLQ_GROUP_TOPIC_PREFIX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_namespace_returns_original_when_empty() {
        assert_eq!(NamespaceUtil::without_namespace(""), "");
    }

    #[test]
    fn without_namespace_returns_original_when_system_resource() {
        assert_eq!(NamespaceUtil::without_namespace("SYS_TOPIC"), "SYS_TOPIC");
    }

    #[test]
    fn without_namespace_removes_namespace() {
        assert_eq!(
            NamespaceUtil::without_namespace("my_namespace%my_resource"),
            "my_resource"
        );
    }

    #[test]
    fn without_namespace_with_namespace_returns_original_when_empty() {
        assert_eq!(NamespaceUtil::without_namespace_with_namespace("", "my_namespace"), "");
    }

    #[test]
    fn without_namespace_with_namespace_removes_namespace() {
        assert_eq!(
            NamespaceUtil::without_namespace_with_namespace("my_namespace%my_resource", "my_namespace"),
            "my_resource"
        );
    }

    #[test]
    fn wrap_namespace_returns_original_when_empty() {
        assert_eq!(NamespaceUtil::wrap_namespace("my_namespace", ""), "");
    }

    #[test]
    fn wrap_namespace_adds_namespace() {
        assert_eq!(
            NamespaceUtil::wrap_namespace("my_namespace", "my_resource"),
            "my_namespace%my_resource"
        );
    }

    #[test]
    fn is_already_with_namespace_returns_false_when_empty() {
        assert!(!NamespaceUtil::is_already_with_namespace("", "my_namespace"));
    }

    #[test]
    fn is_already_with_namespace_returns_true_when_with_namespace() {
        assert!(NamespaceUtil::is_already_with_namespace(
            "my_namespace%my_resource",
            "my_namespace"
        ));
    }

    #[test]
    fn wrap_namespace_and_retry_returns_none_when_empty() {
        assert_eq!(NamespaceUtil::wrap_namespace_and_retry("my_namespace", ""), None);
    }

    #[test]
    fn wrap_namespace_and_retry_adds_namespace_and_retry() {
        assert_eq!(
            NamespaceUtil::wrap_namespace_and_retry("my_namespace", "my_group"),
            Some("%RETRY%my_namespace%my_group".to_string())
        );
    }

    #[test]
    fn get_namespace_from_resource_returns_blank_when_empty() {
        assert_eq!(NamespaceUtil::get_namespace_from_resource(""), "");
    }

    #[test]
    fn get_namespace_from_resource_returns_namespace() {
        assert_eq!(
            NamespaceUtil::get_namespace_from_resource("my_namespace%my_resource"),
            "my_namespace"
        );
    }

    #[test]
    fn without_retry_and_dlq_returns_original_when_empty() {
        assert_eq!(NamespaceUtil::without_retry_and_dlq(""), "");
    }

    #[test]
    fn without_retry_and_dlq_removes_retry_and_dlq() {
        assert_eq!(
            NamespaceUtil::without_retry_and_dlq("RETRY_GROUP_TOPIC_PREFIXmy_resource"),
            "RETRY_GROUP_TOPIC_PREFIXmy_resource"
        );
        assert_eq!(
            NamespaceUtil::without_retry_and_dlq("DLQ_GROUP_TOPIC_PREFIXmy_resource"),
            "DLQ_GROUP_TOPIC_PREFIXmy_resource"
        );
    }

    #[test]
    fn is_system_resource_returns_false_when_empty() {
        assert!(!NamespaceUtil::is_system_resource(""));
    }

    #[test]
    fn is_system_resource_returns_true_when_system_resource() {
        assert!(NamespaceUtil::is_system_resource("CID_RMQ_SYS_"));
        assert!(NamespaceUtil::is_system_resource("TBW102"));
    }

    #[test]
    fn is_retry_topic_returns_false_when_empty() {
        assert!(!NamespaceUtil::is_retry_topic(""));
    }

    #[test]
    fn is_retry_topic_returns_true_when_retry_topic() {
        assert!(!NamespaceUtil::is_retry_topic("RETRY_GROUP_TOPIC_PREFIXmy_topic"));
        assert!(NamespaceUtil::is_retry_topic("%RETRY%RETRY_GROUP_TOPIC_PREFIXmy_topic"));
    }

    #[test]
    fn is_dlq_topic_returns_false_when_empty() {
        assert!(!NamespaceUtil::is_dlq_topic(""));
    }

    #[test]
    fn is_dlq_topic_returns_true_when_dlq_topic() {
        assert!(!NamespaceUtil::is_dlq_topic("DLQ_GROUP_TOPIC_PREFIXmy_topic"));
        assert!(NamespaceUtil::is_dlq_topic("%DLQ%DLQ_GROUP_TOPIC_PREFIXmy_topic"));
    }
}
