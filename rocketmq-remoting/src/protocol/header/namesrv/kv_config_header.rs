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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
pub struct PutKVConfigRequestHeader {
    #[required]
    pub namespace: CheetahString,

    #[required]
    pub key: CheetahString,

    #[required]
    pub value: CheetahString,
}

impl PutKVConfigRequestHeader {
    /// Creates a new instance of `PutKVConfigRequestHeader`.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace.
    /// * `key` - The key.
    /// * `value` - The value.
    pub fn new(
        namespace: impl Into<CheetahString>,
        key: impl Into<CheetahString>,
        value: impl Into<CheetahString>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
pub struct GetKVConfigRequestHeader {
    #[required]
    pub namespace: CheetahString,

    #[required]
    pub key: CheetahString,
}

impl GetKVConfigRequestHeader {
    pub fn new(namespace: impl Into<CheetahString>, key: impl Into<CheetahString>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
pub struct GetKVConfigResponseHeader {
    #[required]
    pub value: Option<CheetahString>,
}

impl GetKVConfigResponseHeader {
    pub fn new(value: Option<CheetahString>) -> Self {
        Self { value }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
pub struct DeleteKVConfigRequestHeader {
    #[required]
    pub namespace: CheetahString,

    #[required]
    pub key: CheetahString,
}

impl DeleteKVConfigRequestHeader {
    pub fn new(namespace: impl Into<CheetahString>, key: impl Into<CheetahString>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
pub struct GetKVListByNamespaceRequestHeader {
    #[required]
    pub namespace: CheetahString,
}

impl GetKVListByNamespaceRequestHeader {
    pub fn new(namespace: impl Into<CheetahString>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn put_kv_config_request_header_new() {
        let header = PutKVConfigRequestHeader::new("namespace1", "key1", "value1");
        assert_eq!(header.namespace, CheetahString::from("namespace1"));
        assert_eq!(header.key, CheetahString::from("key1"));
        assert_eq!(header.value, CheetahString::from("value1"));
    }

    #[test]
    fn put_kv_config_request_header_serialization() {
        let header = PutKVConfigRequestHeader::new("namespace1", "key1", "value1");
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(
            serialized,
            r#"{"namespace":"namespace1","key":"key1","value":"value1"}"#
        );
    }

    #[test]
    fn put_kv_config_request_header_deserialization() {
        let json = r#"{"namespace":"namespace1","key":"key1","value":"value1"}"#;
        let deserialized: PutKVConfigRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.namespace, CheetahString::from("namespace1"));
        assert_eq!(deserialized.key, CheetahString::from("key1"));
        assert_eq!(deserialized.value, CheetahString::from("value1"));
    }

    #[test]
    fn get_kv_config_request_header_new() {
        let header = GetKVConfigRequestHeader::new("namespace1", "key1");
        assert_eq!(header.namespace, CheetahString::from("namespace1"));
        assert_eq!(header.key, CheetahString::from("key1"));
    }

    #[test]
    fn get_kv_config_request_header_serialization() {
        let header = GetKVConfigRequestHeader::new("namespace1", "key1");
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"namespace":"namespace1","key":"key1"}"#);
    }

    #[test]
    fn get_kv_config_request_header_deserialization() {
        let json = r#"{"namespace":"namespace1","key":"key1"}"#;
        let deserialized: GetKVConfigRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.namespace, CheetahString::from("namespace1"));
        assert_eq!(deserialized.key, CheetahString::from("key1"));
    }

    #[test]
    fn get_kv_config_response_header_new() {
        let header = GetKVConfigResponseHeader::new(Some(CheetahString::from("value1")));
        assert_eq!(header.value, Some(CheetahString::from("value1")));
    }

    #[test]
    fn get_kv_config_response_header_serialization() {
        let header = GetKVConfigResponseHeader::new(Some(CheetahString::from("value1")));
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"value":"value1"}"#);
    }

    #[test]
    fn get_kv_config_response_header_deserialization() {
        let json = r#"{"value":"value1"}"#;
        let deserialized: GetKVConfigResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.value, Some(CheetahString::from("value1")));
    }

    #[test]
    fn delete_kv_config_request_header_new() {
        let header = DeleteKVConfigRequestHeader::new("namespace1", "key1");
        assert_eq!(header.namespace, CheetahString::from("namespace1"));
        assert_eq!(header.key, CheetahString::from("key1"));
    }

    #[test]
    fn delete_kv_config_request_header_serialization() {
        let header = DeleteKVConfigRequestHeader::new("namespace1", "key1");
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"namespace":"namespace1","key":"key1"}"#);
    }

    #[test]
    fn delete_kv_config_request_header_deserialization() {
        let json = r#"{"namespace":"namespace1","key":"key1"}"#;
        let deserialized: DeleteKVConfigRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.namespace, CheetahString::from("namespace1"));
        assert_eq!(deserialized.key, CheetahString::from("key1"));
    }

    #[test]
    fn get_kv_list_by_namespace_request_header_new() {
        let header = GetKVListByNamespaceRequestHeader::new("namespace1");
        assert_eq!(header.namespace, CheetahString::from("namespace1"));
    }

    #[test]
    fn get_kv_list_by_namespace_request_header_serialization() {
        let header = GetKVListByNamespaceRequestHeader::new("namespace1");
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"namespace":"namespace1"}"#);
    }

    #[test]
    fn get_kv_list_by_namespace_request_header_deserialization() {
        let json = r#"{"namespace":"namespace1"}"#;
        let deserialized: GetKVListByNamespaceRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.namespace, CheetahString::from("namespace1"));
    }
}
