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

#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct DeleteAclRequestHeader {
    pub subject: CheetahString,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<CheetahString>,
}

impl DeleteAclRequestHeader {
    pub fn new(subject: CheetahString, resource: Option<CheetahString>) -> Self {
        Self { subject, resource }
    }

    pub fn with_subject(subject: CheetahString) -> Self {
        Self {
            subject,
            resource: None,
        }
    }

    pub fn set_subject(&mut self, subject: CheetahString) {
        self.subject = subject;
    }

    pub fn set_resource(&mut self, resource: Option<CheetahString>) {
        self.resource = resource;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn delete_acl_request_header_new() {
        let subject = CheetahString::from_static_str("user:alice");
        let resource = Some(CheetahString::from_static_str("Topic:test-topic"));
        let header = DeleteAclRequestHeader::new(subject.clone(), resource.clone());

        assert_eq!(header.subject, subject);
        assert_eq!(header.resource, resource);
    }

    #[test]
    fn delete_acl_request_header_with_subject() {
        let subject = CheetahString::from_static_str("user:bob");
        let header = DeleteAclRequestHeader::with_subject(subject.clone());

        assert_eq!(header.subject, subject);
        assert!(header.resource.is_none());
    }

    #[test]
    fn delete_acl_request_header_default() {
        let header = DeleteAclRequestHeader::default();

        assert!(header.subject.is_empty());
        assert!(header.resource.is_none());
    }

    #[test]
    fn delete_acl_request_header_set_methods() {
        let mut header = DeleteAclRequestHeader::default();
        let subject = CheetahString::from_static_str("user:charlie");
        let resource = Some(CheetahString::from_static_str("Group:consumer-group"));

        header.set_subject(subject.clone());
        header.set_resource(resource.clone());

        assert_eq!(header.subject, subject);
        assert_eq!(header.resource, resource);
    }

    #[test]
    fn delete_acl_request_header_serializes_correctly() {
        let header = DeleteAclRequestHeader::new(
            CheetahString::from_static_str("user:alice"),
            Some(CheetahString::from_static_str("Topic:test")),
        );

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("subject")),
            Some(&CheetahString::from_static_str("user:alice"))
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("resource")),
            Some(&CheetahString::from_static_str("Topic:test"))
        );
    }

    #[test]
    fn delete_acl_request_header_deserializes_correctly() {
        let mut map: HashMap<CheetahString, CheetahString> = HashMap::new();
        map.insert(
            CheetahString::from_static_str("subject"),
            CheetahString::from_static_str("user:alice"),
        );
        map.insert(
            CheetahString::from_static_str("resource"),
            CheetahString::from_static_str("Topic:test"),
        );

        let header = <DeleteAclRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.subject, CheetahString::from_static_str("user:alice"));
        assert_eq!(header.resource, Some(CheetahString::from_static_str("Topic:test")));
    }
}
