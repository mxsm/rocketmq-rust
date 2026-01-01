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
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct PolicyEntryInfo {
    pub resource: Option<CheetahString>,
    pub actions: Option<CheetahString>,
    pub source_ips: Option<Vec<CheetahString>>,
    pub decision: Option<CheetahString>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct PolicyInfo {
    pub policy_type: Option<CheetahString>,
    pub entries: Option<Vec<PolicyEntryInfo>>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct AclInfo {
    pub subject: Option<CheetahString>,
    pub policies: Option<Vec<PolicyInfo>>,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn policy_entry_info_default_values() {
        let policy_entry = PolicyEntryInfo::default();
        assert!(policy_entry.resource.is_none());
        assert!(policy_entry.actions.is_none());
        assert!(policy_entry.source_ips.is_none());
        assert!(policy_entry.decision.is_none());
    }

    #[test]
    fn policy_entry_info_with_values() {
        let policy_entry = PolicyEntryInfo {
            resource: Some(CheetahString::from("resource")),
            actions: Some(CheetahString::from("actions")),
            source_ips: Some(vec![CheetahString::from("192.168.1.1")]),
            decision: Some(CheetahString::from("allow")),
        };
        assert_eq!(policy_entry.resource, Some(CheetahString::from("resource")));
        assert_eq!(policy_entry.actions, Some(CheetahString::from("actions")));
        assert_eq!(policy_entry.source_ips, Some(vec![CheetahString::from("192.168.1.1")]));
        assert_eq!(policy_entry.decision, Some(CheetahString::from("allow")));
    }

    #[test]
    fn serialize_policy_entry_info() {
        let policy_entry = PolicyEntryInfo {
            resource: Some(CheetahString::from("resource")),
            actions: Some(CheetahString::from("actions")),
            source_ips: Some(vec![CheetahString::from("192.168.1.1")]),
            decision: Some(CheetahString::from("allow")),
        };
        let serialized = serde_json::to_string(&policy_entry).unwrap();
        assert!(serialized.contains("\"resource\":\"resource\""));
        assert!(serialized.contains("\"actions\":\"actions\""));
        assert!(serialized.contains("\"sourceIps\":[\"192.168.1.1\"]"));
        assert!(serialized.contains("\"decision\":\"allow\""));
    }

    #[test]
    fn deserialize_policy_entry_info() {
        let json = r#"{
            "resource": "resource",
            "actions": "actions",
            "sourceIps": ["192.168.1.1"],
            "decision": "allow"
        }"#;
        let deserialized: PolicyEntryInfo = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.resource, Some(CheetahString::from("resource")));
        assert_eq!(deserialized.actions, Some(CheetahString::from("actions")));
        assert_eq!(deserialized.source_ips, Some(vec![CheetahString::from("192.168.1.1")]));
        assert_eq!(deserialized.decision, Some(CheetahString::from("allow")));
    }

    #[test]
    fn deserialize_policy_entry_info_missing_optional_fields() {
        let json = r#"{}"#;
        let deserialized: PolicyEntryInfo = serde_json::from_str(json).unwrap();
        assert!(deserialized.resource.is_none());
        assert!(deserialized.actions.is_none());
        assert!(deserialized.source_ips.is_none());
        assert!(deserialized.decision.is_none());
    }

    #[test]
    fn policy_info_default_values() {
        let policy_info = PolicyInfo::default();
        assert!(policy_info.policy_type.is_none());
        assert!(policy_info.entries.is_none());
    }

    #[test]
    fn policy_info_with_values() {
        let policy_info = PolicyInfo {
            policy_type: Some(CheetahString::from("type")),
            entries: Some(vec![PolicyEntryInfo::default()]),
        };
        assert_eq!(policy_info.policy_type, Some(CheetahString::from("type")));
        assert_eq!(policy_info.entries.unwrap().len(), 1);
    }

    #[test]
    fn serialize_policy_info() {
        let policy_info = PolicyInfo {
            policy_type: Some(CheetahString::from("type")),
            entries: Some(vec![PolicyEntryInfo::default()]),
        };
        let serialized = serde_json::to_string(&policy_info).unwrap();
        assert!(serialized.contains("\"policyType\":\"type\""));
        assert!(serialized.contains("\"entries\":["));
    }

    #[test]
    fn deserialize_policy_info() {
        let json = r#"{
            "policyType": "type",
            "entries": [{}]
        }"#;
        let deserialized: PolicyInfo = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.policy_type, Some(CheetahString::from("type")));
        assert_eq!(deserialized.entries.unwrap().len(), 1);
    }

    #[test]
    fn deserialize_policy_info_missing_optional_fields() {
        let json = r#"{}"#;
        let deserialized: PolicyInfo = serde_json::from_str(json).unwrap();
        assert!(deserialized.policy_type.is_none());
        assert!(deserialized.entries.is_none());
    }

    #[test]
    fn acl_info_default_values() {
        let acl_info = AclInfo::default();
        assert!(acl_info.subject.is_none());
        assert!(acl_info.policies.is_none());
    }

    #[test]
    fn acl_info_with_values() {
        let acl_info = AclInfo {
            subject: Some(CheetahString::from("subject")),
            policies: Some(vec![PolicyInfo::default()]),
        };
        assert_eq!(acl_info.subject, Some(CheetahString::from("subject")));
        assert_eq!(acl_info.policies.unwrap().len(), 1);
    }

    #[test]
    fn serialize_acl_info() {
        let acl_info = AclInfo {
            subject: Some(CheetahString::from("subject")),
            policies: Some(vec![PolicyInfo::default()]),
        };
        let serialized = serde_json::to_string(&acl_info).unwrap();
        assert!(serialized.contains("\"subject\":\"subject\""));
        assert!(serialized.contains("\"policies\":["));
    }

    #[test]
    fn deserialize_acl_info() {
        let json = r#"{
            "subject": "subject",
            "policies": [{}]
        }"#;
        let deserialized: AclInfo = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.subject, Some(CheetahString::from("subject")));
        assert_eq!(deserialized.policies.unwrap().len(), 1);
    }

    #[test]
    fn deserialize_acl_info_missing_optional_fields() {
        let json = r#"{}"#;
        let deserialized: AclInfo = serde_json::from_str(json).unwrap();
        assert!(deserialized.subject.is_none());
        assert!(deserialized.policies.is_none());
    }
}
