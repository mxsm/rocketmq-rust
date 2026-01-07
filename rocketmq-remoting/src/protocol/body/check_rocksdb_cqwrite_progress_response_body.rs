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
pub struct ClusterAclVersionInfo {
    pub diff_result: Option<CheetahString>,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn cluster_acl_version_info_default_values() {
        let info = ClusterAclVersionInfo::default();
        assert!(info.diff_result.is_none());
    }

    #[test]
    fn cluster_acl_version_info_with_diff_result() {
        let info = ClusterAclVersionInfo {
            diff_result: Some(CheetahString::from("diff")),
        };
        assert_eq!(info.diff_result, Some(CheetahString::from("diff")));
    }

    #[test]
    fn serialize_cluster_acl_version_info() {
        let info = ClusterAclVersionInfo {
            diff_result: Some(CheetahString::from("diff")),
        };
        let serialized = serde_json::to_string(&info).unwrap();
        assert_eq!(serialized, r#"{"diffResult":"diff"}"#);
    }

    #[test]
    fn deserialize_cluster_acl_version_info() {
        let json = r#"{"diffResult":"diff"}"#;
        let deserialized: ClusterAclVersionInfo = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.diff_result, Some(CheetahString::from("diff")));
    }

    #[test]
    fn deserialize_cluster_acl_version_info_missing_diff_result() {
        let json = r#"{}"#;
        let deserialized: ClusterAclVersionInfo = serde_json::from_str(json).unwrap();
        assert!(deserialized.diff_result.is_none());
    }
}
