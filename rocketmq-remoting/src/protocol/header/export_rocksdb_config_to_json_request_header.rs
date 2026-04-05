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

const CONFIG_TYPE_SEPARATOR: char = ';';

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExportRocksdbConfigType {
    Topics,
    SubscriptionGroups,
    ConsumerOffsets,
}

impl ExportRocksdbConfigType {
    pub fn parse_many(config_type: &str) -> Result<Vec<Self>, String> {
        config_type
            .split(CONFIG_TYPE_SEPARATOR)
            .filter(|segment| !segment.trim().is_empty())
            .map(|segment| match segment.trim().to_ascii_lowercase().as_str() {
                "topics" => Ok(Self::Topics),
                "subscriptiongroups" => Ok(Self::SubscriptionGroups),
                "consumeroffsets" => Ok(Self::ConsumerOffsets),
                other => Err(format!("unknown config type: {other}")),
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct ExportRocksdbConfigToJsonRequestHeader {
    #[required]
    pub config_type: CheetahString,
}

impl ExportRocksdbConfigToJsonRequestHeader {
    pub fn fetch_config_type(&self) -> Result<Vec<ExportRocksdbConfigType>, String> {
        ExportRocksdbConfigType::parse_many(self.config_type.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn export_rocksdb_config_to_json_request_header_parses_from_map() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("configType"),
            CheetahString::from_static_str("topics;subscriptionGroups;consumerOffsets;"),
        );

        let header = <ExportRocksdbConfigToJsonRequestHeader as FromMap>::from(&map)
            .expect("header should decode from ext fields");

        assert_eq!(
            header.fetch_config_type().expect("config types should parse"),
            vec![
                ExportRocksdbConfigType::Topics,
                ExportRocksdbConfigType::SubscriptionGroups,
                ExportRocksdbConfigType::ConsumerOffsets,
            ]
        );
    }
}
