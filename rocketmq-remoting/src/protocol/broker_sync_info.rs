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

use std::fmt::Display;
use std::fmt::Formatter;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerSyncInfo {
    /// For slave online sync, retrieve HA address before register
    pub master_ha_address: Option<CheetahString>,

    /// Master flush offset
    pub master_flush_offset: i64,

    /// Master address
    pub master_address: Option<CheetahString>,
}

impl Display for BrokerSyncInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BrokerSyncInfo {{ master_ha_address: {}, master_flush_offset: {}, master_address: {} }}",
            match &self.master_ha_address {
                Some(addr) => addr.as_str(),
                None => "None",
            },
            self.master_flush_offset,
            match &self.master_address {
                Some(addr) => addr.as_str(),
                None => "None",
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn creates_default_broker_sync_info() {
        let info = BrokerSyncInfo::default();

        assert!(info.master_ha_address.is_none());
        assert_eq!(info.master_flush_offset, 0);
        assert!(info.master_address.is_none());
    }

    #[test]
    fn creates_broker_sync_info_with_all_fields() {
        let info = BrokerSyncInfo {
            master_ha_address: Some(CheetahString::from("192.168.1.1:10912")),
            master_flush_offset: 12345,
            master_address: Some(CheetahString::from("192.168.1.1:10911")),
        };

        assert_eq!(info.master_ha_address.unwrap(), "192.168.1.1:10912");
        assert_eq!(info.master_flush_offset, 12345);
        assert_eq!(info.master_address.unwrap(), "192.168.1.1:10911");
    }

    #[test]
    fn serializes_to_camel_case_json() {
        let info = BrokerSyncInfo {
            master_ha_address: Some(CheetahString::from("ha.address")),
            master_flush_offset: 100,
            master_address: Some(CheetahString::from("master.address")),
        };

        let json = serde_json::to_string(&info).unwrap();

        assert!(json.contains("masterHaAddress"));
        assert!(json.contains("masterFlushOffset"));
        assert!(json.contains("masterAddress"));
    }

    #[test]
    fn deserializes_from_camel_case_json() {
        let json = r#"{"masterHaAddress":"ha.address","masterFlushOffset":200,"masterAddress":"master.address"}"#;

        let info: BrokerSyncInfo = serde_json::from_str(json).unwrap();

        assert_eq!(info.master_ha_address.unwrap(), "ha.address");
        assert_eq!(info.master_flush_offset, 200);
        assert_eq!(info.master_address.unwrap(), "master.address");
    }

    #[test]
    fn deserializes_with_null_addresses() {
        let json = r#"{"masterHaAddress":null,"masterFlushOffset":300,"masterAddress":null}"#;

        let info: BrokerSyncInfo = serde_json::from_str(json).unwrap();

        assert!(info.master_ha_address.is_none());
        assert_eq!(info.master_flush_offset, 300);
        assert!(info.master_address.is_none());
    }

    #[test]
    fn handles_negative_flush_offset() {
        let info = BrokerSyncInfo {
            master_ha_address: None,
            master_flush_offset: -1,
            master_address: None,
        };

        assert_eq!(info.master_flush_offset, -1);
    }

    #[test]
    fn clones_broker_sync_info() {
        let original = BrokerSyncInfo {
            master_ha_address: Some(CheetahString::from("original")),
            master_flush_offset: 500,
            master_address: Some(CheetahString::from("master")),
        };

        let cloned = original.clone();

        assert_eq!(original.master_ha_address, cloned.master_ha_address);
        assert_eq!(original.master_flush_offset, cloned.master_flush_offset);
        assert_eq!(original.master_address, cloned.master_address);
    }

    #[test]
    fn displays_all_fields_correctly() {
        let info = BrokerSyncInfo {
            master_ha_address: Some(CheetahString::from("127.0.0.1:10000")),
            master_flush_offset: 42,
            master_address: Some(CheetahString::from("127.0.0.1:10001")),
        };
        let display = format!("{}", info);
        assert!(display.contains("master_ha_address: 127.0.0.1:10000"));
        assert!(display.contains("master_flush_offset: 42"));
        assert!(display.contains("master_address: 127.0.0.1:10001"));
    }

    #[test]
    fn displays_none_for_missing_addresses() {
        let info = BrokerSyncInfo {
            master_ha_address: None,
            master_flush_offset: 0,
            master_address: None,
        };
        let display = format!("{}", info);
        assert!(display.contains("master_ha_address: None"));
        assert!(display.contains("master_address: None"));
    }

    #[test]
    fn displays_mixed_some_and_none_addresses() {
        let info = BrokerSyncInfo {
            master_ha_address: Some(CheetahString::from("ha.addr")),
            master_flush_offset: 123,
            master_address: None,
        };
        let display = format!("{}", info);
        assert!(display.contains("master_ha_address: ha.addr"));
        assert!(display.contains("master_address: None"));
    }
}
