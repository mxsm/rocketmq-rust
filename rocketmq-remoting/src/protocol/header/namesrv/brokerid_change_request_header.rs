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
pub struct NotifyMinBrokerIdChangeRequestHeader {
    #[serde(rename = "minBrokerId")]
    pub min_broker_id: Option<u64>,

    #[serde(rename = "brokerName")]
    pub broker_name: Option<CheetahString>,

    #[serde(rename = "minBrokerAddr")]
    pub min_broker_addr: Option<CheetahString>,

    #[serde(rename = "offlineBrokerAddr")]
    pub offline_broker_addr: Option<CheetahString>,

    #[serde(rename = "haBrokerAddr")]
    pub ha_broker_addr: Option<CheetahString>,
}

impl NotifyMinBrokerIdChangeRequestHeader {
    pub fn new(
        min_broker_id: Option<u64>,
        broker_name: Option<CheetahString>,
        min_broker_addr: Option<CheetahString>,
        offline_broker_addr: Option<CheetahString>,
        ha_broker_addr: Option<CheetahString>,
    ) -> Self {
        NotifyMinBrokerIdChangeRequestHeader {
            min_broker_id,
            broker_name,
            min_broker_addr,
            offline_broker_addr,
            ha_broker_addr,
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn new_creates_instance_with_all_fields() {
        let header = NotifyMinBrokerIdChangeRequestHeader::new(
            Some(1),
            Some(CheetahString::from("broker1")),
            Some(CheetahString::from("addr1")),
            Some(CheetahString::from("addr2")),
            Some(CheetahString::from("addr3")),
        );
        assert_eq!(header.min_broker_id, Some(1));
        assert_eq!(header.broker_name.as_deref(), Some("broker1"));
        assert_eq!(header.min_broker_addr.as_deref(), Some("addr1"));
        assert_eq!(header.offline_broker_addr.as_deref(), Some("addr2"));
        assert_eq!(header.ha_broker_addr.as_deref(), Some("addr3"));
    }

    #[test]
    fn new_creates_instance_with_none_fields() {
        let header = NotifyMinBrokerIdChangeRequestHeader::new(None, None, None, None, None);
        assert_eq!(header.min_broker_id, None);
        assert_eq!(header.broker_name, None);
        assert_eq!(header.min_broker_addr, None);
        assert_eq!(header.offline_broker_addr, None);
        assert_eq!(header.ha_broker_addr, None);
    }

    #[test]
    fn default_creates_instance_with_none_fields() {
        let header: NotifyMinBrokerIdChangeRequestHeader = Default::default();
        assert_eq!(header.min_broker_id, None);
        assert_eq!(header.broker_name, None);
        assert_eq!(header.min_broker_addr, None);
        assert_eq!(header.offline_broker_addr, None);
        assert_eq!(header.ha_broker_addr, None);
    }
}
