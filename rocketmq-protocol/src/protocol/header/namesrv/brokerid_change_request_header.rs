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
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.NotifyMinBrokerIdChangeRequestHeader"
)]
pub struct NotifyMinBrokerIdChangeRequestHeader {
    #[serde(rename = "minBrokerId")]
    #[header(range = "i64")]
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
    use super::*;

    #[test]
    fn serde_preserves_the_broker_change_wire_fields() {
        let header = NotifyMinBrokerIdChangeRequestHeader::new(
            Some(1),
            Some(CheetahString::from("broker-a")),
            Some(CheetahString::from("127.0.0.1:10911")),
            Some(CheetahString::from("127.0.0.2:10911")),
            Some(CheetahString::from("127.0.0.1:10912")),
        );

        let json = serde_json::to_string(&header).unwrap();
        let decoded: NotifyMinBrokerIdChangeRequestHeader = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.min_broker_id, Some(1));
        assert_eq!(decoded.broker_name.as_deref(), Some("broker-a"));
        assert_eq!(decoded.min_broker_addr.as_deref(), Some("127.0.0.1:10911"));
        assert_eq!(decoded.offline_broker_addr.as_deref(), Some("127.0.0.2:10911"));
        assert_eq!(decoded.ha_broker_addr.as_deref(), Some("127.0.0.1:10912"));

        let empty = NotifyMinBrokerIdChangeRequestHeader::new(None, None, None, None, None);
        assert!(empty.min_broker_id.is_none());
        assert!(empty.broker_name.is_none());
        assert!(empty.min_broker_addr.is_none());
        assert!(empty.offline_broker_addr.is_none());
        assert!(empty.ha_broker_addr.is_none());
    }
}
