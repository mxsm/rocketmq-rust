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

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
pub struct TopicRequestHeader {
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
    pub lo: Option<bool>,
}

impl TopicRequestHeader {
    pub fn get_lo(&self) -> Option<&bool> {
        self.lo.as_ref()
    }

    pub fn set_lo(&mut self, lo: bool) {
        self.lo = Some(lo);
    }

    pub fn get_broker_name(&self) -> Option<&CheetahString> {
        self.rpc_request_header.as_ref().and_then(|v| v.broker_name.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn get_lo_returns_none_when_lo_is_none() {
        let header = TopicRequestHeader::default();
        assert_eq!(header.get_lo(), None);
    }

    #[test]
    fn get_lo_returns_some_when_lo_is_set() {
        let mut header = TopicRequestHeader::default();
        header.set_lo(true);
        assert_eq!(header.get_lo(), Some(&true));
    }

    #[test]
    fn set_lo_updates_lo_value() {
        let mut header = TopicRequestHeader::default();
        header.set_lo(false);
        assert_eq!(header.get_lo(), Some(&false));
    }

    #[test]
    fn get_broker_name_returns_none_when_rpc_request_header_is_none() {
        let header = TopicRequestHeader::default();
        assert_eq!(header.get_broker_name(), None);
    }

    #[test]
    fn get_broker_name_returns_none_when_broker_name_is_none() {
        let rpc_header = RpcRequestHeader {
            broker_name: None,
            ..Default::default()
        };
        let header = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            ..Default::default()
        };
        assert_eq!(header.get_broker_name(), None);
    }

    #[test]
    fn get_broker_name_returns_some_when_broker_name_is_set() {
        let broker_name = CheetahString::from("TestBroker");
        let rpc_header = RpcRequestHeader {
            broker_name: Some(broker_name.clone()),
            ..Default::default()
        };
        let header = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            ..Default::default()
        };
        assert_eq!(header.get_broker_name(), Some(&broker_name));
    }
}
