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

use std::fmt;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::heartbeat::broker_identity_info::BrokerIdentityInfo;
use crate::heartbeat::broker_live_info::BrokerLiveInfo;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RaftBrokerHeartBeatEventRequest {
    // brokerIdentityInfo fields
    cluster_name_identity_info: Option<CheetahString>,
    broker_name_identity_info: Option<CheetahString>,
    broker_id_identity_info: Option<u64>,

    // brokerLiveInfo fields
    broker_name: Option<CheetahString>,
    broker_addr: Option<CheetahString>,
    heartbeat_timeout_millis: Option<u64>,
    broker_id: Option<i64>,
    last_update_timestamp: Option<u64>,
    epoch: Option<i32>,
    max_offset: Option<i64>,
    confirm_offset: Option<i64>,
    election_priority: Option<i32>,
}

impl RaftBrokerHeartBeatEventRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_broker_info(broker_identity_info: &BrokerIdentityInfo, broker_live_info: &BrokerLiveInfo) -> Self {
        Self {
            cluster_name_identity_info: Some(broker_identity_info.cluster_name.clone()),
            broker_name_identity_info: Some(broker_identity_info.broker_name.clone()),
            broker_id_identity_info: broker_identity_info.broker_id,
            broker_name: Some(broker_live_info.broker_name().into()),
            broker_addr: Some(broker_live_info.broker_addr().into()),
            heartbeat_timeout_millis: Some(broker_live_info.heartbeat_timeout_millis()),
            broker_id: Some(broker_live_info.broker_id()),
            last_update_timestamp: Some(broker_live_info.last_update_timestamp()),
            epoch: Some(broker_live_info.epoch()),
            max_offset: Some(broker_live_info.max_offset()),
            confirm_offset: Some(broker_live_info.confirm_offset()),
            election_priority: broker_live_info.election_priority(),
        }
    }

    pub fn broker_identity_info(&self) -> BrokerIdentityInfo {
        BrokerIdentityInfo::new(
            self.cluster_name_identity_info.clone().unwrap_or_default(),
            self.broker_name_identity_info.clone().unwrap_or_default(),
            self.broker_id_identity_info,
        )
    }

    pub fn set_broker_identity_info(&mut self, info: &BrokerIdentityInfo) {
        self.cluster_name_identity_info = Some(info.cluster_name.clone());
        self.broker_name_identity_info = Some(info.broker_name.clone());
        self.broker_id_identity_info = info.broker_id;
    }

    // Getters for brokerIdentityInfo fields
    pub fn cluster_name_identity_info(&self) -> Option<&CheetahString> {
        self.cluster_name_identity_info.as_ref()
    }

    pub fn broker_name_identity_info(&self) -> Option<&CheetahString> {
        self.broker_name_identity_info.as_ref()
    }

    pub fn broker_id_identity_info(&self) -> Option<u64> {
        self.broker_id_identity_info
    }

    // Getters for brokerLiveInfo fields
    pub fn broker_name(&self) -> Option<&CheetahString> {
        self.broker_name.as_ref()
    }

    pub fn broker_addr(&self) -> Option<&CheetahString> {
        self.broker_addr.as_ref()
    }

    pub fn heartbeat_timeout_millis(&self) -> Option<u64> {
        self.heartbeat_timeout_millis
    }

    pub fn broker_id(&self) -> Option<i64> {
        self.broker_id
    }

    pub fn last_update_timestamp(&self) -> Option<u64> {
        self.last_update_timestamp
    }

    pub fn epoch(&self) -> Option<i32> {
        self.epoch
    }

    pub fn max_offset(&self) -> Option<i64> {
        self.max_offset
    }

    pub fn confirm_offset(&self) -> Option<i64> {
        self.confirm_offset
    }

    pub fn election_priority(&self) -> Option<i32> {
        self.election_priority
    }

    // Setters for brokerLiveInfo fields
    pub fn set_broker_name(&mut self, broker_name: impl Into<CheetahString>) {
        self.broker_name = Some(broker_name.into());
    }

    pub fn set_broker_addr(&mut self, broker_addr: impl Into<CheetahString>) {
        self.broker_addr = Some(broker_addr.into());
    }

    pub fn set_heartbeat_timeout_millis(&mut self, timeout: u64) {
        self.heartbeat_timeout_millis = Some(timeout);
    }

    pub fn set_broker_id(&mut self, broker_id: i64) {
        self.broker_id = Some(broker_id);
    }

    pub fn set_last_update_timestamp(&mut self, timestamp: u64) {
        self.last_update_timestamp = Some(timestamp);
    }

    pub fn set_epoch(&mut self, epoch: i32) {
        self.epoch = Some(epoch);
    }

    pub fn set_max_offset(&mut self, offset: i64) {
        self.max_offset = Some(offset);
    }

    pub fn set_confirm_offset(&mut self, offset: i64) {
        self.confirm_offset = Some(offset);
    }

    pub fn set_election_priority(&mut self, priority: Option<i32>) {
        self.election_priority = priority;
    }
}

impl fmt::Display for RaftBrokerHeartBeatEventRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RaftBrokerHeartBeatEventRequest{{brokerIdentityInfo={}, brokerLiveInfo=BrokerLiveInfo{{brokerName={:?}, \
             brokerAddr={:?}, brokerId={:?}, lastUpdateTimestamp={:?}, heartbeatTimeoutMillis={:?}, epoch={:?}, \
             maxOffset={:?}, confirmOffset={:?}, electionPriority={:?}}}}}",
            self.broker_identity_info(),
            self.broker_name,
            self.broker_addr,
            self.broker_id,
            self.last_update_timestamp,
            self.heartbeat_timeout_millis,
            self.epoch,
            self.max_offset,
            self.confirm_offset,
            self.election_priority
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_defaults() {
        let request = RaftBrokerHeartBeatEventRequest::new();
        assert!(request.cluster_name_identity_info().is_none());
        assert!(request.broker_name_identity_info().is_none());
        assert!(request.broker_id().is_none());
        assert!(request.epoch().is_none());
    }

    #[test]
    fn test_setters_and_getters_live_info() {
        let mut request = RaftBrokerHeartBeatEventRequest::new();

        request.set_broker_name("test_broker");
        request.set_broker_addr("127.0.0.1:8080");
        request.set_broker_id(10);
        request.set_epoch(5);
        request.set_max_offset(1000);
        request.set_confirm_offset(900);
        request.set_heartbeat_timeout_millis(5000);
        request.set_election_priority(Some(1));

        assert_eq!(request.broker_name(), Some(&CheetahString::from("test_broker")));
        assert_eq!(request.broker_addr(), Some(&CheetahString::from("127.0.0.1:8080")));
        assert_eq!(request.broker_id(), Some(10));
        assert_eq!(request.epoch(), Some(5));
        assert_eq!(request.max_offset(), Some(1000));
        assert_eq!(request.confirm_offset(), Some(900));
        assert_eq!(request.heartbeat_timeout_millis(), Some(5000));
        assert_eq!(request.election_priority(), Some(1));
    }

    #[test]
    fn test_broker_identity_info_interaction() {
        let mut request = RaftBrokerHeartBeatEventRequest::new();

        let info = BrokerIdentityInfo::new("my_cluster", "my_broker", Some(123));
        request.set_broker_identity_info(&info);

        assert_eq!(
            request.cluster_name_identity_info(),
            Some(&CheetahString::from("my_cluster"))
        );
        assert_eq!(
            request.broker_name_identity_info(),
            Some(&CheetahString::from("my_broker"))
        );
        assert_eq!(request.broker_id_identity_info(), Some(123));

        let reconstructed = request.broker_identity_info();
        assert_eq!(reconstructed.cluster_name, "my_cluster");
        assert_eq!(reconstructed.broker_id, Some(123));
    }

    #[test]
    fn test_display_formatting() {
        let mut request = RaftBrokerHeartBeatEventRequest::new();
        request.set_broker_name("DisplayBroker");
        request.set_epoch(99);

        let output = format!("{}", request);

        assert!(output.contains("RaftBrokerHeartBeatEventRequest"));
        assert!(output.contains("DisplayBroker"));
        assert!(output.contains("99"));
    }
}
