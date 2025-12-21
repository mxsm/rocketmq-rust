//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use std::fmt;

use cheetah_string::CheetahString;
use rocketmq_remoting::net::channel::Channel;

pub trait RemotingChannel: Send + Sync {}

#[derive(Clone)]
pub struct BrokerLiveInfo {
    broker_name: CheetahString,

    broker_addr: CheetahString,

    heartbeat_timeout_millis: u64,

    channel: Channel,

    broker_id: i64,
    last_update_timestamp: u64,
    epoch: i32,

    max_offset: i64,
    confirm_offset: i64,

    election_priority: Option<i32>,
}

impl BrokerLiveInfo {
    pub fn new(
        broker_name: impl Into<CheetahString>,
        broker_addr: impl Into<CheetahString>,
        broker_id: i64,
        last_update_timestamp: u64,
        heartbeat_timeout_millis: u64,
        channel: Channel,
        epoch: i32,
        max_offset: i64,
        election_priority: Option<i32>,
        confirm_offset: Option<i64>,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_addr: broker_addr.into(),
            broker_id,
            last_update_timestamp,
            heartbeat_timeout_millis,
            channel,
            epoch,
            max_offset,
            confirm_offset: confirm_offset.unwrap_or(0),
            election_priority,
        }
    }
}

impl BrokerLiveInfo {
    // getters
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn broker_addr(&self) -> &str {
        &self.broker_addr
    }

    pub fn broker_id(&self) -> i64 {
        self.broker_id
    }

    pub fn heartbeat_timeout_millis(&self) -> u64 {
        self.heartbeat_timeout_millis
    }

    pub fn last_update_timestamp(&self) -> u64 {
        self.last_update_timestamp
    }

    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    pub fn max_offset(&self) -> i64 {
        self.max_offset
    }

    pub fn confirm_offset(&self) -> i64 {
        self.confirm_offset
    }

    pub fn election_priority(&self) -> Option<i32> {
        self.election_priority
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    // setters（迁移期可接受）
    pub fn set_last_update_timestamp(&mut self, ts: u64) {
        self.last_update_timestamp = ts;
    }

    pub fn set_epoch(&mut self, epoch: i32) {
        self.epoch = epoch;
    }

    pub fn set_max_offset(&mut self, offset: i64) {
        self.max_offset = offset;
    }

    pub fn set_confirm_offset(&mut self, offset: i64) {
        self.confirm_offset = offset;
    }

    pub fn set_election_priority(&mut self, priority: Option<i32>) {
        self.election_priority = priority;
    }

    pub fn set_broker_addr(&mut self, addr: impl Into<CheetahString>) {
        self.broker_addr = addr.into();
    }

    pub fn set_channel(&mut self, channel: Channel) {
        self.channel = channel;
    }
}

impl fmt::Debug for BrokerLiveInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerLiveInfo")
            .field("broker_name", &self.broker_name)
            .field("broker_addr", &self.broker_addr)
            .field("heartbeat_timeout_millis", &self.heartbeat_timeout_millis)
            .field("broker_id", &self.broker_id)
            .field("last_update_timestamp", &self.last_update_timestamp)
            .field("epoch", &self.epoch)
            .field("max_offset", &self.max_offset)
            .field("confirm_offset", &self.confirm_offset)
            .field("election_priority", &self.election_priority)
            .finish()
    }
}
