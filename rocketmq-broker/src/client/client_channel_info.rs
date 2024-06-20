/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::LanguageCode;

#[derive(Debug, Clone, Hash)]
pub struct ClientChannelInfo {
    channel: Channel,
    client_id: String,
    language: LanguageCode,
    version: i32,
    last_update_timestamp: u64,
}

impl ClientChannelInfo {
    pub fn new(channel: Channel, client_id: String, language: LanguageCode, version: i32) -> Self {
        Self {
            channel,
            client_id,
            language,
            version,
            last_update_timestamp: get_current_millis(),
        }
    }

    pub fn client_id(&self) -> &String {
        &self.client_id
    }

    pub fn language(&self) -> LanguageCode {
        self.language
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn last_update_timestamp(&self) -> u64 {
        self.last_update_timestamp
    }

    pub fn set_client_id(&mut self, client_id: String) {
        self.client_id = client_id;
    }

    pub fn set_language(&mut self, language: LanguageCode) {
        self.language = language;
    }

    pub fn set_version(&mut self, version: i32) {
        self.version = version;
    }

    pub fn set_last_update_timestamp(&mut self, last_update_timestamp: u64) {
        self.last_update_timestamp = last_update_timestamp;
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }
    pub fn set_channel(&mut self, channel: Channel) {
        self.channel = channel;
    }
}
