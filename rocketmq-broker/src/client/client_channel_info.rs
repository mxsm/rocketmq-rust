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
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::LanguageCode;

#[derive(Clone, Hash, PartialEq)]
pub struct ClientChannelInfo {
    channel: Channel,
    client_id: CheetahString,
    language: LanguageCode,
    version: i32,
    last_update_timestamp: u64,
}

impl ClientChannelInfo {
    pub fn new(channel: Channel, client_id: CheetahString, language: LanguageCode, version: i32) -> Self {
        Self {
            channel,
            client_id,
            language,
            version,
            last_update_timestamp: get_current_millis(),
        }
    }

    pub fn client_id(&self) -> &CheetahString {
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

    pub fn set_client_id(&mut self, client_id: impl Into<CheetahString>) {
        self.client_id = client_id.into();
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

#[cfg(test)]
mod tests {
    #[test]
    fn client_channel_info_new() {
        // let channel = Channel::new(
        //     "127.0.0.1:8080".parse().unwrap(),
        //     "127.0.0.1:8080".parse().unwrap(),
        // );
        // let client_info = ClientChannelInfo::new(
        //     channel.clone(),
        //     "client1".to_string(),
        //     LanguageCode::JAVA,
        //     1,
        // );
        // assert_eq!(client_info.client_id(), "client1");
        // assert_eq!(client_info.language(), LanguageCode::JAVA);
        // assert_eq!(client_info.version(), 1);
        // assert_eq!(client_info.channel(), &channel);
    }

    #[test]
    fn client_channel_info_setters() {
        // let channel = Channel::new(
        //     "127.0.0.1:8080".parse().unwrap(),
        //     "127.0.0.1:8080".parse().unwrap(),
        // );
        // let mut client_info = ClientChannelInfo::new(
        //     channel.clone(),
        //     "client1".to_string(),
        //     LanguageCode::JAVA,
        //     1,
        // );
        // client_info.set_client_id("client2".to_string());
        // client_info.set_language(LanguageCode::CPP);
        // client_info.set_version(2);
        // let new_channel = Channel::new(
        //     "127.0.0.1:8081".parse().unwrap(),
        //     "127.0.0.1:8080".parse().unwrap(),
        // );
        // client_info.set_channel(new_channel.clone());
        // assert_eq!(client_info.client_id(), "client2");
        // assert_eq!(client_info.language(), LanguageCode::CPP);
        // assert_eq!(client_info.version(), 2);
        // assert_eq!(client_info.channel(), &new_channel);
    }
}
