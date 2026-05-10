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

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct TieredDispatchRequest {
    pub topic: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub commit_log_offset: i64,
    pub message_size: i32,
    pub tags_code: i64,
    pub store_timestamp: i64,
    pub keys: Option<String>,
    pub uniq_key: Option<String>,
    pub offset_id: Option<String>,
    pub sys_flag: i32,
    pub body: Option<Bytes>,
}

impl TieredDispatchRequest {
    pub fn is_valid(&self) -> bool {
        let Some(body) = self.body.as_ref() else {
            return false;
        };
        !self.topic.is_empty()
            && self.queue_id >= 0
            && self.queue_offset >= 0
            && self.message_size > 0
            && body.len() == self.message_size as usize
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::TieredDispatchRequest;

    fn request(body: Option<Bytes>, message_size: i32) -> TieredDispatchRequest {
        TieredDispatchRequest {
            topic: "TopicA".to_owned(),
            queue_id: 0,
            queue_offset: 0,
            commit_log_offset: 0,
            message_size,
            tags_code: 0,
            store_timestamp: 100,
            keys: None,
            uniq_key: None,
            offset_id: None,
            sys_flag: 0,
            body,
        }
    }

    #[test]
    fn valid_dispatch_requires_body_matching_message_size() {
        assert!(request(Some(Bytes::from_static(b"body")), 4).is_valid());
        assert!(!request(None, 4).is_valid());
        assert!(!request(Some(Bytes::from_static(b"body")), 5).is_valid());
        assert!(!request(Some(Bytes::new()), 0).is_valid());
    }
}
