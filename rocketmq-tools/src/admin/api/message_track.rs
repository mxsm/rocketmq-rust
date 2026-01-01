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

use serde::Deserialize;
use serde::Serialize;

use crate::admin::api::track_type::TrackType;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageTrack {
    pub consumer_group: String,
    pub track_type: Option<TrackType>,
    pub exception_desc: String,
}

#[allow(dead_code)]
impl MessageTrack {
    pub fn get_consumer_group(&self) -> String {
        self.consumer_group.clone()
    }

    pub fn set_consumer_group(&mut self, consumer_group: String) {
        self.consumer_group = consumer_group;
    }

    pub fn get_track_type(&self) -> Option<TrackType> {
        self.track_type
    }

    pub fn set_track_type(&mut self, track_type: TrackType) {
        self.track_type = Some(track_type);
    }

    pub fn get_exception_desc(&self) -> String {
        self.exception_desc.clone()
    }

    pub fn set_exception_desc(&mut self, exception_desc: String) {
        self.exception_desc = exception_desc;
    }
}

impl std::fmt::Display for MessageTrack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let track_type_str = self
            .track_type
            .as_ref()
            .map_or("None".to_string(), |tt| format!("{tt}"));
        write!(
            f,
            "MessageTrack [consumerGroup={}, trackType={}, exceptionDesc={}]",
            self.consumer_group, track_type_str, self.exception_desc
        )
    }
}

#[cfg(test)]
mod tests {

    use crate::admin::api::message_track::MessageTrack;
    use crate::admin::api::track_type::TrackType;

    #[test]
    pub fn test_message_track() {
        let mut message_track = MessageTrack {
            consumer_group: "test_consumer_group".to_string(),
            track_type: Some(TrackType::Consumed),
            exception_desc: "test_exception_desc".to_string(),
        };

        assert_eq!(message_track.get_consumer_group(), "test_consumer_group");
        assert_eq!(message_track.get_exception_desc(), "test_exception_desc");
        assert_eq!(message_track.get_track_type(), Some(TrackType::Consumed));

        let display = format!("{}", message_track);
        println!("{}", display);
        assert_eq!(
            display,
            "MessageTrack [consumerGroup=test_consumer_group, trackType=CONSUMED, exceptionDesc=test_exception_desc]"
        );

        message_track.set_consumer_group("test_consumer_group2".to_string());
        message_track.set_track_type(TrackType::Pull);
        message_track.set_exception_desc("test_exception_desc2".to_string());

        assert_eq!(message_track.get_consumer_group(), "test_consumer_group2");
        assert_eq!(message_track.get_exception_desc(), "test_exception_desc2");
    }
}
