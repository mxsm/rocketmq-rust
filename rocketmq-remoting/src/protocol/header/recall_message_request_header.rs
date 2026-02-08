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

use crate::rpc::topic_request_header::TopicRequestHeader;

/// Request header for recalling (withdrawing) a message.
///
/// This header is used with `RequestCode::RECALL_MESSAGE` to recall a previously
/// sent message from the broker. The recall operation requires the producer group,
/// topic, and a recall handle that identifies the message to be recalled.
#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct RecallMessageRequestHeader {
    /// Producer group name (optional).
    ///
    /// The name of the producer group that originally sent the message.
    /// This may be omitted in some scenarios where the recall handle
    /// alone is sufficient to identify the message.
    pub producer_group: Option<CheetahString>,

    /// Topic name (required).
    ///
    /// The topic to which the message was originally sent.
    #[required]
    pub topic: CheetahString,

    /// Recall handle (required).
    ///
    /// A unique identifier or handle that specifies which message to recall.
    /// The format and semantics of this handle are implementation-specific.
    #[required]
    pub recall_handle: CheetahString,

    /// Topic request header containing common request metadata.
    ///
    /// This field is flattened during serialization/deserialization to merge
    /// its fields with the top-level structure.
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl RecallMessageRequestHeader {
    /// Creates a new `RecallMessageRequestHeader` with the specified values.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name
    /// * `recall_handle` - The recall handle identifying the message
    /// * `producer_group` - Optional producer group name
    ///
    /// # Returns
    ///
    /// A new `RecallMessageRequestHeader` instance
    pub fn new(
        topic: impl Into<CheetahString>,
        recall_handle: impl Into<CheetahString>,
        producer_group: Option<impl Into<CheetahString>>,
    ) -> Self {
        Self {
            topic: topic.into(),
            recall_handle: recall_handle.into(),
            producer_group: producer_group.map(|pg| pg.into()),
            topic_request_header: None,
        }
    }

    /// Gets a reference to the producer group, if set.
    pub fn producer_group(&self) -> Option<&CheetahString> {
        self.producer_group.as_ref()
    }

    /// Sets the producer group.
    pub fn set_producer_group(&mut self, producer_group: impl Into<CheetahString>) {
        self.producer_group = Some(producer_group.into());
    }

    /// Gets a reference to the topic.
    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    /// Sets the topic.
    pub fn set_topic(&mut self, topic: impl Into<CheetahString>) {
        self.topic = topic.into();
    }

    /// Gets a reference to the recall handle.
    pub fn recall_handle(&self) -> &CheetahString {
        &self.recall_handle
    }

    /// Sets the recall handle.
    pub fn set_recall_handle(&mut self, recall_handle: impl Into<CheetahString>) {
        self.recall_handle = recall_handle.into();
    }
}

impl std::fmt::Display for RecallMessageRequestHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecallMessageRequestHeader {{ producer_group: {:?}, topic: {}, recall_handle: {} }}",
            self.producer_group, self.topic, self.recall_handle
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn test_new_with_producer_group() {
        let header = RecallMessageRequestHeader::new("TestTopic", "handle123", Some("ProducerGroup1"));

        assert_eq!(header.topic(), &CheetahString::from("TestTopic"));
        assert_eq!(header.recall_handle(), &CheetahString::from("handle123"));
        assert_eq!(header.producer_group(), Some(&CheetahString::from("ProducerGroup1")));
    }

    #[test]
    fn test_new_without_producer_group() {
        let header = RecallMessageRequestHeader::new("TestTopic", "handle123", None::<&str>);

        assert_eq!(header.topic(), &CheetahString::from("TestTopic"));
        assert_eq!(header.recall_handle(), &CheetahString::from("handle123"));
        assert_eq!(header.producer_group(), None);
    }

    #[test]
    fn test_setters() {
        let mut header = RecallMessageRequestHeader::default();

        header.set_topic("NewTopic");
        header.set_recall_handle("newHandle");
        header.set_producer_group("NewGroup");

        assert_eq!(header.topic(), &CheetahString::from("NewTopic"));
        assert_eq!(header.recall_handle(), &CheetahString::from("newHandle"));
        assert_eq!(header.producer_group(), Some(&CheetahString::from("NewGroup")));
    }

    #[test]
    fn test_display() {
        let header = RecallMessageRequestHeader::new("TestTopic", "handle123", Some("Group1"));
        let display = format!("{}", header);

        assert!(display.contains("TestTopic"));
        assert!(display.contains("handle123"));
        assert!(display.contains("Group1"));
    }

    #[test]
    fn test_serialization() {
        let header = RecallMessageRequestHeader::new("TestTopic", "handle123", Some("ProducerGroup1"));

        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"topic\":\"TestTopic\""));
        assert!(json.contains("\"recallHandle\":\"handle123\""));
        assert!(json.contains("\"producerGroup\":\"ProducerGroup1\""));
    }

    #[test]
    fn test_deserialization() {
        let json = r#"{
            "topic": "TestTopic",
            "recallHandle": "handle123",
            "producerGroup": "ProducerGroup1"
        }"#;

        let header: RecallMessageRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.topic(), &CheetahString::from("TestTopic"));
        assert_eq!(header.recall_handle(), &CheetahString::from("handle123"));
        assert_eq!(header.producer_group(), Some(&CheetahString::from("ProducerGroup1")));
    }

    #[test]
    fn test_deserialization_without_producer_group() {
        let json = r#"{
            "topic": "TestTopic",
            "recallHandle": "handle123"
        }"#;

        let header: RecallMessageRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.topic(), &CheetahString::from("TestTopic"));
        assert_eq!(header.recall_handle(), &CheetahString::from("handle123"));
        assert_eq!(header.producer_group(), None);
    }

    #[test]
    fn test_clone() {
        let header = RecallMessageRequestHeader::new("TestTopic", "handle123", Some("ProducerGroup1"));
        let cloned = header.clone();

        assert_eq!(header.topic(), cloned.topic());
        assert_eq!(header.recall_handle(), cloned.recall_handle());
        assert_eq!(header.producer_group(), cloned.producer_group());
    }
}
