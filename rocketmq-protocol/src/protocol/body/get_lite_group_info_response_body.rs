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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::admin::offset_wrapper::OffsetWrapper;
use crate::protocol::body::lite_lag_info::LiteLagInfo;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetLiteGroupInfoResponseBody {
    #[serde(default)]
    group: CheetahString,

    #[serde(default)]
    parent_topic: CheetahString,

    #[serde(default)]
    lite_topic: CheetahString,

    #[serde(default)]
    earliest_unconsumed_timestamp: i64,

    #[serde(default)]
    total_lag_count: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    lite_topic_offset_wrapper: Option<OffsetWrapper>,

    #[serde(default)]
    lag_count_top_k: Vec<LiteLagInfo>,

    #[serde(default)]
    lag_timestamp_top_k: Vec<LiteLagInfo>,
}

impl GetLiteGroupInfoResponseBody {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn with_group(&mut self, group: CheetahString) -> &mut Self {
        self.group = group;
        self
    }

    #[must_use]
    pub fn parent_topic(&self) -> &CheetahString {
        &self.parent_topic
    }

    pub fn with_parent_topic(&mut self, parent_topic: CheetahString) -> &mut Self {
        self.parent_topic = parent_topic;
        self
    }

    #[must_use]
    pub fn lite_topic(&self) -> &CheetahString {
        &self.lite_topic
    }

    pub fn with_lite_topic(&mut self, lite_topic: CheetahString) -> &mut Self {
        self.lite_topic = lite_topic;
        self
    }

    #[must_use]
    pub fn earliest_unconsumed_timestamp(&self) -> i64 {
        self.earliest_unconsumed_timestamp
    }

    pub fn with_earliest_unconsumed_timestamp(&mut self, earliest_unconsumed_timestamp: i64) -> &mut Self {
        self.earliest_unconsumed_timestamp = earliest_unconsumed_timestamp;
        self
    }

    #[must_use]
    pub fn total_lag_count(&self) -> i64 {
        self.total_lag_count
    }

    pub fn with_total_lag_count(&mut self, total_lag_count: i64) -> &mut Self {
        self.total_lag_count = total_lag_count;
        self
    }

    #[must_use]
    pub fn lite_topic_offset_wrapper(&self) -> Option<&OffsetWrapper> {
        self.lite_topic_offset_wrapper.as_ref()
    }

    pub fn with_lite_topic_offset_wrapper(&mut self, lite_topic_offset_wrapper: OffsetWrapper) -> &mut Self {
        self.lite_topic_offset_wrapper = Some(lite_topic_offset_wrapper);
        self
    }

    #[must_use]
    pub fn lag_count_top_k(&self) -> &[LiteLagInfo] {
        &self.lag_count_top_k
    }

    pub fn with_lag_count_top_k(&mut self, lag_count_top_k: Vec<LiteLagInfo>) -> &mut Self {
        self.lag_count_top_k = lag_count_top_k;
        self
    }

    pub fn add_lag_count_top_k(&mut self, lag_info: LiteLagInfo) -> &mut Self {
        self.lag_count_top_k.push(lag_info);
        self
    }

    #[must_use]
    pub fn lag_timestamp_top_k(&self) -> &[LiteLagInfo] {
        &self.lag_timestamp_top_k
    }

    pub fn with_lag_timestamp_top_k(&mut self, lag_timestamp_top_k: Vec<LiteLagInfo>) -> &mut Self {
        self.lag_timestamp_top_k = lag_timestamp_top_k;
        self
    }

    pub fn add_lag_timestamp_top_k(&mut self, lag_info: LiteLagInfo) -> &mut Self {
        self.lag_timestamp_top_k.push(lag_info);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_lite_group_info_response_body_default() {
        let body = GetLiteGroupInfoResponseBody::default();
        assert!(body.group().is_empty());
        assert!(body.parent_topic().is_empty());
        assert!(body.lite_topic().is_empty());
        assert_eq!(body.earliest_unconsumed_timestamp(), 0);
        assert_eq!(body.total_lag_count(), 0);
        assert!(body.lite_topic_offset_wrapper().is_none());
        assert!(body.lag_count_top_k().is_empty());
        assert!(body.lag_timestamp_top_k().is_empty());
    }

    #[test]
    fn get_lite_group_info_response_body_getters_and_setters() {
        let mut body = GetLiteGroupInfoResponseBody::new();
        let wrapper = OffsetWrapper::default();
        let lag_info = LiteLagInfo::new();

        body.with_group("group".into())
            .with_parent_topic("parent".into())
            .with_lite_topic("topic".into())
            .with_earliest_unconsumed_timestamp(100)
            .with_total_lag_count(200)
            .with_lite_topic_offset_wrapper(wrapper)
            .with_lag_count_top_k(vec![lag_info.clone()])
            .add_lag_count_top_k(lag_info.clone())
            .with_lag_timestamp_top_k(vec![lag_info.clone()])
            .add_lag_timestamp_top_k(lag_info);

        assert_eq!(body.group(), "group");
        assert_eq!(body.parent_topic(), "parent");
        assert_eq!(body.lite_topic(), "topic");
        assert_eq!(body.earliest_unconsumed_timestamp(), 100);
        assert_eq!(body.total_lag_count(), 200);
        assert!(body.lite_topic_offset_wrapper().is_some());
        assert_eq!(body.lag_count_top_k().len(), 2);
        assert_eq!(body.lag_timestamp_top_k().len(), 2);
    }

    #[test]
    fn get_lite_group_info_response_body_serialization_and_deserialization() {
        let mut body = GetLiteGroupInfoResponseBody::new();
        body.with_group("group".into())
            .with_parent_topic("parent".into())
            .with_lite_topic("topic".into())
            .with_earliest_unconsumed_timestamp(100)
            .with_total_lag_count(200);

        let json = serde_json::to_string(&body).unwrap();
        let expected = r#"{"group":"group","parentTopic":"parent","liteTopic":"topic","earliestUnconsumedTimestamp":100,"totalLagCount":200,"lagCountTopK":[],"lagTimestampTopK":[]}"#;
        assert_eq!(json, expected);

        let decoded: GetLiteGroupInfoResponseBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.group(), "group");
        assert_eq!(decoded.parent_topic(), "parent");
        assert_eq!(decoded.lite_topic(), "topic");
        assert_eq!(decoded.earliest_unconsumed_timestamp(), 100);
        assert_eq!(decoded.total_lag_count(), 200);
    }
}
