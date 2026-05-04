// Copyright 2026 The RocketMQ Rust Authors
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

//! Shared Dashboard-domain request models for dashboard implementations.

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardBrokerOverviewRequest {
    pub force_refresh: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardBrokerHistoryRequest {
    pub date: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardTopicHistoryRequest {
    pub date: String,
    pub topic_name: String,
}

#[cfg(test)]
mod tests {
    use super::DashboardBrokerHistoryRequest;
    use super::DashboardBrokerOverviewRequest;
    use super::DashboardTopicHistoryRequest;

    #[test]
    fn dashboard_broker_overview_request_uses_camel_case_fields() {
        let request = DashboardBrokerOverviewRequest { force_refresh: true };
        let json = serde_json::to_string(&request).expect("serialize dashboard broker overview request");

        assert!(json.contains("\"forceRefresh\""));
    }

    #[test]
    fn dashboard_topic_history_request_uses_java_dashboard_field_names() {
        let request = DashboardTopicHistoryRequest {
            date: "2026-05-04".to_string(),
            topic_name: "TopicTest".to_string(),
        };
        let json = serde_json::to_string(&request).expect("serialize dashboard topic history request");

        assert!(json.contains("\"topicName\""));
        assert!(json.contains("\"date\""));
    }

    #[test]
    fn dashboard_broker_history_request_uses_date_field() {
        let request = DashboardBrokerHistoryRequest {
            date: "2026-05-04".to_string(),
        };
        let json = serde_json::to_string(&request).expect("serialize dashboard broker history request");

        assert!(json.contains("\"date\""));
    }
}
