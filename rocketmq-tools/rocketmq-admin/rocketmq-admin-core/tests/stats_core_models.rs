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

use rocketmq_admin_core::client_adapter::services::stats::StatsAllQueryRequest;

#[test]
fn stats_all_query_request_trims_optional_topic() {
    let request = StatsAllQueryRequest::new(true, Some(" TopicA ".to_string()))
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert!(request.active_topic());
    assert_eq!(request.topic().map(|topic| topic.as_str()), Some("TopicA"));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn stats_all_query_request_treats_blank_topic_as_all() {
    let request = StatsAllQueryRequest::new(false, Some(" ".to_string()));

    assert!(!request.active_topic());
    assert_eq!(request.topic(), None);
    assert_eq!(request.namesrv_addr(), None);
}
