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

use rocketmq_admin_core::client_adapter::services::cluster::ClusterBrokerNameQueryRequest;
use rocketmq_admin_core::client_adapter::services::cluster::ClusterListMode;
use rocketmq_admin_core::client_adapter::services::cluster::ClusterListQueryRequest;
use rocketmq_admin_core::client_adapter::services::cluster::ClusterSendMessageRtRequest;

#[test]
fn cluster_list_query_request_trims_optional_cluster() {
    let request = ClusterListQueryRequest::new(false, Some(" DefaultCluster ".to_string()))
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(request.mode(), ClusterListMode::Base);
    assert_eq!(request.cluster_name().map(|name| name.as_str()), Some("DefaultCluster"));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn cluster_list_query_request_supports_all_clusters_and_more_stats_mode() {
    let request = ClusterListQueryRequest::new(true, Some(" ".to_string()));

    assert_eq!(request.mode(), ClusterListMode::MoreStats);
    assert_eq!(request.cluster_name(), None);
    assert_eq!(request.namesrv_addr(), None);
}

#[test]
fn cluster_broker_name_query_request_trims_optional_cluster() {
    let request = ClusterBrokerNameQueryRequest::new(Some(" DefaultCluster ".to_string()))
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(request.cluster_name().map(|name| name.as_str()), Some("DefaultCluster"));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn cluster_send_message_rt_request_trims_optional_cluster() {
    let request = ClusterSendMessageRtRequest::try_new(100, 256, Some(" DefaultCluster ".to_string()))
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(request.amount(), 100);
    assert_eq!(request.size(), 256);
    assert_eq!(request.cluster_name().map(|name| name.as_str()), Some("DefaultCluster"));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn cluster_send_message_rt_request_rejects_zero_amount() {
    assert!(ClusterSendMessageRtRequest::try_new(0, 128, None).is_err());
}
