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

use rocketmq_admin_core::client_adapter::services::ha::HaStatusQueryRequest;
use rocketmq_admin_core::client_adapter::services::ha::HaStatusTarget;
use rocketmq_admin_core::client_adapter::services::ha::SyncStateSetQueryRequest;
use rocketmq_admin_core::client_adapter::services::ha::SyncStateSetTarget;

#[test]
fn ha_status_query_request_trims_broker_target() {
    let request = HaStatusQueryRequest::try_new(Some(" 127.0.0.1:10911 ".to_string()), None).unwrap();

    assert_eq!(request.target(), &HaStatusTarget::BrokerAddr("127.0.0.1:10911".into()));
    assert_eq!(request.namesrv_addr(), None);

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn ha_status_query_request_trims_cluster_target() {
    let request = HaStatusQueryRequest::try_new(None, Some(" DefaultCluster ".to_string())).unwrap();

    assert_eq!(request.target(), &HaStatusTarget::ClusterName("DefaultCluster".into()));
}

#[test]
fn ha_status_query_request_rejects_missing_or_ambiguous_target() {
    assert!(HaStatusQueryRequest::try_new(None, None).is_err());
    assert!(
        HaStatusQueryRequest::try_new(Some("127.0.0.1:10911".to_string()), Some("DefaultCluster".to_string())).is_err()
    );
}

#[test]
fn sync_state_set_query_request_trims_controller_and_broker_target() {
    let request =
        SyncStateSetQueryRequest::try_new(" 127.0.0.1:9878;127.0.0.2:9878 ", Some(" broker-a ".to_string()), None)
            .unwrap();

    assert_eq!(request.controller_address().as_str(), "127.0.0.1:9878");
    assert_eq!(request.target(), &SyncStateSetTarget::BrokerName("broker-a".into()));
    assert_eq!(request.namesrv_addr(), None);

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn sync_state_set_query_request_trims_cluster_target() {
    let request =
        SyncStateSetQueryRequest::try_new(" 127.0.0.1:9878 ", None, Some(" DefaultCluster ".to_string())).unwrap();

    assert_eq!(request.controller_address().as_str(), "127.0.0.1:9878");
    assert_eq!(
        request.target(),
        &SyncStateSetTarget::ClusterName("DefaultCluster".into())
    );
}

#[test]
fn sync_state_set_query_request_rejects_blank_controller_or_ambiguous_target() {
    assert!(SyncStateSetQueryRequest::try_new(" ", Some("broker-a".to_string()), None).is_err());
    assert!(SyncStateSetQueryRequest::try_new("127.0.0.1:9878", None, None).is_err());
    assert!(SyncStateSetQueryRequest::try_new(
        "127.0.0.1:9878",
        Some("broker-a".to_string()),
        Some("DefaultCluster".to_string())
    )
    .is_err());
}
