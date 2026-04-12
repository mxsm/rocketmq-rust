use rocketmq_admin_core::core::ha::HaStatusQueryRequest;
use rocketmq_admin_core::core::ha::HaStatusTarget;
use rocketmq_admin_core::core::ha::SyncStateSetQueryRequest;
use rocketmq_admin_core::core::ha::SyncStateSetTarget;

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
