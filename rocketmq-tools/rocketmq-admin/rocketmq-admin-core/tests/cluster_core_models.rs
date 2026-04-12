use rocketmq_admin_core::core::cluster::ClusterBrokerNameQueryRequest;
use rocketmq_admin_core::core::cluster::ClusterListMode;
use rocketmq_admin_core::core::cluster::ClusterListQueryRequest;

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
