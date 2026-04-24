use rocketmq_admin_core::core::controller::ControllerConfigQueryRequest;
use rocketmq_admin_core::core::controller::ControllerConfigUpdateRequest;
use rocketmq_admin_core::core::controller::ControllerElectMasterRequest;
use rocketmq_admin_core::core::controller::ControllerMetadataCleanRequest;
use rocketmq_admin_core::core::controller::ControllerMetadataQueryRequest;

#[test]
fn controller_config_query_request_splits_and_trims_addresses() {
    let request = ControllerConfigQueryRequest::try_new(" 127.0.0.1:9878 ; ; 127.0.0.2:9878 ").unwrap();

    assert_eq!(
        request
            .controller_servers()
            .iter()
            .map(|addr| addr.as_str())
            .collect::<Vec<_>>(),
        vec!["127.0.0.1:9878", "127.0.0.2:9878"]
    );
    assert_eq!(request.namesrv_addr(), None);

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn controller_config_query_request_rejects_blank_addresses() {
    assert!(ControllerConfigQueryRequest::try_new(" ; ").is_err());
}

#[test]
fn controller_metadata_query_request_trims_address() {
    let request = ControllerMetadataQueryRequest::try_new(" 127.0.0.1:9878 ").unwrap();

    assert_eq!(request.controller_addr().as_str(), "127.0.0.1:9878");
    assert_eq!(request.namesrv_addr(), None);

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn controller_metadata_query_request_rejects_blank_address() {
    assert!(ControllerMetadataQueryRequest::try_new(" ").is_err());
}

#[test]
fn controller_config_update_request_trims_targets_and_property() {
    let request =
        ControllerConfigUpdateRequest::try_new(" 127.0.0.1:9878 ; ; 127.0.0.2:9878 ", "  key  ", " value ").unwrap();

    assert_eq!(
        request
            .controller_servers()
            .iter()
            .map(|addr| addr.as_str())
            .collect::<Vec<_>>(),
        vec!["127.0.0.1:9878", "127.0.0.2:9878"]
    );
    assert_eq!(request.properties().get("key").unwrap().as_str(), "value");
}

#[test]
fn controller_config_update_request_rejects_blank_fields() {
    assert!(ControllerConfigUpdateRequest::try_new(" ", "key", "value").is_err());
    assert!(ControllerConfigUpdateRequest::try_new("127.0.0.1:9878", " ", "value").is_err());
    assert!(ControllerConfigUpdateRequest::try_new("127.0.0.1:9878", "key", " ").is_err());
}

#[test]
fn controller_elect_master_request_trims_fields_and_rejects_negative_broker_id() {
    let request =
        ControllerElectMasterRequest::try_new(" 127.0.0.1:9878 ", " DefaultCluster ", " broker-a ", 1).unwrap();

    assert_eq!(request.controller_addr().as_str(), "127.0.0.1:9878");
    assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
    assert_eq!(request.broker_name().as_str(), "broker-a");
    assert_eq!(request.broker_id(), 1);
    assert_eq!(request.namesrv_addr(), None);

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));

    assert!(ControllerElectMasterRequest::try_new("127.0.0.1:9878", "DefaultCluster", "broker-a", -1).is_err());
}

#[test]
fn controller_metadata_clean_request_trims_ids_and_allows_living_clean_without_cluster() {
    let request =
        ControllerMetadataCleanRequest::try_new(" 127.0.0.1:9878 ", " broker-a ", Some(" 1 ; ; 2 ".into()), None, true)
            .unwrap();

    assert_eq!(request.controller_addr().as_str(), "127.0.0.1:9878");
    assert_eq!(request.broker_name().as_str(), "broker-a");
    assert_eq!(request.cluster_name(), None);
    assert_eq!(
        request.broker_controller_ids_to_clean().map(|ids| ids.as_str()),
        Some("1;2")
    );
    assert!(request.clean_living_broker());
}

#[test]
fn controller_metadata_clean_request_requires_cluster_when_not_cleaning_living_brokers() {
    assert!(ControllerMetadataCleanRequest::try_new("127.0.0.1:9878", "broker-a", None, None, false).is_err());

    let request =
        ControllerMetadataCleanRequest::try_new("127.0.0.1:9878", "broker-a", None, Some(" cluster-a ".into()), false)
            .unwrap();
    assert_eq!(
        request.cluster_name().map(|cluster| cluster.as_str()),
        Some("cluster-a")
    );
}

#[test]
fn controller_metadata_clean_request_rejects_invalid_ids() {
    assert!(ControllerMetadataCleanRequest::try_new(
        "127.0.0.1:9878",
        "broker-a",
        Some("1;bad".into()),
        Some("cluster-a".into()),
        false,
    )
    .is_err());
}
